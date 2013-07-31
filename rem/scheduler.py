from __future__ import with_statement
import copy
import time
import logging
import os
import re
from collections import deque
from cPickle import Pickler, Unpickler
import gc

from queue import *
from connmanager import *
from rem.storages import *
from rem.storages import PacketNamesStorage


class SchedWatcher(Unpickable(tasks=TimedSet.create,
                              lock=PickableLock.create,
                              workingQueue=list
                            ),
                   ICallbackAcceptor):
    def OnTick(self, ref):
        tm = time.time()
        while len(self.tasks) > 0:
            runner, runtm = self.tasks.peak()
            if runtm > tm: break
            with self.lock:
                if self.tasks.peak()[1] > tm: break
                runner, runtm = self.tasks.pop()
                self.workingQueue.append(runner)

    def AddTask(self, runtm, fn, *args, **kws):
        if "skip_logging" in kws:
            skipLoggingFlag = bool(kws.pop("skip_logging"))
        else:
            skipLoggingFlag = False
        if not skipLoggingFlag:
            logging.debug("new task %r scheduled on %s", fn, time.ctime(time.time() + runtm))
        with self.lock:
            self.tasks.add(FuncRunner(fn, args, kws), runtm + time.time())

    def GetTask(self):
        if self.workingQueue:
            with self.lock:
                if self.workingQueue:
                    return self.workingQueue.pop(0)

    def ListTasks(self):
        task_lst = [(str(o), tm) for o, tm in self.tasks.items()]
        task_lst += [(str(o), None) for o, tm in self.workingQueue]
        return task_lst

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["tasks"] = sdict["tasks"].copy()
        return getattr(super(SchedWatcher, self), "__getstate__", lambda: sdict)()


class Scheduler(Unpickable(lock=PickableLock.create,
                           qList=deque, #list of all known queue
                           qRef=dict, #inversed qList for searching queues by name
                           tagRef=TagStorage, #inversed taglist for searhing tags by name
                           alive=(bool, False),
                           binStorage=BinaryStorage.create,
                           #storage with knowledge about saved binary objects (files for packets)
                           packStorage=GlobalPacketStorage, #storage of all known packets
                           tempStorage=ShortStorage,
                           #storage with knowledge about nonassigned packets (packets that was created but not yet assigned to appropriate queue)
                           schedWatcher=SchedWatcher, #watcher for time scheduled events
                           connManager=ConnectionManager, #connections to others rems
                        ),
                ICallbackAcceptor):
    BackupFilenameMatchRe = re.compile("sched-\d*.dump$")
    UnsuccessfulBackupFilenameMatchRe = re.compile("sched-\d*.dump.tmp$")

    def __init__(self, context):
        getattr(super(Scheduler, self), "__init__")()
        self.UpdateContext(context)
        self.ObjectRegistratorClass = FakeObjectRegistrator if context.execMode == "start" else ObjectRegistrator
        self.packetNames = PacketNamesStorage()
        if context.useMemProfiler:
            self.initProfiler()

    def __getstate__(self):
        sdict = getattr(super(Scheduler, self), "__getstate__", lambda: self.__dict__)()
        if 'packetNames' in sdict.keys():
            del sdict['packetNames']
        return sdict

    def UpdateContext(self, context=None):
        if context is not None:
            self.context = context
            self.poolSize = context.thread_pool_size
            self.initBackupSystem(context)
            context.registerScheduler(self)
        self.binStorage.UpdateContext(self.context)
        self.tagRef.UpdateContext(self.context)
        PacketCustomLogic.UpdateContext(self.context)
        self.connManager.UpdateContext(self.context)

    def OnWaitingStart(self, ref):
        if isinstance(ref, JobPacket):
            self.ScheduleTask(ref.waitingTime, ref.stopWaiting)

    def initBackupSystem(self, context):
        self.backupPeriod = context.backup_period
        self.backupDirectory = context.backup_directory
        self.backupCount = context.backup_count

    def initProfiler(self):
        import guppy

        self.HpyInstance = guppy.hpy()
        self.LastHeap = None

    def DeleteUnusedQueue(self, qname):
        if qname in self.qRef:
            with self.lock:
                q = self.qRef.get(qname, None)
                if q:
                    if not q.Empty():
                        raise AttributeError("can't delete non-empty queue")
                    self.qRef.pop(qname)
                    return True
        return False

    def Queue(self, qname, create=True):
        if qname in self.qRef:
            return self.qRef[qname]
        if not create:
            raise KeyError("Queue '%s' doesn't exist" % qname)
        with self.lock:
            q = self.qRef[qname] = Queue(qname)
            q.UpdateContext(self.context)
            q.AddCallbackListener(self)
            self.qList.append(q)
            return q

    def Get(self):
        schedRunner = self.schedWatcher.GetTask()
        if schedRunner:
            return FuncJob(schedRunner)
        for _ in xrange(len(self.qList)):
            try:
                q = self.qList[0]
                if q and q.IsAlive():
                    job = q.Get(self.context)
                    if job:
                        return job
            finally:
                self.qList.rotate(-1)

    def CheckBackupFilename(self, filename):
        return bool(self.BackupFilenameMatchRe.match(filename))

    def CheckUnsuccessfulBackupFilename(self, filename):
        return bool(self.UnsuccessfulBackupFilenameMatchRe.match(filename))

    def RollBackup(self):
        try:
            if not os.path.isdir(self.backupDirectory):
                os.makedirs(self.backupDirectory)
            start_time = time.time()
            self.tagRef.tag_logger.Rotate()
            self.SaveData(os.path.join(self.backupDirectory, "sched-%.0f.dump" % time.time()))
            backupFiles = sorted(filter(self.CheckBackupFilename, os.listdir(self.backupDirectory)), reverse=True)
            unsuccessfulBackupFiles = filter(self.CheckUnsuccessfulBackupFilename, os.listdir(self.backupDirectory))
            for filename in backupFiles[self.backupCount:] + unsuccessfulBackupFiles:
                os.unlink(os.path.join(self.backupDirectory, filename))
            self.tagRef.tag_logger.Clear(start_time)
        finally:
            pass

    def SaveData(self, filename):
        gc.collect()
        sdict = {"qList": copy.copy(self.qList),
                 "qRef": copy.copy(self.qRef),
                 "tagRef": self.tagRef,
                 "binStorage": self.binStorage,
                 "tempStorage": self.tempStorage,
                 "schedWatcher": self.schedWatcher,
                 "connManager": self.connManager}
        tmpFilename = filename + ".tmp"
        with open(tmpFilename, "w") as backup_printer:
            pickler = Pickler(backup_printer, 2)
            pickler.dump(sdict)
        os.rename(tmpFilename, filename)
        if self.context.useMemProfiler:
            try:
                last_heap = self.LastHeap
                self.LastHeap = self.HpyInstance.heap()
                heapsDiff = self.LastHeap.diff(last_heap) if last_heap else self.LastHeap
                logging.info("memory changes: %s", heapsDiff)
                logging.debug("GC collecting result %s", gc.collect())
            except Exception, e:
                logging.exception("%s", e)

    def __reduce__(self):
        return nullobject, ()

    def Deserialize(self, stream):
        import common

        with self.lock:
            common.ObjectRegistrator_ = ObjectRegistrator_ = self.ObjectRegistratorClass()
            unpickler = Unpickler(stream)
            sdict = unpickler.load()
            assert isinstance(sdict, dict)
            #update internal structures
            qRef = sdict.pop("qRef")
            self.__setstate__(sdict)
            self.UpdateContext(None)
            self.RegisterQueues(qRef)
            #output objects statistics
            ObjectRegistrator_.LogStats()
            names = [self.packStorage[packet_id].name for packet_id in self.packStorage.keys() if self.packStorage[packet_id].name != PacketState.HISTORIED]
            self.packetNames.Update(names)

    def Restore(self):
        self.tagRef.Restore()

    def RegisterQueues(self, qRef):
        for qname, q in qRef.iteritems():
            q.UpdateContext(self.context)
            q.AddCallbackListener(self)
            for pck in list(q.ListAllPackets()):
                dstStorage = self.packStorage
                pck.UpdateTagDependencies(self.tagRef)
                if pck.directory and os.path.isdir(pck.directory):
                    parentDir, dirname = os.path.split(pck.directory)
                    if parentDir != self.context.packets_directory:
                        dst_loc = os.path.join(self.context.packets_directory, pck.id)
                        try:
                            logging.warning("relocates directory %s to %s", pck.directory, dst_loc)
                            shutil.copytree(pck.directory, dst_loc)
                            pck.directory = dst_loc
                        except Exception, e:
                            logging.exception("relocation FAIL : %s", e)
                            dstStorage = None
                else:
                    if pck.state != PacketState.SUCCESSFULL:
                        dstStorage = None
                if dstStorage is None:
                    #do not print about already errored packets
                    if not pck.CheckFlag(PacketFlag.RCVR_ERROR):
                        logging.warning(
                            "can't restore packet directory: %s for packet %s. Packet marked as error from old state %s",
                            pck.directory, pck.name, pck.state)
                        pck.SetFlag(PacketFlag.RCVR_ERROR)
                        pck.changeState(PacketState.ERROR)
                    dstStorage = self.packStorage
                dstStorage.Add(pck)
                q.relocatePacket(pck)
            if q.IsAlive():
                q.Resume(resumeWorkable=True)
            self.qRef[qname] = q

    def RegisterPacket(self, qname, pck):
        queue = self.Queue(qname)
        self.packStorage.Add(pck)
        queue.Add(pck)

    def GetPacket(self, pck_id):
        return self.packStorage.GetPacket(pck_id)

    def ScheduleTask(self, runtm, fn, *args, **kws):
        self.schedWatcher.AddTask(runtm, fn, *args, **kws)

    def Start(self):
        self.alive = True
        self.connManager.Start()

    def Stop(self):
        self.alive = False
        self.connManager.Stop()

    def GetConnectionManager(self):
        return self.connManager
