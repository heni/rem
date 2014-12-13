from __future__ import with_statement
import copy
import shutil
import threading
import time
import logging
import os
import re
from collections import deque
from cPickle import Pickler, Unpickler
import gc
import sys
from threading import Condition
from common import PickableStdQueue
from common import PickableStdPriorityQueue

from job import FuncJob, FuncRunner
from common import Unpickable, TimedSet, PickableLock, FakeObjectRegistrator, ObjectRegistrator, nullobject
from rem import PacketCustomLogic
from connmanager import ConnectionManager
from packet import JobPacket, PacketState, PacketFlag
from queue import Queue
from storages import PacketNamesStorage, TagStorage, ShortStorage, BinaryStorage, GlobalPacketStorage, MessageStorage
from callbacks import ICallbackAcceptor



class SchedWatcher(Unpickable(tasks=PickableStdPriorityQueue.create,
                              lock=PickableLock.create,
                              workingQueue=PickableStdQueue.create
                            ),
                   ICallbackAcceptor):
    def OnTick(self, ref):
        tm = time.time()
        while not self.tasks.empty():
            with self.lock:
                runtm, runner = self.tasks.peak()
                if runtm > tm:
                    break
                else:
                    self.tasks.get()
                    self.workingQueue.put(runner)
                    self.scheduler.Notify(self)

    def AddTask(self, runtm, fn, *args, **kws):
        if "skip_logging" in kws:
            skipLoggingFlag = bool(kws.pop("skip_logging"))
        else:
            skipLoggingFlag = False
        if not skipLoggingFlag:
            logging.debug("new task %r scheduled on %s", fn, time.ctime(time.time() + runtm))
        self.tasks.put((runtm + time.time(), (FuncRunner(fn, args, kws))))

    def GetTask(self):
        if not self.workingQueue.empty():
            try:
                return self.workingQueue.get_nowait()
            except:
                return None

    def HasStartableJobs(self):
        return not self.workingQueue.empty()

    def Empty(self):
        return not self.HasStartableJobs()

    def UpdateContext(self, context):
        self.scheduler = context.Scheduler

    def ListTasks(self):
        task_lst = [(str(o), tm) for o, tm in self.tasks.items()]
        task_lst += [(str(o), None) for o, tm in self.workingQueue.queue]
        return task_lst

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["tasks"] = sdict["tasks"].__getstate__()
        return getattr(super(SchedWatcher, self), "__getstate__", lambda: sdict)()


class Scheduler(Unpickable(lock=PickableLock.create,
                           qList=deque, #list of all known queue
                           qRef=dict, #inversed qList for searching queues by name
                           in_deque=dict,
                           tagRef=TagStorage, #inversed taglist for searhing tags by name
                           alive=(bool, False),
                           backupable=(bool, True),
                           binStorage=BinaryStorage.create,
                           #storage with knowledge about saved binary objects (files for packets)
                           packStorage=GlobalPacketStorage, #storage of all known packets
                           tempStorage=ShortStorage,
                           #storage with knowledge about nonassigned packets (packets that was created but not yet assigned to appropriate queue)
                           schedWatcher=SchedWatcher, #watcher for time scheduled events
                           connManager=ConnectionManager, #connections to others rems
                           packetNamesTracker=PacketNamesStorage
                        ),
                ICallbackAcceptor):
    BackupFilenameMatchRe = re.compile("sched-\d*.dump$")
    UnsuccessfulBackupFilenameMatchRe = re.compile("sched-\d*.dump.tmp$")

    def __init__(self, context):
        getattr(super(Scheduler, self), "__init__")()
        self.UpdateContext(context)
        self.ObjectRegistratorClass = FakeObjectRegistrator if context.execMode == "start" else ObjectRegistrator
        if context.useMemProfiler:
            self.initProfiler()

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
        self.HasScheduledTask = threading.Condition(self.lock)
        self.schedWatcher.UpdateContext(self.context)

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
            return q

    def Get(self):
        with self.lock:
            while self.alive and not self.qList and self.schedWatcher.Empty():
                self.HasScheduledTask.wait()

            if self.alive and not self.schedWatcher.Empty():
                schedRunner = self.schedWatcher.GetTask()
                if schedRunner:
                    return FuncJob(schedRunner)

            if self.alive and self.qList:
                qname = self.qList.popleft()
                if isinstance(qname, Queue):
                    qname = qname.name
                self.in_deque[qname] = False
                job = self.qRef[qname].Get(self.context)
                if self.qRef[qname].HasStartableJobs():
                    self.qList.append(qname)
                    self.in_deque[qname] = True
                    self.HasScheduledTask.notify()
                return job

    def Notify(self, ref):
        if isinstance(ref, Queue):
            queue = ref
            if not queue.HasStartableJobs():
                return
            with self.lock:
                if queue.HasStartableJobs():
                    if not self.in_deque.get(queue.name, False):
                        self.qList.append(queue.name)
                        self.in_deque[queue.name] = True
                        self.HasScheduledTask.notify()
        elif isinstance(ref, SchedWatcher):
            if ref.Empty():
                return
            with self.lock:
                if not ref.Empty():
                    self.HasScheduledTask.notify()


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
            if not self.backupable:
                logging.warning("REM is currently not in backupable state; change it back to backupable as soon as possible")
                return
            self.SaveData(os.path.join(self.backupDirectory, "sched-%.0f.dump" % time.time()))
            backupFiles = sorted(filter(self.CheckBackupFilename, os.listdir(self.backupDirectory)), reverse=True)
            unsuccessfulBackupFiles = filter(self.CheckUnsuccessfulBackupFilename, os.listdir(self.backupDirectory))
            for filename in backupFiles[self.backupCount:] + unsuccessfulBackupFiles:
                os.unlink(os.path.join(self.backupDirectory, filename))
            self.tagRef.tag_logger.Clear(start_time)
        finally:
            pass

    def SuspendBackups(self):
        self.backupable = False

    def ResumeBackups(self):
        self.backupable = True

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
            qList = sdict.pop("qList")
            qList = deque([q for q in qList if q in qRef])
            self.qList = qList
            self.__setstate__(sdict)
            self.UpdateContext(None)
            self.RegisterQueues(qRef)
            #output objects statistics
            ObjectRegistrator_.LogStats()

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
                if pck.state != PacketState.HISTORIED:
                    self.packetNamesTracker.Add(pck.name)
                    pck.AddCallbackListener(self.packetNamesTracker)
                q.relocatePacket(pck)
            if q.IsAlive():
                q.Resume(resumeWorkable=True)
            self.qRef[qname] = q

    def AddPacketToQueue(self, qname, pck):
        queue = self.Queue(qname)
        self.packStorage.Add(pck)
        queue.Add(pck)
        self.packetNamesTracker.Add(pck.name)
        pck.AddCallbackListener(self.packetNamesTracker)

    def RegisterNewPacket(self, pck, wait_tags):
        for tag in wait_tags:
            self.connManager.Subscribe(tag)
        self.tempStorage.StorePacket(pck)

    def GetPacket(self, pck_id):
        return self.packStorage.GetPacket(pck_id)

    def ScheduleTask(self, runtm, fn, *args, **kws):
        self.schedWatcher.AddTask(runtm, fn, *args, **kws)

    def Start(self):
        with self.lock:
            self.alive = True
        self.connManager.Start()

    def Stop(self):
        self.connManager.Stop()
        with self.lock:
            self.alive = False
            self.HasScheduledTask.notify_all()

    def GetConnectionManager(self):
        return self.connManager

    def OnTaskPending(self, ref):
        self.Notify(ref)
    
    def OnPacketNoninitialized(self, ref):
        if ref.noninitialized:
            ref.ScheduleNonitializedRestoring(self.context)


