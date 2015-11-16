from __future__ import with_statement
import shutil
import time
import logging
import os
import re
from collections import deque
import gc
from common import PickableStdQueue, PickableStdPriorityQueue
import common
from Queue import Empty
import cStringIO
import StringIO
import itertools

import fork_locking
from job import FuncJob, FuncRunner
from common import Unpickable, PickableLock, PickableRLock, FakeObjectRegistrator, ObjectRegistrator, nullobject
from rem import PacketCustomLogic
from connmanager import ConnectionManager
from packet import JobPacket, PacketState, PacketFlag
from queue import Queue
from storages import PacketNamesStorage, TagStorage, ShortStorage, BinaryStorage, GlobalPacketStorage
from callbacks import ICallbackAcceptor, CallbackHolder
import osspec

class SchedWatcher(Unpickable(tasks=PickableStdPriorityQueue.create,
                              lock=PickableLock,
                              workingQueue=PickableStdQueue.create
                            ),
                   ICallbackAcceptor,
                   CallbackHolder):
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
                    self.FireEvent("task_pending")

    def AddTaskD(self, deadline, fn, *args, **kws):
        if "skip_logging" in kws:
            skipLoggingFlag = bool(kws.pop("skip_logging"))
        else:
            skipLoggingFlag = False
        if not skipLoggingFlag:
            logging.debug("new task %r scheduled on %s", fn, time.ctime(deadline))
        self.tasks.put((deadline, (FuncRunner(fn, args, kws))))

    def AddTaskT(self, timeout, fn, *args, **kws):
        self.AddTaskD(time.time() + timeout, fn, *args, **kws)

    def GetTask(self):
        if not self.workingQueue.empty():
            try:
                return self.workingQueue.get_nowait()
            except Empty:
                logging.warning("Try to take task from emty SchedWatcher`s queue")
                return None
            except Exception:
                logging.exception("Some exception when SchedWatcher try take task")
                return None

    def HasStartableJobs(self):
        return not self.workingQueue.empty()

    def Empty(self):
        return not self.HasStartableJobs()

    def UpdateContext(self, context):
        self.AddNonpersistentCallbackListener(context.Scheduler)

    def ListTasks(self):
        with self.lock:
            return list(self.tasks.queue) \
                 + [(None, task) for task in list(self.workingQueue.queue)]

    def Clear(self):
        with self.lock:
            self.workingQueue.queue.clear()
            self.tasks.queue[:] = []

    def __len__(self):
        with self.lock:
            return len(self.workingQueue.queue) + len(self.tasks.queue)

    def __nonzero__(self):
        return bool(len(self))

    def __getstate__(self):
        # SchedWatcher can be unpickled for compatibility, but not pickled
        return {}

class QueueList(object):
    __slots__ = ['__list', '__exists']

    def __init__(self):
        self.__list = deque()
        self.__exists = set()

    def push(self, q):
        if q.name in self.__exists:
            raise KeyError("Stack already contains %s queue" % q.name)

        self.__exists.add(q.name)
        self.__list.append(q)

    def pop(self, *args):
        if args and not self.__list:
            return args[0] # default

        q = self.__list.popleft()
        self.__exists.remove(q.name)

        return q

    def __contains__(self, q):
        return q.name in self.__exists

    def __nonzero__(self):
        return bool(len(self.__exists))

    def __len__(self):
        return len(self.__exists)

class Scheduler(Unpickable(lock=PickableRLock,
                           qRef=dict, #queues by name
                           tagRef=TagStorage, #inversed taglist for searhing tags by name
                           alive=(bool, False),
                           backupable=(bool, True),
                           queues_with_jobs=QueueList,
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
    BackupFilenameMatchRe = re.compile("sched-(\d+).dump$")
    UnsuccessfulBackupFilenameMatchRe = re.compile("sched-\d*.dump.tmp$")
    SerializableFields = ["qRef", "tagRef", "binStorage", "tempStorage", "connManager"]

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
        self.HasScheduledTask = fork_locking.Condition(self.lock)
        self.schedWatcher.UpdateContext(self.context)

    def OnWaitingStart(self, ref):
        if isinstance(ref, JobPacket):
            self.ScheduleTaskD(ref.waitingDeadline, ref.stopWaiting)

    def initBackupSystem(self, context):
        self.backupPeriod = context.backup_period
        self.backupDirectory = context.backup_directory
        self.backupCount = context.backup_count
        self.backupInChild = context.backup_in_child

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
            q = Queue(qname)
            self.RegisterQueue(q)
            return q

    def Get(self):
        with self.lock:
            while self.alive and not self.queues_with_jobs and self.schedWatcher.Empty():
                self.HasScheduledTask.wait()

            if self.alive:
                if not self.schedWatcher.Empty():
                    schedRunner = self.schedWatcher.GetTask()
                    if schedRunner:
                        return FuncJob(schedRunner)

                if self.queues_with_jobs:
                    queue = self.queues_with_jobs.pop()
                    job = queue.Get(self.context)
                    if queue.HasStartableJobs():
                        self.queues_with_jobs.push(queue)
                        self.HasScheduledTask.notify()
                    return job # may be None

                logging.warning("No tasks for execution after condition waking up")

    def Notify(self, ref):
        if isinstance(ref, Queue):
            queue = ref
            if not queue.HasStartableJobs():
                return
            with self.lock:
                if queue.HasStartableJobs():
                    if queue not in self.queues_with_jobs:
                        self.queues_with_jobs.push(queue)
                        self.HasScheduledTask.notify()
        elif isinstance(ref, SchedWatcher):
            if ref.Empty():
                return
            with self.lock:
                if not ref.Empty():
                    self.HasScheduledTask.notify()


    def CheckBackupFilename(self, filename):
        return bool(self.BackupFilenameMatchRe.match(filename))

    def ExtractTimestampFromBackupFilename(self, filename):
        name = os.path.basename(filename)
        if self.CheckBackupFilename(name):
            return int(self.BackupFilenameMatchRe.match(name).group(1))
        return None

    def CheckUnsuccessfulBackupFilename(self, filename):
        return bool(self.UnsuccessfulBackupFilenameMatchRe.match(filename))

    @common.logged()
    def forgetOldItems(self):
        for queue_name, queue in self.qRef.copy().iteritems():
            queue.forgetOldItems()

        self.binStorage.forgetOldItems()
        self.tempStorage.forgetOldItems()
        self.tagRef.tofileOldItems()

    @common.logged()
    def RollBackup(self, force=False, child_max_working_time=None):
        child_max_working_time = child_max_working_time \
            or self.context.backup_child_max_working_time

        if not os.path.isdir(self.backupDirectory):
            os.makedirs(self.backupDirectory)

        self.forgetOldItems()
        gc.collect() # for JobPacket -> Job -> JobPacket cyclic references

        start_time = time.time()

        self.tagRef.tag_logger.Rotate(start_time)

        if not self.backupable and not force:
            logging.warning("REM is currently not in backupable state; change it back to backupable as soon as possible")
            return

        def backup(fast_strings):
            self.SaveBackup(
                os.path.join(self.backupDirectory, "sched-%.0f.dump" % start_time),
                cStringIO.StringIO if fast_strings else StringIO.StringIO
            )

        if self.backupInChild:
            child = fork_locking.run_in_child(lambda : backup(True), child_max_working_time)

            logging.debug("backup fork stats: %s", child.timings)

            if child.errors:
                logging.warning("Backup child process stderr: " + child.errors)

            if child.term_status:
                raise RuntimeError("Child process failed to write backup: %s" \
                    % osspec.repr_term_status(child.term_status))
        else:
            backup(False)

        backupFiles = sorted(filter(self.CheckBackupFilename, os.listdir(self.backupDirectory)), reverse=True)
        unsuccessfulBackupFiles = filter(self.CheckUnsuccessfulBackupFilename, os.listdir(self.backupDirectory))
        for filename in backupFiles[self.backupCount:] + unsuccessfulBackupFiles:
            os.unlink(os.path.join(self.backupDirectory, filename))

        self.tagRef.tag_logger.Clear(start_time - self.context.journal_lifetime)

    def SuspendBackups(self):
        self.backupable = False

    def ResumeBackups(self):
        self.backupable = True

    def DisableBackupsInChild(self):
        self.backupInChild = False

    def EnableBackupsInChild(self):
        self.backupInChild = True

    def Serialize(self, out):
        import cPickle as pickle

        sdict = {k: getattr(self, k) for k in self.SerializableFields}
        sdict['qRef'] = sdict['qRef'].copy()

        p = pickle.Pickler(out, 2)
        p.dump(sdict)

    def SaveBackup(self, filename, string_cls=StringIO.StringIO):
        tmpFilename = filename + ".tmp"
        with open(tmpFilename, "w") as out:
            mem_out = string_cls()
            try:
                self.Serialize(mem_out)
                out.write(mem_out.getvalue())
            finally:
                mem_out.close()

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

    @classmethod
    def Deserialize(cls, stream, additional_objects_registrator=FakeObjectRegistrator()):
        import packet
        import cPickle as pickle

        class PacketsRegistrator(object):
            __slots__ = ['packets']

            def __init__(self):
                self.packets = []

            def register(self, obj, state):
                if isinstance(obj, packet.JobPacket):
                    self.packets.append(obj)

            def LogStats(self):
                pass

        packets_registrator = PacketsRegistrator()

        common.ObjectRegistrator_ = objects_registrator \
            = common.ObjectRegistratorsChain([
                packets_registrator,
                additional_objects_registrator
            ])

        unpickler = pickle.Unpickler(stream)

        try:
            sdict = unpickler.load()
            assert isinstance(sdict, dict)
        finally:
            common.ObjectRegistrator_ = FakeObjectRegistrator()

        sdict = {k: sdict[k] for k in cls.SerializableFields + ['schedWatcher'] if k in sdict}

        objects_registrator.LogStats()

        return sdict, packets_registrator.packets

    def LoadBackup(self, filename, restorer=None):
        with self.lock:
            with open(filename, "r") as stream:
                sdict, packets = self.Deserialize(stream, self.ObjectRegistratorClass())

            if restorer:
                restorer(sdict, packets)

            qRef = sdict.pop("qRef")
            prevWatcher = sdict.pop("schedWatcher", None) # from old backups

            self.__setstate__(sdict)

            self.UpdateContext(None)

            tagStorage = self.tagRef
            for pck in packets:
                pck.VivifyDoneTagsIfNeed(tagStorage)

            self.tagRef.Restore(self.ExtractTimestampFromBackupFilename(filename) or 0)

            self.RegisterQueues(qRef)

            self.schedWatcher.Clear() # remove tasks from Queue.relocatePacket
            self.FillSchedWatcher(prevWatcher)

    def FillSchedWatcher(self, prev_watcher=None):
        def list_packets_in_queues(state):
            return [
                pck for q in self.qRef.itervalues()
                    for pck in q.ListAllPackets()
                        if pck.state == state
            ]

        def list_schedwatcher_tasks(obj_type, method_name):
            if not prev_watcher:
                return []

            return (
                (deadline, task)
                    for deadline, task in prev_watcher.ListTasks()
                        if task.object \
                            and isinstance(task.object, obj_type) \
                            and task.methName == method_name
            )

        def produce_packets_to_wait():
            packets = list_packets_in_queues(PacketState.WAITING)

            logging.debug("WAITING packets in Queue's for schedWatcher: %s" % [pck.id for pck in packets])

            now = time.time()

            prev_deadlines = {
                task.object.id: deadline or now
                    for deadline, task in list_schedwatcher_tasks(JobPacket, 'stopWaiting')
            }

            if prev_watcher:
                logging.debug("old backup schedWatcher WAITING packets deadlines: %s" % prev_deadlines)

            for pck in packets:
                pck.waitingDeadline = pck.waitingDeadline \
                    or prev_deadlines.get(pck.id, None) \
                    or time.time() # missed in old backup

            return packets

        def produce_packets_to_reinit():
            packets1 = list_packets_in_queues(PacketState.NONINITIALIZED)

            logging.debug("NONINITIALIZED packets in Queue's for schedWatcher: %s" % [pck.id for pck in packets1])

            packets2 = [
                task.args[0]
                    for _, task in list_schedwatcher_tasks(Queue, 'RestoreNoninitialized')]

            if prev_watcher:
                logging.debug("NONINITIALIZED packets in old schedWatcher: %s" % [pck.id for pck in packets2])

            return packets1 + packets2

        for pck in produce_packets_to_wait():
            self.ScheduleTaskD(pck.waitingDeadline, pck.stopWaiting)

        for pck in produce_packets_to_reinit():
            pck.Reinit(self.context)

    def RegisterQueues(self, qRef):
        for q in qRef.itervalues():
            self.RegisterQueue(q)

    def RegisterQueue(self, q):
        q.UpdateContext(self.context)
        q.AddCallbackListener(self)
        for pck in list(q.ListAllPackets()):
            dstStorage = self.packStorage
            pck.UpdateTagDependencies(self.tagRef)
            if pck.directory:
                if os.path.isdir(pck.directory):
                    parentDir, dirname = os.path.split(pck.directory)
                    if parentDir != self.context.packets_directory:
                        dst_loc = os.path.join(self.context.packets_directory, pck.id)
                        try:
                            logging.warning("relocates directory %s to %s", pck.directory, dst_loc)
                            shutil.copytree(pck.directory, dst_loc)
                            pck.directory = dst_loc
                        except:
                            logging.exception("relocation FAIL")
                            dstStorage = None
                else:
                    if pck.AreLinksAlive(self.context):
                        try:
                            logging.warning("resurrects directory for packet %s", pck.id)
                            pck.directory = None
                            pck.CreatePlace(self.context)
                        except:
                            logging.exception("resurrecton FAIL")
                            dstStorage = None
                    else:
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
        self.qRef[q.name] = q
        if q.HasStartableJobs() and q not in self.queues_with_jobs:
            self.queues_with_jobs.push(q)

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

    def ScheduleTaskD(self, deadline, fn, *args, **kws):
        self.schedWatcher.AddTaskD(deadline, fn, *args, **kws)

    def ScheduleTaskT(self, timeout, fn, *args, **kws):
        self.schedWatcher.AddTaskT(timeout, fn, *args, **kws)

    def Start(self):
        with self.lock:
            self.alive = True
            self.HasScheduledTask.notify_all()
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

    def OnPacketReinitRequest(self, pck):
        pck.Reinit(self.context)
