from __future__ import with_statement
import itertools
import time
import logging

from common import emptyset, TimedSet, PackSet, PickableRLock, Unpickable
from callbacks import CallbackHolder, ICallbackAcceptor
from packet import JobPacket, PacketCustomLogic, PacketState


class Queue(Unpickable(pending=PackSet.create,
                       worked=TimedSet.create,
                       errored=TimedSet.create,
                       suspended=set,
                       waited=set,
                       working=emptyset,
                       isSuspended=bool,
                       noninitialized=set,
                       lock=PickableRLock,
                       errorForgetTm=int,
                       successForgetTm=int,
                       workingLimit=(int, 1),
                       success_lifetime=(int, 0),
                       errored_lifetime=(int, 0)),
            CallbackHolder,
            ICallbackAcceptor):
    VIEW_BY_ORDER = "pending", "waited", "errored", "suspended", "worked", "noninitialized"
    VIEW_BY_STATE = {PacketState.SUSPENDED: "suspended", PacketState.WORKABLE: "suspended",
                     PacketState.PENDING: "pending", PacketState.ERROR: "errored", PacketState.SUCCESSFULL: "worked",
                     PacketState.WAITING: "waited", PacketState.NONINITIALIZED: "noninitialized"}

    def __init__(self, name):
        super(Queue, self).__init__()
        self.name = name

    def __getstate__(self):
        sdict = getattr(super(Queue, self), "__getstate__", lambda: self.__dict__)()
        with self.lock:
            for q in self.VIEW_BY_ORDER:
                sdict[q] = sdict[q].copy()
        return sdict

    def SetSuccessLifeTime(self, lifetime):
        self.success_lifetime = lifetime

    def SetErroredLifeTime(self, lifetime):
        self.errored_lifetime = lifetime

    def OnJobGet(self, ref):
        #lock has been already gotten in Queue.Get
        self.working.add(ref)

    def OnJobDone(self, ref):
        with self.lock:
            self.working.discard(ref)
        if self.HasStartableJobs():
            self.FireEvent("task_pending")

    def OnChange(self, ref):
        if isinstance(ref, JobPacket):
            self.relocatePacket(ref)
            if ref.state == PacketState.WAITING:
                self.FireEvent("waiting_start", ref)

    def UpdateContext(self, context):
        self.successForgetTm = context.success_lifetime
        self.errorForgetTm = context.error_lifetime

    def forgetOldItems(self):
        self.forgetQueueOldItems(self.worked, self.success_lifetime or self.successForgetTm)
        self.forgetQueueOldItems(self.errored, self.errored_lifetime or self.errorForgetTm)

    def forgetQueueOldItems(self, queue, expectedLifetime):
        barrierTm = time.time() - expectedLifetime
        while len(queue) > 0:
            job, tm = queue.peak()
            if tm < barrierTm:
                job.changeState(PacketState.HISTORIED)
            else:
                break

    def relocatePacket(self, pck):
        dest_queue_name = self.VIEW_BY_STATE.get(pck.state, None)
        dest_queue = getattr(self, dest_queue_name) if dest_queue_name else None
        if not (dest_queue and pck in dest_queue):
            logging.debug("queue %s\tmoving packet %s typed as %s", self.name, pck.name, pck.state)
            self.movePacket(pck, dest_queue)

    def movePacket(self, pck, dest_queue):
        src_queue = None
        for qname in self.VIEW_BY_ORDER:
            queue = getattr(self, qname, {})
            if pck in queue:
                if src_queue is not None:
                    logging.warning("packet %r is in several queues", pck)
                src_queue = queue
        if src_queue != dest_queue:
            with self.lock:
                if src_queue is not None:
                    src_queue.remove(pck)
                if dest_queue is not None:
                    dest_queue.add(pck)
            if pck.state == PacketState.PENDING:
                self.FireEvent("task_pending")

    def OnPacketReinitRequest(self, pck):
        self.FireEvent('packet_reinit_request', pck)

    def OnPendingPacket(self, ref):
        self.FireEvent("task_pending")

    def IsAlive(self):
        return not self.isSuspended

    def Add(self, pck):
        if pck.state not in (PacketState.CREATED, PacketState.SUSPENDED, PacketState.ERROR):
            raise RuntimeError("can't add \"live\" packet into queue")
        with self.lock:
            pck.AddCallbackListener(self)
            self.working.update(pck.GetWorkingJobs())
        self.relocatePacket(pck)
        pck.Resume()

    def Remove(self, pck):
        if pck.state not in (PacketState.CREATED, PacketState.SUSPENDED, PacketState.ERROR):
            raise RuntimeError("can't remove \"live\" packet from queue")
        if pck not in self.ListAllPackets():
            logging.info("%s", list(self.ListAllPackets()))
            raise RuntimeError("packet %s is not in queue %s" % (pck.id, self.name))
        with self.lock:
            pck.DropCallbackListener(self)
            self.working.difference_update(pck.GetWorkingJobs())
        self.movePacket(pck, None)


    def _CheckStartableJobs(self):
        return self.pending and len(self.working) < self.workingLimit and self.IsAlive()

    def HasStartableJobs(self, block=True):
        if block:
            with self.lock:
                return self._CheckStartableJobs()
        return self._CheckStartableJobs()

    def Get(self, context):
        pckIncorrect = None
        while True:
            with self.lock:
                if not self.HasStartableJobs(False):
                    return None
                pck, prior = self.pending.peak()
                pendingJob = pck.Get()
                if pendingJob == JobPacket.INCORRECT:
                    #possibly incorrect situation -> will fix if it would be repeated after small delay
                    if pck == pckIncorrect:
                        PacketCustomLogic(pck).DoEmergencyAction()
                        self.pending.pop()
                        return None
                    else:
                        pckIncorrect = pck
                else:
                    return pendingJob

    def ListAllPackets(self):
        return itertools.chain(*(getattr(self, q) for q in self.VIEW_BY_ORDER))

    def GetWorkingPackets(self):
        return set(job.packetRef for job in self.working)

    def FilterPackets(self, filter=None):
        filter = filter or "all"
        pf, parg = {"errored": (list, self.errored), "suspended": (list, self.suspended),
                    "pending": (list, self.pending), "worked": (list, self.worked),
                    "working": (Queue.GetWorkingPackets, self),
                    "waiting": (list, self.waited), "all": (Queue.ListAllPackets, self)}[filter]
        for pck in pf(parg):
            yield pck

    def ListPackets(self, filter=None, name_regex=None, prefix=None, last_modified=None):
        packets = []
        for pck in self.FilterPackets(filter):
            if name_regex and not name_regex.match(pck.name):
                continue
            if prefix and not pck.name.startswith(prefix):
                continue
            if last_modified and (not pck.History() or pck.History()[-1][1] < last_modified):
                continue
            packets.append(pck)
        return packets

    def Resume(self, resumeWorkable=False):
        self.isSuspended = False
        for pck in list(self.suspended):
            try:
                pck.Resume(resumeWorkable)
            except:
                logging.error("can't resume packet %s", pck.id)
                try:
                    pck.changeState(PacketState.ERROR)
                except:
                    logging.error("can't mark packet %s as errored")
        self.FireEvent("task_pending")

    def Suspend(self):
        self.isSuspended = True

    def Status(self):
        return {"alive": self.IsAlive(), "pending": len(self.pending), "suspended": len(self.suspended),
                "errored": len(self.errored), "worked": len(self.worked),
                "waiting": len(self.waited), "working": len(self.working), "working-limit": self.workingLimit, 
                "success-lifetime": self.success_lifetime if self.success_lifetime > 0 else self.successForgetTm,
                "error-lifetime": self.errored_lifetime if self.errored_lifetime > 0 else self.errorForgetTm}

    def ChangeWorkingLimit(self, lmtValue):
        self.workingLimit = int(lmtValue)
        if self._CheckStartableJobs:
            self.FireEvent('task_pending')

    def Empty(self):
        return not any(getattr(self, subq_name, None) for subq_name in self.VIEW_BY_ORDER)
