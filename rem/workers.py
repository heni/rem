import copy
import threading
import logging
import time
import Queue as StdQueue

import osspec
from callbacks import CallbackHolder


STACK_SZ = 1 << 18 # 4MB default stack size for threads
threading.stack_size(STACK_SZ)


class KillableWorker(threading.Thread):
    def __init__(self):
        super(KillableWorker, self).__init__()
        self.killed = False
        self.tickTime = self.TICK_PERIOD

    def do(self):
        pass

    def run(self):
        while not self.IsKilled():
            try:
                self.do()
            except Exception, e:
                logging.exception("worker\tjob execution error %s", e)
            time.sleep(self.tickTime)

    def IsKilled(self):
        return self.killed

    def Kill(self):
        self.killed = True


class ThreadJobWorker(KillableWorker):
    TICK_PERIOD = 0.0

    def __init__(self, scheduler):
        super(ThreadJobWorker, self).__init__()
        self.pids = None
        self.scheduler = scheduler
        self.suspended = False

    def do(self):
        if not self.IsSuspended() and self.scheduler.alive:
            try:
                self.pids = set()
                job = self.scheduler.Get()
                if job:
                    job.Run(self.pids)
            finally:
                self.pids = None

    def IsSuspended(self):
        return self.suspended

    def Kill(self):
        super(ThreadJobWorker, self).Kill()
        if self.pids:
            for pid in copy.copy(self.pids):
                logging.debug("worker\ttrying to kill process with pid %s", pid)
                osspec.terminate(pid)

    def Resume(self):
        self.suspended = False

    def Suspend(self):
        self.suspended = True


class XMLRPCWorker(KillableWorker):
    TICK_PERIOD = 0.01

    def __init__(self, requests, func):
        super(XMLRPCWorker, self).__init__()
        self.requests = requests
        self.func = func

    def do(self):
        try:
            obj = self.requests.get(True, self.TICK_PERIOD)
        except StdQueue.Empty:
            return
        else:
            self.func(*obj)


#awful threading.Thread doesn't care about starting other constructors over super object, 
#       therefore threading classes must be last in parents list
class TimeTicker(CallbackHolder, KillableWorker):
    TICK_PERIOD = 1.0

    def __init__(self):
        super(TimeTicker, self).__init__()

    def do(self):
        self.FireEvent("tick")
