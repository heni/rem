import threading
__all__ = ["acquire_fork", "acquire_lock", "release_fork", "release_lock"]


class TwoExclusiveResourcesDispatcher(object):

    def __init__(self):
        self.FirstResourceUsage = 0
        self.SecondResourceUsage = 0
        self.lock = threading.Lock()
        self.FirstResourceEvent = threading.Condition(self.lock)
        self.SecondResourceEvent = threading.Condition(self.lock)

    def AcquireFirstResource(self):
        with self.lock:
            while self.SecondResourceUsage > 0:
                self.SecondResourceEvent.wait()
            self.FirstResourceUsage += 1

    def ReleaseFirstResource(self):
        with self.lock:
            if self.FirstResourceUsage <= 0:
                raise RuntimeError("try to release already released object")
            self.FirstResourceUsage -= 1
            if self.FirstResourceUsage == 0:
                self.FirstResourceEvent.notifyAll()

    def AcquireSecondResource(self):
        with self.lock:
            while self.FirstResourceUsage > 0:
                self.FirstResourceEvent.wait()
            self.SecondResourceUsage += 1

    def ReleaseSecondResource(self):
        with self.lock:
            if self.SecondResourceUsage <= 0:
                raise RuntimeError("try to release already released object")
            self.SecondResourceUsage -= 1
            if self.SecondResourceUsage == 0:
                self.SecondResourceEvent.notifyAll()

_ForkLockDispatcher = TwoExclusiveResourcesDispatcher()


def acquire_fork():
    _ForkLockDispatcher.AcquireFirstResource()


def release_fork():
    _ForkLockDispatcher.ReleaseFirstResource()


def acquire_lock():
    _ForkLockDispatcher.AcquireSecondResource()


def release_lock():
    _ForkLockDispatcher.ReleaseSecondResource()
