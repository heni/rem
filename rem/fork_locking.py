import threading
import os
import time
import sys
from sys import stderr
import signal
from collections import namedtuple

__all__ = ["Lock", "RLock", "Condition", "LockWrapper", "RunningChildInfo", "TerminatedChildInfo", "run_in_child"]

if 'DUMMY_FORK_LOCKING' not in os.environ:
    try:
        import _fork_locking
    except ImportError:
        import _dummy_fork_locking as _fork_locking
else:
    import _dummy_fork_locking as _fork_locking

def acquire_lock(lock, blocking=True):
    if not blocking:
        raise RuntimeError("Non-blocking acquire not implemented")
    _fork_locking.acquire_lock()
    return lock.acquire()

def release_lock(lock):
    lock.release()
    _fork_locking.release_lock()

def acquire_restore_lock(lock, count_owner):
    _fork_locking.acquire_lock()
    return lock._acquire_restore(count_owner)

def release_save_lock(lock):
    ret = lock._release_save()
    _fork_locking.release_lock()
    return ret

def acquire_fork():
    _fork_locking.acquire_fork()

def release_fork():
    _fork_locking.release_fork()

class LockWrapper(object):
    def __init__(self, backend, name=None):
        if hasattr(backend, '_acquire_restore'):
            self._acquire_restore = self.__acquire_restore

        if hasattr(backend, '_release_save'):
            self._release_save = self.__release_save

        if hasattr(backend, '_is_owned'):
            self._is_owned = backend._is_owned

        self.__name = name or '__noname__'
        self.__backend = backend

    def __acquire_restore(self, count_owner):
        acquire_restore_lock(self.__backend, count_owner)

    def __release_save(self):
        return release_save_lock(self.__backend)

    def acquire(self, blocking=True, label=None):
        acquire_lock(self.__backend, blocking)

    def release(self):
        release_lock(self.__backend)

    # Needed because non-blocking acquire is not implemented in LockWrapper
    # copy-pasted from threading.py
    def _is_owned(self):
        if self.__backend.acquire(0):
            self.__backend.release()
            return False
        else:
            return True

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args):
        self.release()

def Lock(name=None):
    return LockWrapper(threading.Lock(), name)

def RLock(name=None):
    return LockWrapper(threading.RLock(), name)

def Condition(lock, verbose=None):
    return threading.Condition(lock, verbose)

def _timed(func): # noexcept
    t0 = time.time()
    ret = func()
    return ret, time.time() - t0

def _dup_dev_null(file, flags):
    file_fd = file.fileno()
    dev_null = os.open('/dev/null', flags)
    os.dup2(dev_null, file_fd)
    os.close(dev_null)

RunningChildInfo = namedtuple('RunningChildInfo', ['pid', 'stderr', 'timings'])

def start_in_child(func, child_max_working_time=None):
    assert(child_max_working_time is None or int(child_max_working_time))

    _, acquire_time = _timed(acquire_fork)

    try:
        child_err_rd, child_err_wr = os.pipe()

        try:
            pid, fork_time = _timed(os.fork)
        except:
            t, v, tb = sys.exc_info()
            try:
                os.close(child_err_rd)
                os.close(child_err_wr)
            except:
                pass
            raise t, v, tb
    finally:
        release_fork()

    if not pid:
        try:
            os.close(child_err_rd)
            _dup_dev_null(sys.stdin, os.O_RDONLY)
            _dup_dev_null(sys.stdout, os.O_WRONLY)
            # stderr is character-buffered, so no buffer cleanup needed
            os.dup2(child_err_wr, sys.stderr.fileno())
            os.close(child_err_wr)

            if child_max_working_time is not None:
                signal.alarm(child_max_working_time)

            func()
        except Exception as e:
            try:
                print >>stderr, "Failed to execute child function: %s" % e
            except:
                pass
            exit_code = 1
        else:
            exit_code = 0
        os._exit(exit_code)

    os.close(child_err_wr)

    return RunningChildInfo(
        pid=pid,
        stderr=os.fdopen(child_err_rd),
        timings={
            'acquire_time': acquire_time,
            'fork_time': fork_time
        }
    )

TerminatedChildInfo = namedtuple('TerminatedChildInfo', ['term_status', 'errors', 'timings'])

def run_in_child(func, child_max_working_time=None):
    child = start_in_child(func, child_max_working_time)

    with child.stderr:
        errors = child.stderr.read()

    _, status = os.waitpid(child.pid, 0)

    return TerminatedChildInfo(status, errors, child.timings)
