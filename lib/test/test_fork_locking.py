import logging
import os
import time
import threading
import unittest

import _fork_locking
#import _dummy_fork_locking as _fork_locking


class ForkLockingHelpers(object):
    LockActionsCounter = 0
    ForkActionsCounter = 0
    LockActionsGuard = threading.Lock()

    @classmethod
    def LockActionsThreadProc(cls):
        lock = threading.Lock()
        for _ in range(1000):
            _fork_locking.acquire_lock()
            try:
                with lock:
                    time.sleep(0.001)
                    logging.debug("lock action done")
                with cls.LockActionsGuard:
                    cls.LockActionsCounter += 1
            finally:
                _fork_locking.release_lock()
            time.sleep(0.001)

    @classmethod
    def ForkActionsThreadProc(cls):
        for _ in range(1000):
            _fork_locking.acquire_fork()
            try:
                pid = os.fork()
                if pid == 0:
                    with cls.LockActionsGuard:
                        logging.debug("LockActionsCounter value is %d", cls.LockActionsCounter)
                    os._exit(0)
                assert pid > 0
                os.waitpid(pid, 0)
                cls.ForkActionsCounter += 1
            finally:
                _fork_locking.release_fork()
            time.sleep(0.001)


class TestForkLocking(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.INFO)

    def testForkLocking(self):
        allThreads = [threading.Thread(target=ForkLockingHelpers.LockActionsThreadProc) for _ in range(10)]
        allThreads += [threading.Thread(target=ForkLockingHelpers.ForkActionsThreadProc)]
        stTime = time.time()
        for th in allThreads:
            th.start()
        for th in allThreads:
            th.join()
        eTime = time.time()
        logging.info("testForkLocking time %.4f", eTime - stTime)
        self.assertEqual(ForkLockingHelpers.LockActionsCounter, 10000)
        self.assertEqual(ForkLockingHelpers.ForkActionsCounter, 1000)


if __name__ == "__main__":
    unittest.main()

