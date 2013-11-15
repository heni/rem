#OS specific functions
#FreeBSD implementation
from __future__ import with_statement
import logging
import os
import signal
import stat
import subprocess
import sys
import threading
import time


class Signals(object):
    def __init__(self):
        self.handlers = {signal.SIGINT: []}
        self.lock = threading.Lock()

    def handler(self, signum, frame):
        with self.lock:
            try:
                for fn in reversed(self.handlers.get(signum, [])):
                    if callable(fn):
                        fn(signum, frame)
            except:
                logging.exception("PANIC while signal %s processing", signum)
                sys.exit(1)

    def register(self, signum, handler):
        assert callable(handler)
        with self.lock:
            self.handlers.setdefault(signum, [signal.signal(signum, self.handler)])
            self.handlers[signum].append(handler)

    def release(self, signum, handler):
        with self.lock:
            self.handlers[signum].remove(handler)
            if len(self.handlers) == 1:
                signal.signal(signum, self.handlers.pop(signum)[0])


signals = Signals()


def is_pid_alive(pid):
    return os.path.isdir(os.path.join("/proc", str(pid)))


KILL_TICK = 0.001


def terminate(pid):
    try:
        '''old kill procedure
        os.kill(pid, signal.SIGTERM)
        time.sleep(KILL_TICK)
        if is_pid_alive(pid):
            os.killpg(pid, signal.SIGKILL)
        '''
        os.killpg(pid, signal.SIGKILL)
    except OSError:
        pass


def get_null_input():
    return open("/dev/null", "r")


def get_null_output():
    return open("/dev/null", "w")


def reg_signal_handler(signum, handler):
    signals.register(signum, handler)


def release_signal_handler(signum, handler):
    signals.release(signum, handler)


def create_symlink(src, dst, reallocate=True):
    if reallocate and os.path.islink(dst):
        os.unlink(dst)
    return os.symlink(src, dst)


def set_common_executable(path):
    mode = os.stat(path)[0] | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    os.chmod(path, mode)


def set_common_readable(path):
    mode = os.stat(path)[0] | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
    os.chmod(path, mode)


def get_shell_location(_cache=[]):
    if not _cache:
        _cache += [path for path in ("/bin/bash", "/usr/local/bin/bash", "/bin/sh") if os.access(path, os.X_OK)]
    return _cache[0]


def send_email(emails, subject, message):
    sender = subprocess.Popen(["sendmail"] + map(str, emails), stdin=subprocess.PIPE)
    print >> sender.stdin, \
        """Subject: %(subject)s
To: %(email-list)s

%(message)s
.""" % {"subject": subject, "email-list": ", ".join(emails), "message": message}
    sender.stdin.close()
    sender.communicate()
    return sender.poll()
