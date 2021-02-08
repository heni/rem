#OS specific functions
#Linux/FreeBSD implementation
from __future__ import with_statement
import logging
import os
import signal
import shutil
import stat
import subprocess
import sys
import time

import fork_locking

def should_execute_maker(max_tries=20, penalty_factor=5, *exception_list):
    exception_list = exception_list or []

    def should_execute(f):
        tries = max_tries

        def func(*args, **kwargs):
            penalty = 0.01
            _tries = tries
            while _tries:
                try:
                    return f(*args, **kwargs)
                    break
                except tuple(exception_list), e:
                    time.sleep(penalty)
                    penalty = min(penalty * penalty_factor, 5)
                    _tries -= 1
                    logging.error('Exception in %s, exception message: %s, attempts left:  %s', f.func_name, e.message, _tries)

        return func
    return should_execute


class Signals(object):
    def __init__(self):
        self.handlers = {signal.SIGINT: []}
        self.lock = fork_locking.Lock()

    def handler(self, signum, frame):
        self.lock.acquire()
        try:
            for fn in reversed(self.handlers.get(signum, [])):
                if callable(fn):
                    fn(signum, frame)
        except:
            logging.exception("PANIC while signal %s processing", signum)
            sys.exit(1)
        finally:
            self.lock.release()

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
        os.kill(pid, signal.SIGTERM)
        time.sleep(KILL_TICK)
        if is_pid_alive(pid):
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


def set_common_writable(path):
    mode = os.stat(path)[0] | stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
    os.chmod(path, mode)


def get_shell_location(_cache=[]):
    if not _cache:
        _cache += [path for path in ("/bin/bash", "/usr/local/bin/bash", "/bin/sh") if os.access(path, os.X_OK)]
    return _cache[0]


def get_sudo_location(_cache=[]):
    if not _cache:
        _cache += [path for path in ("/usr/bin/sudo", "/usr/local/bin/sudo", "/sbin/sudo") if os.access(path, os.X_OK)]
    return _cache[0]

def sudo_command(command, user):
    assert isinstance(command, list), "malformatted command: %s" % (command,)
    return [get_sudo_location(), "-u", user] + command


@should_execute_maker(20, 5, Exception)
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


def set_process_title(proc_title):
    """Sets custom title to current process
        Requires installed python-prctl module - http://pythonhosted.org/python-prctl/
    """
    try:
        import prctl
        prctl.set_name(proc_title)
        prctl.set_proctitle(proc_title)
        return True
    except (ImportError, AttributeError):
        return False

def repr_term_status(status):
    return 'exit(%d)' % os.WEXITSTATUS(status) if os.WIFEXITED(status) \
      else 'kill(%d)' % os.WTERMSIG(status)

def add_acl_permission(path, username, permissions):
    try:
        hndl = subprocess.Popen(
            ["setfacl", "-m", "u:%s:%s" % (username, permissions), path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=get_null_input()
        )
        _out, _err = hndl.communicate()
    except Exception, e:
        raise RuntimeError("can't add ACL permissions, reason: %s" % e)
    assert hndl.poll() == 0, "can't set ACL on %s: %s" % (path, (_out, _err))

def rmtree_with_privelleged_user(dirname, user):
    try:
        dir_content = [os.path.join(dirname, f) for f in os.listdir(dirname)]
        content_rm_command = ["rm", "-rf"] + dir_content
        hndl = subprocess.Popen(
            sudo_command(content_rm_command, user),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=get_null_input()
        )
        _out, _err = hndl.communicate()
        if hndl.poll() != 0:
            logging.error("can't remove directory %s content: rm non-zero exitcode: %d\nstdout: %s\nstderr: %s" % (
                dirname, hndl.poll(), _out, _err
            ))
        logging.debug("running as user %s command: %s", user, content_rm_command)
        logging.debug("exit_code: %d", hndl.poll())
    except Exception, e:
        logging.exception("can't remove directory %s content", dirname)
    shutil.rmtree(dirname)
