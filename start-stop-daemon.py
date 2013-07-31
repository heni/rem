#!/usr/bin/env python
from __future__ import with_statement
import os
import logging
import sys
import optparse
import signal
import time


INFINITY = float('inf')


def cwd_to_path():
    dir, name = os.path.split(sys.argv[0])
    os.chdir(dir)


def parse_args():
    parser = optparse.OptionParser()
    modes = ("check-start", "restart", "start", "status", "stop")
    parser.set_defaults()
    parser.add_option("--restart", dest="mode", action="store_const", const="restart", help="restart service")
    parser.add_option("--stop", dest="mode", action="store_const", const="stop", help="stop service")
    parser.add_option("--start", dest="mode", action="store_const", const="start", help="start service")
    parser.add_option("--check-start", dest="mode", action="store_const", const="check-start",
                      help="check service and start if needed (default)")
    parser.add_option("--status", dest="mode", action="store_const", const="status", help="print service work status")
    opt, args = parser.parse_args()
    if opt.mode is None:
        if len(args) >= 1 and args[0] in modes:
            opt.mode = args[0]
            args = args[1:]
        else:
            opt.mode = "check-start"
    return opt, args


def waitpid_ex(pid, opts):
    class WaitPidResult(object):
        def __init__(self):
            self.pid = self.exitCode = self.error = None

    result = WaitPidResult()
    try:
        wpid, status = os.waitpid(pid, opts)
        result.pid = wpid
        result.exitCode = os.WTERMSIG(status) if os.WIFSIGNALED(status) else \
            (os.WEXITSTATUS(status) if os.WIFEXITED(status) else None)
    except OSError, e:
        result.error = e
    return result


class LogPrinter(object):
    def __init__(self, print_timeout=1.0):
        self.timeout = print_timeout
        self.last = time.time()

    def start(self, message):
        sys.stdout.write(message)
        sys.stdout.flush()
        self.last = time.time()

    def process(self):
        if time.time() - self.last > self.timeout:
            sys.stdout.write(" .")
            sys.stdout.flush()
            self.last = time.time()

    @classmethod
    def ok(cls):
        sys.stdout.write("\033[80D\033[76C\033[32m[OK]\033[0m\n")

    @classmethod
    def error(cls):
        sys.stdout.write("\033[80D\033[75C\033[31m[ERR]\033[0m\n")

    @classmethod
    def interrupt(cls):
        sys.stdout.write("\033[80D\033[67C\033[31m[Interrupted]\033[0m\n")


class Service(object):
    def __init__(self, runargs, pidfile, logfile, name=None, checkname=None):
        self.runArgs = runargs
        self.pidFile = pidfile
        self.logFile = logfile
        self.name = name or os.path.split(self.runArgs[0])[-1]
        self.checkExecName = checkname or self.runArgs[0]

    def CheckProcess(self):
        """Checks if daemon is started
                args - process start arguments
                pidfile - file with stored process pid
           
           If daemon is running returns process ID
           If daemon is not started returns False
           If pidFile is not exist returns None"""
        try:
            with open(self.pidFile) as pidReader:
                pid = int(pidReader.readline())
        except (ValueError, IOError), e:
            return None
        try:
            cmdfile = "/proc/%s/cmdline" % pid
            if not os.path.isfile(cmdfile):
                return False
            with open(cmdfile) as psReader:
                psLine = psReader.read()
            if psLine.startswith(self.checkExecName):
                return pid
            return False
        except:
            logging.exception("error while reading process status")

    def CheckService(self, timeout=0.0):
        time.sleep(timeout)
        return True

    def StartDaemon(self, timeout=10.0):
        stTime = time.time()
        logger = LogPrinter(print_timeout=1.0)
        logger.start("starting daemon %s" % self.name)
        if os.path.isfile(self.pidFile):
            os.unlink(self.pidFile)
        pid = os.fork()
        if 0 == pid:
            os.setsid()
            executable = self.runArgs[0]
            pid = os.fork()
            if 0 == pid:
                logwrite = open(self.logFile, "a")
                os.dup2(logwrite.fileno(), 1)
                os.dup2(logwrite.fileno(), 2)
                os.setpgid(0, 0)
                os.execvp(executable, self.runArgs)
            else:
                with open(self.pidFile, "w") as writer:
                    print >> writer, pid
                while time.time() - stTime <= timeout:
                    wres = waitpid_ex(pid, os.WNOHANG)
                    if wres.pid == pid:
                        if wres.error:
                            import traceback

                            logwrite = open(self.logFile, "a")
                            traceback.print_exc(file=logwrite)
                            wres.exitCode = 1
                        sys.exit(wres.exitCode)
                    if self.CheckService(timeout=1.0):
                        sys.exit(0)
                sys.exit(1)
        try:
            while time.time() - stTime <= timeout:
                wres = waitpid_ex(pid, os.WNOHANG)
                if wres.pid == pid:
                    status = wres.error or wres.exitCode
                    break
                logger.process()
                time.sleep(0.05)
            result = (status == 0 and self.CheckProcess() and self.CheckService())
            if not result:
                logger.error()
                sys.exit(1)
            logger.ok()
            sys.exit(0)
        except KeyboardInterrupt:
            logger.interrupt()

    def Stop(self, signum=signal.SIGTERM, timeout=10.0):
        stTime = time.time()
        logger = LogPrinter(print_timeout=1.0)
        logger.start("stopping daemon %s" % self.name)
        pid = self.CheckProcess()
        if pid > 0:
            try:
                pgid = os.getpgid(pid)
                os.killpg(pgid, signum)
                time.sleep(0.1)
                while os.path.isdir("/proc/%s" % pid) and time.time() - stTime < timeout:
                    time.sleep(0.01)
                    logger.process()
                if os.path.isdir("/proc/%s" % pid):
                    os.killpg(pgid, signal.SIGKILL)
                result = not os.path.isdir("/proc/%s" % pid)
                if result:
                    logger.ok()
                else:
                    logger.error()
                return result
            except KeyboardInterrupt:
                logger.interrupt()
            except:
                logger.error()
                logging.exception("Service.Stop error")
        else:
            raise RuntimeError("can't determine daemon pid")


class REMService(Service):
    def __init__(self):
        self.Configure()
        runArgs = ["python", "rem-server.py", "start"]
        if self.setupScript: runArgs = ["/bin/sh", "-c", " ".join([self.setupScript, "&&", "exec"] + runArgs)]
        super(REMService, self).__init__(runargs=runArgs, pidfile="var/rem.pid", logfile="var/rem.errlog", name="remd",
                                         checkname="python")

    def Configure(self):
        import ConfigParser

        configFile = "rem.cfg"
        configParser = ConfigParser.ConfigParser()
        if configFile not in configParser.read(configFile):
            raise EnvironmentError("some errors in configuration file \"%s\"" % configFile)
        self.serverURL = "http://localhost:%d/" % configParser.getint("server", "port")
        self.setupScript = ". %s" % configParser.get("run", "setup_script") if configParser.has_option("run",
                                                                                                       "setup_script") else ""

    def CheckService(self, timeout=0.0):
        endTime = time.time() + timeout
        try:
            import xmlrpclib

            proxy = xmlrpclib.ServerProxy(self.serverURL, allow_none=True)
            proxy.check_tag("start_tag")
            return True
        except:
            time.sleep(max(endTime - time.time(), 0))
            with open(self.logFile, "a") as log_printer:
                eTp, eVal, tb = sys.exc_info()
                co = tb.tb_frame.f_code
                print >> log_printer, "service checking error (file=\"%s:%d\", method=\"%s\"): %s" \
                                      % (co.co_filename, tb.tb_lineno, co.co_name, eVal)
            return False


def dispatch_work(service, opt, args):
    if opt.mode in ("status", ):
        pid = service.CheckProcess()
        if pid:
            print "%s is running as pid %s." % (service.name, pid)
            sys.stdout.write("Functionality check result:")
            sys.stdout.flush()
            if service.CheckService():
                LogPrinter.ok()
            else:
                LogPrinter.error()
            sys.exit(0)
        print "can't find runned %s" % service.name
        sys.exit(1)
    if opt.mode in ("stop", "restart"):
        pid = service.CheckProcess()
        if pid:
            service.Stop(timeout=INFINITY)
    if opt.mode in ("start", "restart", "check-start") and not service.CheckProcess():
        service.StartDaemon(timeout=INFINITY)


if __name__ == "__main__":
    try:
        cwd_to_path()
        service = REMService()
        opt, args = parse_args()
        dispatch_work(service, opt, args)
    except EnvironmentError, e:
        LogPrinter().error()

