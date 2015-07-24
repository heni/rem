import time
import logging
import os.path
import subprocess
import remclient

__all__ = ["PrintPacketResults", "TestingQueue", "LmtTestQueue", "Config",
           "WaitForExecution", "WaitForExecutionList", "PrintCurrentWorkingJobs",
           "ServiceTemporaryShutdown", "RestartService"]


class SharedValue(object):
    def __init__(self, value=None):
        self.value = value

    def Get(self):
        return self.value


TestingQueue = SharedValue()
LmtTestQueue = SharedValue()
Config = SharedValue()


def WaitForExecution(pckInfo, fin_states=("SUCCESSFULL", "ERROR"), timeout=1.0):
    while True:
        pckInfo.update()
        if pckInfo.state in fin_states:
            break
        logging.info("packet state: %s", pckInfo.state)
        time.sleep(timeout)
    return pckInfo.state


def PrintPacketResults(pckInfo):
    for job in pckInfo.jobs:
        print job.shell, "\n".join(r.data for r in job.results)


def WaitForExecutionList(pckList, fin_states=("SUCCESSFULL", "ERROR"), timeout=1.0):
    while True:
        remclient.JobPacketInfo.multiupdate(pckList)
        waitPckCount = sum(1 for pck in pckList if pck.state not in fin_states)
        logging.info("wait for %d packets, current states: %s", waitPckCount, [pck.state for pck in pckList])
        if waitPckCount == 0:
            return
        time.sleep(timeout)


def PrintCurrentWorkingJobs(queue):
    workingPackets = queue.ListPackets("working")
    if not workingPackets:
        logging.info("empty working set")
    for pck in workingPackets:
        for job in pck.jobs:
            if job.state == "working":
                logging.info("working packet %s: \"%s\"", pck.name, job.shell)


class ServiceTemporaryShutdown(object):
    def __init__(self, path_to_daemon='./'):
        self.cmd = os.path.join(path_to_daemon, 'start-stop-daemon.py')

    def __enter__(self):
        try:
            subprocess.check_call([self.cmd, "stop"])
        except OSError:
            logging.exception("can't stop service")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        subprocess.check_call([self.cmd, "start"])


def RestartService(path_to_daemon="./"):
    cmd = os.path.join(path_to_daemon, "start-stop-daemon.py")
    subprocess.check_call([cmd, "restart"])
