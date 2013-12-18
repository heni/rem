import unittest
import logging
import time
import tempfile

import remclient
from testdir import *


class T03(unittest.TestCase):
    """Checking for environment intaractions"""

    def setUp(self):
        self.connector = Config.Get().server1.connector

    def testEnv(self):
        pckname = "envtest-%.0f" % time.time()
        tFile = tempfile.NamedTemporaryFile(suffix=".txt")
        pck = self.connector.Packet(pckname, time.time())
        j1 = pck.AddJob("echo ${TEST_SHELL_VAR} > %s" % tFile.name)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        logging.info("packet %s(%s) added to queue %s, waiting until it be done", pckname, pck.id, "test")
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        testShellVar = open(tFile.name, "r").read().strip()
        print "[TEST_SHELL_VAR = \"%s\"]" % testShellVar
        pckInfo.Delete()

