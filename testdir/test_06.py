import unittest
import logging
import time
import os
import subprocess

import remclient
from testdir import *


class T06(unittest.TestCase):
    """Checking REM restarts consistency"""

    def RestartService(self):
        projectDir = Config.Get().server1.projectDir
        subprocess.check_call([os.path.join(projectDir, 'start-stop-daemon.py'), "restart"])

    def setUp(self):
        self.connector = Config.Get().server1.connector
        self.timestamp = int(time.time())

    def testPacketConsistency(self):
        pck0name = "worker-%d" % self.timestamp
        pck1name = "worker-next-%d" % self.timestamp
        tag0name = "worker-done0-%d" % self.timestamp
        tag1name = "worker-done1-%s" % self.timestamp

        pck0 = self.connector.Packet(pck0name, self.timestamp, set_tag=tag0name)
        pck0.AddJob("sleep 3", set_tag=tag1name)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck0)
        logging.info("packet %s(%s) added to queue %s", pck0name, pck0.id, TestingQueue.Get())

        self.RestartService()
        logging.info("service restarted")

        pck1 = self.connector.Packet(pck1name, self.timestamp, wait_tags=[tag0name, tag1name])
        pck1.AddJob("true")
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck1)
        logging.info("packet %s(%s) added to queue %s", pck1name, pck1.id, TestingQueue.Get())

        pckList = [self.connector.PacketInfo(pck.id) for pck in [pck0, pck1]]
        WaitForExecutionList(pckList)
        for pck in pckList:
            self.assertEqual(pck.state, "SUCCESSFULL")
            pck.Delete()

        self.assertTrue(self.connector.Tag(tag0name).Check())
        self.assertTrue(self.connector.Tag(tag1name).Check())            
