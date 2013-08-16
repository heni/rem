import unittest
import logging
import time
import xmlrpclib

from testdir import *


class T13(unittest.TestCase):
    """Test killing job on timeout and killing packet on error feature"""

    def setUp(self):
        self.connector = Config.Get().server1.connector
        self.timestamp = int(time.time())

    def _create_slow_packet(self, pckname):
        pck = self.connector.Packet(pckname, self.timestamp, kill_all_jobs_on_error=True)
        pck.AddJob("sleep 50", max_working_time=10, tries=3)
        pck.AddJob("sleep 50", max_working_time=1000, tries=3)
        pck.AddJob("sleep 50", max_working_time=1000, tries=3)
        pck.AddJob("sleep 50", max_working_time=1000, tries=3)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        self.connector.Queue(TestingQueue.Get()).ChangeWorkingLimit(5)
        return pck

    def testErrorAndKilling(self):
        """Test killing job on timeout"""
        pck_name = 'kill-test-%s' % self.timestamp
        pck = self._create_slow_packet(pckname=pck_name)
        pck_info = self.connector.PacketInfo(pck)
        state = WaitForExecution(pck_info, timeout=1)
        self.assertEqual(state, 'ERROR')



