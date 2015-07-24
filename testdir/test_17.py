import unittest
import logging
import remclient
import time
from testdir import Config, TestingQueue, WaitForExecution


class T17(unittest.TestCase):
    """Test for check tag storing requirements"""

    def setUp(self):
        self.connector = Config.Get().server1.connector

    def testTagStoreLifetime(self):
        timestamp = int(time.time())
        tagBefore = "test-taglifetime-bfr-%d" % timestamp
        tagAfter = "test-taglifetime-aftr-%d" % timestamp
        pck = self.connector.Packet("test-%d" % timestamp, wait_tags=(tagBefore,), set_tag=tagAfter)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)

        lst = set(tagname for tagname, value in self.connector.ListObjects("tags", prefix="test-taglifetime"))
        self.assertIn(tagBefore, lst)
        self.assertIn(tagAfter, lst)

        self.connector.Tag(tagBefore).Set()
        WaitForExecution(self.connector.PacketInfo(pck))
        self.connector.proxy.do_backup()

        lst = set(tagname for tagname, value in self.connector.ListObjects("tags", prefix="test-taglifetime"))
        self.assertIn(tagBefore, lst)
        self.assertIn(tagAfter, lst)

        self.connector.PacketInfo(pck).Delete()
        self.connector.proxy.do_backup()

        lst = set(tagname for tagname, value in self.connector.ListObjects("tags", prefix="test-taglifetime"))
        self.assertNotIn(tagBefore, lst)
        self.assertNotIn(tagAfter, lst)


