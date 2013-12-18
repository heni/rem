import unittest
import logging
import remclient
from testdir import *


class T01(unittest.TestCase):
    """Statistics functionality"""

    def setUp(self):
        self.connector = Config.Get().server1.connector

    def testFlags(self):
        self.connector.Tag("_simple_flag").Set()
        assert self.connector.Tag("_simple_flag").Check()
        self.connector.Tag("_simple_flag").Unset()
        assert not self.connector.Tag("_simple_flag").Check()

    def testQueueStatus(self):
        queue = self.connector.Queue(TestingQueue.Get())
        queue.ChangeWorkingLimit(25)
        logging.info("TestingQueue %s status: %s", TestingQueue.Get(), queue.Status())
        queue = self.connector.Queue(LmtTestQueue.Get())
        queue.ChangeWorkingLimit(1)
        logging.info("LmtTestQueue %s status: %s", LmtTestQueue.Get(), queue.Status())

    def testPrefixListings(self):
        __tags = self.connector.ListObjects("tags", "_")
        u_tags = self.connector.ListObjects("tags", "u")
        user_qs = self.connector.ListObjects("queues", "user")
        logging.info("__tags count: %d, u_tags count: %d, user_qs count: %d", len(__tags), len(u_tags), len(user_qs))


