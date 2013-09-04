import unittest
import logging
import time
import xmlrpclib

from testdir import *


class T11(unittest.TestCase):
    """Testing readonly server interface"""

    def setUp(self):
        self.connector = Config.Get().server1.connector
        self.readonly_connector = Config.Get().server1.readonly_connector
        self.timestamp = int(time.time())

    def _create_packet(self, pckname):
        pck = self.connector.Packet(pckname, self.timestamp)
        pck.AddJob("sleep 1")
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)

    def testReadonlyOperations(self):
        """Test readonly operations in readonly interface"""
        pckname = "readonlytest-read-%d" % self.timestamp
        self._create_packet(pckname)
        tagname = "tag-readonlytest-write-%d" % self.timestamp
        self.connector.Tag(tagname).Set()

        tags = self.readonly_connector.ListObjects('tags', prefix=tagname)
        logging.info('tags: %s' % tags)
        queues = self.readonly_connector.ListObjects('queues')
        logging.info('queues: %s' % queues)
        queue = self.readonly_connector.Queue(TestingQueue.Get())
        queue_status = queue.Status()
        packet = queue.ListPackets("all", prefix=pckname)[0]
        pckstate = packet.state
        logging.info('packet.state: %s' % pckstate)

    def testChangingOperations(self):
        """Test that changing operations throw in readonly interface"""
        pckname = "readonlytest-write-%d" % self.timestamp
        self._create_packet(pckname)

        tagname = "tag-readonlytest-write-%d" % self.timestamp
        self.assertRaises(xmlrpclib.Fault, lambda: self.readonly_connector.Tag(tagname).Set())
        queue = self.readonly_connector.Queue(TestingQueue.Get())
        self.assertRaises(xmlrpclib.Fault, lambda: queue.Suspend())
        self.assertRaises(xmlrpclib.Fault, lambda: queue.Resume())
        packet = queue.ListPackets("all", prefix=pckname)[0]
        self.assertRaises(xmlrpclib.Fault, lambda: packet.Suspend())
        self.assertRaises(xmlrpclib.Fault, lambda: packet.Resume())
