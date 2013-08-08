import unittest
import logging
import time
import xmlrpclib
from client import remclient

from testdir import *
import functools


class T14(unittest.TestCase):
    """Testing name collisions policy"""

    def setUp(self):
        self.connector_default = remclient.Connector(Config.Get().server1.url)
        self.ignore_duplicate_connector = Config.Get().server1.connector
        self.timestamp = int(time.time())

    def _create_packet(self, connector, pckname):
        pck = connector.Packet(pckname, self.timestamp)
        pck.AddJob("sleep 1")
        self.connector_default.Queue(TestingQueue.Get()).AddPacket(pck)
        return pck

    def testNameCollisionPolicy(self):
        """Testing name collisions policy"""
        pckname = "duplicate-packet-name-%s" % self.timestamp
        self._create_packet(self.connector_default, pckname)
        add_duplicate = functools.partial(self._create_packet, self.connector_default, pckname)
        self.assertRaises(RuntimeError, add_duplicate)
        self.assertTrue(self._create_packet(self.ignore_duplicate_connector, pckname))
