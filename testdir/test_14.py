import unittest
import logging
import time
import xmlrpclib
import remclient

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
        connector.Queue(TestingQueue.Get()).AddPacket(pck)
        return pck

    def testNameCollisionPolicy(self):
        """Testing name collisions policy"""
        pckname = "duplicate-packet-name-%s" % self.timestamp
        self._create_packet(self.connector_default, pckname)
        add_duplicate = functools.partial(self._create_packet, self.connector_default, pckname)
        self.assertRaises(remclient.DuplicatePackageNameException, add_duplicate)
        self.assertTrue(self._create_packet(self.ignore_duplicate_connector, pckname))

    def testIncompletePacket(self):
        try:
            pckname = "duplicate-packet-name1-%s" % self.timestamp
            pck = self.connector_default.Packet(pckname)
            pck.AddJob('true')
            pck.AddJob('true')
            pck.AddJob('true')
            pck.AddJob('true')
            pck.AddJob(-1)
        finally:
            pckname = "duplicate-packet-name1-%s" % self.timestamp
            pck = self.connector_default.Packet(pckname)
            pck.AddJob('true')
            pck.AddJob('true')
            pck.AddJob('true')
            pck.AddJob('true')
            self.assertIsNone(self.connector_default.Queue(TestingQueue.Get()).AddPacket(pck))

    def testWarning(self):
        warningConnector = remclient.Connector(Config.Get().server1.url, packet_name_policy=remclient.WARN_DUPLICATE_NAMES_POLICY)
        pckname = 'test_duplicate_warning-%s' % self.timestamp
        p1 = self._create_packet(self.connector_default, pckname)
        p2 = warningConnector.Packet(pckname)
        p2.AddJob('true')
        empty_packet = warningConnector.Packet('empty-%s' % self.timestamp)
        warningConnector.Queue('test_warning').AddPacket(empty_packet)
        warningConnector.Queue('test_warning').AddPacket(p2)
        self.assertEqual(1, len(warningConnector.Queue('test_warning').ListPackets('all')))
        for p in warningConnector.Queue('test_warning').ListPackets('all'):
            p.Stop()
            p.Delete()
        warningConnector.Queue('test_warning').Delete()


