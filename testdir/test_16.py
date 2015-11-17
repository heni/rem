import unittest
import logging
import remclient
from testdir import *
import six
from six.moves import xmlrpc_client


class T16(unittest.TestCase):
    """Test for queue/level lifetimes functionality"""

    def setUp(self):
        self.connector = Config.Get().server1.connector
        self.connector2 = Config.Get().server2.connector

    def testCustomQueueSuccessLifetime(self):
        timestamp = time.time()
        pck = self.connector.Packet('test_lifetime-%d' % int(timestamp))
        pck.AddJob('true')
        self.connector.Queue('test_lifetime').AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        self.connector.Queue('test_lifetime').SetSuccessLifeTime(1)
        WaitForExecution(pckInfo, "SUCCESSFULL")
        RestartService(Config.Get().server1.projectDir)
        self.assertRaises(xmlrpc_client.Fault, pckInfo.update)
