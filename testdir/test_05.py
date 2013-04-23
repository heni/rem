import unittest
import logging
import remclient 
import time
from testdir import *


class T05(unittest.TestCase):
    """Checking for various reported problems"""
    
    def setUp(self):
        self.connector = Config.Get().server1.connector

    def testInterruptedSuspend(self):
        """problem description: suspended packets should not be runned by tag signals"""
        pckname = "interrupted-suspend-%.0f" % time.time()
        tagname = "interruption-%.0f" % time.time()
        pck = self.connector.Packet(pckname, time.time(), wait_tags=[tagname])
        j0 = pck.AddJob("sleep 10")
        j1 = pck.AddJob("echo done", parents=[j0])
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertEqual(pckInfo.state, "SUSPENDED")
        self.connector.Tag(tagname).Set()
        WaitForExecution(pckInfo, "WORKABLE")
        self.connector.Tag(tagname).Unset()
        pckInfo.Suspend()
        self.connector.Tag(tagname).Set()
        pckInfo.update()
        self.assertEqual(pckInfo.state, "SUSPENDED")
        pckInfo.Delete()

    def testWrongStatus(self):
        """problem description: exceptions.OverflowError during status checking 
        of running packet with not-resolved jobs dependencies"""
        pckname = "dep-chain-%.0f" % time.time()
        pck = self.connector.Packet(pckname, time.time(), wait_tags=[])
        j0 = pck.AddJob("sleep 10")
        j1 = pck.AddJob("echo done", parents=[j0])
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        print [job.__dict__ for job in pckInfo.jobs]
        WaitForExecution(pckInfo, "SUCCESSFULL")
        pckInfo.Delete()
