import unittest
import logging
import time
import xmlrpclib

from testdir import Config, RestartService, TestingQueue, WaitForExecution, WaitForExecutionList


class T06(unittest.TestCase):
    """Checking REM restarts consistency"""

    def RestartService(self):
        RestartService(Config.Get().server1.projectDir)

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

    def testSpecificBackups(self):
        self.RestartService()
        initialChildFlag = self.connector.proxy.get_backupable_state()["child-flag"]
        logging.info("initialChildFlag value: %s", initialChildFlag)
        self.connector.proxy.set_backupable_state(None, False)
        self.connector.proxy.do_backup()
        self.connector.proxy.set_backupable_state(None, True)
        self.connector.proxy.do_backup()
        self.RestartService()
        self.assertEqual(self.connector.proxy.get_backupable_state()["child-flag"], initialChildFlag)

    def testTagsJournal(self):
        tagname = pckname = "lostpacket-%d" % self.timestamp

        self.connector.proxy.set_backupable_state(False)
        self.assertFalse(self.connector.Tag(tagname).Check())
        pck = self.connector.Packet(pckname, self.timestamp, set_tag=tagname)
        pck.AddJob("true")
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        WaitForExecution(self.connector.PacketInfo(pck.id))
        self.assertEqual("SUCCESSFULL", self.connector.PacketInfo(pck.id).state)
        self.assertTrue(self.connector.Tag(tagname).Check())

        self.RestartService()
        self.assertTrue(self.connector.Tag(tagname).Check())
        self.assertRaises(xmlrpclib.Fault, lambda: self.connector.PacketInfo(pck.id).state)

