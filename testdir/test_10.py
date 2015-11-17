import logging
import time
import unittest
import subprocess

import remclient
from testdir import *


class T10(unittest.TestCase):
    """Check remote tags"""

    def setUp(self):
        self.connector1 = Config.Get().server1.connector
        self.connector2 = Config.Get().server2.connector
        self.admin_connector = Config.Get().server1.admin_connector
        self.servername1 = Config.Get().server1.name
        self.servername2 = Config.Get().server2.name
        self.srvdir1 = Config.Get().server1.projectDir
        self.srvdir2 = Config.Get().server2.projectDir

    def testNetworkUnavailableSubscribe(self):
        tag = "nonetwork-tag-%.0f-" % time.time()
        remote_tag1 = "%s:%s1" % (self.servername1, tag)
        tag1 = "%s1" % tag
        pck = self.connector1.Packet("pck-%.0f" % time.time(), set_tag=tag1)
        self.connector1.Queue(TestingQueue.Get()).AddPacket(pck)
        with ServiceTemporaryShutdown(self.srvdir1):
            pck2 = self.connector2.Packet("pck-%.0f" % time.time(), wait_tags=[remote_tag1])
            self.connector2.Queue(TestingQueue.Get()).AddPacket(pck2)
            time.sleep(4)
        pckInfo = self.connector2.PacketInfo(pck2.id)
        self.assertEqual(WaitForExecution(pckInfo, timeout=1.0), "SUCCESSFULL")

    def testNetworkUnavailableSetTag(self):
        tag = "network-tag-%.0f-" % time.time()
        remote_tag1 = "%s:%s1" % (self.servername1, tag)
        tag1 = "%s1" % tag

        pck2 = self.connector2.Packet("pck-%.0f" % time.time(), wait_tags=[remote_tag1])
        self.connector2.Queue(TestingQueue.Get()).AddPacket(pck2)

        time.sleep(2)
        with ServiceTemporaryShutdown(self.srvdir2):
            pck = self.connector1.Packet("pck-%.0f" % time.time(), set_tag=tag1)
            self.connector1.Queue(TestingQueue.Get()).AddPacket(pck)
            time.sleep(2)

        pckInfo = self.connector2.PacketInfo(pck2.id)
        self.assertEqual(WaitForExecution(pckInfo, timeout=1.0), "SUCCESSFULL")

    def testRemoteTag(self):
        tag = "remotetag-%.0f-" % time.time()
        remote_tag1 = "%s:%s1" % (self.servername1, tag)
        remote_tag2 = "%s:%s2" % (self.servername2, tag)
        tag1 = "%s1" % tag
        tag2 = "%s2" % tag
        pck = self.connector1.Packet("pck-%.0f" % time.time(), set_tag=tag1)
        self.connector1.Queue(TestingQueue.Get()).AddPacket(pck)

        pck2 = self.connector2.Packet("pck-%.0f" % time.time(), set_tag=tag2, wait_tags=[remote_tag1])
        self.connector2.Queue(TestingQueue.Get()).AddPacket(pck2)

        pck3 = self.connector1.Packet("pck-%.0f" % time.time(), wait_tags=[remote_tag2])
        self.connector1.Queue(TestingQueue.Get()).AddPacket(pck3)

        pckInfo = self.connector1.PacketInfo(pck3.id)
        self.assertEqual(WaitForExecution(pckInfo, timeout=1.0), "SUCCESSFULL")

    def testManyRemoteTags(self):
        tag = "mass-remote-tag-%.0f" % time.time()

        NUMBER_OF_TAGS = 404
        remote_tags = []
        for i in range(NUMBER_OF_TAGS):
            tag1 = "(1)%s-%d" % (tag, i)
            remote_tag1 = self.servername1 + ":" + tag1

            tag2 = "(2)%s-%d" % (tag, i)
            remote_tag2 = self.servername2 + ":" + tag2
            remote_tags.append(remote_tag2)

            pck1 = self.connector1.Packet("pck1-%d-%.0f" % (i, time.time()), set_tag=tag1)
            self.connector1.Queue(TestingQueue.Get()).AddPacket(pck1)

            pck2 = self.connector2.Packet("pck2-%d-%.0f" % (i, time.time()), set_tag=tag2, wait_tags=[remote_tag1])
            self.connector2.Queue(TestingQueue.Get()).AddPacket(pck2)

        pckMany = self.connector1.Packet("pckMany-%.0f" % time.time(), wait_tags=remote_tags)
        self.connector1.Queue(TestingQueue.Get()).AddPacket(pckMany)

        logging.debug(self.admin_connector.ListDeferedTags(self.servername1))
        logging.debug(self.admin_connector.ListDeferedTags(self.servername2))

        pckInfo = self.connector1.PacketInfo(pckMany.id)
        self.assertEqual(WaitForExecution(pckInfo, timeout=1.0), "SUCCESSFULL")

    def testSubscribing(self):
        tag1 = "subscription-tag-%.0f-1" % time.time()
        tag2 = "subscription-tag-%.0f-2" % time.time()
        remote_tag1 = self.servername1 + ":" + tag1
        remote_tag2 = self.servername1 + ":" + tag2

        # self.admin_connector.RegisterShare(tag1, "servername2")
        # self.assertEqual(self.admin_connector.UnregisterShare(tag1, "servername2"), True)
        # self.assertEqual(self.admin_connector.UnregisterShare(tag1, "servername2"), False)
        # self.admin_connector.RegisterShare(tag1, "servername2")
        RestartService(self.srvdir1)

        pck = self.connector1.Packet("pck-%.0f" % time.time(), set_tag=tag1)
        self.connector1.Queue(TestingQueue.Get()).AddPacket(pck)

        pck = self.connector1.Packet("pck-%.0f" % time.time(), set_tag=tag2)
        self.connector1.Queue(TestingQueue.Get()).AddPacket(pck)

        pck1 = self.connector2.Packet("pck1-%.0f" % time.time(), wait_tags=[remote_tag1])
        self.connector2.Queue(TestingQueue.Get()).AddPacket(pck1)

        pck2 = self.connector2.Packet("pck3-%.0f" % time.time(), wait_tags=[remote_tag2])
        self.connector2.Queue(TestingQueue.Get()).AddPacket(pck2)

        time.sleep(2)
        RestartService(self.srvdir1)
        # self.admin_connector.RegisterShare(tag2, "servername2")
        RestartService(self.srvdir1)

        pckInfo1 = self.connector2.PacketInfo(pck1.id)
        pckInfo2 = self.connector2.PacketInfo(pck2.id)
        WaitForExecutionList([pckInfo1, pckInfo2], timeout=1.0)

    def testRestoringTagFromFile(self):
        tag = "restoring-tag-%.0f-1" % time.time()
        remote_tag = self.servername1 + ":" + tag
        self.connector1.Tag(tag).Set()
        self.connector1.Tag(tag).Unset()
        RestartService(self.srvdir1)
        pck = self.connector2.Packet("restoring-pck-%.0f" % time.time(), wait_tags=[remote_tag])
        self.connector2.Queue(TestingQueue.Get()).AddPacket(pck)
        time.sleep(3)
        self.connector1.Tag(tag).Set()
        pckInfo = self.connector2.PacketInfo(pck.id)
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")

        # def testWrongServername(self):
        # tag = "wrongserver:tag-wrong-servername-%.f" % time.time()
        # pck = "pck-wrong-servername-%.f" % time.time()
        # self.connector1.Packet(pck, time.time(), wait_tags=[tag])
        # self.Queue(TestingQueue.Get()).AddPacket(pck)

    def testCheckConnection(self):
        self.assertTrue(self.admin_connector.CheckConnection(self.servername2))
