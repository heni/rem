import unittest
import logging
import remclient
from testdir import *


class T15(unittest.TestCase):
    """Statistics functionality"""

    def setUp(self):
        self.connector = Config.Get().server1.connector
        self.connector2 = Config.Get().server2.connector
        self.notifyEmails = [Config.Get().notify_email]

    def testTagReset(self):
        timestamp = time.time()
        p = self.connector.Packet('test_tag_reset-{}'.format(timestamp), wait_tags=['tag_a-{}'.format(timestamp), 'tag_b-{}'.format(timestamp), 'tag_c-{}'.format(timestamp)])
        self.connector.Queue(TestingQueue.Get()).AddPacket(p)
        self.connector.Tag('tag_c-{}'.format(timestamp)).Set()
        self.connector.Tag('tag_a-{}'.format(timestamp)).Set()
        self.connector.Tag('tag_a-{}'.format(timestamp)).Reset()
        self.connector.Tag('tag_a-{}'.format(timestamp)).Set()
        self.connector.Tag('tag_b-{}'.format(timestamp)).Set()
        self.assertEqual(self.connector.PacketInfo(p).wait, [])

    def testUnresetablePacket(self):
        timestamp = time.time()
        with self.connector as connector:
            tags = ["start-point-{}".format(timestamp), "middle-point-{}".format(timestamp), "end-point-{}".format(timestamp)]
            p0 = connector.Packet("start-packet-{}".format(timestamp), wait_tags=[tags[0]], set_tag=tags[1], notify_emails=self.notifyEmails)
            p1 = connector.Packet("secnd-packet-{}".format(timestamp), wait_tags=[tags[1]], set_tag=tags[2], notify_emails=self.notifyEmails, resetable=False)
            p1.AddJob("sleep 1")
            p2 = connector.Packet("third-packet-{}".format(timestamp), wait_tags=[tags[1]], notify_emails=self.notifyEmails, resetable=True)
            p2.AddJob("sleep 1")
            connector.Queue(TestingQueue.Get()).AddPacket(p0)
            connector.Queue(TestingQueue.Get()).AddPacket(p1)
            connector.Queue(TestingQueue.Get()).AddPacket(p2)
            connector.Tag(tags[0]).Set()
            self.assertEqual(WaitForExecution(connector.PacketInfo(p1.id)), "SUCCESSFULL")
            self.assertEqual(WaitForExecution(connector.PacketInfo(p2.id)), "SUCCESSFULL")
            connector.Tag(tags[0]).Reset("reset for testing purpose")
            self.assertEqual(connector.PacketInfo(p1.id).state, "SUCCESSFULL")
            self.assertEqual(WaitForExecution(connector.PacketInfo(p2.id), ("SUSPENDED",)), "SUSPENDED")
            connector.Tag(tags[0]).Set()
            self.assertEqual(WaitForExecution(connector.PacketInfo(p2.id)), "SUCCESSFULL")

    def testRemoteTagReset(self):
        timestamp = time.time()
        p = self.connector.Packet('test_tag_reset-{}'.format(time.time()), wait_tags=['{}:r_tag_a-{}'.format(Config.Get().server2.name, timestamp), '{}:r_tag_b-{}'.format(Config.Get().server2.name, timestamp), 'r_tag_c-{}'.format(timestamp)])
        self.connector.Queue(TestingQueue.Get()).AddPacket(p)
        self.connector.Tag('r_tag_c-{}'.format(timestamp)).Set()
        self.connector2.Tag('r_tag_a-{}'.format(timestamp)).Set()
        time.sleep(4)
        self.connector.Tag('r_tag_c-{}'.format(timestamp)).Reset()
        self.connector.Tag('r_tag_c-{}'.format(timestamp)).Set()
        self.connector2.Tag('r_tag_b-{}'.format(timestamp)).Set()
        time.sleep(4)
        self.assertEqual(self.connector.PacketInfo(p).wait, [])

