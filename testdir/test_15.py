import unittest
import logging
import remclient
from testdir import *


class T15(unittest.TestCase):
    """Statistics functionality"""

    def setUp(self):
        self.connector = Config.Get().server1.connector
        self.connector2 = Config.Get().server2.connector

    def testTagReset(self):
        timestamp = time.time()
        p = self.connector.Packet('test_tag_reset-{}'.format(timestamp), wait_tags=['tag_a-{}'.format(timestamp), 'tag_b-{}'.format(timestamp), 'tag_c-{}'.format(timestamp)])
        self.connector.Queue('test').AddPacket(p)
        self.connector.Tag('tag_c-{}'.format(timestamp)).Set()
        self.connector.Tag('tag_a-{}'.format(timestamp)).Set()
        self.connector.Tag('tag_a-{}'.format(timestamp)).Reset()
        self.connector.Tag('tag_a-{}'.format(timestamp)).Set()
        self.connector.Tag('tag_b-{}'.format(timestamp)).Set()
        self.assertEqual(self.connector.PacketInfo(p).wait, [])

    def testRemoteTagReset(self):
        timestamp = time.time()
        p = self.connector.Packet('test_tag_reset-{}'.format(time.time()), wait_tags=['local-02:r_tag_a-{}'.format(timestamp), 'local-02:r_tag_b-{}'.format(timestamp), 'r_tag_c-{}'.format(timestamp)])
        self.connector.Queue('test').AddPacket(p)
        self.connector.Tag('r_tag_c-{}'.format(timestamp)).Set()
        self.connector2.Tag('r_tag_a-{}'.format(timestamp)).Set()
        time.sleep(4)
        self.connector.Tag('r_tag_c-{}'.format(timestamp)).Reset()
        self.connector.Tag('r_tag_c-{}'.format(timestamp)).Set()
        self.connector2.Tag('r_tag_b-{}'.format(timestamp)).Set()
        time.sleep(4)
        self.assertEqual(self.connector.PacketInfo(p).wait, [])

