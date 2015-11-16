import unittest
import logging
import threading
import time
import remclient
from testdir import Config, WaitForExecution


class T09(unittest.TestCase):
    """Race conditions and deadlocks"""

    def setUp(self):
        self.connector = Config.Get().server1.connector
        self.remUrl = Config.Get().server1.url
        self.notifyEmails = [Config.Get().notify_email]

    #def testConcurrentAddingOfPacketWithSameBinaryFile(self):
    #    TODO

    def testTagDuplication(self):
        """ In this test two threads are created.
            The first one creates packet which sets some tag.
            The second one creates packet depending on the same tag.
            The aim is to check the following error: two tag-object with the same name are created on server"""

        class ClientThread(threading.Thread):

            def __init__(self, signal, rem_url, queue, tag, notify_emails, client_type, use_debug=False):
                super(ClientThread, self).__init__()
                self.signal = signal
                self.remUrl = rem_url
                self.queue = queue
                self.tag = tag
                self.notifyEmails = notify_emails
                self.clientType = client_type
                self.pck = None
                self.useDebug = use_debug

            def run(self):
                if self.useDebug:
                    logging.debug("Client thread started: tag=%s clientType=%s" % (self.tag, self.clientType))

                self.signal.wait()

                conn = remclient.Connector(self.remUrl, packet_name_policy=remclient.IGNORE_DUPLICATE_NAMES_POLICY)
                queue = conn.Queue(self.queue)
                tag = self.tag
                if self.clientType == 'tag_creator':
                    self.pck = conn.Packet(
                        'tag_creator.' + tag,
                        time.time(),
                        wait_tags=[],
                        set_tag=tag,
                        notify_emails=self.notifyEmails
                    )
                    self.pck.AddJob(shell='echo tag_creator')
                elif self.clientType == 'tag_checker':
                    self.pck = conn.Packet(
                        'tag_checker.' + tag,
                        time.time(),
                        wait_tags=[tag],
                        notify_emails=self.notifyEmails
                    )
                    self.pck.AddJob(shell='echo tag_checker')
                else:
                    raise RuntimeError('Undefined clientType field value: %s!' % self.clientType)

                queue.AddPacket(self.pck)
                if self.useDebug:
                    logging.debug("Client thread finished: tag=%s clientType=%s" % (self.tag, self.clientType))

        N = 1000
        ts = time.time()
        tags = []
        creatorPackets = []
        checkerPackets = []
        logging.info("massive multithreading packets append");
        for i in range(N):
            signal = threading.Event()
            queue = 'duplicate_tags_test'
            tag = 'dup_tag_%d_%d' % (int(ts), i)
            tags.append(tag)

            threadCreator = ClientThread(signal, self.remUrl, queue, tag, self.notifyEmails, 'tag_creator')
            threadCreator.start()

            threadChecker = ClientThread(signal, self.remUrl, queue, tag, self.notifyEmails, 'tag_checker')
            threadChecker.start()

            signal.set()

            threadCreator.join()
            threadChecker.join()

            creatorPackets.append(
                self.connector.PacketInfo(threadCreator.pck.id)
            )
            checkerPackets.append(
                self.connector.PacketInfo(threadChecker.pck.id)
            )

        for pck in creatorPackets:
            self.assertTrue(WaitForExecution(pck), "SUCCESSFULL")
        for pck in checkerPackets:
            self.assertTrue(WaitForExecution(pck), "SUCCESSFULL")

        logging.info('Checking tags "%s"', "dup_tag_*")
        for tagName in tags:
            tag = self.connector.Tag(tagName)
            self.assertTrue(tag.Check())
