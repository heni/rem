import logging
import time
import unittest
import tempfile
import remclient
from testdir import *


class T12(unittest.TestCase):
    """REM speed tests"""

    def setUp(self):
        self.connector = Config.Get().server1.connector
        self.notifyEmails = [Config.Get().notify_email]
        self.checksumDbPath = 'checksum_cache.db'

    @classmethod
    def __createHugeFile(cls):
        f = tempfile.NamedTemporaryFile(dir='.')
        for _ in xrange(10 ** 2):
            s = '%s\n' % random.randint(0, 1e9)
            for _ in xrange(10 ** 5):
                f.write(s)
        f.flush()
        return f

    def __addPackages(self, filePath, count):
        queue = self.connector.Queue('checksum_cache_test')
        for _ in xrange(count):
            pck = self.connector.Packet(
                'checksum_test %s' % time.time(),
                time.time(),
                wait_tags=[],
                notify_emails=self.notifyEmails
            )
            time.sleep(0.001)
            pck.AddJob(shell='true')
            pck.AddFiles({'test.bin': filePath})
            queue.AddPacket(pck)

    def testChecksumCache(self):
        """This test creates a huge file (about 100MB) and adds packages with this file
        without checksum-cache usage and with it.
        The idea of test is to measure time in both cases."""
        try:
            repeat_count = 100

            start = time.time()
            hugeFile = T12.__createHugeFile()
            logging.info('Huge binary file creation time: %f seconds' % (time.time() - start, ))

            start = time.time()
            self.__addPackages(hugeFile.name, 1)
            logging.info('Binary file added to server: %f seconds' % (time.time() - start, ))

            self.connector.checksumDbPath = None
            start = time.time()
            self.__addPackages(hugeFile.name, repeat_count)
            logging.info(
                'Adding %d packages without usage checksum cache: %f seconds' % (repeat_count, time.time() - start))

            self.connector.checksumDbPath = self.checksumDbPath
            start = time.time()
            self.__addPackages(hugeFile.name, repeat_count)
            logging.info('Adding %d packages using checksum cache: %f seconds' % (repeat_count, time.time() - start))

            self.connector.checksumDbPath = None
        finally:
            hugeFile.close()
