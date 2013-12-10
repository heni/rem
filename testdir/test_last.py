from testdir import *

class TLast(unittest.TestCase):

    def setUp(self):
        self.connector1 = Config.Get().server1.connector
        self.connector2 = Config.Get().server2.connector
        self.srvdir1 = Config.Get().server1.projectDir
        self.srvdir2 = Config.Get().server2.projectDir

    def testSerialization(self):
        timestamp = int(time.time())
        tag = "serialization-check-tag-%s" % timestamp
        self.connector1.Tag(tag).Set()
        self.connector2.Tag(tag).Set()
        with ServiceTemporaryShutdown(self.srvdir1):
            pass
        with ServiceTemporaryShutdown(self.srvdir2):
            pass
        self.assertTrue(self.connector1.Tag(tag).Check())
        self.assertTrue(self.connector2.Tag(tag).Check())

