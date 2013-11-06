import time
from testdir import *

class T13(unittest.TestCase):
    """test max_working_time"""

    def setUp(self):
        self.connector = Config.Get().server1.connector

    def testBreakForLongExecution(self):
        timestamp = int(time.time())
        pck = self.connector.Packet(
            "long_execution-%s" % timestamp,
            timestamp,
            wait_tags=[],
        )
        pck.AddJob(shell="sleep 5", max_working_time=3, tries=2)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        WaitForExecution(pckInfo)
        self.assertEqual(pckInfo.state, "ERROR")
