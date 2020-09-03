import unittest
import logging
import remclient
import time
from testdir import Config, TestingQueue, WaitForExecution


class T18(unittest.TestCase):
    """Test to check run_as functionality"""

    def setUp(self):
        self.connector = Config.Get().server1.connector
        self.alt_user = Config.Get().alt_user
        self.assertTrue(
            Config.Get().server1.rem_configuration.getboolean("server", "allow_runas"),
            "allow_runas option has to be explicitly configured"
        )
        try: # check alt_users exists
            import pwd
            pwd.getpwnam(self.alt_user)
        except ImportError:
            logging.warning("can't check user %s, please setup pwd module" % self.alt_user)
        except KeyError:
            self.assertTrue(False, "can't find user %s at your system" % self.alt_user)

    def testRunAsSimpleTask(self):
        timestamp = int(time.time())
        pckname = "testRunAsSimpleTask-%d" % timestamp
        pck = self.connector.Packet(pckname, run_as=self.alt_user)
        pck.AddJob("true", tries=1)
        #pck.AddJob("ls -lhd .", output_to_status=True)
        #pck.AddJob("getfacl .", output_to_status=True)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        logging.info("packet %s(%s) added to queue %s, waiting until doing", pckname, pck.id, TestingQueue.Get())
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        for job in pckInfo.jobs:
            logging.info("command '%s' result: %s", job.shell, '\n'.join(map(str, job.results)))
        pckInfo.Delete()

    def testRunAsWithOutput(self):
        timestamp = int(time.time())
        pckname = "testRunAsWithOutput-%d" % timestamp
        pck = self.connector.Packet(pckname, run_as=self.alt_user)
        who_job = pck.AddJob("whoami", tries=1)
        cat_job = pck.AddJob("cat", pipe_parents=[who_job])
        wc_job = pck.AddJob("awk 'NR == 1 {print length($0)}'", pipe_parents=[cat_job], output_to_status=True)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        logging.info("packet %s(%s) added to queue %s, waiting until doing", pckname, pck.id, TestingQueue.Get())
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        wc_job_status = next((job for job in pckInfo.jobs if "awk" in str(job.shell)), None)
        self.assertIsNotNone(wc_job_status)
        wc_job_message = str(wc_job_status.results[-1])
        self.assertTrue((str(len(self.alt_user)) + '\n') in wc_job_message, "unexpected job output: %s" % wc_job_message)
        pckInfo.Delete()

    def testRunAsWithLocalFiles(self):
        timestamp = int(time.time())
        pckname = "testRunAsWithLocalFiles-%d" % timestamp
        pck = self.connector.Packet(pckname, run_as=self.alt_user)
        who_job = pck.AddJob("whoami > username", tries=1)
        cat_job = pck.AddJob("cat username", parents=[who_job], output_to_status=True)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        logging.info("packet %s(%s) added to queue %s, waiting until doing", pckname, pck.id, TestingQueue.Get())
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        cat_job_status = next((job for job in pckInfo.jobs if "cat" in str(job.shell)), None)
        self.assertIsNotNone(cat_job_status)
        cat_job_message = str(cat_job_status.results[-1])
        self.assertTrue(self.alt_user in cat_job_message, "unexpected job output: %s" % cat_job_message)
        pckInfo.Delete()

