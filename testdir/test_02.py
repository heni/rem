import unittest
import logging
import time
import tempfile
import subprocess
from threading import Thread
import copy
import xmlrpclib

import remclient
from testdir import *


class T02(unittest.TestCase):
    """Checking common server functionality"""

    def setUp(self):
        self.connector = Config.Get().server1.connector
        self.timestamp = int(time.time())
        self.notifyEmail = Config.Get().notify_email

    def testSimplePacket(self):
        pckname = "simpletest-%d" % self.timestamp
        pck = self.connector.Packet(pckname, self.timestamp)
        pck.AddJob("sleep 1", tries=2)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        logging.info("packet %s(%s) added to queue %s, waiting until doing", pckname, pck.id, TestingQueue.Get())
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        pckInfo.Delete()

    def testPythonImport(self):
        pckname = "pythonimport-%d" % self.timestamp
        pck = self.connector.Packet(pckname, self.timestamp)
        f0 = tempfile.NamedTemporaryFile(dir=".", suffix="module.py")
        f1 = tempfile.NamedTemporaryFile(dir=".", suffix="s.py")
        print >> f0, "X = 'OK'"
        f0.flush()
        print >> f1, "#!/usr/bin/env python\nimport module\nprint module.X";
        f1.flush()
        j0 = pck.AddJob("PYTHONPATH=. ./s.py", files={"module.py": f0.name, "s.py": f1.name}, tries=1)
        j1 = pck.AddJob("grep OK", pipe_parents=[j0], tries=1)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        logging.info("packet %s(%s) added to queue %s, waiting until doing", pckname, pck.id, TestingQueue.Get())
        pckInfo = self.connector.PacketInfo(pck.id)
        try:
            self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        except:
            PrintPacketResults(pckInfo)
        finally:
            pckInfo.Delete()

    def testWorkingPacket(self):
        pckname = "worktest-%d" % self.timestamp
        pck = self.connector.Packet(pckname, self.timestamp)
        j1 = pck.AddJob("echo \"qwerty\"")
        j2 = pck.AddJob("wc -l", pipe_parents=[j1])
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        logging.info("packet %s(%s) added to queue %s, waiting until doing", pckname, pck.id, TestingQueue.Get())
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        PrintPacketResults(pckInfo)
        pckInfo.Delete()

    def testEmptyPacket(self):
        pckname = "emptypck-%d" % self.timestamp
        tagname = "emptytag-%d" % self.timestamp
        barriertag = "brtag-%d" % self.timestamp
        pck = self.connector.Packet(pckname, self.timestamp, wait_tags=[tagname], set_tag=barriertag)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        logging.info("packet %s(%s) added to queue %s, waiting until doing", pckname, pck.id, TestingQueue.Get())
        self.assertEqual(pckInfo.state, "SUSPENDED")
        self.connector.Tag(tagname).Set()
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        self.assertEqual(self.connector.Tag(barriertag).Check(), True)
        pckInfo.Delete()

    def testBulkAdding(self):
        pckList = []
        tagList = []
        queue = self.connector.Queue(TestingQueue.Get())
        for index in xrange(10):
            pckname = "bulkpck-%d-%d" % (index, self.timestamp)
            tagList += ["tg-%d-psx-%d" % (index, self.timestamp), "tg-%d-echo-%d" % (index, self.timestamp)]
            pck = self.connector.Packet(pckname, self.timestamp, notify_emails=[self.notifyEmail])
            pck.AddJobsBulk({"shell": "ps x", "set_tag": tagList[-2]}, {"shell": "echo xxx", "set_tag": tagList[-1]},
                            {"shell": "sleep 1"})
            queue.AddPacket(pck)
            pckList.append(self.connector.PacketInfo(pck.id))
        logging.info("packets with prefix bulkpck(%s) added to queue %s, waiting until doing",
                     [pi.pck_id for pi in pckList], TestingQueue.Get())
        PrintCurrentWorkingJobs(queue)
        WaitForExecutionList(pckList)
        for tagname in tagList:
            self.assertEqual(self.connector.Tag(tagname).Check(), True)
        for pck in pckList:
            self.assertEqual(pck.state, "SUCCESSFULL")
            pck.Delete()

    def testBrokenPacket(self):
        pckname = "brktest-%d" % self.timestamp
        pck = self.connector.Packet(pckname, self.timestamp, notify_emails=[self.notifyEmail])
        j1 = pck.AddJob("echo \"qwerty\"")
        j2 = pck.AddJob("perl -e \"exit $(wc -l);\"", pipe_parents=[j1], tries=2)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        logging.info("packet %s(%s) added to queue %s, waiting until doing", pckname, pck.id, TestingQueue.Get())
        pckInfo = self.connector.PacketInfo(pck)
        self.assertEqual(WaitForExecution(pckInfo, "WAITING"), "WAITING")
        self.assertEqual(len(pckInfo.jobs), 2)
        self.assertEqual(WaitForExecution(pckInfo), "ERROR")
        pckInfo.Delete()

    def testFalse(self):
        pckname = "falsetest-%d" % self.timestamp
        pck = self.connector.Packet(pckname, self.timestamp, notify_emails=[self.notifyEmail])
        j = pck.AddJob("false", tries=2)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        logging.info("packet %s(%s) added to queue %s, waiting until doing", pckname, pck.id, TestingQueue.Get())
        pckInfo = self.connector.PacketInfo(pck)
        self.assertEqual(WaitForExecution(pckInfo), "ERROR")
        pckInfo.Delete()

    def testExtendedDescription(self):
        pckname = "descrtest-%d" % self.timestamp
        pck = self.connector.Packet(pckname, self.timestamp, notify_emails=[self.notifyEmail])
        j = pck.AddJob("exit $REM_JOB_DESCRIPTION", extended_description='1', tries=2)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        logging.info("packet %s(%s) added to queue %s, waiting until doing", pckname, pck.id, TestingQueue.Get())
        pckInfo = self.connector.PacketInfo(pck)
        self.assertEqual(WaitForExecution(pckInfo), "ERROR")
        pckInfo.Delete()

    def testHugeOutput(self):
        pckname = "hugeout-%d" % self.timestamp
        pck = self.connector.Packet(pckname, self.timestamp, notify_emails=[self.notifyEmail])
        with tempfile.NamedTemporaryFile(dir=".") as script_printer:
            print >> script_printer, "#!/usr/bin/env python"
            print >> script_printer, "import sys"
            print >> script_printer, r"print 'Hello, World!\n'*10000000"
            print >> script_printer, r"print >>sys.stderr, 'Hello, World!\n'*10000000"
            script_printer.flush()
            j1 = pck.AddJob("./huge.py", tries=2, files={"huge.py": script_printer.name})
        j2 = pck.AddJob("wc -l", pipe_parents=[j1], tries=2)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        logging.info("packet %s(%s) added to queue %s, waiting until doing", pckname, pck.id, TestingQueue.Get())
        pckInfo = self.connector.PacketInfo(pck)
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        pckInfo.Delete()

    def testSuspendKillJobs(self):
        def contains(lst, val):
            lst = list(lst)
            logging.info(lst)
            return any(val in el for el in lst)

        pckname = "suspend-kill-%.f" % time.time()
        pck = self.connector.Packet(pckname, time.time())
        pck.AddJob("sleep 10 && echo 12344321")
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        pckInfo.Stop()
        time.sleep(0.2)
        popen = subprocess.Popen(["ps", "x", "-o", "command"], stdout=subprocess.PIPE)
        logging.info("")
        self.assertEqual(contains(popen.stdout, "12344321"), False)
        pckInfo.Resume()
        time.sleep(0.2)
        pckInfo.Suspend()
        popen = subprocess.Popen(["ps", "x", "-o", "command"], stdout=subprocess.PIPE)
        logging.info("")
        self.assertEqual(contains(popen.stdout, "12344321"), True)
        pckInfo.Stop()
        time.sleep(0.2)
        popen = subprocess.Popen(["ps", "x", "-o", "command"], stdout=subprocess.PIPE)
        logging.info("")
        self.assertEqual(contains(popen.stdout, "12344321"), False)
        pckInfo.Delete()

    def testTagsBulk(self):
        tags = ["bulk-tag-%d-%.0f" % (i, time.time()) for i in range(10)]
        bulkObj = self.connector.TagsBulk(tags)
        self.assertEqual(len(tags), len(bulkObj.FilterUnset().GetTags()))
        self.assertEqual(0, len(bulkObj.FilterSet().GetTags()))
        bulkObj.Set()
        self.assertEqual(0, len(bulkObj.FilterUnset().GetTags()))
        self.assertEqual(len(tags), len(bulkObj.FilterSet().GetTags()))
        bulkObj.Unset()
        self.assertEqual(len(tags), len(bulkObj.FilterUnset().GetTags()))
        self.assertEqual(0, len(bulkObj.FilterSet().GetTags()))

    def testMovePacket(self):
        pckname = "moving-packet-%d" % self.timestamp
        pck = self.connector.Packet(pckname, self.timestamp)
        pck.AddJob("sleep 2", tries=1)
        fakePck = self.connector.Packet(pckname + "-fake", self.timestamp)
        queue1 = TestingQueue.Get()
        queue2 = LmtTestQueue.Get()
        self.connector.Queue(queue1).AddPacket(pck)
        self.connector.Queue(queue2).AddPacket(fakePck)
        logging.info('Packet id: %s', pck.id)
        pckInfo = self.connector.PacketInfo(pck.id)
        pckInfo.Suspend()
        pckInfo.MoveToQueue(queue1, queue2)
        pckInfo.MoveToQueue(queue2, queue2)
        pckInfo.MoveToQueue(queue2, queue1)
        pckInfo.Resume()
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")

    def testMoveErroredPacket(self):
        pckname = "moving-packet-%d" % self.timestamp
        pck = self.connector.Packet(pckname, self.timestamp)
        pck.AddJob("some_wrong_command", tries=1);
        queue1 = TestingQueue.Get()
        queue2 = LmtTestQueue.Get()
        self.connector.Queue(queue1).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        time.sleep(2.0)
        pckInfo.MoveToQueue(queue1, queue2)
        pckInfo.MoveToQueue(queue2, queue2)

    def testQueueDelete(self):
        qname = "sample-queue-%.0f" % self.timestamp
        qobj = self.connector.Queue(qname)
        pck = self.connector.Packet(qname, self.timestamp)
        pck.AddJob("sleep 1")
        qobj.AddPacket(pck)
        logging.info("queue %s: %s", qname, qobj.Status())
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertRaises(xmlrpclib.Fault, qobj.Delete)
        WaitForExecution(pckInfo)
        logging.info("queue %s: %s", qname, qobj.Status())
        self.assertRaises(xmlrpclib.Fault, qobj.Delete)
        pckInfo.Delete()
        logging.info("queue %s: %s", qname, qobj.Status())
        qobj.Delete()
        qList = self.connector.ListObjects("queues")
        self.assertFalse(any(qname == qn for qn, qstat in qList))

    def testPipeFail(self):
        pckname = "failing-packet-%d" % self.timestamp
        pck = self.connector.Packet(pckname, self.timestamp)
        pck.AddJob("false | wc", tries=1, pipe_fail=1, description="sample failing job")
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertEqual(WaitForExecution(pckInfo), "ERROR")
        self.assertEqual(pckInfo.jobs[0].desc, "sample failing job")
        pckInfo.Delete()

    def testAsyncQueries(self):
        NUM_TAGS = 1001

        def queryFunction(connector):
            tags = connector.ListObjects("tags", prefix="async-query-tag-", memory_only=False)
            self.assertEqual(NUM_TAGS, len(tags))

        for i in range(NUM_TAGS):
            tagname = "async-query-tag-%d" % i
            self.connector.Tag(tagname).Set()
        logging.info("tags are set")
        requesters = [Thread(target=queryFunction, args=[remclient.Connector(self.connector.GetURL())]) for _ in xrange(5)]
        map(lambda t: t.start(), requesters)
        map(lambda t: t.join(), requesters)

    def testUniquePacket(self):
        pckname = "unique-pck-%.0f" % time.time()
        tagname = "unique-tag-%.0f" % time.time()
        pckname2 = "unique-pck2-%.0f" % time.time()
        pck = self.connector.Packet(pckname, self.timestamp, set_tag=tagname, check_tag_uniqueness=True)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        time.sleep(1.0)
        self.assertRaises(RuntimeError, self.connector.Packet, pckname2, self.timestamp, set_tag=tagname,
                          check_tag_uniqueness=True)

    def testQueueDoesntExist(self):
        """Tests that read operations on non-existent queue fail"""
        self.assertRaises(xmlrpclib.Fault, lambda: self.connector.Queue('queue-that-doesnt-exist').Status())
