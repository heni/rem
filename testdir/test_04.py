from __future__ import print_function
import bsddb3
import copy
import logging
import os
import shutil
import tempfile
import unittest
import remclient
from testdir import *


class T04(unittest.TestCase):
    """Checking for interesting boundary cases"""

    def setUp(self):
        self.connector = Config.Get().server1.connector

    def testWorkingLimits(self):
        pckname = "lmt-worker-%.0f" % time.time()
        pckList = [self.connector.Packet(pckname, time.time()) for _ in range(10)]

        for pck in pckList:
            osType = os.uname()[0]
            if osType == "FreeBSD":
                pck.AddJob("lockf -t 0 /tmp/%s sleep 2" % pckname, tries=1)
            elif osType == "Linux":
                pck.AddJob("./lockf.sh", tries=1)
                with tempfile.NamedTemporaryFile(suffix="-lockf.sh") as script_pr:
                    print("""
                                        #!/usr/bin/env bash
                                        set -eu
                                        lockfile-create {lockfile}
                                        lockfile-touch {lockfile} &
                                        BADGER="$!"
                                        sleep 2
                                        kill $BADGER
                                        lockfile-remove {lockfile}
                                        """.format(lockfile="/tmp/" + pckname), file=script_pr)
                    script_pr.flush()
                    pck.AddFiles({"lockf.sh": script_pr.name})
            else:
                raise RuntimeError("unsupport os: %s" % osType)
        for pck in pckList:
            self.connector.Queue(LmtTestQueue.Get()).AddPacket(pck)
        pckList = list(map(self.connector.PacketInfo, pckList))
        WaitForExecutionList(pckList)
        for pck in pckList:
            self.assertEqual(pck.state, "SUCCESSFULL")
            pck.Delete()

    def testCycledPacket(self):
        pckname = "worktest-%.0f" % time.time()
        tagname = "sometag-%.0f" % time.time()
        pck = self.connector.Packet(pckname, time.time(), wait_tags=[tagname], set_tag=tagname)
        pck.AddJob("echo \"idiot's task\"")
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        if pckInfo.state == "SUSPENDED":
            self.connector.Tag(tagname).Set()
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        pckInfo.Delete()
        self.connector.Tag(tagname).Unset()

    def testBrokenFork(self):
        pckname = "brkfork-%.0f" % time.time()
        pck = self.connector.Packet(pckname, time.time())
        pck.AddJob("./x.py", tries=1)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertEqual(WaitForExecution(pckInfo), "ERROR")
        pckInfo.Delete()

    def testManyPackets(self):
        tm = time.time()
        tgPrfx = "chain-%.0f" % tm
        strtTag = "chain-start-%.0f" % tm
        waitings = [strtTag]
        pckList = []
        logging.info("start long chain adding")
        for idx in range(1000):
            pckname = "sleeptest-%.0f-%s" % (tm, idx)
            pck = self.connector.Packet(pckname, time.time(), wait_tags=waitings, set_tag="%s-%d" % (tgPrfx, idx))
            pck.AddJob("echo .")
            self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
            waitings = ["%s-%d" % (tgPrfx, idx)]
            pckList.append(pck)
        self.connector.Tag(strtTag).Set()
        pckInfo = self.connector.PacketInfo(pckList[-1].id)
        self.assertEqual(WaitForExecution(pckInfo, timeout=10.0), "SUCCESSFULL")
        for pck in pckList:
            self.connector.PacketInfo(pck.id).Delete()

    def testManyParallels(self):
        tm = time.time()
        pckname = "prlexec-%.0f" % tm
        pck = self.connector.Packet(pckname, time.time(), set_tag=pckname)
        for idx in range(1000):
            pck.AddJob("echo -n . >&2")
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertEqual(WaitForExecution(pckInfo, timeout=2.0), "SUCCESSFULL")
        pckInfo.Delete()

    def testManyParallelSequentialJobs(self):
        """Many jobs with the following dependencies graph: j0 -> j1 -> ... -> jN"""
        tm = time.time()
        pckname = "prl_seq_exec-%.0f" % tm
        pck = self.connector.Packet(pckname, time.time(), set_tag=pckname)

        parentJobs = []
        for idx in range(1000):
            job = pck.AddJob('true', parents=parentJobs)
            parentJobs = [job]

        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertEqual(WaitForExecution(pckInfo, timeout=2.0), "SUCCESSFULL")
        pckInfo.Delete()

    def testSingleJobWaitingManyParallelJobs(self):
        tm = time.time()
        pckname = "prl_single_waiter_exec-%.0f" % tm
        pck = self.connector.Packet(pckname, time.time(), set_tag=pckname)

        parentJobs = []
        for idx in range(1000):
            job = pck.AddJob('true')
            parentJobs.append(job)

        job = pck.AddJob('true', parents=parentJobs)

        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        self.assertEqual(WaitForExecution(pckInfo, timeout=2.0), "SUCCESSFULL")
        pckInfo.Delete()

    def testCleanWorked(self):
        for qname in (TestingQueue.Get(), LmtTestQueue.Get()):
            q = self.connector.Queue(qname)
            pckList = q.ListPackets("worked")
            pckList = remclient.JobPacketInfo.multiupdate(pckList)
            for pck in pckList:
                try:
                    pck.Delete()
                except:
                    logging.error("can't delete packet '%s'", pck.pck_id)
            print(q.Status())

    def testRemoveWorking(self):
        pckname = "running-%.f" % time.time()
        newPck = self.connector.Packet(pckname, time.time())
        newPck.AddJob("sleep 3 && echo test")
        self.connector.Queue(TestingQueue.Get()).AddPacket(newPck)
        time.sleep(0.01)
        q = self.connector.Queue(TestingQueue.Get())
        pckList = q.ListPackets("working")
        pckList = remclient.JobPacketInfo.multiupdate(pckList)
        for pck in pckList:
            self.assertRaises(Exception, pck.Delete)
            self.assertTrue(pck in pckList)
        pckInfo = self.connector.PacketInfo(newPck.id)
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")

    def testUnreadableDB(self):
        tmp_dir = tempfile.mkdtemp(dir=".", prefix="unreadable")
        pckInfo = None
        try:
            dbFile = os.path.join(tmp_dir, "test.db")
            bsddb3.btopen(dbFile).close()
            os.chmod(dbFile, 0)

            connector = copy.copy(self.connector)
            connector.checksumDbPath = dbFile

            pckname = "unreadable-db-%.f" % time.time()
            pck = connector.Packet(pckname, time.time())
            pck.AddJob("cat data.txt")

            with tempfile.NamedTemporaryFile(dir=tmp_dir, suffix=".txt") as f:
                for _ in range(100):
                    print(_, file=f)
                f.flush()
                pck.AddFiles({"data.txt": f.name})

            connector.Queue(TestingQueue.Get()).AddPacket(pck)
            pckInfo = connector.PacketInfo(pck.id)
            self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        finally:
            if pckInfo:
                pckInfo.Delete()
            shutil.rmtree(tmp_dir)

