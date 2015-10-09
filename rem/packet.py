from __future__ import with_statement
import logging
import tempfile
import os
import time
import shutil

from callbacks import CallbackHolder, ICallbackAcceptor, Tag, tagset
from common import BinaryFile, PickableRLock, SendEmail, Unpickable, safeStringEncode
from job import Job, PackedExecuteResult
import osspec

class PacketState(object):
    CREATED = "CREATED"                 #created only packet
    WORKABLE = "WORKABLE"               #working packet without pending jobs
    PENDING = "PENDING"                 #working packet with pending jobs 
    SUSPENDED = "SUSPENDED"            #suspended packet
    ERROR = "ERROR"                     #unresolved error exists
    SUCCESSFULL = "SUCCESSFULL"         #successfully done packet
    HISTORIED = "HISTORIED"             #only for history archives
    WAITING = "WAITING"                 #wait for executing (until job.continueTime)
    NONINITIALIZED = "NONINITIALIZED"   #after reset
    allowed = {
        CREATED: (WORKABLE, SUSPENDED, NONINITIALIZED),
        WORKABLE: (SUSPENDED, ERROR, SUCCESSFULL, PENDING, WAITING, NONINITIALIZED),
        PENDING: (SUSPENDED, WORKABLE, ERROR, WAITING, NONINITIALIZED),
        SUSPENDED: (WORKABLE, HISTORIED, WAITING, ERROR, NONINITIALIZED),
        WAITING: (PENDING, SUSPENDED, ERROR, NONINITIALIZED),
        ERROR: (SUSPENDED, HISTORIED, NONINITIALIZED),
        SUCCESSFULL: (HISTORIED, NONINITIALIZED),
        NONINITIALIZED: (CREATED,),
        HISTORIED: ()}


class PacketFlag:
    USER_SUSPEND = 0x01              #suspended by user
    RCVR_ERROR = 0x02              #recovering error detected


class PCL_StateChange(object):
    state_dispatcher = {}

    @classmethod
    def on_success(cls, pck):
        pck.SetDoneTags()
        pck.ReleasePlace()

    state_dispatcher[PacketState.SUCCESSFULL] = "on_success"

    @classmethod
    def on_delete(cls, pck):
        pck.ReleasePlace()

    state_dispatcher[PacketState.HISTORIED] = "on_delete"

    @classmethod
    def on_error(cls, pck):
        if pck.directory and not os.path.isdir(pck.directory):
            pck.ReleasePlace()

    state_dispatcher[PacketState.ERROR] = "on_error"


class PacketCustomLogic(object):
    from messages import GetHelperByPacketState, GetEmergencyHelper, GetLongExecutionWarningHelper, GetResetNotificationHelper

    SchedCtx = None
    StateMessageHelper = staticmethod(GetHelperByPacketState)
    EmergencyMessageHelper = staticmethod(GetEmergencyHelper)
    LongExecutionWorkningHelper = staticmethod(GetLongExecutionWarningHelper)
    ResetNotificationHelper = staticmethod(GetResetNotificationHelper)

    def __init__(self, pck):
        self.pck = pck

    def DoChangeStateAction(self):
        action_name = PCL_StateChange.state_dispatcher.get(self.pck.state, None)
        if action_name:
            getattr(PCL_StateChange, action_name)(self.pck)
        msgHelper = self.StateMessageHelper(self.pck, self.SchedCtx)
        if msgHelper:
            SendEmail(self.pck.notify_emails, msgHelper)

    def DoEmergencyAction(self):
        logging.error("incorrect state for \"Get\" operation: %s, packet %s will be markered for delete",
                      self.pck.state, self.pck.name)
        msgHelper = self.EmergencyMessageHelper(self.pck, self.SchedCtx)
        if msgHelper:
            SendEmail(self.pck.notify_emails, msgHelper)

    def DoResetNotification(self, message):
        msgHelper = self.ResetNotificationHelper(self.pck, self.SchedCtx, message)
        if msgHelper:
            SendEmail(self.pck.notify_emails, msgHelper)

    def DoLongExecutionWarning(self, job):
        logging.warning("Packet's '%s' job '%s' execution takes too long time", job.packetRef.name, job.id)
        msgHelper = self.LongExecutionWorkningHelper(job, self.SchedCtx)
        logging.warning('msgHelper: %s, ', type(msgHelper))
        SendEmail(self.pck.notify_emails, msgHelper)

    @classmethod
    def UpdateContext(cls, context):
        cls.SchedCtx = context


class JobPacketImpl(object):
    """tags manipulation methods"""

    def SetWaitingTags(self, wait_tags):
        for tag in wait_tags:
            tag.AddCallbackListener(self)
        self.allTags = set(tag.GetFullname() for tag in wait_tags)
        self.waitTags = set(tag.GetFullname() for tag in wait_tags if not tag.IsSet())

    def SetDoneTags(self):
        if self.done_indicator:
            self.done_indicator.Set()

    def ProcessTagEvent(self, tag):
        if tag.IsSet():
            tagname = tag.GetFullname()
            if tagname in self.waitTags:
                self.waitTags.remove(tagname)
            if len(self.waitTags) == 0 and self.state == PacketState.SUSPENDED:
                self.Resume()

    def VivifyDoneTagsIfNeed(self, tagStorage):
        if isinstance(self.done_indicator, str):
            self.done_indicator = tagStorage.AcquireTag(self.done_indicator)
        for jid, cur_val in self.job_done_indicator.iteritems():
            if isinstance(cur_val, str):
                self.job_done_indicator[jid] = tagStorage.AcquireTag(cur_val)

    def UpdateTagDependencies(self, tagStorage):
        self.waitTags = tagset(self.waitTags)
        for tagname in list(self.waitTags):
            if tagStorage.CheckTag(tagname):
                self.ProcessTagEvent(tagStorage.AcquireTag(tagname))
        if isinstance(self.done_indicator, Tag):
            self.done_indicator = tagStorage.AcquireTag(self.done_indicator.name)
        for jid in self.job_done_indicator:
            if isinstance(self.job_done_indicator[jid], Tag):
                self.job_done_indicator[jid] = tagStorage.AcquireTag(self.job_done_indicator[jid].name)

    """links manipulation methods"""

    def ReleaseLinks(self):
        tmpLinks, self.binLinks = self.binLinks, {}
        while tmpLinks:
            binname, file = tmpLinks.popitem()
            if isinstance(file, BinaryFile):
                file.Unlink(self, binname)
                filehash = file.checksum
            elif isinstance(file, str):
                filehash = file
            else:
                filehash = None
            if filehash is not None:
                self.binLinks[binname] = filehash

    def CreateLink(self, binname, file):
        file.Link(self, binname)
        self.binLinks[binname] = file

    def AddLink(self, binname, file):
        if binname in self.binLinks:
            old_file = self.binLinks.pop(binname)
            if isinstance(old_file, BinaryFile):
                old_file.Unlink(self, binname)
        self.CreateLink(binname, file)

    def VivifyLink(self, context, link):
        if isinstance(link, str):
            link = context.Scheduler.binStorage.GetFileByHash(link)
        elif isinstance(link, BinaryFile):
            link = context.Scheduler.binStorage.GetFileByHash(link.checksum)
        return link

    def CreateLinks(self, context):
        tmpLinks, self.binLinks = self.binLinks, {}
        while tmpLinks:
            binname, link = tmpLinks.popitem()
            file = self.VivifyLink(context, link)
            if file is not None:
                self.CreateLink(binname, file)

    def AreLinksAlive(self, context):
        return all(self.VivifyLink(context, link) for link in self.binLinks.itervalues())

    """file resource manipulation methods"""

    def ReleasePlace(self):
        self.ReleaseLinks()
        if self.directory and os.path.isdir(self.directory):
            try:
                shutil.rmtree(self.directory, onerror=None)
            except Exception, e:
                logging.error("Packet %s release place error: %s", self.id, e)
        self.directory = None
        self.streams.clear()

    def CreatePlace(self, context):
        if getattr(self, "directory", None):
            raise RuntimeError("can't create duplicate working directory")
        if getattr(self, "id", None):
            self.directory = os.path.join(context.packets_directory, self.id)
            os.makedirs(self.directory)
        while not getattr(self, "id", None):
            directory = tempfile.mktemp(dir=context.packets_directory, prefix="pck-")
            id = os.path.split(directory)[-1]
            if not (context.Scheduler.GetPacket(id) or os.path.isdir(directory)):
                try:
                    os.makedirs(directory)
                    self.directory = directory
                    self.id = id
                except OSError: #os.makedirs failed"
                    pass
        osspec.set_common_readable(self.directory)
        osspec.set_common_executable(self.directory)
        self.CreateLinks(context)
        self.streams = dict()

    def ListFiles(self):
        files = []
        if self.directory:
            try:
                files = os.listdir(self.directory)
            except Exception, e:
                logging.exception("directory %s listing error: %s", self.directory, e)
        return files

    def GetFile(self, filename):
        if not self.directory:
            raise RuntimeError("working directory doesn't exist")
        path = os.path.join(self.directory, filename)
        if not os.path.isfile(path):
            raise AttributeError("not existing file: %s" % filename)
        if os.path.dirname(path) != self.directory:
            raise AttributeError("file %s is outside working directory" % filename)
        with open(path, "r") as reader:
            data = reader.read()
            return data

    """process internal job start/stop"""

    def ProcessJobStart(self, job):
        job.input = self.createInput(job.id)
        job.output = self.createOutput(job.id)
        isStarted = not self.working
        self.working.add(job.id)
        return isStarted

    def ProcessJobDone(self, job):
        if not hasattr(self, "waitJobs"):
            self.UpdateJobsDependencies()
        nState, nTimeout = None, 0
        self.working.remove(job.id)
        result = job.Result()
        if self.state in (PacketState.NONINITIALIZED, PacketState.SUSPENDED):
            self.leafs.add(job.id)
        elif result is not None and result.IsSuccessfull():
            self.done.add(job.id)
            if self.job_done_indicator.get(job.id):
                self.job_done_indicator[job.id].Set()
            for nid in self.edges[job.id]:
                self.waitJobs[nid].remove(job.id)
                if not self.waitJobs[nid]:
                    self.leafs.add(nid)
            if len(self.done) == len(self.jobs):
                nState = PacketState.SUCCESSFULL
            elif self.leafs and self.state != PacketState.WAITING:
                nState = PacketState.PENDING
        elif result is None or result.CanRetry():
            self.leafs.add(job.id)
            nState = PacketState.WAITING
            nTimeout = getattr(job, "retry_delay", None) or job.ERR_PENALTY_FACTOR ** job.tries
            logging.debug("packet %s\twaiting for %s sec", self.name, nTimeout)
        else:
            self.result = PackedExecuteResult(len(self.done), len(self.jobs))
            nState = PacketState.ERROR
        return nState, nTimeout, not self.working


# job module.
class JobPacket(Unpickable(lock=PickableRLock,
                           jobs=dict,
                           job_done_indicator=dict,
                           edges=dict,
                           binLinks=dict,
                           done=set,
                           leafs=set,
                           working=set,
                           waitTags=set,
                           waitingDeadline=int,
                           state=(str, PacketState.CREATED),
                           history=(list, []),
                           notify_emails=(list, []),
                           flags=int,
                           kill_all_jobs_on_error=(bool, True),
                           isResetable=(bool, True)),
                CallbackHolder,
                ICallbackAcceptor,
                JobPacketImpl):
    INCORRECT = -1

    def __init__(self, name, priority, context, notify_emails, wait_tags=(), set_tag=None, kill_all_jobs_on_error=True, isResetable=True):
        super(JobPacket, self).__init__()
        self.name = name
        self.state = PacketState.NONINITIALIZED
        self.Init(context)
        self.priority = priority
        self.notify_emails = list(notify_emails)
        self.id = os.path.split(self.directory)[-1]
        self.history.append((self.state, time.time()))
        self.kill_all_jobs_on_error = kill_all_jobs_on_error
        self.isResetable = isResetable
        self.done_indicator = set_tag
        self.SetWaitingTags(wait_tags)

    def __getstate__(self):
        sdict = CallbackHolder.__getstate__(self)

        if sdict['done_indicator']:
            sdict['done_indicator'] = sdict['done_indicator'].name

        job_done_indicator = sdict['job_done_indicator'] = sdict['job_done_indicator'].copy()
        for job_id, tag in job_done_indicator.iteritems():
            job_done_indicator[job_id] = tag.name

        sdict.pop('waitingTime', None) # obsolete

        return sdict

    def Init(self, context):
        logging.info("packet init: %r %s", self, self.state)
        self.result = None
        if not getattr(self, "directory", None):
            self.CreatePlace(context)
        self.changeState(PacketState.CREATED)

    def __repr__(self):
        return "<JobPacket(id:%s; name: %s)>" % (getattr(self, "id", None), getattr(self, "name", None))

    def stream_file(self, jid, type):
        stream = self.streams.get((type, jid), None)
        if stream is not None:
            if not stream.closed:
                stream.close()
            if stream.name != "<uninitialized file>":
                return stream.name
        filename = os.path.join(self.directory, "%s-%s" % (type, jid))
        if os.path.isfile(filename):
            return filename
        return None

    def createInput(self, jid):
        if jid in self.jobs:
            filename = self.stream_file(jid, "in")
            job = self.jobs[jid]
            if filename is not None:
                stream = self.streams[("in", jid)] = open(filename, "r")
            elif len(job.inputs) == 0:
                stream = self.streams[("in", jid)] = osspec.get_null_input()
            elif len(job.inputs) == 1:
                pid, = job.inputs
                logging.debug("pid: %s, pck_id: %s", pid, self.id)
                stream = self.streams[("in", jid)] = open(self.stream_file(pid, "out"), "r")
            else:
                with open(os.path.join(self.directory, "in-%s" % jid), "w") as writer:
                    for pid in job.inputs:
                        with open(self.streams[("out", pid)].name, "r") as reader:
                            writer.write(reader.read())
                stream = self.streams[("in", jid)] = open(writer.name, "r")
            return stream
        raise RuntimeError("alien job input request")

    def createOutput(self, jid):
        if jid in self.jobs:
            filename = self.stream_file(jid, "out")
            if filename is None:
                filename = os.path.join(self.directory, "out-%s" % jid)
            stream = self.streams[("out", jid)] = open(filename, "w")
            return stream
        raise RuntimeError("alien job output request")

    def canChangeState(self, state):
        return state in PacketState.allowed[self.state]

    def stopWaiting(self):
        if self.state == PacketState.WAITING:
            self.changeState(PacketState.PENDING)

    def changeState(self, state):
        if state == self.state:
            return
        if not self.canChangeState(state):
            logging.warning("packet %s\tincorrect state change request %r => %r" % (self.name, self.state, state))
            if state != PacketState.SUCCESSFULL:
            #temporary hack for fast fix NONPRJ-898 task
                return False
        self.state = state
        logging.debug("packet %s\tnew state %r", self.name, self.state)
        self.FireEvent("change")
        self.history.append((self.state, time.time()))
        PacketCustomLogic(self).DoChangeStateAction()
        return True

    def OnStart(self, ref):
        if not hasattr(self, "waitJobs"):
            self.UpdateJobsDependencies()
        if isinstance(ref, Job):
            if self.state not in (PacketState.WORKABLE, PacketState.PENDING, PacketState.WAITING) \
                or ref.id not in self.jobs \
                or self.waitJobs[ref.id] \
                or not self.directory:
                raise RuntimeError("not all conditions are met for starting job %s; waitJobs: %s; packet state: %s; directory: %s" % (
                    ref.id, self.waitJobs[ref.id], self.state, self.directory))
            logging.debug("job %s\tstarted", ref.shell)
            with self.lock:
                self.ProcessJobStart(ref)

    def OnDone(self, ref):
        if isinstance(ref, Job):
            logging.debug("job %s\tdone [%s]", ref.shell, ref.Result())
            self.FireEvent("job_done", ref)
            if ref.id in self.jobs and ref.id in self.working:
                with self.lock:
                    nState, nTimeout, isStopped = self.ProcessJobDone(ref)
                if nState:
                    if nState == PacketState.WAITING:
                        self.waitingDeadline = time.time() + nTimeout
                    self.changeState(nState)
        elif isinstance(ref, Tag):
            self.ProcessTagEvent(ref)

    def Add(self, shell, parents, pipe_parents, set_tag, tries,
            max_err_len, retry_delay, pipe_fail, description, notify_timeout, max_working_time, output_to_status):
        if self.state not in (PacketState.CREATED, PacketState.SUSPENDED):
            raise RuntimeError("incorrect state for \"Add\" operation: %s" % self.state)
        with self.lock:
            parents = list(set(p.id for p in parents + pipe_parents))
            pipe_parents = list(p.id for p in pipe_parents)
            job = Job(shell, parents, pipe_parents, self, maxTryCount=tries,
                      limitter=None, max_err_len=max_err_len, retry_delay=retry_delay,
                      pipe_fail=pipe_fail, description=description, notify_timeout=notify_timeout, max_working_time=max_working_time, output_to_status=output_to_status)
            self.jobs[job.id] = job
            if set_tag:
                self.job_done_indicator[job.id] = set_tag
            self.edges[job.id] = []
            for p in parents:
                self.edges[p].append(job.id)
            return job

    def UpdateJobsDependencies(self):
        def visit(startJID):
            st = [[startJID, 0]]
            discovered.add(startJID)

            while st:
                # num - number of neighbours discovered
                jid, num = st[-1]
                adj = self.edges[jid]
                if num < len(adj):
                    st[-1][1] += 1
                    nid = adj[num]
                    self.waitJobs[nid].add(jid)
                    if nid not in discovered:
                        discovered.add(nid)
                        st.append([nid, 0])
                    elif nid not in finished:
                        raise RuntimeError("job dependencies cycle exists")
                else:
                    st.pop()
                    finished.add(jid)

        with self.lock:
            discovered = set()
            finished = set()
            self.waitJobs = dict((jid, set()) for jid in self.jobs if jid not in self.done)
            for jid in self.jobs:
                if jid not in discovered and jid not in self.done:
                    visit(jid)
                    #reset tries count for discovered jobs
            for jid in discovered:
                self.jobs[jid].tries = 0
            self.leafs = set(jid for jid in discovered if not self.waitJobs[jid])

    def Resume(self, resumeWorkable=False):
        allowed_states = [PacketState.CREATED, PacketState.SUSPENDED]
        if resumeWorkable:
            allowed_states.append(PacketState.WORKABLE)
        if self.state in allowed_states and not self.CheckFlag(PacketFlag.USER_SUSPEND):
            self.ClearFlag(~0)
            if len(self.waitTags) != 0:
                self.changeState(PacketState.SUSPENDED)
            else:
                self.UpdateJobsDependencies()
                self.working = set()
                self.changeState(PacketState.WORKABLE)
                if self.leafs:
                    self.changeState(PacketState.PENDING)
                elif len(self.done) == len(self.jobs):
                    self.changeState(PacketState.SUCCESSFULL)

    def Get(self):
        if self.state not in (PacketState.WORKABLE, PacketState.PENDING):
            return self.INCORRECT
        try:
            self.lock.acquire()
            for leaf in self.leafs:
                if self.jobs[leaf].CanStart():
                    self.FireEvent("job_get", self.jobs[leaf])
                    self.leafs.remove(leaf)
                    return self.jobs[leaf]
        finally:
            self.lock.release()
            if not self.leafs:
                self.changeState(PacketState.WORKABLE)

    def IsDone(self):
        return self.state in (PacketState.SUCCESSFULL, PacketState.HISTORIED, PacketState.ERROR)

    def History(self):
        return self.history or []

    def Status(self):
        history = self.History()
        total_time = history[-1][1] - history[0][1]
        wait_time = 0

        for ((state, start_time), (_, end_time)) in zip(history, history[1:] + [("", time.time())]):
            if state in (PacketState.SUSPENDED, PacketState.WAITING):
                wait_time += end_time - start_time

        result_tag = self.done_indicator.name if self.done_indicator else None

        waiting_time = max(int(self.waitingDeadline - time.time()), 0) \
            if self.state == PacketState.WAITING else None

        all_tags = list(getattr(self, 'allTags', []))

        status = dict(name=self.name,
                      state=self.state,
                      wait=list(self.waitTags),
                      all_tags=all_tags,
                      result_tag=result_tag,
                      priority=self.priority,
                      history=history,
                      total_time=total_time,
                      wait_time=wait_time,
                      last_modified=history[-1][1],
                      waiting_time=waiting_time)
        extra_flags = set()
        if self.state == PacketState.ERROR and self.CheckFlag(PacketFlag.RCVR_ERROR):
            extra_flags.add("can't-be-recovered")
        if self.CheckFlag(PacketFlag.USER_SUSPEND):
            extra_flags.add("manually-suspended")
        if extra_flags:
            status["extra_flags"] = ";".join(extra_flags)

        if self.state in (PacketState.ERROR, PacketState.SUSPENDED,
                          PacketState.WORKABLE, PacketState.PENDING,
                          PacketState.SUCCESSFULL, PacketState.WAITING):
            status["jobs"] = []
            for jid, job in self.jobs.iteritems():
                result = job.Result()
                results = []
                if result:
                    results = [safeStringEncode(str(res)) for res in job.results]

                state = "done" if jid in self.done \
                    else "working" if jid in self.working \
                    else "pending" if jid in self.leafs \
                    else "errored" if result and not result.IsSuccessfull() \
                    else "suspended"

                wait_jobs = []
                if self.state == PacketState.WORKABLE:
                    wait_jobs = map(str, self.waitJobs.get(jid, []))

                parents = map(str, job.parents or [])
                pipe_parents = map(str, job.inputs or [])

                output_filename = None
                if getattr(job, 'output', None) and os.path.isfile(job.output.name):
                    output_filename = os.path.basename(job.output.name)


                status["jobs"].append(
                    dict(id=str(job.id),
                         shell=job.shell,
                         desc=job.description,
                         state=state,
                         results=results,
                         parents=parents,
                         pipe_parents=pipe_parents,
                         output_filename=output_filename,
                         wait_jobs=wait_jobs,
                     )
                )
        return status

    def AddBinary(self, binname, file):
        self.AddLink(binname, file)

    def CheckFlag(self, flag):
        return True if (self.flags & flag) else False

    def SetFlag(self, flag):
        self.flags |= flag

    def ClearFlag(self, flag):
        self.flags &= ~flag

    def UserSuspend(self, kill_jobs=False):
        self.SetFlag(PacketFlag.USER_SUSPEND)
        self.Suspend(kill_jobs)

    def Suspend(self, kill_jobs=False):
        self.changeState(PacketState.SUSPENDED)
        if kill_jobs:
            self.KillJobs()

    def UserResume(self):
        self.ClearFlag(PacketFlag.USER_SUSPEND)
        self.Resume()

    def GetWorkingJobs(self):
        with self.lock:
            working_copy = list(self.working)
        for jid in working_copy:
            yield self.jobs[jid]

    def CloseStreams(self):
        try:
            for key, stream in self.streams.iteritems():
                if stream is not None:
                    if isinstance(stream, file):
                        if not stream.closed:
                            try:
                                stream.close()
                            except:
                                pass
        except:
            logging.exception('Close stream error')

    def KillJobs(self):
        self.CloseStreams()
        for job in self.GetWorkingJobs():
            job.Terminate()

    def Reset(self):
        self.changeState(PacketState.NONINITIALIZED)
        self.KillJobs()
        if self.done_indicator:
            self.done_indicator.Unset()
        for job_id in list(self.done):
            tag = self.job_done_indicator.get(job_id)
            if tag:
                tag.Unset()

        self.done.clear()
        for job in self.jobs.values():
            job.results = []
        self.Resume()

    def OnReset(self, (ref, message)):
        if isinstance(ref, Tag):
            self.waitTags.add(ref.GetFullname())
            if self.isResetable:
                if self.state == PacketState.SUCCESSFULL and self.done_indicator:
                    self.done_indicator.Reset(message)
                for job_id in list(self.done):
                    tag = self.job_done_indicator.get(job_id)
                    if tag:
                        tag.Reset(message)
                self.Reset()
            else:
                PacketCustomLogic(self).DoResetNotification(message)


# Hack to restore from old backups (before refcatoring), when JobPacket was in
import job

job.JobPacket = JobPacket
