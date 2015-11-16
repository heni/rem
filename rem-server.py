#!/usr/bin/env python
from __future__ import with_statement
import logging
import os
import re
import select
import signal
import socket
import time
import threading
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
from SocketServer import ThreadingMixIn
import Queue as StdQueue
import xmlrpclib
import datetime

from rem import constants, osspec
from rem import traced_rpc_method
from rem import CheckEmailAddress, DefaultContext, JobPacket, PacketState, Scheduler, ThreadJobWorker, TimeTicker, XMLRPCWorker

class DuplicatePackageNameException(Exception):
    def __init__(self, pck_name, serv_name, *args, **kwargs):
        super(DuplicatePackageNameException, self).__init__(*args, **kwargs)
        self.message = 'DuplicatePackageNameException: Packet with name %s already exists in REM[%s]' % (pck_name, serv_name)


class AsyncXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    def __init__(self, poolsize, *args, **kws):
        if socket.has_ipv6:
            self.address_family = socket.AF_INET6
        SimpleXMLRPCServer.__init__(self, *args, **kws)
        self.poolsize = poolsize
        self.requests = StdQueue.Queue(poolsize)

    def handle_request(self):
        try:
            request = self.get_request()
        except socket.error:
            logging.error("XMLRPCServer: socket error")
            return
        if self.verify_request(*request):
            self.requests.put(request)


class AuthRequestHandler(SimpleXMLRPCRequestHandler):
    def _dispatch(self, method, params):
        func = self.server.funcs.get(method)
        if not func:
            raise Exception('method "%s" is not supported' % method)
        username = self.headers.get("X-Username", "Unknown")
        log_level = getattr(func, "log_level", None)
        log_func = getattr(logging, log_level, None) if log_level else None
        if callable(log_func):
            log_func("RPC method (user: %s, host: %s): %s %r", username, self.address_string(), method, params)
        return func(*params)


_scheduler = None
_context = None

def bind1st(f, arg):
    return lambda *args, **kwargs: f(arg, *args, **kwargs)

def CreateScheduler(context, canBeClear=False, restorer=None):
    sched = Scheduler(context)
    wasRestoreTry = False
    if restorer:
        restorer = bind1st(restorer, sched)
    if os.path.isdir(context.backup_directory):
        for name in sorted(os.listdir(context.backup_directory), reverse=True):
            if sched.CheckBackupFilename(name):
                backupFile = os.path.join(context.backup_directory, name)
                try:
                    sched.LoadBackup(backupFile, restorer)
                    return sched
                except Exception, e:
                    logging.exception("can't restore from file \"%s\" : %s", backupFile, e)
                    wasRestoreTry = True
    if wasRestoreTry and not canBeClear:
        raise RuntimeError("can't restore from backup")
    sched.tagRef.Restore(0)
    return sched


def readonly_method(func):
    func.readonly_method = True
    return func


@traced_rpc_method("info")
def create_packet(packet_name, priority, notify_emails, wait_tagnames, set_tag, kill_all_jobs_on_error=True, packet_name_policy=constants.DEFAULT_DUPLICATE_NAMES_POLICY, resetable=True):
    if packet_name_policy & constants.DENY_DUPLICATE_NAMES_POLICY and _scheduler.packetNamesTracker.Exist(packet_name):
        ex = DuplicatePackageNameException(packet_name, _context.network_name)
        raise xmlrpclib.Fault(1, ex.message)
    if notify_emails is not None:
        assert isinstance(notify_emails, list), "notify_emails must be list or None"
        for email in notify_emails:
            assert CheckEmailAddress(email), "incorrect e-mail: " + email
    wait_tags = [_scheduler.tagRef.AcquireTag(tagname) for tagname in wait_tagnames]
    pck = JobPacket(packet_name, priority, _context, notify_emails,
                    wait_tags=wait_tags, set_tag=_scheduler.tagRef.AcquireTag(set_tag),
                    kill_all_jobs_on_error=kill_all_jobs_on_error, isResetable=resetable)
    _scheduler.RegisterNewPacket(pck, wait_tags)
    logging.info('packet %s registered as %s', packet_name, pck.id)
    return pck.id


@traced_rpc_method()
def pck_add_job(pck_id, shell, parents, pipe_parents, set_tag, tries,
                max_err_len=None, retry_delay=None, pipe_fail=False, description="", notify_timeout=constants.NOTIFICATION_TIMEOUT, max_working_time=constants.KILL_JOB_DEFAULT_TIMEOUT, output_to_status=False):
    pck = _scheduler.tempStorage.GetPacket(pck_id)
    if pck is not None:
        if isinstance(shell, unicode):
            shell = shell.encode('utf-8')
        parents = [pck.jobs[int(jid)] for jid in parents]
        pipe_parents = [pck.jobs[int(jid)] for jid in pipe_parents]
        job = pck.Add(shell, parents, pipe_parents, _scheduler.tagRef.AcquireTag(set_tag), tries, \
                      max_err_len, retry_delay, pipe_fail, description, notify_timeout, max_working_time, output_to_status)
        return str(job.id)
    raise AttributeError("nonexisted packet id: %s" % pck_id)


@traced_rpc_method("info")
def pck_addto_queue(pck_id, queue_name, packet_name_policy=constants.IGNORE_DUPLICATE_NAMES_POLICY):
    pck = _scheduler.tempStorage.PickPacket(pck_id)
    packet_name = pck.name
    if packet_name_policy & (constants.DENY_DUPLICATE_NAMES_POLICY | constants.WARN_DUPLICATE_NAMES_POLICY) and _scheduler.packetNamesTracker.Exist(packet_name):
        ex = DuplicatePackageNameException(packet_name, _context.network_name)
        raise xmlrpclib.Fault(1, ex.message)
    if pck is not None:
        _scheduler.AddPacketToQueue(queue_name, pck)
        return
    raise AttributeError("nonexisted packet id: %s" % pck_id)


@traced_rpc_method("info")
def pck_moveto_queue(pck_id, src_queue, dst_queue):
    pck = _scheduler.GetPacket(pck_id)
    if pck is not None:
        if pck.state not in (PacketState.CREATED, PacketState.SUSPENDED, PacketState.ERROR):
            raise RuntimeError("can't move \"live\" packet between queues")
        _scheduler.Queue(src_queue).Remove(pck)
        _scheduler.Queue(dst_queue).Add(pck)
        return
    raise AttributeError("nonexisted packet id: %s" % pck_id)


@readonly_method
@traced_rpc_method()
def check_tag(tagname):
    return _scheduler.tagRef.CheckTag(tagname)


@traced_rpc_method("info")
def set_tag(tagname):
    return _scheduler.tagRef.SetTag(tagname)


@traced_rpc_method("info")
def unset_tag(tagname):
    return _scheduler.tagRef.UnsetTag(tagname)


@traced_rpc_method()
def reset_tag(tagname, message=""):
    tag = _scheduler.tagRef.AcquireTag(tagname)
    tag.Reset(message)


@readonly_method
@traced_rpc_method()
def get_dependent_packets_for_tag(tagname):
    return _scheduler.tagRef.ListDependentPackets(tagname)


@traced_rpc_method("info")
def queue_suspend(queue_name):
    _scheduler.Queue(queue_name).Suspend()


@traced_rpc_method("info")
def queue_resume(queue_name):
    _scheduler.Queue(queue_name).Resume()


@readonly_method
@traced_rpc_method()
def queue_status(queue_name):
    q = _scheduler.Queue(queue_name, create=False)
    return q.Status()


@readonly_method
@traced_rpc_method()
def queue_list(queue_name, filter, name_regex=None, prefix=None):
    name_regex = name_regex and re.compile(name_regex)
    q = _scheduler.Queue(queue_name, create=False)
    return [pck.id for pck in q.ListPackets(filter=filter, name_regex=name_regex, prefix=prefix)]


@readonly_method
@traced_rpc_method()
def queue_list_updated(queue_name, last_modified, filter=None):
    q = _scheduler.Queue(queue_name, create=False)
    return [pck.id for pck in q.ListPackets(last_modified=last_modified, filter=filter)]


@traced_rpc_method("info")
def queue_change_limit(queue_name, limit):
    _scheduler.Queue(queue_name).ChangeWorkingLimit(limit)


@traced_rpc_method("info")
def queue_delete(queue_name):
    return _scheduler.DeleteUnusedQueue(queue_name)


@readonly_method
@traced_rpc_method()
def list_tags(name_regex=None, prefix=None, memory_only=True):
    name_regex = name_regex and re.compile(name_regex)
    return list(set(_scheduler.tagRef.ListTags(name_regex, prefix, memory_only)))


@readonly_method
@traced_rpc_method()
def list_queues(name_regex=None, prefix=None, *args):
    name_regex = name_regex and re.compile(name_regex)
    return [(q.name, q.Status()) for q in _scheduler.qRef.itervalues()
            if (not name_regex or name_regex.match(q.name)) and \
               (not prefix or q.name.startswith(prefix))]


@readonly_method
@traced_rpc_method()
def pck_status(pck_id):
    pck = _scheduler.GetPacket(pck_id) or _scheduler.tempStorage.GetPacket(pck_id)
    if pck is not None:
        return pck.Status()
    raise AttributeError("nonexisted packet id: %s" % pck_id)


@traced_rpc_method("info")
def pck_suspend(pck_id, kill_jobs=False):
    pck = _scheduler.GetPacket(pck_id)
    if pck is not None:
        return pck.UserSuspend(kill_jobs)
    raise AttributeError("nonexisted packet id: %s" % pck_id)


@traced_rpc_method("info")
def pck_resume(pck_id):
    pck = _scheduler.GetPacket(pck_id)
    if pck is not None:
        return pck.UserResume()
    raise AttributeError("nonexisted packet id: %s" % pck_id)


@traced_rpc_method("info")
def pck_delete(pck_id):
    pck = _scheduler.GetPacket(pck_id)
    if pck is not None:
        if not pck.canChangeState(PacketState.HISTORIED):
            raise AssertionError("couldn't delete packet '%s' stated as '%s'" % (pck_id, pck.state))
        return pck.changeState(PacketState.HISTORIED)
    raise AttributeError("nonexisted packet id: %s" % pck_id)


@traced_rpc_method("info")
def pck_reset(pck_id):
    pck = _scheduler.GetPacket(pck_id)
    if pck is not None:
        wait_tags = [_scheduler.tagRef.AcquireTag(tagname) for tagname in pck.waitTags]
        result = pck.Reset()
        pck.SetWaitingTags(wait_tags)
        return result
    raise AttributeError("nonexisted packet id: %s" % pck_id)


@traced_rpc_method()
def check_binary_exist(checksum):
    return _scheduler.binStorage.HasBinary(checksum)


@traced_rpc_method("info")
def save_binary(bindata):
    _scheduler.binStorage.CreateFile(bindata.data)


@traced_rpc_method("info")
def check_binary_and_lock(checksum, localPath, tryLock=None):
    if tryLock is None:
        return _scheduler.binStorage.HasBinary(checksum) \
            or _scheduler.binStorage.CreateFileLocal(localPath, checksum)
    else:
        raise NotImplementedError('tryLock==True branch is not implemented yet!')


@traced_rpc_method()
def pck_add_binary(pck_id, binname, checksum):
    pck = _scheduler.tempStorage.GetPacket(pck_id) or _scheduler.GetPacket(pck_id)
    file = _scheduler.binStorage.GetFileByHash(checksum)
    if pck is not None and file is not None:
        pck.AddBinary(binname, file)
        return
    raise AttributeError("nonexisted packet id: %s" % pck_id)


@readonly_method
@traced_rpc_method()
def pck_list_files(pck_id):
    pck = _scheduler.GetPacket(pck_id)
    if pck is not None:
        files = pck.ListFiles()
        return files
    raise AttributeError("nonexisted packet id: %s" % pck_id)


@readonly_method
@traced_rpc_method()
def pck_get_file(pck_id, filename):
    pck = _scheduler.GetPacket(pck_id)
    if pck is not None:
        file = pck.GetFile(filename)
        return xmlrpclib.Binary(file)
    raise AttributeError("nonexisted packet id: %s" % pck_id)


@traced_rpc_method()
def queue_set_success_lifetime(queue_name, lifetime):
    q = _scheduler.Queue(queue_name, create=False)
    q.SetSuccessLifeTime(lifetime)


@traced_rpc_method()
def queue_set_error_lifetime(queue_name, lifetime):
    q = _scheduler.Queue(queue_name, create=False)
    q.SetErroredLifeTime(lifetime)


@traced_rpc_method("warning")
def set_backupable_state(bckpFlag, chldFlag=None):
    if bckpFlag is not None:
        if bckpFlag:
            _scheduler.ResumeBackups()
        else:
            _scheduler.SuspendBackups()
    if chldFlag is not None:
        if chldFlag:
            _scheduler.EnableBackupsInChild()
        else:
            _scheduler.DisableBackupsInChild()


@traced_rpc_method()
def get_backupable_state():
    return {"backup-flag": _scheduler.backupable, "child-flag": _scheduler.backupInChild}


@traced_rpc_method("warning")
def do_backup():
    return _scheduler.RollBackup(force=True, child_max_working_time=None)

class RemServer(object):
    def __init__(self, port, poolsize, scheduler, allow_backup_method=False, readonly=False):
        self.scheduler = scheduler
        self.readonly = readonly
        self.allow_backup_method = allow_backup_method
        self.rpcserver = AsyncXMLRPCServer(poolsize, ("", port), AuthRequestHandler, allow_none=True)
        self.rpcserver.register_multicall_functions()
        self.register_all_functions()

    def _non_readonly_func_stub(self, name):
        def stub(*args, **kwargs):
            raise NotImplementedError('Function %s is not available in readonly interface' % name)

        return stub

    def register_function(self, func, name):
        if self.readonly:
            is_readonly_method = getattr(func, 'readonly_method', False)
            if not is_readonly_method:
                self.rpcserver.register_function(self._non_readonly_func_stub(name), name)
                return
        self.rpcserver.register_function(func, name)

    def register_all_functions(self):
        self.register_function(create_packet, "create_packet")
        self.register_function(pck_add_job, "pck_add_job")
        self.register_function(pck_addto_queue, "pck_addto_queue")
        self.register_function(pck_moveto_queue, "pck_moveto_queue")
        self.register_function(check_tag, "check_tag")
        self.register_function(set_tag, "set_tag")
        self.register_function(unset_tag, "unset_tag")
        self.register_function(reset_tag, "reset_tag")
        self.register_function(get_dependent_packets_for_tag, "get_dependent_packets_for_tag")
        self.register_function(queue_suspend, "queue_suspend")
        self.register_function(queue_resume, "queue_resume")
        self.register_function(queue_status, "queue_status")
        self.register_function(queue_list, "queue_list")
        self.register_function(queue_list_updated, "queue_list_updated")
        self.register_function(queue_change_limit, "queue_change_limit")
        self.register_function(queue_delete, "queue_delete")
        self.register_function(list_tags, "list_tags")
        self.register_function(list_queues, "list_queues")
        self.register_function(pck_status, "pck_status")
        self.register_function(pck_suspend, "pck_suspend")
        self.register_function(pck_resume, "pck_resume")
        self.register_function(pck_delete, "pck_delete")
        self.register_function(pck_reset, "pck_reset")
        self.register_function(check_binary_exist, "check_binary_exist")
        self.register_function(save_binary, "save_binary")
        self.register_function(check_binary_and_lock, "check_binary_and_lock")
        self.register_function(pck_add_binary, "pck_add_binary")
        self.register_function(pck_list_files, "pck_list_files")
        self.register_function(pck_get_file, "pck_get_file")
        self.register_function(queue_set_success_lifetime, "queue_set_success_lifetime")
        self.register_function(queue_set_error_lifetime, "queue_set_error_lifetime")
        self.register_function(set_backupable_state, "set_backupable_state")
        self.register_function(get_backupable_state, "get_backupable_state")
        if self.allow_backup_method:
            self.register_function(do_backup, "do_backup")

    def request_processor(self):
        rpc_fd = self.rpcserver.fileno()
        while self.alive:
            rout, _, _ = select.select((rpc_fd,), (), (), 0.01)
            if rpc_fd in rout:
                self.rpcserver.handle_request()

    def start(self):
        self.xmlrpcworkers = [XMLRPCWorker(self.rpcserver.requests, self.rpcserver.process_request_thread)
                              for _ in xrange(self.rpcserver.poolsize)]
        self.alive = True
        self.main_thread = threading.Thread(target=self.request_processor)
        for worker in self.xmlrpcworkers:
            worker.start()
        self.main_thread.start()

    def stop(self):
        self.alive = False
        map(lambda worker: worker.Kill(), self.xmlrpcworkers)


class RemDaemon(object):
    def __init__(self, scheduler, context):
        self.scheduler = scheduler
        self.api_servers = [
            RemServer(context.manager_port, context.xmlrpc_pool_size, scheduler,
                      allow_backup_method=context.allow_backup_rpc_method)
        ]
        if context.manager_readonly_port:
            self.api_servers.append(RemServer(context.manager_readonly_port,
                                              context.readonly_xmlrpc_pool_size,
                                              scheduler,
                                              allow_backup_method=context.allow_backup_rpc_method,
                                              readonly=True))
        self.regWorkers = []
        self.timeWorker = None

    def process_backups(self):
        nextBackupTime = time.time() + self.scheduler.backupPeriod
        while self.scheduler.alive:
            if time.time() >= nextBackupTime:
                try:
                    self.scheduler.RollBackup()
                except:
                    pass

                nextBackupTime = time.time() + self.scheduler.backupPeriod

                logging.debug("rem-server\tnext backup time: %s" \
                    % datetime.datetime.fromtimestamp(nextBackupTime).strftime('%H:%M'))

            time.sleep(max(self.scheduler.backupPeriod, 0.01))

    def signal_handler(self, signum, frame):
        logging.warning("rem-server\tsignal %s has gotten", signum)
        if self.scheduler.alive:
            self.permitFinalBackup = False
            for server in self.api_servers:
                server.stop()
            if self.timeWorker:
                self.timeWorker.Kill()
            self.scheduler.Stop()
            for method in [ThreadJobWorker.Suspend, ThreadJobWorker.Kill, ThreadJobWorker.join]:
                for worker in self.regWorkers:
                    method(worker)
            self.permitFinalBackup = True
            import multiprocessing
            logging.debug("%s children founded after custom kill", len(multiprocessing.active_children()))
            for proc in multiprocessing.active_children():
                proc.terminate()

        else:
            logging.warning("rem-server\talredy dead scheduler, wait for a minute")

    def start_workers(self):
        self.permitFinalBackup = False
        self.scheduler.Start()
        self.regWorkers = [ThreadJobWorker(self.scheduler) for _ in xrange(self.scheduler.poolSize)]
        self.timeWorker = TimeTicker()
        self.timeWorker.AddCallbackListener(self.scheduler.schedWatcher)
        for worker in self.regWorkers + [self.timeWorker]:
            worker.start()

    def start(self):
        osspec.reg_signal_handler(signal.SIGINT, self.signal_handler)
        osspec.reg_signal_handler(signal.SIGTERM, self.signal_handler)

        self.start_workers()

        for server in self.api_servers:
            server.start()

        logging.debug("rem-server\tall_started")

        self.process_backups()

        while not self.permitFinalBackup:
            time.sleep(0.01)

        self.scheduler.RollBackup()


def scheduler_test():
    def tag_listeners_stats(tagRef):
        tag_listeners = {}
        for tag in tagRef.inmem_items.itervalues():
            listenCnt = tag.GetListenersNumber()
            tag_listeners[listenCnt] = tag_listeners.get(listenCnt, 0) + 1
        return tag_listeners

    def print_tags(sc):
        for tagname, tagvalue in sc.tagRef.ListTags():
            if tagvalue: print "tag: [{0}]".format(tagname)

    print_tags(_scheduler)
    qname = "userdata"
    for q_stat in list_queues():
        print q_stat
    pendingLength = workedLength = suspendLength = 0
    if qname in _scheduler.qRef:
        pendingLength = len(_scheduler.qRef[qname].pending)
        workedLength = len(_scheduler.qRef[qname].worked)
        suspendLength = len(_scheduler.qRef[qname].suspended)
        for pck_id in queue_list(qname, "waiting"):
            pck_suspend(pck_id)
            pck_resume(pck_id)
    print "tags listeners statistics: %s" % tag_listeners_stats(_scheduler.tagRef)

    #serialize all data to data.bin file
    stTime = time.time()
    _scheduler.SaveBackup("data.bin")
    print "serialize time: %.3f" % (time.time() - stTime)

    #print memory usage statistics
    try:
        import guppy

        mem = guppy.hpy()
        print mem.heap()
    except:
        logging.exception("guppy error")
    #deserialize backward attempt
    stTime = time.time()
    tmpContext = DefaultContext("copy")
    sc = Scheduler(tmpContext)
    sc.LoadBackup("data.bin")
    if qname in sc.qRef:
        print "PENDING: %s => %s" % (pendingLength, len(sc.qRef[qname].pending))
        print "WORKED: %s => %s" % (workedLength, len(sc.qRef[qname].worked))
        print "SUSPEND: %s => %s" % (suspendLength, len(sc.qRef[qname].suspended))
        print "deserialize time: %.3f" % (time.time() - stTime)
    print "tags listeners statistics: %s" % tag_listeners_stats(sc.tagRef)
    print "scheduled tasks: ", sc.schedWatcher.tasks.qsize(), sc.schedWatcher.workingQueue.qsize()
    while not sc.schedWatcher.tasks.empty():
        runner, runtm = sc.schedWatcher.tasks.get()
        print runtm, runner


if __name__ == "__main__":
    _context = DefaultContext()
    osspec.set_process_title("[remd]%s" % ((" at " + _context.network_name) if _context.network_name else ""))
    _scheduler = CreateScheduler(_context)
    if _context.execMode == "test":
        scheduler_test()
    elif _context.execMode == "start":
        RemDaemon(_scheduler, _context).start()
