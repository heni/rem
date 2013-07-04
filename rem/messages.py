import cStringIO, time
from packet import PacketState, PacketFlag


class IMessageHelper(object): pass


def GetHelper(helper_cls, *args):
    if helper_cls:
        assert issubclass(helper_cls, IMessageHelper)
        return helper_cls(*args)


def GetHelperByPacketState(pck, ctx):
    if ctx and ctx.send_emails:
        helper_cls = {PacketState.ERROR: PACKET_ERROR}#, PacketState.SUCCESSFULL: PACKET_SUCCESS}
        return GetHelper(helper_cls.get(pck.state, None), pck, ctx)


def GetEmergencyHelper(pck, ctx):
    if ctx and ctx.send_emails:
        return EmergencyError(pck, ctx)


class PacketExecutionError(IMessageHelper):
    def __init__(self, pck, ctx):
        self.pck = pck
        self.ctx = ctx

    def subject(self):
        reason = "packet recovering error" if self.pck.CheckFlag(PacketFlag.RCVR_ERROR) \
            else "packet execution error"
        return "[REM@%(sname)s] Task '%(pname)s': %(reason)s" % {"pname": self.pck.name, "reason": reason,
                                                                 "sname": self.ctx.network_name}

    def message(self):
        mbuf = cStringIO.StringIO()

        def appendJobItem(job, itemName):
            print >> mbuf, "\t%s: %s" % (itemName, job.get(itemName, "N/A"))

        def appendJobResults(job):
            if job.get("results"):
                print >> mbuf, "\n".join("\tresult: %s" % v for v in job.get("results"))

        print >> mbuf, "Packet '%(pname)s' has been aborted because of some error states" % {"pname": self.pck.name}
        p_state = self.pck.Status()
        jobs = p_state.get("jobs", [])
        for job in jobs:
            if job.get("state") == "errored":
                print >> mbuf, "--== failed subjob ==--"
                appendJobItem(job, "shell")
                appendJobResults(job)
        print >> mbuf, "=" * 80
        print >> mbuf, "Extended packet status:"
        print >> mbuf, "packet id:", self.pck.id
        print >> mbuf, "\n".join("%s: %s" % (k, v) for k, v in p_state.iteritems() if k not in ("jobs", "history"))
        print >> mbuf, "history:"
        for state, timestamp in p_state.get("history", []):
            print >> mbuf, "\t%s: %s" % (time.ctime(timestamp), state)
        for job in jobs:
            print >> mbuf, "--== subjob info ==--"
            for k in sorted(job):
                if k != "results":
                    appendJobItem(job, k)
            appendJobResults(job)
        return mbuf.getvalue()


class PacketExecutionSuccess(IMessageHelper):
    def __init__(self, pck, ctx):
        self.pck = pck
        self.ctx = ctx

    def subject(self):
        return "[REM@%(sname)s] Task '%(pname)s': successfully executed." % {"pname": self.pck.name,
                                                                             "sname": self.ctx.network_name}

    def message(self):
        mbuf = cStringIO.StringIO()
        print >> mbuf, "Packet %(pname)s has successfully executed" % {"pname": self.pck.name}
        print >> mbuf, "Extended packet status:"
        print >> mbuf, "packet id:", self.pck.id
        p_state = self.pck.Status()
        print >> mbuf, "\n".join("%s: %s" % (k, v) for k, v in p_state.iteritems() if k not in ("jobs", "history"))
        print >> mbuf, "history:"
        for state, timestamp in p_state.get("history", []):
            print >> mbuf, "\t%s: %s" % (time.ctime(timestamp), state)
        return mbuf.getvalue()


class EmergencyError(IMessageHelper):
    def __init__(self, pck, ctx):
        self.pck = pck
        self.ctx = ctx

    def subject(self):
        return "[REM@%(sname)s] Task '%(pname)s'(%(pid)s) has been marked to delete by EMERGENCY situation" \
               % {"pname": self.pck.name, "pid": self.pck.id, "sname": self.ctx.network_name}

    def message(self):
        mbuf = cStringIO.StringIO()
        print >> mbuf, "Packet '%(pname)s' has been marked to delete by EMERGENCY situation" % {"pname": self.pck.name}
        print >> mbuf, "Extended packet status:"
        print >> mbuf, "packet id:", self.pck.id
        p_state = self.pck.Status()
        print >> mbuf, "\n".join("%s: %s" % (k, v) for k, v in p_state.iteritems() if k not in ("jobs", "history"))
        print >> mbuf, "history:"
        for state, timestamp in p_state.get("history", []):
            print >> mbuf, "\t%s: %s" % (time.ctime(timestamp), state)
        for job in p_state.get("jobs", []):
            print >> mbuf, "--== subjob info ==--"
            print >> mbuf, "\n".join("\t%s: %s" % (k, v) for k, v in job.iteritems() if k != "results")
            if job.get("results"):
                print >> mbuf, "\n".join("\tresult: %s" % v for v in job.get("results"))
        return mbuf.getvalue()


PACKET_ERROR = PacketExecutionError
PACKET_SUCCESS = PacketExecutionSuccess

