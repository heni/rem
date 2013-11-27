import weakref
import logging
import itertools
from common import *


class ICallbackAcceptor(object):
    def AcceptCallback(self, reference, event):
        methName = "On" + event.title().replace("_", "")
        fn = getattr(self, methName, None)
        if callable(fn):
            fn(reference)
        else:
            logging.warning("can't invoke %s method for object %s", methName, self)


class CallbackHolder(Unpickable(callbacks=weakref.WeakKeyDictionary,
                                nonpersistent_callbacks=weakref.WeakKeyDictionary)):
    def AddCallbackListener(self, obj):
        if not isinstance(obj, ICallbackAcceptor):
            raise RuntimeError("callback %r\tcan't use object %r as acceptor" % (self, obj))
        self.callbacks[obj] = 1

    def AddNonpersistentCallbackListener(self, obj):
        if not isinstance(obj, ICallbackAcceptor):
            raise RuntimeError("callback %r\tcan't use object %r as acceptor" % (self, obj))
        self.nonpersistent_callbacks[obj] = 1

    def DropCallbackListener(self, obj):
        if obj in self.callbacks:
            del self.callbacks[obj]
        if obj in self.nonpersistent_callbacks:
            del self.nonpersistent_callbacks[obj]

    def FireEvent(self, event, reference=None, allow_defferred=True):
        bad_listeners = set()
        scheduler = self.message_queue.scheduler if hasattr(self, 'message_queue') and hasattr(self.message_queue, 'scheduler') else None
        for obj in itertools.chain(self.callbacks.keyrefs(), self.nonpersistent_callbacks.keyrefs()):
            if isinstance(obj(), ICallbackAcceptor):
                if scheduler is not None and scheduler.IsFrozen():
                    if not allow_defferred:
                        scheduler.WaitUnfreeze()
                    else:
                        self.message_queue.StoreMessage(acceptor=obj, event=event, ref=reference or self)
                        continue
                obj().AcceptCallback(reference or self, event)
            else:
                logging.warning("callback %r\tincorrect acceptor found: %s", self, obj())
                bad_listeners.add(obj())
        for obj in bad_listeners:
            self.DropCallbackListener(obj)

    def GetListenersNumber(self):
        return len(self.callbacks)

    def __getstate__(self):
        sdict = self.__dict__.copy()
        callbacks = dict(sdict.pop("callbacks"))
        if callbacks:
            sdict["callbacks"] = callbacks
        del sdict["nonpersistent_callbacks"]
        return sdict


class Tag(CallbackHolder):
    def __init__(self, tagname):
        CallbackHolder.__init__(self)
        self.done = False
        self.name = tagname

    def Set(self):
        logging.debug("tag %s\tset", self.name)
        self.done = True
        self.FireEvent("done")

    def IsSet(self):
        return self.done

    def Unset(self):
        """unset function without event firing"""
        logging.debug("tag %s\tunset", self.name)
        self.done = False
        self.FireEvent("undone")

    def Reset(self):
        logging.debug("tag %s\treset", self.name)
        self.done = False
        self.FireEvent("reset")

    def GetName(self):
        return self.name

    def GetFullname(self):
        return self.name

    def IsRemote(self):
        return False

    def GetListenersIds(self):
        return [k.id for k in self.callbacks.iterkeys()]


class RemoteTag(Tag):
    def __init__(self, tagname):
        Tag.__init__(self, tagname)
        self.remotehost, self.name = tagname.split(":")

    def Set(self):
        raise RuntimeError("Attempt to set RemoteTag %r", self)

    def Reset(self):
        raise RuntimeError("Attempt to reset RemoteTag %r", self)

    def SetRemote(self):
        return super(RemoteTag, self).Set()

    def GetRemoteHost(self):
        return self.remotehost

    def GetName(self):
        return self.name

    def GetFullname(self):
        return ':'.join((self.remotehost, self.name))

    def IsRemote(self):
        return True


"""Unpickler helper"""


def tagset(st=None):
    return set((v.name if isinstance(v, Tag) else v) for v in st) if st else set()
