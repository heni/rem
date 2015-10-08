from __future__ import with_statement
import logging
import os
import sys
import time
import weakref
import bsddb3
import cPickle

from common import *
from callbacks import Tag, RemoteTag, CallbackHolder
from journal import TagLogger
from callbacks import ICallbackAcceptor
from packet import PacketState, JobPacket
from Queue import Queue
import fork_locking

__all__ = ["GlobalPacketStorage", "BinaryStorage", "ShortStorage", "TagStorage", "PacketNamesStorage", "MessageStorage"]


class GlobalPacketStorage(object):
    def __init__(self):
        self.box = weakref.WeakValueDictionary()
        self.iteritems = self.box.iteritems

    def add(self, pck):
        self.box[pck.id] = pck

    def update(self, list):
        map(self.add, list)

    def __getitem__(self, item):
        return self.box[item]

    def __setitem__(self, key, value):
        self.box[key] = value

    def keys(self):
        return self.box.keys()

    Add = add

    def GetPacket(self, id):
        return self.box.get(id)


class ShortStorage(Unpickable(packets=(TimedMap.create, {}),
                              lock=PickableLock)):
    PCK_LIFETIME = 1800

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["packets"] = sdict["packets"].copy()
        return getattr(super(ShortStorage, self), "__getstate__", lambda: sdict)()

    def forgetOldItems(self):
        barrierTm = time.time() - self.PCK_LIFETIME
        with self.lock:
            while len(self.packets) > 0:
                pck_id, (tm, pck) = self.packets.peak()
                if tm < barrierTm:
                    pck.ReleasePlace()
                    self.packets.pop(pck.id)
                else:
                    break

    def StorePacket(self, pck):
        with self.lock:
            self.packets.add(pck.id, pck)

    def GetPacket(self, id):
        with self.lock:
            idx = self.packets.revIndex.get(id, None)
            if idx is not None:
                return self.packets.values[idx][1]

    def PickPacket(self, id):
        with self.lock:
            return self.packets.pop(id)[1][1]


class BinaryStorage(Unpickable(files=dict, lifeTime=(int, 3600), binDirectory=str)):
    digest_length = 32

    def __init__(self):
        getattr(super(BinaryStorage, self), "__init__")()

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["files"] = sdict["files"].copy()
        return getattr(super(BinaryStorage, self), "__getstate__", lambda: sdict)()

    @classmethod
    def create(cls, o=None):
        if o is None:
            return cls()
        if isinstance(o, BinaryStorage):
            return o

    def forgetOldItems(self):
        curTime = time.time()
        bad_files = set()
        for checksum, file in self.files.items():
            if file.LinksCount() == 0 and curTime - file.accessTime > self.lifeTime:
                bad_files.add(checksum)
        for checksum in bad_files:
            self.files.pop(checksum).release()

    def UpdateContext(self, context):
        for file in self.files.itervalues():
            file.FixLinks()
        if self.binDirectory != context.binary_directory:
            badFiles = set()
            for checksum, file in self.files.iteritems():
                estimated_path = os.path.join(context.binary_directory, checksum)
                if not os.path.isfile(estimated_path):
                    #if os.path.isfile(file.path):
                    if False:
                        shutil.move(file.path, estimated_path)
                    elif file.LinksCount() > 0:
                        print file.links
                        raise RuntimeError("binstorage\tcan't recover file %r" % file.path)
                    else:
                        badFiles.add(checksum)
                file.Relink(estimated_path)
            if badFiles:
                for checksum in badFiles:
                    del self.files[checksum]
                    logging.warning("binstorage\tnonexisted file %s cleaning attempt", checksum)
                logging.warning("can't recover %d files; %d files left in storage", len(badFiles), len(self.files))
        self.binDirectory = context.binary_directory
        self.lifeTime = context.binary_lifetime

    def GetFileByHash(self, checksum):
        file = self.files.get(checksum, None)
        return file

    def RegisterFile(self, fileObject):
        return self.files.setdefault(fileObject.checksum, fileObject)

    def CreateFile(self, data):
        return self.RegisterFile(BinaryFile.createFile(self.binDirectory, data))

    def CreateFileLocal(self, path, checksum):
        tmpfile = None
        try:
            if not os.path.isfile(path) or not os.access(path, os.R_OK):
                return False

            fd, tmpfile = tempfile.mkstemp(dir=self.binDirectory)
            os.close(fd)
            shutil.copy2(path, tmpfile)

            fileChecksum = BinaryFile.calcFileChecksum(tmpfile)
            if fileChecksum != checksum:
                return False

            remPath = os.path.join(self.binDirectory, checksum)
            os.rename(tmpfile, remPath)
            tmpfile = None
            self.RegisterFile(BinaryFile(remPath, checksum, True))
        finally:
            if tmpfile is not None:
                os.unlink(tmpfile)
        return True

    def HasBinary(self, checksum):
        file = self.GetFileByHash(checksum)
        return file and os.path.isfile(file.path)


class TagWrapper(object):
    __slots__ = ["inner", "__reduce_ex__"]

    def __init__(self, tag):
        self.inner = tag

    def __reduce_ex__(self, proto):
        return TagWrapper, (self.inner, )

    def __getattribute__(self, attr):
        if attr in TagWrapper.__slots__:
            return object.__getattribute__(self, attr)
        return self.inner.__getattribute__(attr)


class TagStorage(object):
    __slots__ = ["db_file", "infile_items", "inmem_items", "lock", "additional_listeners", "conn_manager", "tag_logger", 'db_file_opened']

    def __init__(self, *args):
        self.lock = PickableLock()
        self.inmem_items = {}
        self.infile_items = None
        self.db_file = ""
        self.db_file_opened = False
        self.additional_listeners = set()
        self.tag_logger = TagLogger(self)
        if len(args) == 1:
            if isinstance(args[0], dict):
                self.inmem_items = args[0]
            elif isinstance(args[0], TagStorage):
                self.inmem_items = args[0].inmem_items
                self.infile_items = args[0].infile_items
                self.db_file = args[0].db_file

    def __reduce__(self):
        return TagStorage, (self.inmem_items.copy(), )

    def SetTag(self, tagname):
        self.AcquireTag(tagname).Set()

    def SetRemoteTag(self, tagname):
        tag = self.AcquireTag(tagname)
        if not isinstance(tag, RemoteTag):
            logging.error("Expected RemoteTag, got %r", tag)
            return
        tag.SetRemote()

    def UnsetTag(self, tagname):
        self.AcquireTag(tagname).Unset()

    def ResetTag(self, tagname, message):
        self.AcquireTag(tagname).Reset(message)

    def CheckTag(self, tagname):
        return self.RawTag(tagname).IsSet()

    def IsRemoteName(self, tagname):
        return ':' in tagname

    def AcquireTag(self, tagname):
        if tagname:
            tag = self.RawTag(tagname)
            with self.lock:
                return TagWrapper(self.inmem_items.setdefault(tagname, tag))

    def RawTag(self, tagname):
        if tagname:
            tag = self.inmem_items.get(tagname, None)
            if tag is None:
                if not self.db_file_opened:
                    self.DBConnect()
                tagDescr = self.infile_items.get(tagname, None)
                if tagDescr:
                    tag = cPickle.loads(tagDescr)
                else:
                    tag = RemoteTag(tagname) if self.IsRemoteName(tagname) else Tag(tagname)
            for obj in self.additional_listeners:
                tag.AddNonpersistentCallbackListener(obj)
            return tag

    def ListTags(self, name_regex=None, prefix=None, memory_only=True):
        for name, tag in self.inmem_items.items():
            if name and (not prefix or name.startswith(prefix)) \
                and (not name_regex or name_regex.match(name)):
                yield name, tag.IsSet()
        if memory_only:
            return
        inner_db = bsddb3.btopen(self.db_file, "r")
        try:
            name, tagDescr = inner_db.set_location(prefix) if prefix else inner_db.first()
            while True:
                if prefix and not name.startswith(prefix):
                    break
                if not name_regex or name_regex.match(name):
                    yield name, cPickle.loads(tagDescr).IsSet()
                name, tagDescr = inner_db.next()
        except bsddb3._pybsddb.DBNotFoundError:
            pass
        inner_db.close()

    def DBConnect(self):
        self.infile_items = bsddb3.btopen(self.db_file, "c")
        self.db_file_opened = True

    def UpdateContext(self, context):
        self.db_file = context.tags_db_file
        self.DBConnect()
        self.conn_manager = context.Scheduler.connManager
        self.tag_logger.UpdateContext(context)
        self.additional_listeners = set()
        self.additional_listeners.add(context.Scheduler.connManager)
        self.additional_listeners.add(self.tag_logger)

    def Restore(self, timestamp):
        self.tag_logger.Restore(timestamp)

    def ListDependentPackets(self, tag_name):
        return self.RawTag(tag_name).GetListenersIds()

    def tofileOldItems(self):
        old_tags = set()
        for name, tag in self.inmem_items.items():
            #tag for removing have no listeners and have no external links for himself (actualy 4 links)
            if tag.GetListenersNumber() == 0 and sys.getrefcount(tag) == 4:
                old_tags.add(name)
        if not self.db_file_opened:
            with self.lock:
                self.DBConnect()
        with self.lock:
            for name in old_tags:
                tag = self.inmem_items.pop(name)
                tag.callbacks.clear()
                try:
                    self.infile_items[name] = cPickle.dumps(tag)
                except bsddb3.error as e:
                    if 'BSDDB object has already been closed' in e.message:
                        self.db_file_opened = False
                        self.db_file = None
                    raise
            self.infile_items.sync()


class PacketNamesStorage(ICallbackAcceptor):
    def __init__(self, *args, **kwargs):
        self.names = set(kwargs.get('names_list', []))
        self.lock = fork_locking.Lock()

    def __getstate__(self):
        return {}

    def Add(self, pck_name):
        with self.lock:
            self.names.add(pck_name)

    def Update(self, names_list=None):
        with self.lock:
            self.names.update(names_list or [])

    def Exist(self, pck_name):
        return pck_name in self.names

    def Delete(self, pck_name):
        with self.lock:
            if pck_name in self.names:
                self.names.remove(pck_name)

    def OnChange(self, packet_ref):
        if isinstance(packet_ref, JobPacket) and packet_ref.state == PacketState.HISTORIED:
            self.Delete(packet_ref.name)

    def OnJobDone(self, job_ref):
        pass

    def OnJobGet(self, job_ref):
        pass

    def OnPacketReinitRequest(self, pck):
        pass

class MessageStorage(object):
    pass
