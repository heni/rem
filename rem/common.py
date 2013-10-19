from __future__ import with_statement
import bisect
import copy
import hashlib
import logging
import os
import shutil
import tempfile
import threading
import time
import types
import re
import xmlrpclib

from heap import PriorityQueue
import osspec


def logged(func):
    def f(*args):
        MAX_LEN = 100
        argStr = str(args)
        if len(argStr) > MAX_LEN:
            argStr = argStr[:MAX_LEN]
        logging.debug("function \"%s(%s)\" started", func.func_name, argStr)
        res = func(*args)
        logging.debug("function \"%s(%s)\" finished", func.func_name, argStr)
        return res

    return f


def traced_rpc_method(level="debug"):
    log_method = getattr(logging, level)
    assert callable(log_method)

    def traced_rpc_method(func):
        def f(*args):
            try:
                return func(*args)
            except:
                logging.exception("")
                raise

        f.log_level = level
        return f

    return traced_rpc_method


class FakeObjectRegistrator(object):
    def register(self, obj, sdict):
        pass

    def LogStats(self):
        pass


class ObjectRegistrator(object):
    TOP_SZ = 10

    def __init__(self):
        self.sum_size = 0
        self.max_objects = []
        self.count = 0
        self.szCache = {}
        self.tpCache = {}

    def register(self, obj, sdict):
        self.szCache[id(obj)] = fullSz = sum(
            len(k) + self.szCache.get(id(obj), object.__sizeof__(obj)) for k, obj in sdict.iteritems())
        smallSz = object.__sizeof__(sdict)
        self.sum_size += smallSz
        self.count += 1
        bisect.insort(self.max_objects, (fullSz, repr(obj)))
        if len(self.max_objects) >= self.TOP_SZ:
            self.max_objects.pop(0)
        tp = type(obj).__name__
        self.tpCache.setdefault(tp, [0, 0])
        self.tpCache[tp][0] += 1
        self.tpCache[tp][1] += smallSz

    def LogStats(self):
        logging.debug("summary deserialization objects size: %s(%s objects)\nmore huge objects: %s\nby types: %s",
                      self.sum_size, self.count, self.max_objects, self.tpCache)


ObjectRegistrator_ = FakeObjectRegistrator()
#ObjectRegistrator_ = ObjectRegistrator()


def Unpickable(**kws):
    class ObjBuilder(object):
        def __init__(self, desc):
            if callable(desc):
                self.fn = desc
                self.defargs = ()
            elif isinstance(desc, (tuple, list)):
                self.fn = desc[0]
                self.defargs = desc[1] if len(desc) == 2 and isinstance(desc[1], tuple) else desc[1:]
            else:
                raise RuntimeError("incorrect unpickle plan: %r" % desc)

        def __call__(self, *args):
            if args:
                return self.fn(*args)
            return self.fn(*self.defargs)

    class ObjUnpickler(object):
        def __setstate__(self, sdict):
            for attr, builder in scheme.iteritems():
                try:
                    if attr in sdict:
                        sdict[attr] = builder(sdict[attr])
                    else:
                        sdict[attr] = builder()
                except:
                    logging.exception("unpickable\tcan't deserialize attribute %s with builder %r", attr, builder)
                    raise
            setter = getattr(super(ObjUnpickler, self), "__setstate__", self.__dict__.update)
            setter(sdict)
            ObjectRegistrator_.register(self, sdict)

        def __init__(self, obj=None):
            if obj is not None:
                self.__dict__ = obj.__dict__.copy()
            else:
                for attr, builder in scheme.iteritems():
                    setattr(self, attr, builder())
            getattr(super(ObjUnpickler, self), "__init__")()

    scheme = dict((attr, ObjBuilder(desc)) for attr, desc in kws.iteritems())
    return ObjUnpickler


def Pickable(fields_to_copy):
    class PickableClass(object):
        def __getstate__(self):
            sdict = self.__dict__.copy()
            for k in fields_to_copy:
                if isinstance(sdict[k], (dict, set)):
                    sdict[k] = sdict[k].copy()
                elif isinstance(sdict[k], list):
                    sdict[k] = sdict[k][:]
            return sdict

    return PickableClass


"""set of unpickable helpers"""


def runtime_object(init_value):
    """object with value equal to init_value after each deserialization (for living at run-time objects)"""

    def _constructor(*args):
        return copy.deepcopy(init_value)

    return _constructor


def emptyset(*args):
    return set()


def zeroint(*args):
    return int()

def safeint(oth=None):
    if not isinstance(oth, int):
        return int()
    return int(oth)


class nullobject(object):
    __instance = None

    def __new__(cls, *args):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls, *args)
        return cls.__instance

    def __init__(self, *args):
        pass


class PickableLock(Unpickable(_object=threading.Lock)):
    @classmethod
    def create(cls, o=None):
        if isinstance(o, cls):
            return o
        return cls()

    def __getattr__(self, attrname):
        return getattr(self._object, attrname)

    def __enter__(self):
        return self._object.__enter__()

    def __exit__(self, *args):
        return self._object.__exit__(*args)

    def __getstate__(self):
        return {}


class PickableRLock(Unpickable(_object=threading.RLock)):
    @classmethod
    def create(cls, o=None):
        if isinstance(o, cls):
            return o
        return cls()

    def __getattr__(self, attrname):
        return getattr(self._object, attrname)

    def __enter__(self):
        return self._object.__enter__()

    def __exit__(self, *args):
        return self._object.__exit__(*args)

    def __getstate__(self):
        return {}


"""Legacy structs for correct deserialization from old backups"""


class PickableLocker(object): pass


NullObject = nullobject

"""Usefull sets based on PriorityQueue"""


class TimedSet(PriorityQueue, Unpickable(lock=PickableLock)):
    def __init__(self):
        super(TimedSet, self).__init__()

    @classmethod
    def create(cls, list=None):
        if isinstance(list, cls):
            return list
        obj = cls()
        map(obj.add, list or [])
        return obj

    def add(self, obj, tm=None):
        if isinstance(obj, tuple):
            obj, tm = obj
        if obj not in self:
            PriorityQueue.add(self, obj, tm or time.time())

    def remove(self, obj):
        return self.pop(obj)

    def lockedAdd(self, *args):
        with self.lock:
            self.add(*args)

    def lockedPop(self, *args):
        with self.lock:
            self.remove(*args)

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["objects"] = self.objects[:]
        sdict["values"] = self.values[:]
        sdict["revIndex"] = self.revIndex.copy()
        return sdict


class TimedMap(PriorityQueue):
    @classmethod
    def create(cls, dct=None):
        if not dct: dct = {}
        if isinstance(dct, cls):
            return dct
        obj = cls()
        for key, value in dct.iteritems():
            obj.add(key, value)
        return obj

    def add(self, obj, value, tm=None):
        if obj not in self:
            PriorityQueue.add(self, obj, (tm or time.time(), value))

    def remove(self, obj):
        return self.pop(obj)


def GeneralizedSet(priorAttr):
    class _packset(PriorityQueue):
        @classmethod
        def create(cls, list=None):
            if isinstance(list, cls):
                return list
            obj = cls()
            map(obj.add, list or [])
            return obj

        def add(self, pck):
            if pck not in self:
                PriorityQueue.add(self, pck, getattr(pck, priorAttr, 0))

        def remove(self, obj):
            return self.pop(obj)

    return _packset


class PackSet(GeneralizedSet("priority")): pass


class FuncRunner(object):
    """simple function running object with cPickle support
    WARNING: this class works only with pure function and nondynamic class methods"""

    def __init__(self, fn, args, kws):
        self.object = None
        if isinstance(fn, types.MethodType):
            self.object = fn.im_self or fn.im_class
            self.methName = fn.im_func.func_name
        else:
            self.fn = fn
        self.args = args
        self.kws = kws

    def __call__(self):
        fn = getattr(self.object, self.methName, None) if self.object else self.fn
        if callable(fn):
            fn(*self.args, **self.kws)
        else:
            logging.error("FuncRunner\tobject %r can't be executed", fn)

    def __str__(self):
        return str(getattr(self.object, self.methName, None)) if self.object \
            else str(self.fn)


class BinaryFile(Unpickable(
    links=dict,
    lock=PickableRLock.create)):
    BUF_SIZE = 256 * 1024

    @classmethod
    def createFile(cls, directory, data):
        checksum = hashlib.md5(data).hexdigest()
        filename = os.path.join(directory, checksum)
        #if os.path.isfile(filename):
        #    raise RuntimeError("can't create file %s" % filename)
        fd, tmpfile = tempfile.mkstemp(dir=directory)
        with os.fdopen(fd, "w") as binWriter:
            binWriter.write(data)
            binWriter.flush()
        shutil.move(tmpfile, filename)
        return cls(filename, checksum, True)

    @classmethod
    def calcFileChecksum(cls, path):
        with open(path, "r") as reader:
            cs_calc = hashlib.md5()
            while True:
                buff = reader.read(BinaryFile.BUF_SIZE)
                if not buff:
                    break
                cs_calc.update(buff)
            return cs_calc.hexdigest()

    def __init__(self, path, checksum=None, set_rx_flag=False):
        assert os.path.isfile(path)
        if set_rx_flag:
            osspec.set_common_readable(path)
            osspec.set_common_executable(path)
        getattr(super(BinaryFile, self), "__init__")()
        self.path = path
        self.checksum = checksum if checksum else BinaryFile.calcFileChecksum(self.path)
        self.accessTime = time.time()

    def release(self):
        if os.path.isfile(self.path):
            os.unlink(self.path)

    def FixLinks(self):
        bad_links = set()
        for (pck_id, name), dest in self.links.iteritems():
            if not os.path.islink(dest):
                logging.warning("%s link item not found for packet %s", dest, pck_id)
                bad_links.add((pck_id, name))
        for i in bad_links:
            self.links.pop(i)

    def LinksCount(self):
        return len(self.links)

    def Link(self, pck, name):
        dstname = os.path.join(pck.directory, name)
        if (pck.id, name) in self.links:
            self.Unlink(pck, name)
        with self.lock:
            self.links[(pck.id, name)] = dstname
            osspec.create_symlink(self.path, dstname, reallocate=False)
            self.accessTime = time.time()

    def Unlink(self, pck, name):
        with self.lock:
            dstname = self.links.get((pck.id, name), None)
            if dstname is not None:
                self.links.pop((pck.id, name))
                self.accessTime = time.time()
                if os.path.islink(dstname):
                    os.unlink(dstname)

    def Relink(self, estimated_path):
        with self.lock:
            self.path = estimated_path
            for dstname in self.links.itervalues():
                dstdir = os.path.split(dstname)[0]
                if not os.path.isdir(dstdir):
                    logging.warning("binfile\tcan't relink nonexisted packet data %s", dstdir)
                elif os.path._resolve_link(dstname) != self.path:
                    osspec.create_symlink(self.path, dstname, reallocate=True)


def safeStringEncode(str):
    return xmlrpclib.Binary(str)


CheckEmailRe = re.compile("[\w._-]+@[\w_-]+\.[\w._-]+$")


def CheckEmailAddress(email):
    if isinstance(email, str) and CheckEmailRe.match(email):
        return True
    return False


def SendEmail(emails, msg_helper):
    if msg_helper:
        return osspec.send_email(emails, msg_helper.subject(), msg_helper.message())
