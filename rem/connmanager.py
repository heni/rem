import threading
import xmlrpclib
import select
import bsddb3
from SimpleXMLRPCServer import SimpleXMLRPCServer
from ConfigParser import ConfigParser
import cPickle
import subprocess

from common import *
from callbacks import Tag, ICallbackAcceptor


class ClientInfo(Unpickable(taglist=set,
                            subscriptions=set,
                            errorsCnt=(int, 0),
                            active=(bool, True))):
    MAX_TAGS_BULK = 100
    PENALTY_FACTOR = 6

    def __init__(self, *args, **kws):
        getattr(super(ClientInfo, self), "__init__")(*args, **kws)
        self.update(*args, **kws)
        self.lastError = None

    def Connect(self):
        self.connection = xmlrpclib.ServerProxy(self.systemUrl)

    def SetTag(self, tagname):
        self.taglist.add(tagname)

    def Subscribe(self, tagname):
        self.subscriptions.add(tagname)

    def update(self, name=None, url=None, systemUrl=None):
        if name:
            self.name = name
        if url:
            self.url = url
        if systemUrl:
            self.systemUrl = systemUrl
            self.Connect()

    def Resume(self):
        self.Connect()
        self.active = True

    def Suspend(self):
        self.active = False

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["taglist"] = self.taglist.copy()
        sdict.pop("connection", None)
        return getattr(super(ClientInfo, self), "__getstate__", lambda: sdict)()

    def __repr__(self):
        return "<ClientInfo %s alive: %r>" % (self.name, self.active)


class TopologyInfo(Unpickable(servers=dict, location=str)):
    def ReloadConfig(self, location=None):
        if location is not None:
            self.location = location
        self.Update(TopologyInfo.ReadConfig(self.location))

    @classmethod
    def ReadConfig(cls, location):
        if location.startswith("local://"):
            return cls.ReadConfigFromFile(location[8:])
        elif location.startswith("svn+ssh://"):
            return cls.ReadConfigFromSVN(location)
        raise AttributeError("unknown config location %s" % location)

    @classmethod
    def ReadConfigFromSVN(cls, location):
        tmp_dir = tempfile.mkdtemp(dir=".", prefix="network-topology")
        try:
            config_temporary_path = os.path.join(tmp_dir, os.path.split(location)[1])
            subprocess.check_call(
                ["svn", "export", "--force", "--non-interactive", "-q", location, config_temporary_path])
            return cls.ReadConfigFromFile(config_temporary_path)
        finally:
            if os.path.isdir(tmp_dir):
                shutil.rmtree(tmp_dir)

    @classmethod
    def ReadConfigFromFile(cls, filename):
        configParser = ConfigParser()
        assert filename in configParser.read(filename), \
            "error in network topology file %s" % filename
        return configParser.items("servers")

    def Update(self, data):
        for k, v in data:
            server_info = map(lambda s: s.strip(), v.split(','))
            self.servers.setdefault(k, ClientInfo()).update(k, *server_info)

    def GetClient(self, hostname, checkname=True):
        if checkname and hostname not in self.servers:
            raise RuntimeError("unknown host '%s'" % hostname)
        return self.servers.get(hostname, None)

    def UpdateContext(self, context):
        self.location = context.network_topology

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["servers"] = self.servers.copy()
        return getattr(super(TopologyInfo, self), "__getstate__", lambda: sdict)()


class ConnectionManager(Unpickable(topologyInfo=TopologyInfo,
                                   scheduledTasks=TimedSet.create,
                                   lock=PickableLock,
                                   alive=(bool, False),
                                   tags_file=str),
                        ICallbackAcceptor):
    def InitXMLRPCServer(self):
        self.rpcserver = SimpleXMLRPCServer(("", self.port), allow_none=True)
        self.rpcserver.register_function(self.set_tags, "set_tags")
        self.rpcserver.register_function(self.list_clients, "list_clients")
        self.rpcserver.register_function(self.list_tags, "list_tags")
        self.rpcserver.register_function(self.suspend_client, "suspend_client")
        self.rpcserver.register_function(self.resume_client, "resume_client")
        self.rpcserver.register_function(self.reload_config, "reload_config")
        self.rpcserver.register_function(self.register_share, "register_share")
        self.rpcserver.register_function(self.unregister_share, "unregister_share")
        self.rpcserver.register_function(self.get_client_info, "get_client_info")
        self.rpcserver.register_function(self.list_shares, "list_shares")
        self.rpcserver.register_function(self.list_subscriptions, "list_subscriptions")
        self.rpcserver.register_function(self.check_connection, "check_connection")
        self.rpcserver.register_function(self.ping, "ping")

    def UpdateContext(self, context):
        self.scheduler = context.Scheduler
        self.network_name = context.network_name
        self.tags_file = context.remote_tags_db_file
        self.port = context.system_port
        if self.tags_file:
            self.acceptors = bsddb3.btopen(self.tags_file, "c")
        self.topologyInfo.UpdateContext(context)
        self.max_remotetags_resend_delay = context.max_remotetags_resend_delay

    def Start(self):
        if not self.network_name or not self.tags_file or not self.port:
            logging.warning("ConnectionManager could'n start: wrong configuration. " +
                            "network_name: %s, remote_tags_db_file: %s, system_port: %r",
                            self.network_name, self.tags_file, self.port)
            return
        self.ReloadConfig()
        self.alive = True
        self.InitXMLRPCServer()
        threading.Thread(target=self.ServerLoop).start()
        for client in self.topologyInfo.servers.values():
            self.scheduler.ScheduleTask(0, self.SendData, client, skip_logging=True)

    def Stop(self):
        self.alive = False

    def ServerLoop(self):
        rpc_fd = self.rpcserver.fileno()
        while self.alive:
            rout, _, _ = select.select((rpc_fd,), (), (), 0.01)
            if rpc_fd in rout:
                self.rpcserver.handle_request()

    def SendData(self, client):
        if (len(client.subscriptions) > 0 or len(client.taglist) > 0) and client.active and self.alive:
            client.Connect()
            numTags = 1 if client.errorsCnt > 0 else client.MAX_TAGS_BULK
            tags = list(client.taglist)[:numTags]
            subscriptions = list(client.subscriptions)[:numTags]
            try:
                if len(tags) > 0:
                    logging.debug("SendData to %s: %d tags", client.name, len(tags))
                    client.connection.set_tags(tags)
                    client.taglist.difference_update(tags)

                if len(subscriptions) > 0:
                    logging.debug("SendData to %s: %d subscriptions", client.name, len(subscriptions))
                    client.connection.register_share(subscriptions, self.network_name)
                    client.subscriptions.difference_update(subscriptions)

                client.errorsCnt = 0
                logging.debug("SendData to %s: ok", client.name)
            except IOError as e:
                logging.warning("SendData to %s: failed", client.name)
                client.lastError = e
                client.errorsCnt += 1
            except Exception as e:
                logging.error("SendData to %s: failed: %s", client.name, e)
        if hasattr(self, "scheduler"):
            self.scheduler.ScheduleTask(
                min(client.PENALTY_FACTOR ** client.errorsCnt, self.max_remotetags_resend_delay),
                self.SendData,
                client,
                skip_logging=True
            )

    def OnDone(self, tag):
        if not self.alive:
            return
        if not isinstance(tag, Tag):
            logging.error("%s is not Tag class instance", tag.GetName())
            return
        tagname = tag.GetName()
        if not tag.IsRemote():
            acceptors = self.GetTagAcceptors(tagname)
            if acceptors:
                logging.debug("ondone connmanager %s with acceptors list %s", tagname, acceptors)
                for clientname in acceptors:
                    self.SetTag(tagname, clientname)

    def SetTag(self, tagname, clientname):
        logging.debug("set remote tag %s on host %s", tagname, clientname)
        client = self.topologyInfo.GetClient(clientname, checkname=False)
        if client is None:
            logging.error("unknown client %s appeared", clientname)
            return False
        client.SetTag("%s:%s" % (self.network_name, tagname))

    def ReloadConfig(self, filename=None):
        old_servers = set(self.topologyInfo.servers.keys())
        self.topologyInfo.ReloadConfig()
        new_servers = set(self.topologyInfo.servers.keys())
        new_servers -= old_servers
        if self.alive:
            for client in new_servers:
                self.scheduler.ScheduleTask(0, self.SendData, self.topologyInfo.servers[client], skip_logging=True)

    def GetTagAcceptors(self, tagname):
        if self.acceptors.has_key(tagname):
            data = self.acceptors[tagname]
            return cPickle.loads(data)
        return set()

    def AddTagAcceptor(self, tagname, clientname):
        with self.lock:
            subscribers = self.GetTagAcceptors(tagname)
            subscribers.add(clientname)
            self.acceptors[tagname] = cPickle.dumps(subscribers)
            self.acceptors.sync()

    def RemoveTagAcceptor(self, tagname, clientname):
        with self.lock:
            subscribers = self.GetTagAcceptors(tagname)
            if clientname not in subscribers:
                return False
            subscribers.discard(clientname)
            self.acceptors[tagname] = cPickle.dumps(subscribers)
            self.acceptors.sync()
            return True

    def Subscribe(self, tag):
        if tag.IsRemote():
            client = self.topologyInfo.GetClient(tag.GetRemoteHost(), checkname=True)
            client.Subscribe(tag.GetName())
            return True
        return False

    @traced_rpc_method()
    def set_tags(self, tags):
        logging.debug("set %d remote tags", len(tags))
        for tagname in tags:
            self.scheduler.tagRef.SetRemoteTag(tagname)
        return True

    @traced_rpc_method()
    def list_clients(self):

        return [{"name": client.name,
                 "url": client.url,
                 "systemUrl": client.systemUrl,
                 "active": client.active,
                 "errorsCount": client.errorsCnt,
                 "tagsCount": len(client.taglist),
                 "subscriptionsCount": len(client.subscriptions),
                 "lastError": str(client.lastError)} for client in self.topologyInfo.servers.values()]

    @traced_rpc_method()
    def list_tags(self, name_prefix):
        data = set()
        for server in self.topologyInfo.servers.values():
            if name_prefix is None or server.name.startswith(name_prefix):
                data.update(server.taglist)
        return list(data)

    @traced_rpc_method()
    def suspend_client(self, name):
        client = self.topologyInfo.GetClient(name)
        return client.Suspend()

    @traced_rpc_method()
    def resume_client(self, name):
        client = self.topologyInfo.GetClient(name)
        return client.Resume()

    @traced_rpc_method()
    def reload_config(self, location=None):
        self.ReloadConfig(location)

    @traced_rpc_method()
    def register_share(self, tags, clientname):
        if not isinstance(tags, list):
            tags = [tags]
        for tagname in tags:
            self.AddTagAcceptor(tagname, clientname)
            if self.scheduler.tagRef.CheckTag(tagname):
                self.SetTag(tagname, clientname)

    @traced_rpc_method()
    def unregister_share(self, tagname, clientname):
        return self.RemoveTagAcceptor(tagname, clientname)

    @traced_rpc_method()
    def get_client_info(self, clientname):
        client = self.topologyInfo.GetClient(clientname)
        res = {"name": client.name,
               "url": client.url,
               "systemUrl": client.systemUrl,
               "active": client.active,
               "errorsCount": client.errorsCnt,
               "deferedTagsCount": len(client.taglist),
               "subscriptionsCount": len(client.subscriptions),
               "lastError": str(client.lastError)}
        return res

    @traced_rpc_method()
    def list_shares(self, clientname):
        client = self.topologyInfo.GetClient(clientname)
        return list(client.taglist)

    @traced_rpc_method()
    def list_subscriptions(self, clientname):
        client = self.topologyInfo.GetClient(clientname)
        return list(client.subscriptions)

    @traced_rpc_method()
    def check_connection(self, clientname):
        client = self.topologyInfo.GetClient(clientname)
        return client.connection.ping()

    @traced_rpc_method()
    def ping(self):
        return True

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["scheduledTasks"] = self.scheduledTasks.copy()
        sdict.pop("scheduler", None)
        sdict.pop("rpcserver", None)
        sdict.pop("acceptors", None)
        sdict["alive"] = False
        return getattr(super(ConnectionManager, self), "__getstate__", lambda: sdict)()
