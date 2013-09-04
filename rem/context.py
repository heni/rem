import logging
import logging.handlers
import os
import time
from ConfigParser import ConfigParser, NoOptionError
import codecs


class StableRotateFileHandler(logging.handlers.TimedRotatingFileHandler):
    REOPEN_TM = 60

    def __init__(self, filename, **kws):
        logging.handlers.TimedRotatingFileHandler.__init__(self, filename, **kws)
        self.lastReopen = time.time()

    def shouldRollover(self, record):
        if time.time() - self.lastReopen > self.REOPEN_TM:
            self.stream.close()
            if self.encoding:
                self.stream = codecs.open(self.baseFilename, "a", self.encoding)
            else:
                self.stream = open(self.baseFilename, "a")
            lastReopen = time.time()
        return logging.handlers.TimedRotatingFileHandler.shouldRollover(self, record)


class ConfigReader(ConfigParser):
    def safe_get(self, section, option, default=""):
        try:
            return self.get(section, option)
        except NoOptionError:
            return default

    def safe_getint(self, section, option, default=0):
        try:
            return self.getint(section, option)
        except NoOptionError:
            return default

    def safe_getboolean(self, section, option, default=False):
        try:
            return self.getboolean(section, option)
        except NoOptionError:
            return default

    def safe_getlist(self, section, option, default=None):
        try:
            value = self.get(section, option)
            return [item.strip() for item in value.split(",") if item.strip()]
        except NoOptionError:
            return default or []


class Context(object):
    @classmethod
    def prep_dir(cls, dir_name):
        dir_name = os.path.abspath(dir_name)
        if not os.path.isdir(dir_name):
            os.makedirs(dir_name)
        if not os.path.isdir(dir_name):
            raise RuntimeError("can't create directory: \"%s\"" % dir_name)
        return dir_name

    def __init__(self, cfgFile, execMode):
        config = ConfigReader()
        assert cfgFile in config.read(cfgFile), "error in configuration file \"%s\"" % cfgFile
        self.logs_directory = self.prep_dir(config.get("log", "dir"))
        self.packets_directory = self.prep_dir(config.get("store", "pck_dir"))
        self.backup_directory = self.prep_dir(config.get("store", "backup_dir"))
        self.backup_period = config.getint("store", "backup_period")
        self.backup_count = config.getint("store", "backup_count")
        self.binary_directory = self.prep_dir(config.get("store", "binary_dir"))
        self.binary_lifetime = config.getint("store", "binary_lifetime")
        self.error_lifetime = config.getint("store", "error_packet_lifetime")
        self.success_lifetime = config.getint("store", "success_packet_lifetime")
        self.tags_db_file = config.get("store", "tags_db_file")
        self.recent_tags_file = config.get("store", "recent_tags_file")
        self.remote_tags_db_file = config.safe_get("store", "remote_tags_db_file")
        self.thread_pool_size = config.getint("run", "poolsize")
        self.xmlrpc_pool_size = config.safe_getint("run", "xmlrpc_poolsize", 1)
        self.readonly_xmlrpc_pool_size = config.safe_getint("run", "readonly_xmlrpc_pool_size", 1)
        self.manager_port = config.getint("server", "port")
        self.manager_readonly_port = config.safe_getint("server", "readonly_port")
        self.system_port = config.safe_getint("server", "system_port")
        self.network_topology = config.safe_get("server", "network_topology")
        self.network_name = config.safe_get("server", "network_hostname")
        self.send_emails = config.getboolean("server", "send_emails")
        self.execMode = execMode
        self.useMemProfiler = config.getboolean("server", "use_memory_profiler")
        self.max_remotetags_resend_delay = config.safe_getint("server", "max_remotetags_resend_delay", 300)
        self.initLogger(config, self.execMode != "start")

    def initLogger(self, config, isTestMode):
        logLevel = logging.DEBUG if isTestMode \
            else getattr(logging, config.get("log", "warnlevel").upper())
        logger = logging.getLogger()
        if not isTestMode:
            logHandler = StableRotateFileHandler(
                os.path.join(self.logs_directory, config.get("log", "filename")),
                when="midnight", backupCount=config.getint("log", "rollcount"))
            logHandler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(module)s:\t%(message)s"))
            logger.addHandler(logHandler)
        logger.setLevel(logLevel)

    def registerScheduler(self, scheduler):
        if getattr(self, "Scheduler", None):
            raise RuntimeError("can't relocate scheduler for context object")
        self.Scheduler = scheduler
