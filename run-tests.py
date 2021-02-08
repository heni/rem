#!/usr/bin/env python

import logging
import os
import subprocess
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(sys.argv[0]), "client", "src"))
import six
from six.moves.configparser import ConfigParser
import remclient
import testdir


class ClientInfo(object):
    def __init__(self, name, projectDir, hostname):
        self.name = name
        self.projectDir = projectDir.split("://", 1)[-1]
        configPath = os.path.join(projectDir, 'rem.cfg')
        tmp_dir = tempfile.mkdtemp(dir=".", prefix="configuration-")
        try:
            cp = self.LoadConfiguration(configPath, tmp_dir)
        finally:
            if os.path.isdir(tmp_dir):
                shutil.rmtree(tmp_dir)
        self.binDir = cp.get('store', 'binary_dir')
        self.packetsDir = cp.get('store', 'pck_dir')
        self.url = "http://%s:%d" % (hostname, cp.getint("server", "port"))
        self.admin_url = "http://%s:%d" % (hostname, cp.getint("server", "system_port"))
        self.readonly_url = "http://%s:%d" % (hostname, cp.getint("server", "readonly_port"))
        self.rem_configuration = cp
        self.connector = remclient.Connector(self.url, verbose=True, packet_name_policy=remclient.IGNORE_DUPLICATE_NAMES_POLICY)
        self.admin_connector = remclient.AdminConnector(self.admin_url, verbose=True)
        self.readonly_connector = remclient.Connector(self.readonly_url, verbose=True, packet_name_policy=remclient.IGNORE_DUPLICATE_NAMES_POLICY)

    def LoadConfiguration(self, config_path, tmpdir):
        if config_path.startswith("svn+ssh://"):
            config_temporary_path = os.path.join(tmpdir, os.path.basename(config_path))
            subprocess.check_call(
                ["svn", "export", "--force", "--non-interactive", "-q", config_path, config_temporary_path])
        elif config_path.startswith("local://"):
            config_temporary_path = config_path[8:]
            self.path = os.path.dirname(config_temporary_path)
        elif os.path.isfile(config_path):
            config_temporary_path = config_path
        else:
            raise RuntimeError("not implemented scheme type for location %s" % config_path)
        cp = ConfigParser()
        assert config_temporary_path in cp.read(config_temporary_path)
        return cp


class Configuration(object):
    @classmethod
    def GetConfigFromFile(cls, filename):
        cp = ConfigParser()
        assert filename in cp.read([filename])
        config = cls()
        servers = cls.parse_multiline_options(cp.get("tests", "servers"))
        assert len(servers) == 2 and len(servers[0]) == 3 and len(servers[1]) == 3, "incorrect servers configuration"
        config.server1 = ClientInfo(*servers[0])
        config.server2 = ClientInfo(*servers[1])
        config.notify_email = cp.get("tests", "notify_email")
        config.alt_user = cp.get("tests", "alt_user")
        return config

    @staticmethod
    def parse_multiline_options(optvalue):
        tuples = list(filter(None, (optvalue or "").split("\n")))
        return [[v.strip() for v in tpl.split(",")] for tpl in tuples]

    @staticmethod
    def __sync_dir(srcdir, dstdir, paths):
        for p in paths:
            srcp, dstp = os.path.join(srcdir, p), os.path.join(dstdir, p)
            try:
                if os.path.isdir(dstp) and not os.path.islink(dstp):
                    shutil.rmtree(dstp)
                else:
                    os.unlink(dstp)
            except OSError:
                logging.exception("rem servers synchronization")
            if os.path.islink(srcp):
                os.symlink(os.readlink(srcp), dstp)
            elif os.path.isfile(srcp):
                shutil.copy2(srcp, dstp)
            elif os.path.isdir(srcp):
                shutil.copytree(srcp, dstp)
            else:
                raise RuntimeError("syncdir unexpected situation")

    def setUp(self):
        path1 = getattr(getattr(self, "server1", None), "path", None)
        path2 = getattr(getattr(self, "server2", None), "path", None)
        if path1 and path2:
            testdir.common.RestartService(path1)
            with testdir.common.ServiceTemporaryShutdown(path2):
                self.__sync_dir(path1, path2, ["client", "rem", "rem-server.py", "start-stop-daemon.py", "setup_env.sh",
                                               "network_topology.cfg"])


if __name__ == "__main__":
    config = Configuration.GetConfigFromFile("tests.cfg")
    testdir.setUp(config, "userdata")
    unittest.TestProgram(module=testdir)
