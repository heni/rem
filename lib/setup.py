from distutils.core import setup, Extension
import os
from ConfigParser import ConfigParser


def LoadMetadata():
    setup_dir = os.path.dirname(__file__)
    cp = ConfigParser()
    cp.read(os.path.join(setup_dir, "setup.cfg"))
    return dict(cp.items("global"))


setup(
    name="_fork_locking",
    ext_modules=[Extension("_fork_locking", ["_fork_locking.c"])],
    **LoadMetadata()
)
