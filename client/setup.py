from setuptools import setup
import os

def prepare_sources():
    srcdir = "src/remclient"
    modules = ["__init__.py", "remclient.py", "constants.py"]
    if not os.path.isdir(srcdir):
        os.makedirs(srcdir)
    for filename in modules:
        target = os.path.join("src/remclient", filename)
        if os.path.islink(target) or os.path.isfile(target):
            os.unlink(target)
        os.symlink(os.path.join("../..", filename), target)


prepare_sources()

setup(
    packages = [ "remclient" ],
    package_dir = { '': "src" },
    scripts = [ "rem-tool" ],
)
