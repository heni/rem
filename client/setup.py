from setuptools import setup
import os

def prepare_sources():
    srcdir = "src/remclient"
    modules = ["__init__.py", "remclient.py", "constants.py"] 
    if not os.path.isdir(srcdir):
        os.makedirs(srcdir)
    for filename in modules:
        target = os.path.join("src/remclient", filename)
        if os.path.islink(target):
            os.unlink(target)
        os.symlink(os.path.join("../..", filename), target)


prepare_sources()

setup(
    name = "remclient",
    description = "client library for REM server; see at https://github.com/heni/rem",
    maintainer = "Eugene Krokhalev",
    maintainer_email = "Eugene.Krokhalev@gmail.com",
    version = "1.0.2",
    packages = [ "remclient" ],
    package_dir = { '': "src" },
    scripts = [ "rem-tool" ],
    install_requires = [ "six", "bsddb3" ],
    classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Operating System :: POSIX :: BSD",
        "Operating System :: POSIX :: Linux",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Topic :: System :: Systems Administration",
        "Topic :: Software Development"
    ]
)
