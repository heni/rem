from setuptools import setup, Extension
import os


setup(
    ext_modules=[Extension("_fork_locking", ["_fork_locking.c"])],
)
