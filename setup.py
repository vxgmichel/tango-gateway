#!/usr/bin/env python3

# Imports
from setuptools import setup


# Read function
def safe_read(fname):
    try:
        return open(fname).read()
    except IOError:
        return ""

# Setup
setup(name="tango-gateway",
      version="0.2.0",
      packages=['tangogateway'],
      entry_points={
          'console_scripts': ['tango-gateway = tangogateway:main']},

      license="GPLv3",
      install_requires=['aiozmq'],
      description="A Tango gateway server",
      long_description=safe_read("README.md"),

      author="Vincent Michel",
      author_email="vincent.michel@maxlab.lu.se",
      url="http://www.maxlab.lu.se",
      )
