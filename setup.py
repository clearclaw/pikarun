#! /usr/bin/env python

try:
  import pyver # pylint: disable=W0611
except ImportError:
  import os, subprocess
  try:
    environment = os.environ.copy()
    cmd = "pip install pyver".split (" ")
    subprocess.check_call (cmd, env = environment)
  except subprocess.CalledProcessError:
    import sys
    print >> sys.stderr, "Problem installing 'pyver' dependency."
    print >> sys.stderr, "Please install pyver manually."
    sys.exit (1)
  import pyver # pylint: disable=W0611

from setuptools import setup, find_packages

__version__, __version_info__ = pyver.get_version (pkg = "pikarun")

setup (name = "pikarun",
  version = __version__,
  description = "Base classes for a RabbitMQ-based queue-runner and a (persistent) RabbitMQ-based publisher.",
  long_description = "Base classes for a RabbitMQ-based queue-runner and a (persistent) RabbitMQ-based publisher.",
  classifiers = [],
  keywords = "",
  author = "J C Lawrence",
  author_email = "claw@kanga.nu",
  url = "http://kanga.nu/~claw/",
  license = "GPL v3.0",
  packages = find_packages (exclude = ["tests",]),
  package_data = {
  },
  zip_safe = True,
  install_requires = [
    "logtool",
    "pyver",
  ],
  entry_points = {
    "console_scripts": [
      ],
    },
  )
