#! /usr/bin/env python

import pyver
__version__, __version_info__ = pyver.get_version (pkg = __name__)
from pikarun.mq_consumer import MQ_Consumer
from pikarun.mq_publisher import MQ_Publisher
