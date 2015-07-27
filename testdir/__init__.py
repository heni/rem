import logging

from common import *
from test_01 import *
from test_02 import *
from test_03 import *
from test_04 import *
from test_05 import *
from test_06 import *
from test_07 import *
from test_08 import *
from test_09 import *
from test_10 import *
from test_11 import *
from test_12 import *
from test_13 import *
from test_14 import *
from test_15 import *
from test_16 import *
from test_17 import *
from test_last import *


def setUp(config, queueName="test"):
    global Config, TestingQueue, LmtTestQueue
    Config.value = config
    TestingQueue.value = queueName
    LmtTestQueue.value = queueName + "-lmt"
    logging.basicConfig(level=logging.DEBUG,
                        format="\x1b[1m<%(levelname)s:%(module)s:%(funcName)s %(asctime)s>\x1b[m %(message)s")
    config.setUp()
