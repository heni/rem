from packet import *
from job import *
from workers import *
from callbacks import Tag
from scheduler import Queue, Scheduler
from context import Context
from common import CheckEmailAddress


def DefaultContext(execMode=None):
    import optparse

    parser = optparse.OptionParser()
    parser.set_defaults(config="rem.cfg")
    parser.add_option("-c", dest="config", metavar="FILE", help="use configuration FILE")
    opt, args = parser.parse_args()
    return Context(opt.config, execMode=execMode or args[0])
