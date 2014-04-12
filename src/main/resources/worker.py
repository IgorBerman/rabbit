import sys
import os
import traceback
from collections import deque
import logging

logger = logging.getLogger('worker')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
#fh = logging.FileHandler('c:/tmp/out%s.log' % os.getpid())
fh = logging.FileHandler('c:/tmp/out.log' )
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

try:
    import simplejson as json
except ImportError:
    import json

json_encode = lambda x: json.dumps(x)
json_decode = lambda x: json.loads(x)

#reads lines and reconstructs newlines appropriately
def readMsg():
    msg = ""
    while True:
        line = sys.stdin.readline()[0:-1]
        logger.info("read " +line)
        if line == "end":
            break
        msg = msg + line + "\n"
    return json_decode(msg[0:-1])

#queue up commands we read while trying to read taskids
pending_commands = deque()

def readTaskIds():
    if pending_taskids:
        return pending_taskids.popleft()
    else:
        msg = readMsg()
        while type(msg) is not list:
            pending_commands.append(msg)
            msg = readMsg()
        return msg

#queue up taskids we read while trying to read commands/tuples
pending_taskids = deque()

def readCommand():
    if pending_commands:
        return pending_commands.popleft()
    else:
        msg = readMsg()
        while type(msg) is list:
            pending_taskids.append(msg)
            msg = readMsg()
        return msg

def readWorkerMessage():
    cmd = readCommand()
    return WorkerMessage(cmd["id"], cmd["task"], cmd["args"])

def sendMsgToParent(msg):
    logger.info("sendMsgToParent:"+ str(msg))
    print json_encode(msg)
    print "end"
    sys.stdout.flush()

def sync():
    sendMsgToParent({'command':'sync'})

def sendpid(heartbeatdir):
    pid = os.getpid()
    sendMsgToParent({'pid':pid})
    open(heartbeatdir + "/" + str(pid), "w").close()

def ack(workerMessage):
    sendMsgToParent({"command": "ack", "id": workerMessage.id})

def fail(workerMessage):
    sendMsgToParent({"command": "fail", "id": workerMessage.id})

def reportError(msg):
    sendMsgToParent({"command": "error", "msg": str(msg)})

def log(msg):
    sendMsgToParent({"command": "log", "msg": str(msg)})

def initComponent():
    logger.info("init component")
    setupInfo = readMsg()
    logger.info("read setupInfo" +str(setupInfo))
    sendpid(setupInfo['pidDir'])
    return [setupInfo['conf'], setupInfo['context']]

class WorkerMessage(object):
    def __init__(self, id, task, args):
        self.id = id
        self.task = task
        self.args = args

    def __repr__(self):
        return '<%s%s>' % (
                self.__class__.__name__,
                ''.join(' %s=%r' % (k, self.__dict__[k]) for k in sorted(self.__dict__.keys())))

class Worker(object):
    def initialize(self, stormconf, context):
        pass

    def process(self, workerMessage):
        pass

    def run(self):
        conf, context = initComponent()
        try:
            self.initialize(conf, context)
            while True:
                workerMessage = readWorkerMessage()
                self.process(workerMessage)
        except Exception, e:
            reportError(traceback.format_exc(e))

class BasicWorker(object):
    def initialize(self, stormconf, context):
        pass

    def process(self, workerMessage):
        pass

    def run(self):
        conf, context = initComponent()
        try:
            logger.info("before initialize")
            self.initialize(conf, context)
            logger.info("after initialize")
            while True:
                workerMessage = readWorkerMessage()
                self.process(workerMessage)
                ack(workerMessage)
        except Exception, e:
            reportError(traceback.format_exc(e))
