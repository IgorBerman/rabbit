import worker
from random import random
import traceback
import time

class TesterWorker(worker.Worker):
    def initialize(self, conf, context):
        worker.log('initializing, opening db connection')

    def process(self, workerMessage):
        worker.log("starting " + workerMessage.task + " with args " + str(workerMessage.args))
        if workerMessage.task == 'task':
            time.sleep(3)
            worker.ack(workerMessage)
        elif workerMessage.task == 'failtask':
            worker.fail(workerMessage)
        elif workerMessage.task == 'exceptiontask':
            raise ValueError("some exception inside python")
            
TesterWorker().run()
