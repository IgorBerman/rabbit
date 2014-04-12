# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This Python file uses the following encoding: utf-8

import worker
from random import random
import traceback
import time

class TesterWorker(worker.Worker):
    def initialize(self, conf, context):
        worker.log('initializing')

    def process(self, workerMessage):
        worker.log("starting " + workerMessage.task + " with args " + str(workerMessage.args))
        time.sleep(3)
        
        worker.ack(workerMessage)
try:
    TesterWorker().run()
except Exception as e:
    worker.reportError(traceback.format_exc(e))
