#-*- coding:utf-8 -*-
#!/usr/bin/env python

import multiprocessing as mp

class ProcessMgr:
    def __init__(self, processNum, worker):
        self.processNum = processNum
        self.processList = []
        if not worker: return False
        self.worker = worker

    def createProcess(self, args=()):
        for i in xrange(self.processNum):
            p = mp.Process(target=self.worker.process, args=args)
            self.processList.append(p)

    def startAll(self):
        for p in self.processList:
            p.start()

    def stopAll(self):
        for p in self.processList:
            p.join()

    def run(self):
        self.startAll()
        self.stopAll()
