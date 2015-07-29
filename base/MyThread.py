#-*- coding:utf-8 -*-
#!/usr/bin/env python

import Queue
import threading
from multiprocessing.dummy import threading as th
from threading import stack_size

class MyThread():
    '''A class of my thread'''
    def __init__(self, thread_num = 10):
        # 线程堆栈设置，防止占用大量内存
        stack_size(1024*1024)
        self.threadList = []

        self.threadNum = thread_num
        self.queue = Queue.Queue()

    def __del_(self):
        pass
        #self.queue.clear()
        #self.queue = None

    def createthread(self, args=()):
        for i in xrange(self.threadNum):
            thread = th.Thread(target=self.crawl, args=args)
            self.threadList.append(thread)

    def put_q(self, _data):
        self.queue.put(_data,block=False)

    def get_q(self):
        return self.queue.get(block=False)

    def empty_q(self):
        return self.queue.empty()

    def startAll(self):
        for thr in self.threadList:
            thr.start()

    def stopAll(self):
        for thr in self.threadList:
            thr.join()

    def run(self):
        self.startAll()
        self.stopAll()
