#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import re
import random
import json
import time
import traceback
from Message import Message
sys.path.append('../base')
import Common as Common
import Config as Config
sys.path.append('../db')
from RedisQueue  import RedisQueue

class JMQ():
    '''A class of jm redis queue'''
    def __init__(self, _obj, _q_type=None):
        self._obj        = _obj
        self._q_type     = _q_type           # crawler type
        self.jm_type    = Config.JM_TYPE    # queue type
        # DB
        self.redisQueue  = RedisQueue()      # redis queue

        # message
        self.message     = Message()

        # queue key
        if self._q_type:
            self._key    = '%s_%s_%s' % (self.jm_type, self._obj, self._q_type)
        else:
            self._key    = '%s_%s' % (self.jm_type, self._obj)

    # clear queue
    def clearQ(self):
        self.redisQueue.clear_q(self._key)

    # 写入redis queue
    def putQ(self, _msg):
        self.redisQueue.put_q(self._key, _msg)

    # 转换msg
    def putlistQ(self, item_list):
        for _item in item_list:
            _val = (0, self._obj, self.jm_type) + _item
            msg = self.message.jmQueueMsg(self._obj, _val)
            if msg:
                self.putQ(msg)

