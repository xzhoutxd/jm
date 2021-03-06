#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import re
import random
import json
import time
from JMQ import JMQ
from JMWorker import JMWorker
sys.path.append('../base')
import Common as Common
import Config as Config
import Logger as Logger
sys.path.append('../db')
from MysqlAccess import MysqlAccess

class JMGlobal():
    '''A class of JM global'''
    def __init__(self, m_type):
        # DB
        #self.mysqlAccess   = MysqlAccess()     # mysql access

        # channel queue
        self.chan_queue = JMQ('channel','global')

        # item queue
        self.item_queue = JMQ('globalitem','main')

        self.work = JMWorker()

        # 默认类别
        self.channel_list = [
                (2,'聚美极速免税店','http://www.jumeiglobal.com')
                ]

        # 页面
        self.site_page  = None

        # 抓取开始时间
        self.begin_time = Common.now()

        # 分布式主机标志
        self.m_type = m_type

    def antPage(self):
        try:
            # 主机器需要配置redis队列
            if self.m_type == 'm':
                #channel_list = self.mysqlAccess.selectJMChannel()
                #if not channel_list or len(channel_list) == 0:
                channel_list = self.channel_list
                if channel_list and len(channel_list) > 0:
                    channel_val_list = []
                    for c in channel_list:
                        channel_val_list.append(c+(self.begin_time,))
                    # 清空channel redis队列
                    self.chan_queue.clearQ()
                    # 保存channel redis队列
                    self.chan_queue.putlistQ(channel_val_list)

                    # 清空item redis队列
                    self.item_queue.clearQ()
                    Common.log('# channel queue end')
                else:
                    Common.log('# not find channel...')

            # global items
            obj = 'channel'
            crawl_type = 'global'
            _val = None
            self.work.process(obj,crawl_type,_val)

            # 商品数据
            item_val_list = []
            item_val = self.work.items
            if item_val and len(item_val.keys()) > 0:
                if item_val.has_key('sale'):
                    Common.log('# item on sale nums: %d' % len(item_val['sale']))
                    item_val_list.extend(item_val['sale'])
                if item_val.has_key('coming'):
                    Common.log('# item will coming nums: %s' % len(item_val['coming']))
                    item_val_list.extend(item_val['coming'])

            Common.log('# item val nums: %s' % len(item_val_list))
            # 保存到redis队列
            self.item_queue.putlistQ(item_val_list)
            Common.log('# item queue end')

            #if self.m_type == 'm':
            #    val = (Common.add_hours(self.begin_time, -2),Common.add_hours(self.begin_time, -2),Common.add_hours(self.begin_time, -1))
            #    # 删除Redis中上个小时结束的商品
            #    _items = self.mysqlAccess.selectJMItemEndLastOneHour(val)
            #    Common.log('# end items num: %d' % len(_items))
            #    self.work.delItem(_items)
        except Exception as e:
            Common.log('# JMGlobal antpage error: %s' % e)
            Common.traceback_log()

if __name__ == '__main__':
    loggername = 'global'
    filename = 'add_channel_%s' % (time.strftime("%Y%m%d%H", time.localtime()))
    Logger.config_logging(loggername, filename)
    args = sys.argv
    #args = ['JMGlobal','m']
    if len(args) < 2:
        Common.log('#err not enough args for JMGlobal...')
        exit()
    # 是否是分布式中主机
    m_type = args[1]
    j = JMGlobal(m_type)
    Common.log('# JMGlobal start')
    j.antPage()
    time.sleep(1)
    Common.log('# JMGlobal end')

