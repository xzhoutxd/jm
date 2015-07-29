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
from RetryCrawler import RetryCrawler
sys.path.append('../db')
from MysqlAccess import MysqlAccess

class JMBrand():
    '''A class of JM channel'''
    def __init__(self, m_type):
        # DB
        #self.mysqlAccess   = MysqlAccess()     # mysql access

        # channel queue
        self.chan_queue = JMQ('channel','main')

        # act queue
        self.act_queue = JMQ('act','main')

        self.work = JMWorker()

        # 默认类别
        self.channel_list = [
                (1,'美妆','http://beauty.jumei.com/?from=all_null_index_top_nav_cosmetics&lo=3481&mat=30573')
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

                    # 清空act redis队列
                    self.act_queue.clearQ()
                    print '# channel queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                else:
                    print '# not find channel...'

            # channel acts
            obj = 'channel'
            crawl_type = 'main'
            # 获取还没有开团的活动id
            #val = (Common.time_s(Common.now()),)
            #acts = self.mysqlAccess.selectJMActNotStart(val)
            #act_id_list = []
            #if acts:
            #    for act in acts:
            #        act_id_list.append(str(act[1]))
            #_val = (self.begin_time, brandact_id_list)
            _val = None
            self.work.process(obj,crawl_type,_val)

            # 活动数据
            act_val_list = []
            act_val = self.work.items
            if act_val and len(act_val.keys()) > 0:
                if act_val.has_key('sale'):
                    print '# act on sale nums:', len(act_val['sale'])
                    act_val_list.extend(act_val['sale'])
                if act_val.has_key('coming'):
                    print '# act will coming nums:', len(act_val['coming'])
                    act_val_list.extend(act_val['coming'])

            # 保存到redis队列
            self.act_queue.putlistQ(act_val_list)
            print '# act queue end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

            #if self.m_type == 'm':
            #    val = (Common.add_hours(self.begin_time, -2),Common.add_hours(self.begin_time, -2),Common.add_hours(self.begin_time, -1))
            #    # 删除Redis中上个小时结束的活动
            #    _acts = self.mysqlAccess.selectJMActEndLastOneHour(val)
            #    print '# end acts num:',len(_acts)
            #    self.work.delAct(_acts)
            #    # 删除Redis中上个小时结束的商品
            #    _items = self.mysqlAccess.selectJMItemEndLastOneHour(val)
            #    print '# end items num:',len(_items)
            #    self.work.delItem(_items)
        except Exception as e:
            print '# antpage error :',e
            Common.traceback_log()

if __name__ == '__main__':
    args = sys.argv
    #args = ['JMBrand','m']
    if len(args) < 2:
        print '#err not enough args for JMBrand...'
        exit()
    # 是否是分布式中主机
    m_type = args[1]
    j = JMBrand(m_type)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    j.antPage()
    time.sleep(1)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
