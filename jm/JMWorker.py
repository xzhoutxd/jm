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
import logging
from Message import Message
from JMChannel import Channel
from JMAct import Act
from JMItemM import JMItemM
from JMItemM import JMItemRedisM
sys.path.append('../base')
import Common as Common
import Config as Config
from JMCrawler import JMCrawler
sys.path.append('../dial')
from DialClient import DialClient
sys.path.append('../db')
from MysqlAccess import MysqlAccess
from RedisQueue  import RedisQueue
from RedisAccess import RedisAccess
from MongofsAccess import MongofsAccess

class JMWorker():
    '''A class of jm worker'''
    def __init__(self):
        # jm brand type
        self.worker_type   = Config.JM_Brand
        # DB
        self.jm_type       = Config.JM_TYPE    # queue type
        self.mysqlAccess   = MysqlAccess()     # mysql access
        self.redisQueue    = RedisQueue()      # redis queue
        self.redisAccess   = RedisAccess()     # redis db
        self.mongofsAccess = MongofsAccess()   # mongodb fs access

        # 抓取设置
        self.crawler       = JMCrawler()

        # message
        self.message       = Message()

        # 抓取时间设定
        self.crawling_time = Common.now() # 当前爬取时间
        self.begin_time    = Common.now()
        self.begin_date    = Common.today_s()
        self.begin_hour    = Common.nowhour_s()

    def init_crawl(self, _obj, _crawl_type):
        self._obj          = _obj
        self._crawl_type   = _crawl_type

        # dial client
        self.dial_client   = DialClient()

        # local ip
        self._ip           = Common.local_ip()

        # router tag
        self._router_tag   = 'ikuai'
        #self._router_tag  = 'tpent'

        # items
        self.items = {}

        # giveup items
        self.giveup_items  = []

        # giveup msg val
        self.giveup_val    = None

    # To dial router
    def dialRouter(self, _type, _obj):
        try:
            _module = '%s_%s' %(_type, _obj)
            self.dial_client.send((_module, self._ip, self._router_tag))
        except Exception as e:
            print '# To dial router exception :', e

    # To crawl retry
    def crawlRetry(self, _key, msg):
        if not msg: return
        msg['retry'] += 1
        _retry = msg['retry']
        _obj = msg["obj"]
        max_time = Config.crawl_retry
        if _obj == 'channel':
            max_time = Config.channel_crawl_retry
        elif _obj == 'act':
            max_time = Config.act_crawl_retry
        elif _obj == 'item':
            max_time = Config.item_crawl_retry
        if _retry < max_time:
            self.redisQueue.put_q(_key, msg)
        else:
            #self.push_back(self.giveup_items, msg)
            print "# retry too many time, no get:", msg

    # To crawl page
    def crawlPage(self, _obj, _crawl_type, _key, msg, _val):
        try:
            if _obj == 'channel':
                self.run_channel(msg)
            elif _obj == 'act':
                self.run_act(msg)
            elif _obj == 'item':
                self.run_item(msg, _val)
            else:
                print '# crawlPage unknown obj = %s' % _obj
        except Common.InvalidPageException as e:
            print '# Invalid page exception:',e
            self.crawlRetry(_key,msg)
        except Common.DenypageException as e:
            print '# Deny page exception:',e
            self.crawlRetry(_key,msg)
            # 重新拨号
            try:
                self.dialRouter(4, 'chn')
            except Exception as e:
                print '# DailClient Exception err:', e
                time.sleep(random.uniform(10,30))
            time.sleep(random.uniform(10,30))
        except Common.SystemBusyException as e:
            print '# System busy exception:',e
            self.crawlRetry(_key,msg)
            time.sleep(random.uniform(10,30))
        except Common.RetryException as e:
            print '# Retry exception:',e
            if self.giveup_val:
                msg['val'] = self.giveup_val
            self.crawlRetry(_key,msg)
            time.sleep(random.uniform(20,30))
        except Exception as e:
            print '# exception err:',e
            self.crawlRetry(_key,msg)
            # 重新拨号
            try:
                self.dialRouter(4, 'chn')
            except Exception as e:
                print '# DailClient Exception err:', e
            time.sleep(random.uniform(10,30))
            Common.traceback_log()

    def run_channel(self, msg):
        msg_val = msg["val"]
        c = Channel()
        c.antPage(msg_val)
        if self._crawl_type == 'global':
            if len(c.channel_sale_items) > 0:
                self.items['sale'] = c.channel_sale_items
            if len(c.channel_coming_items) > 0:
                self.items['coming'] = c.channel_coming_items
        else:
            if len(c.channel_sale_acts) > 0:
                self.items['sale'] = c.channel_sale_acts
            if len(c.channel_coming_acts) > 0:
                self.items['coming'] = c.channel_coming_acts

    def run_act(self, msg):
        msg_val = msg["val"]
        print '# act start:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        a = Act()
        a.antPage(msg_val)
        print '# act end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        #act_keys = [self.worker_type, str(a.act_id)]
        #prev_act = self.redisAccess.read_jmact(act_keys)
        prev_act = None
        # 多线程抓商品
        items_list = self.run_actItems(a, prev_act)
        self.putActDB(a, prev_act, items_list)

    # 并行获取品牌团商品
    def run_actItems(self, act, prev_act):
        print '# act items start:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        # 需要抓取的item
        item_val_list = []
        # 过滤已经抓取过的商品ID列表
        item_ids = act.act_itemids
        if prev_act:
            prev_item_ids = prev_act["item_ids"]
            item_ids      = Common.diffSet(item_ids, prev_item_ids)
            # 如果已经抓取过的活动没有新上线商品，则退出
            if len(item_ids) == 0:
                print '# Activity no new Items'
                print '# Activity Items end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), act.act_id, act.act_name
                return None

            for item in act.act_itemval_d.values():
                #if str(item[6]) in item_ids or str(item[7]) in item_ids:
                    item_val_list.append(item)
        else:
            item_val_list = act.act_itemval_d.values()

        # 如果活动没有商品, 则退出
        if len(item_ids) == 0:
            print '# run_brandItems: no items in activity, act_id=%s, act_name=%s' % (act.act_id, act.act_name)
            return None

        print '# Activity Items crawler start:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), act.act_id, act.act_name
        # 多线程 控制并发的线程数
        _val = (act.crawling_begintime,)
        if len(item_val_list) > Config.item_max_th:
            m_itemsObj = JMItemM('main', Config.item_max_th, _val)
        else:
            m_itemsObj = JMItemM('main', len(item_val_list), _val)
        m_itemsObj.createthread()
        m_itemsObj.putItems(item_val_list)
        m_itemsObj.run()

        item_list = m_itemsObj.items
        print '# Activity find new Items num:', len(item_val_list)
        print '# Activity crawl Items num:', len(item_list)
        giveup_items = m_itemsObj.giveup_items
        if len(giveup_items) > 0:
            print '# Activity giveup Items num:',len(giveup_items)
            raise Common.RetryException('# run_actItems: actid:%s actname:%s some items retry more than max times..'%(str(act.act_id),str(act.act_name)))
        print '# Activity Items end:',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), act.act_id, act.act_name
        return item_list

    # To merge activity
    def mergeAct(self, act, prev_act):
        if prev_act:
            # 合并本次和上次抓取的商品ID列表
            prev_item_ids   = prev_act["item_ids"]
            act.act_itemids = Common.unionSet(act.act_itemids, prev_item_ids)

    # 修正活动开始时间
    def startTime(self, act_stime, item_stime):
        # 先取活动开始时间
        _start_time = act_stime
        # 如果商品开始时间非空
        if item_stime != '':
            # 如果活动开始时间为空，则取商品开始时间
            if _start_time == '' or _start_time == 0.0: _start_time = item_stime
            # 如果活动开始时间非空且商品开始时间<活动开始时间,则取商品开始时间
            if _start_time != '' and _start_time != 0.0 and item_stime < _start_time: _start_time = item_stime
        ## 如果计算后的活动开始时间还是为空, 则取当前时间
        #if _start_time == '' and _start_time == 0.0:
        #    _start_time = crawling_begintime
        return _start_time

    # 修正活动结束时间
    def endTime(self, act_etime, item_etime):
        # 取活动结束时间
        _end_time = act_etime
        # 商品结束时间非空
        if item_etime != '':
            # 如果活动结束时间为空,则取商品结束时间
            if _end_time == '' or _end_time == 0.0: _end_time = item_etime
            # 如果活动结束时间非空且商品结束时间>活动结束时间,则取商品结束时间
            if _end_time != '' and _end_time != 0.0 and item_etime > _end_time: _end_time = item_etime

        return _end_time

    def backActinfo(self, act, items_list):
        if items_list and len(items_list) > 0:
            a_stime = act.act_start_time
            a_etime = act.act_end_time
            #if not a_stime or float(a_stime) == 0.0 or not a_etime or float(a_etime) == 0.0:
            for item in items_list:
                if not a_stime or float(a_stime) == 0.0:
                    if item[20] != '':
                        i_stime = Common.str2timestamp(item[20])
                        if not a_stime or float(a_stime) == 0.0:
                            act.act_start_time = self.startTime(act.act_start_time, i_stime)
                if item[21] != '':
                    i_etime = Common.str2timestamp(item[21])
                    if not a_etime or float(a_etime) == 0.0:
                        act.act_end_time = self.endTime(act.act_end_time, i_etime)

    # To put act db
    def putActDB(self, act, prev_act, items_list):
        # redis
        #self.mergeAct(act, prev_act)

        if self._crawl_type == 'main':
        #    # mysql
        #    if prev_act:
        #        print '# update activity, id:%s name:%s'%(act.act_id, act.act_name)
        #        self.mysqlAccess.updateJMAct(act.outSqlForUpdate())
        #    else:
            print '# insert activity, id:%s name:%s'%(act.act_id, act.act_name)
            # 回填数据
            self.backActinfo(act, items_list)
            self.mysqlAccess.insertJMActHour(act.outSql())

        # mongo
        # 存网页
        #_pages = act.outItemPage(self._crawl_type)
        #self.mongofsAccess.insertJMPages(_pages)

    # To process activity in redis
    def procActRedis(self, act, prev_act, items_list):
        pass
        ## 活动抓取的item ids
        #act.act_itemids = []
        #if items_list:
        #    for item in items_list:
        #        # item id
        #        if str(item[1]) != '':
        #            act.act_itemids.append(str(item[1]))

        ## redis
        #self.mergeAct(act, prev_act)
        #keys = [self.worker_type, str(act.act_id)]
        #val = act.outTupleForRedis()
        #self.redisAccess.write_jmact(keys, val)
                
    def process(self, _obj, _crawl_type, _val=None):
        if _obj == 'globalitem':
            self.processMulti(_obj, _crawl_type, _val)
        else:
            self.processOne(_obj, _crawl_type, _val)

    def processOne(self, _obj, _crawl_type, _val=None):
        self.init_crawl(_obj, _crawl_type)

        i, M = 0, 20
        if _obj == 'channel':
            M = 10
        M = 1
        n = 0
        while True:
            if _crawl_type and _crawl_type != '':
                _key = '%s_%s_%s' % (self.jm_type,_obj,_crawl_type)
            else:
                _key = '%s_%s' % (self.jm_type,_obj)
            _msg = self.redisQueue.get_q(_key)

            # 队列为空
            if not _msg:
                i += 1
                if i > M:
                    print '# not get queue of key:',_key,time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    print '# all get num of item in queue:',n
                    break
                time.sleep(10)
                continue
            n += 1
            try:
                self.crawlPage(_obj, _crawl_type, _key, _msg, _val)
            except Exception as e:
                print '# exception err in process of JMWorker:',e,_key,_msg

    def processMulti(self, _obj, _crawl_type, _val=None):
        self.init_crawl(_obj, _crawl_type)
        if _crawl_type and _crawl_type != '':
            _key = '%s_%s_%s' % (self.jm_type,_obj,_crawl_type)
        else:
            _key = '%s_%s' % (self.jm_type,_obj)

        try:
            self.crawlPageMulti(_obj, _crawl_type, _key,  _val)
        except Exception as e:
            print '# exception err in processMulti of JMWorker:',e,_key

    # To crawl page
    def crawlPageMulti(self, _obj, _crawl_type, _key, _val):
        if _obj == 'globalitem':
            self.run_globalitem(_key, _val)
        else:
            print '# crawlPageMulti unknown obj = %s' % _obj

    def run_globalitem(self, _key, _val):
        mitem = JMItemRedisM(_key, self._crawl_type, 20, _val)
        mitem.createthread()
        mitem.run()
        item_list = mitem.items
        #self.items = item_list
        print '# crawl Items num:', len(item_list)

    # 删除redis数据库过期活动
    def delAct(self, _acts):
        i = 0
        for _act in _acts:
            keys = [self.worker_type, str(_act[0])]

            item = self.redisAccess.read_jmact(keys)
            if item:
                end_time = item["end_time"]
                now_time = Common.time_s(self.crawling_time)
                # 删除过期的活动
                if now_time > end_time:
                    i += 1
                    self.redisAccess.delete_jmact(keys)
        print '# delete acts num:',i

    def delItem(self, _items):
        i = 0
        for _item in _items:
            keys = [self.worker_type, str(_item[0])]

            item = self.redisAccess.read_jmitem(keys)
            if item:
                end_time = item["end_time"]
                now_time = Common.time_s(self.crawling_time)
                # 删除过期的商品
                if now_time > end_time:
                    i += 1
                    self.redisAccess.delete_jmitem(keys)
        print '# delete items num:',i

if __name__ == '__main__':
    pass

