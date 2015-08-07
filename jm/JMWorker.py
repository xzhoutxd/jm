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
import Logger as Logger
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
        self.init_log(_obj, _crawl_type)

    def init_log(self, _obj, _crawl_type):
        if not Logger.logger:
            loggername = 'other'
            filename = 'crawler_%s' % (time.strftime("%Y%m%d%H", time.localtime(self.begin_time)))
            if _obj == 'act':
                loggername = 'brand'
                filename = 'add_brands_%s' % (time.strftime("%Y%m%d%H", time.localtime(self.begin_time)))
            #elif _obj == 'item':
                
            elif _obj == 'globalitem':
                loggername = 'global'
                filename = 'add_items_%s' % (time.strftime("%Y%m%d%H", time.localtime(self.begin_time)))
            Logger.config_logging(loggername, filename)

    # To dial router
    def dialRouter(self, _type, _obj):
        try:
            _module = '%s_%s' %(_type, _obj)
            self.dial_client.send((_module, self._ip, self._router_tag))
        except Exception as e:
            Common.log('# To dial router exception: %s' % e)

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
            Common.log("# retry too many time, no get msg:")
            Common.log(msg)

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
                Common.log('# crawlPage unknown obj = %s' % _obj)
        except Common.InvalidPageException as e:
            Common.log('# Invalid page exception: %s' % e)
            self.crawlRetry(_key,msg)
        except Common.DenypageException as e:
            Common.log('# Deny page exception: %s' % e)
            self.crawlRetry(_key,msg)
            # 重新拨号
            try:
                self.dialRouter(4, 'chn')
            except Exception as e:
                Common.log('# DailClient Exception err: %s' % e)
                time.sleep(random.uniform(10,30))
            time.sleep(random.uniform(10,30))
        except Common.SystemBusyException as e:
            Common.log('# System busy exception: %s' % e)
            self.crawlRetry(_key,msg)
            time.sleep(random.uniform(10,30))
        except Common.RetryException as e:
            Common.log('# Retry exception: %s' % e)
            if self.giveup_val:
                msg['val'] = self.giveup_val
            self.crawlRetry(_key,msg)
            time.sleep(random.uniform(20,30))
        except Exception as e:
            Common.log('# exception err: %s' % e)
            self.crawlRetry(_key,msg)
            Common.traceback_log()
            if str(e).find('Read timed out') == -1:
                # 重新拨号
                try:
                    self.dialRouter(4, 'chn')
                except Exception as e:
                    Common.log('# DailClient Exception err: %s' % e)
                time.sleep(random.uniform(10,30))

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
        Common.log('# act start')
        a = Act()
        a.antPage(msg_val)
        Common.log('# act end')

        #act_keys = [self.worker_type, str(a.act_id)]
        #prev_act = self.redisAccess.read_jmact(act_keys)
        prev_act = None
        # 多线程抓商品
        items_list = self.run_actItems(a, prev_act)
        self.putActDB(a, prev_act, items_list)

    # 并行获取品牌团商品
    def run_actItems(self, act, prev_act):
        Common.log('# act items start')
        # 需要抓取的item
        item_val_list = []
        # 过滤已经抓取过的商品ID列表
        item_ids = act.act_itemids
        if prev_act:
            prev_item_ids = prev_act["item_ids"]
            item_ids      = Common.diffSet(item_ids, prev_item_ids)
            # 如果已经抓取过的活动没有新上线商品，则退出
            if len(item_ids) == 0:
                Common.log('# Activity no new Items')
                Common.log('# Activity Items end')
                return None

            for item in act.act_itemval_d.values():
                item_val_list.append(item)
        else:
            item_val_list = act.act_itemval_d.values()

        # 如果活动没有商品, 则退出
        if len(item_ids) == 0:
            Common.log('# run_brandItems: no items in activity, act_id=%s, act_name=%s' % (act.act_id, act.act_name))
            return None

        Common.log('# Activity Items crawler start')
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
        Common.log('# Activity Items crawler end')
        Common.log('# Activity find new Items num: %d' % len(item_val_list))
        Common.log('# Activity crawl Items num: %d' % len(item_list))
        giveup_items = m_itemsObj.giveup_items
        if len(giveup_items) > 0:
            Common.log('# Activity giveup Items num: %d' % len(giveup_items))
            raise Common.RetryException('# run_actItems: actid:%s actname:%s some items retry more than max times..'%(str(act.act_id),str(act.act_name)))
        Common.log('# Activity Items end')
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
        #        self.mysqlAccess.updateJMAct(act.outSqlForUpdate())
        #    else:
            Common.log('# insert activity, id:%s name:%s' % (act.act_id, act.act_name))
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
                    Common.log('# not get queue of key: %s' % _key)
                    Common.log('# all get num of item in queue: %d' % n)
                    break
                time.sleep(10)
                continue
            n += 1
            try:
                self.crawlPage(_obj, _crawl_type, _key, _msg, _val)
            except Exception as e:
                Common.log('# exception err in process of JMWorker: %s , key: %s' % (e,_key))
                Common.log(_msg)

    def processMulti(self, _obj, _crawl_type, _val=None):
        self.init_crawl(_obj, _crawl_type)
        if _crawl_type and _crawl_type != '':
            _key = '%s_%s_%s' % (self.jm_type,_obj,_crawl_type)
        else:
            _key = '%s_%s' % (self.jm_type,_obj)

        try:
            self.crawlPageMulti(_obj, _crawl_type, _key,  _val)
        except Exception as e:
            Common.log('# exception err in processMulti of JMWorker: %s, key: %s' % (e,_key))

    # To crawl page
    def crawlPageMulti(self, _obj, _crawl_type, _key, _val):
        if _obj == 'globalitem':
            self.run_globalitem(_key, _val)
        else:
            Common.log('# crawlPageMulti unknown obj = %s' % _obj)

    def run_globalitem(self, _key, _val):
        mitem = JMItemRedisM(_key, self._crawl_type, 20, _val)
        mitem.createthread()
        mitem.run()
        item_list = mitem.items
        #self.items = item_list
        Common.log('# crawl Items num: %d' % len(item_list))

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
        Common.log('# delete acts num: %d' % i)

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
        Common.log('# delete items num: %d' % i)

if __name__ == '__main__':
    pass

