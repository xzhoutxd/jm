#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import time
import random
import traceback
import threading
from Queue import Empty
from Message import Message
from JMItem import Item
sys.path.append('../base')
import Common as Common
import Config as Config
from MyThread  import MyThread
from JMCrawler import JMCrawler
sys.path.append('../dial')
from DialClient import DialClient
sys.path.append('../db')
from MysqlAccess import MysqlAccess
from RedisAccess import RedisAccess
from MongofsAccess import MongofsAccess

import warnings
warnings.filterwarnings("ignore")

class JMItemM(MyThread):
    '''A class of jm item thread manager'''
    def __init__(self, _q_type, thread_num=10, a_val=None):
        # parent construct
        MyThread.__init__(self, thread_num)

        # thread lock
        self.mutex          = threading.Lock()

        self.worker_type    = Config.JM_Brand

        # message
        self.message        = Message()

        # db
        self.mysqlAccess    = MysqlAccess()   # mysql access
        self.redisAccess    = RedisAccess()   # redis db
        self.mongofsAccess  = MongofsAccess() # mongodb fs access

        # jm queue type
        self._q_type        = _q_type # main:新增商品, day:每天一次的商品, hour:每小时一次的商品, update:更新

        # appendix val
        self.a_val          = a_val
        
        # activity items
        self.items          = []

        # dial client
        self.dial_client    = DialClient()

        # local ip
        self._ip            = Common.local_ip()

        # router tag
        self._tag           = 'ikuai'
        #self._tag          = 'tpent'

        # give up item, retry too many times
        self.giveup_items   = []

    # To dial router
    def dialRouter(self, _type, _obj):
        try:
            _module = '%s_%s' %(_type, _obj)
            self.dial_client.send((_module, self._ip, self._tag))
        except Exception as e:
            print '# To dial router exception :', e

    def push_back(self, L, v):
        if self.mutex.acquire(1):
            L.append(v)
            self.mutex.release()

    def putItem(self, _item):
        self.put_q((0, _item))

    def putItems(self, _items):
        for _item in _items: self.put_q((0, _item))

    # To merge item
    def mergeAct(self, item, prev_item):
        if prev_item:
            if not item.item_position or item.item_position == 0:
                item.item_position      = prev_item["item_position"]
            if not item.item_juName or item.item_juName == '':
                item.item_juName        = prev_item["item_juname"]
            if not item.item_juDesc or item.item_juDesc == '':
                item.item_juDesc        = prev_item["item_judesc"]
            if not item.item_juPic_url or item.item_juPic_url == '':
                item.item_juPic_url     = prev_item["item_jupic_url"]
            if not item.item_url or item.item_url == '':
                item.item_url           = prev_item["item_url"]
            if not item.item_oriPrice or item.item_oriPrice == '':
                item.item_oriPrice      = prev_item["item_oriprice"]
            if not item.item_actPrice or item.item_actPrice == '':
                item.item_actPrice      = prev_item["item_actprice"]
            if not item.item_discount or item.item_discount == '':
                item.item_discount      = prev_item["item_discount"]
            if not item.item_coupons or item.item_coupons == []:
                item.item_coupons       = prev_item["item_coupons"].split(Config.sep)
            if not item.item_promotions or item.item_promotions == []:
                item.item_promotions    = prev_item["item_promotions"].split(Config.sep)
            if not item.item_remindNum or item.item_remindNum == '':
                item.item_remindNum     = prev_item["item_remindnum"]
            if not item.item_isLock_time or item.item_isLock_time == '':
                if prev_item["item_islock_time"] and prev_item["item_islock_time"] != '':
                    item.item_isLock_time   = Common.str2timestamp(prev_item["item_islock_time"])
                    item.item_isLock        = prev_item["item_islock"]
            if not item.item_starttime or item.item_starttime == 0.0:
                if prev_item["start_time"] and prev_item["start_time"] != '':
                    item.item_starttime     = Common.str2timestamp(prev_item["start_time"])
            if not item.item_endtime or item.item_endtime == 0.0:
                if prev_item["end_time"] and prev_item["end_time"] != '':
                    item.item_endtime       = Common.str2timestamp(prev_item["end_time"])

    # To put item redis db
    def putItemDB(self, item):
        # redis
        keys = [self.worker_type, str(item.item_juId)]
        prev_item = self.redisAccess.read_jmitem(keys)
        self.mergeAct(item, prev_item)
        val = item.outTupleForRedis()
        msg = self.message.jmitemMsg(val)
        self.redisAccess.write_jmitem(keys, msg)

    # To crawl retry
    def crawlRetry(self, _data):
        if not _data: return
        _retry, _val = _data
        _retry += 1
        if _retry < Config.item_crawl_retry:
            _data = (_retry, _val)
            self.put_q(_data)
        else:
            self.push_back(self.giveup_items, _val)
            print "# retry too many times, no get item:", _val

    # insert item info
    def insertIteminfo(self, iteminfosql_list, f=False):
        if f or len(iteminfosql_list) >= Config.item_max_arg:
            if len(iteminfosql_list) > 0:
                self.mysqlAccess.insertJMItemHour(iteminfosql_list)
                #print '# insert data to database'
            return True
        return False


    # To crawl item
    def crawl(self):
        # item sql list
        _iteminfosql_list = []
        _itemdaysql_list = []
        _itemhoursql_list = []
        _itemupdatesql_list = []
        while True:
            _data = None
            try:
                try:
                    # 取队列消息
                    _data = self.get_q()
                except Empty as e:
                    # 队列为空，退出
                    #print '# queue is empty', e
                    # info
                    self.insertIteminfo(_iteminfosql_list, True)
                    _iteminfosql_list = []

                    break

                item = None
                obj = 'item'
                if self._q_type == 'main':
                    # 新商品实例
                    item = Item()
                    _val = _data[1]
                    if self.a_val: _val = _val + self.a_val
                    item.antPage(_val)
                    #print '# To crawl activity item val : ', Common.now_s()
                    # 汇聚
                    # redis
                    #self.putItemDB(item)
                    self.push_back(self.items, item.outSql())
                    # 入库
                    iteminfoSql = item.outSql()
                    _iteminfosql_list.append(iteminfoSql)
                    if self.insertIteminfo(_iteminfosql_list): _iteminfosql_list = []

                # 存网页
                #if item:
                #    _pages = item.outItemPage(obj, self._q_type)
                #    self.mongofsAccess.insertJMPages(_pages)

                # 延时
                time.sleep(1)
                # 通知queue, task结束
                self.queue.task_done()

            except Common.NoItemException as e:
                print 'Not item exception :', e
                # 通知queue, task结束
                self.queue.task_done()

            except Common.NoPageException as e:
                print 'Not page exception :', e
                # 通知queue, task结束
                self.queue.task_done()

            except Common.InvalidPageException as e:
                self.crawlRetry(_data)
                print 'Invalid page exception :', e
                # 通知queue, task结束
                self.queue.task_done()

            except Exception as e:
                print 'Unknown exception crawl item :', e
                Common.traceback_log()
                self.crawlRetry(_data)
                # 通知queue, task结束
                self.queue.task_done()
                if str(e).find('Name or service not known') != -1 or str(e).find('Temporary failure in name resolution') != -1:
                    print _data
                # 重新拨号
                try:
                    self.dialRouter(4, 'item')
                except Exception as e:
                    print '# DailClient Exception err:', e 
                    time.sleep(10)
                time.sleep(random.uniform(10,40))


from RedisQueue  import RedisQueue
class JMItemRedisM(MyThread):
    '''A class of jm Item redis queue'''
    def __init__(self, key, q_type, thread_num=10, a_val=None):
        # parent construct
        MyThread.__init__(self, thread_num)
        # thread lock
        self.mutex          = threading.Lock()

        self.jm_type        = Config.JM_TYPE # jm type
        #self.item_type      = q_type        # item queue type

        # db
        self.mysqlAccess    = MysqlAccess() # mysql access
        self.redisQueue     = RedisQueue()  # redis queue
        self.mongofsAccess  = MongofsAccess() # mongodb fs access

        # jm queue type
        self.jm_queue_type  = q_type # main hour...
        self._key           = key   # redis queue key

        # appendix val
        self.a_val          = a_val

        # return items
        self.items          = []

        # dial client
        self.dial_client    = DialClient()

        # local ip
        self._ip            = Common.local_ip()

        # router tag
        self._tag           = 'ikuai'
        #self._tag          = 'tpent'

        # give up item, retry too many times
        self.giveup_items   = []

    # To dial router
    def dialRouter(self, _type, _obj):
        try:
            _module = '%s_%s' %(_type, _obj)
            self.dial_client.send((_module, self._ip, self._tag))
        except Exception as e:
            print '# To dial router exception :', e

     # clear item queue
    def clearItemQ(self):
        self.redisQueue.clear_q(self._key)

    # 写入redis queue
    def putItemQ(self, _msg):
        _data = (0, _msg)
        self.redisQueue.put_q(self._key, _data)

    # 转换msg
    def putItemlistQ(self, item_list):
        for _item in item_list:
            #msg = self.q_message.itemMsg(_item)
            msg = _item
            self.putItemQ(msg)

    def push_back(self, L, v):
        if self.mutex.acquire(1):
            L.append(v)
            self.mutex.release()

    # To crawl retry
    """
    def crawlRetry(self, _data):
        if not _data: return
        _retry, _val = _data
        _retry += 1
        if _retry < Config.crawl_retry:
            _data = (_retry, _val)
            self.redisQueue.put_q(self._key, _data)
        else:
            self.push_back(self.giveup_items, _val)
            print "# retry too many times, no get item:", _val
    """

    def crawlRetry(self, _key, msg):
        if not msg: return
        msg['retry'] += 1
        _retry = msg['retry']
        _obj = msg["obj"]
        max_time = Config.crawl_retry
        if _obj == 'globalitem':
            max_time = Config.item_crawl_retry
        if _retry < max_time:
            self.redisQueue.put_q(_key, msg)
        else:
            #self.push_back(self.giveup_items, msg)
            print "# retry too many time, no get:", msg

    # insert Global item hour
    def insertGlobalItemHour(self, iteminfosql_list, f=False):
        if f or len(iteminfosql_list) >= Config.item_max_arg:
            if len(iteminfosql_list) > 0:
                self.mysqlAccess.insertJMGlobalitemHour(iteminfosql_list)
                #print '# insert data to database'
            return True
        return False

    # item sql list
    def crawl(self):
        _iteminfosql_list = []
        i, M = 0, 2
        n = 0
        while True:
            try:
                _data = self.redisQueue.get_q(self._key)

                # 队列为空
                if not _data:
                    # 队列为空，退出
                    #print '# queue is empty', e
                    # info
                    self.insertGlobalItemHour(_iteminfosql_list, True)
                    _iteminfosql_list = []

                    i += 1
                    if i > M:
                        print '# all get itemQ item num:',n
                        print '# not get itemQ of key:',self._key
                        break
                    time.sleep(10)
                    continue
                n += 1
                item = None
                obj = 'globalitem'
                if self.jm_queue_type == 'main':
                    # 商品实例
                    item = Item()
                    #_val = _data[1]
                    _val = _data["val"]
                    if self.a_val: _val = _val + self.a_val

                    item.antPageGlobal(_val)
                    #print '# Hour To crawl item val : ', Common.now_s(), _val[0], _val[4], _val[5]
                    # 汇聚
                    self.push_back(self.items, item.outGlobalSql())

                    iteminfoSql = item.outGlobalSql()
                    _iteminfosql_list.append(iteminfoSql)
                    if self.insertGlobalItemHour(_iteminfosql_list): _iteminfosql_list = []
                else:
                    continue

                # 存网页
                #if item and obj != '':
                #    _pages = item.outItemPage(obj, self.jm_queue_type)
                #    self.mongofsAccess.insertJMPages(_pages)

                # 延时
                time.sleep(1)

            except Common.NoItemException as e:
                print 'Not item exception :', e

            except Common.NoPageException as e:
                print 'Not page exception :', e

            except Common.InvalidPageException as e:
                self.crawlRetry(self._key, _data)
                print 'Invalid page exception :', e

            except Exception as e:
                print 'Unknown exception crawl item :', e
                print Common.traceback_log()

                self.crawlRetry(self._key, _data)
                # 重新拨号
                try:
                    self.dialRouter(4, 'item')
                except Exception as e:
                    print '# DailClient Exception err:', e
                    time.sleep(10)
                time.sleep(random.uniform(10,30))

if __name__ == '__main__':
    pass

