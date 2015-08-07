#-*- coding:utf-8 -*-
#!/usr/bin/env python

from sys import path

import json
import pickle
from RedisPool import RedisPool
path.append(r'../base')
import Common as Common

@Common.singleton
class RedisAccess:
    def __init__(self):
        # redis db instance
        self.redis_pool = RedisPool()

        # redis db id
        self.DEFAULT_DB    = 0   # default db
        
        self.JM_ACT_DB     = 30  # jm activity        
        self.JM_ITEM_DB    = 31  # jm item
        self.JM_SKU_DB     = 32  # jm SKU

        self.COOKIE_DB     = 9   # cookie
        self.QUEUE_DB      = 10  # queue db

    ######################## Cookie部分 ########################

    # 判断是否存在cookie
    def exist_cookie(self, keys):
        return self.redis_pool.exists(keys, self.COOKIE_DB)

    # 删除cookie
    def remove_cookie(self, keys):
        return self.redis_pool.remove(keys, self.COOKIE_DB)

    # 查询cookie
    def read_cookie(self, keys):
        try:
            val = self.redis_pool.read(keys, self.COOKIE_DB)
            if val:
                cookie_dict = pickle.loads(val)
                _time  = cookie_dict["time"]            
                _cookie= cookie_dict["cookie"]
                return (_time, _cookie)
        except Exception, e:
            Common.log('# Redis access read cookie exception: %s' % e)
            return None

    # 写入cookie
    def write_cookie(self, keys, val):
        try:
            _time, _cookie = val
            cookie_dict = {}
            cookie_dict["time"]   = _time
            cookie_dict["cookie"] = _cookie
            cookie_json = pickle.dumps(cookie_dict)
            
            self.redis_pool.write(keys, cookie_json, self.COOKIE_DB)
        except Exception, e:
            Common.log('# Redis access write cookie exception: %s' % e)

    # 扫描cookie
    def scan_cookie(self):
        try:
            cookie_list = []
            cookies = self.redis_pool.scan_db(self.COOKIE_DB)
            for cookie in cookies:
                val = cookie[1]
                if val:
                    cookie_dict = pickle.loads(val)
                    _time   = cookie_dict["time"]   
                    _cookie = cookie_dict["cookie"]
                    cookie_list.append((_time, _cookie))
            return cookie_list
        except Exception, e:
            Common.log('# Redis access scan cookie exception: %s' % e)
            return None

    ######################## JM Activity ###################
    # 判断是否存在JM活动
    def exist_jmact(self, keys):
        return self.redis_pool.exists(keys, self.JM_ACT_DB)

    # 删除jm活动
    def delete_jmact(self, keys):
        self.redis_pool.remove(keys, self.JM_ACT_DB)

    # 查询jm活动
    def read_jmact(self, keys):
        try:
            val = self.redis_pool.read(keys, self.JM_ACT_DB)
            return json.loads(val) if val else None
        except Exception, e:
            Common.log('# Redis access read jm activity exception: %s' % e)
            return None

    # 写入jm活动
    def write_jmact(self, keys, val):
        try:
            if type(val) is dict:
                act_dict = val
            else:
                crawl_time, category_id, act_id, act_name, act_url, act_position, act_enterpic_url, act_remindnum, act_coupon, act_coupons, act_sign, _act_ids, start_time, end_time, item_ids = val
                act_dict = {}
                act_dict["crawl_time"]          = str(crawl_time)
                act_dict["category_id"]         = str(category_id)
                act_dict["act_id"]              = str(act_id)
                act_dict["act_name"]            = act_name
                act_dict["act_url"]             = act_url
                act_dict["act_position"]        = str(act_position)
                act_dict["act_enterpic_url"]    = act_enterpic_url
                act_dict["act_remindnum"]       = str(act_remindnum)
                act_dict["act_coupon"]          = str(act_coupon)
                act_dict["act_coupons"]         = act_coupons
                act_dict["_act_ids"]            = str(_act_ids)
                act_dict["act_sign"]            = str(act_sign)
                act_dict["start_time"]          = str(start_time)
                act_dict["end_time"]            = str(end_time)
                act_dict["item_ids"]            = item_ids
            act_json = json.dumps(act_dict)
            self.redis_pool.write(keys, act_json, self.JM_ACT_DB)
        except Exception, e:
            Common.log('# Redis access write jm activity exception: %s' % e)

    # 扫描jm活动 - 性能不好
    def scan_jmact(self):
        try:
            for act in self.redis_pool.scan_db(self.JM_ACT_DB):
                key, val = act
                if not val: continue
                act_dict       = json.loads(val)
                #Common.log("# scan_jmact %s:" %key)
                #Common.log(act_dict)
        except Exception, e:
            Common.log('# Redis access scan jm activity exception: %s' % e)

    ######################## JM ITEM ###################

    # 判断是否存在jm item
    def exist_jmitem(self, keys):
        return self.redis_pool.exists(keys, self.JM_ITEM_DB)

    # 删除jm item
    def delete_jmitem(self, keys):
        self.redis_pool.remove(keys, self.JM_ITEM_DB)

    # 查询jm item
    def read_jmitem(self, keys):
        try:
            val = self.redis_pool.read(keys, self.JM_ITEM_DB)
            return json.loads(val) if val else None 
        except Exception, e:
            Common.log('# Redis access read jm item exception: %s' % e)
            return None

    # 写入jm item
    def write_jmitem(self, keys, val):
        try:
            item_json = json.dumps(val)
            self.redis_pool.write(keys, item_json, self.JM_ITEM_DB)
        except Exception, e:
            Common.log('# Redis access write jm item exception: %s' % e)

    # 扫描jm item - 性能不好
    def scan_jmitem(self):
        try:
            for item in self.redis_pool.scan_db(self.JM_ITEM_DB):
                key, val = item
                if not val: continue
                item_dict    = json.loads(val)
                #Common.log("# scan_jmitem %s:" %key)
                #Common.log(item_dict)
        except Exception as e:
            Common.log('# Redis access scan jm item exception: %s' % e)


if __name__ == '__main__':
    pass


