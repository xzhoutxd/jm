#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys

sys.path.append('../base')
import Common as Common

@Common.singleton
class Message():
    '''A class of jm message'''
    def __init__(self):
        pass

    ##### 品牌团 #####
    def jmQueueMsg(self, _obj, _val):
        if _obj == "channel":
            return self.jmChannelQueueMsg(_val)
        elif _obj == "act":
            return self.jmActQueueMsg(_val)
        elif _obj == "item":
            return self.jmItemQueueMsg(_val)
        elif _obj == "globalitem":
            return self.jmGlobalitemQueueMsg(_val)
        else:
            return None

    def jmChannelQueueMsg(self, _cat):
        cat = {}
        cat["retry"]  = _cat[0]
        cat["obj"]    = _cat[1]
        cat["type"]   = _cat[2]
        cat["val"]    = _cat[3:]
        return cat

    def jmActQueueMsg(self, _act):
        act = {}
        act["retry"]  = _act[0]
        act["obj"]    = _act[1]
        act["type"]   = _act[2]
        act["val"]    = _act[3:]
        return act

    def jmItemQueueMsg(self, _item):
        item = {}
        item["retry"]  = _item[0]
        item["obj"]    = _item[1]
        item["type"]   = _item[2]
        item["val"]    = _item[3:]
        return item

    def jmGlobalitemQueueMsg(self, _item):
        item = {}
        item["retry"]  = _item[0]
        item["obj"]    = _item[1]
        item["type"]   = _item[2]
        item["val"]    = _item[3:]
        return item

    # 商品Redis数据
    def jhsitemMsg(self, _item):
        item_juid, item_id, item_position, item_ju_url, item_juname, item_judesc, item_jupic_url, item_url, item_oriprice, item_actprice, item_discount, item_coupons, item_promotions, item_remindnum, item_islock_time, item_islock, start_time, end_time = _item
        item = {}
        item["item_juid"]           = str(item_juid)
        item["item_id"]             = str(item_id)
        item["item_position"]       = str(item_position)
        item["item_ju_url"]         = item_ju_url
        item["item_juname"]         = item_juname
        item["item_judesc"]         = item_judesc
        item["item_jupic_url"]      = item_jupic_url
        item["item_url"]            = item_url
        item["item_oriprice"]       = str(item_oriprice)
        item["item_actprice"]       = str(item_actprice)
        item["item_discount"]       = str(item_discount)
        item["item_coupons"]        = item_coupons
        item["item_promotions"]     = item_promotions
        item["item_remindnum"]      = str(item_remindnum)
        if item_islock_time:
            item["item_islock_time"]    = str(item_islock_time)
        else:
            item["item_islock_time"]    = ''
        item["item_islock"]         = str(item_islock)
        item["start_time"]          = str(start_time)
        item["end_time"]            = str(end_time)
        return item

