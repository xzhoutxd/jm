#-*- coding:utf-8 -*-
#!/usr/bin/env python

from sys import path
path.append(r'../base')

import traceback
import Common as Common
from MongoPool import MongoPool

@Common.singleton
class MongofsAccess():
    '''A class of mongodb gridfs access'''
    def __init__(self):
        self.mongo_db  = MongoPool()

    def __del__(self):
        self.mongo_db = None

    # 插入网页列表
    def insertJMPages(self, pages):
        try:
            _key, _pages_d = pages
            data = {"key":_key, "pages":_pages_d}
            c = _key[:8]
            db_name = "jm" + c
            self.mongo_db.insertPage(db_name, c, data)
        except Exception, e:
            print '# insertJMPages exception:', e
            traceback.print_exc()

    # 删除网页
    def removeJMPage(self, _key):
        c = _key[:8]
        db_name = "jm" + c
        self.mongo_db.removePage(db_name, c, _key)

    # 查询网页
    def findJMPage(self, _key):
        c = _key[:8]
        db_name = "jm" + c
        return self.mongo_db.findPage(db_name, c, _key)

    # 遍历网页
    def scanJMPage(self, c):
        db_name = "jm" + c
        for pg in self.mongo_db.findPages(db_name, c):
            _key   = pg['key']
            _pages = pg['pages']
            print _key,_pages
            #for k in _pages.keys(): print _key, k

    # 创建索引
    def crtJMIndex(self, c):
        db_name = "jm" + c
        self.mongo_db.crtIndex(db_name, c)

    # 删除表格
    def dropTable(self, c):
        self.mongo_db.dropTable(c)

    def dropJMTableNew(self, c):
        db_name = "jm" + c
        self.mongo_db.dropTableNew(db_name, c)

if __name__ == '__main__':
    pass
    #m = MongofsAccess()
    #m.removeJMPage("2015050618_4_1_item_groupposition_10000006759307")
    #vals = m.findJMPage("2015050618_4_1_item_groupposition_10000006759307")
    #print vals

    #m.scanJMPage("20150506")
    #vals = m.findTBPage('20150303151645_1_61773004_43790383280')
    #vals = m.findTBPage('20150308003057_1_58501945')
    #if vals and vals.has_key('pages'):
    #    _dict = vals['pages']
    #    for (_tag, _content) in _dict.items():
    #        print _tag, _content.encode('utf8', 'ignore')
