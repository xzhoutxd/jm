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
            Common.log('# insertJMPages exception: %s' % e)

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
            #Common.log(_key)
            #Common.log(_pages)
            #for k in _pages.keys(): 
            #    Common.log(k)

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
