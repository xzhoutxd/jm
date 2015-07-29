#-*- coding:utf-8 -*-
#!/usr/bin/env python

from sys import path
path.append(r'../base')

import pymongo  # 只能用2.8版本
import gridfs
import Environ as Environ

class MongoPool(object):
    '''A class of mongodb connection pool'''
    def __init__(self):
        # 数据库配置
        db_config    = Environ.mongodb_config
        _host, _port = db_config['host'], db_config['port']

        # fs表名
        self.fs_name = Environ.mongodb_fs

        # mongodb connect pool
        self.mongo   = pymongo.MongoClient(host=_host, port=_port)

        # Jm网页库
        self.jm_db  = self.mongo["jm"]

        # 网页库
        self._db = None

        # fs
        self._fs = None

    def __del__(self):
         self.mongo.close()

    # 创建索引
    def crtJMIndex(self, c):
        self.jm_db[c].ensure_index('key', unique=True)

    # 删除表格
    def dropTable(self, c):
        self.jm_db[c].drop()

    def insertJMPage(self, c, _data):
        self.jm_db[c].insert(_data)

    def removeJMPage(self, c, _key):
        self.jm_db[c].remove({"key":_key})

    def findJMPage(self, c, _key):
        return self.jm_db[c].find_one({"key":_key})

    def findJMPages(self, c):
        return self.jm_db[c].find()

    # NEW
    @property
    def db(self):
        return self._db

    # 创建库
    @db.setter
    def db(self, db_name):
        self._db = self.mongo[db_name]

    # 创建索引
    def crtIndex(self, db_name, c):
        self.db = db_name
        self.db[c].ensure_index('key', unique=True)
    
    # 删除表
    def dropTableNew(self, db_name, c):
        self.db = db_name
        self.db[c].drop()

    def insertPage(self, db_name, c, _data):
        key  = _data["key"]
        pages = _data["pages"]
        f_id = self.putfsPage(db_name, pages)
        _key_data = {"key":key,"file_id":f_id}
        self.db = db_name
        self.db[c].insert(_key_data)

    def removePage(self, db_name, c, _key):
        self.db = db_name
        _key_data = self.db[c].find_one({"key":_key})
        f_id = _key_data["file_id"]
        self.deletefsPage(db_name, f_id)
        self.db[c].remove({"key":_key})

    def findPage(self, db_name, c, _key):
        self.db = db_name
        _key_data = self.db[c].find_one({"key":_key})
        f_id = _key_data["file_id"]
        return self.getfsPage(db_name, f_id)

    def findPages(self, db_name, c):
        page_list = []
        self.db = db_name
        for _key_data in self.db[c].find():
            f_id = _key_data["file_id"]
            page_list.append({"key":_key_data["key"],"pages":self.getfsPage(db_name, f_id)})
        return page_list

    # gridfs page
    @property
    def fs(self):
        return self._fs

    @fs.setter
    def fs(self, db_name):
        self.db = db_name
        self._fs = gridfs.GridFS(self.db,self.fs_name)

    def deletefsPage(self, db_name, _id):
        self.fs = db_name
        self.fs.delete(_id)

    def putfsPage(self, db_name, _data):
        self.fs = db_name
        f_id = self.fs.put(str(_data))
        return f_id

    def getfsPage(self, db_name, _id):
        self.fs = db_name
        out = self.fs.get(_id)
        return out.read()

if __name__ == '__main__':
    mongo = MongoPool()
    mongo = None


