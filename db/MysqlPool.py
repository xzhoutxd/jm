#-*- coding:utf-8 -*-
#!/usr/bin/env python

from sys import path
path.append(r'../base')

import MySQLdb
from DBUtils.PooledDB import PooledDB
import Common as Common

class MysqlPool:
    '''A class of connect pool to Mysql Database'''
    def __init__(self, args):
        # mysql connect params
        args["charset"]    = "utf8"
        args["setsession"] =['SET AUTOCOMMIT = 1']

        # mysql connect pool
        self.pooldb = PooledDB(MySQLdb, **args)

    def __del__(self):
        if self.pooldb: self.pooldb.close()

    def select(self, sql, args=None):
        try:
            conn= self.pooldb.connection()
            cur = conn.cursor()
            cur.execute(sql, args)
            results = cur.fetchall()
            cur.close()
            conn.close()
            return results
        except Exception as e:
            Common.log('# MyDatabase select exception: %s %s' % (e, sql))
            Common.log(args)
            return None

    def execute(self, sql, args=None):
        try:
            conn= self.pooldb.connection()
            cur = conn.cursor()
            cur.execute(sql, args)
            cur.close()
            conn.close()
        except Exception as e:
            Common.log('# MyDatabase execute exception: %s %s' % (e, sql))
            Common.log(args)

    def executemany(self, sql, args_list=[]):
        try:
            conn= self.pooldb.connection()
            cur = conn.cursor()
            cur.executemany(sql, args_list)
            cur.close()
            conn.close()
        except Exception as e:
            Common.log('# MyDatabase executemany exception: %s %s' % (e, sql))
            Common.log(args_list)

# mysql database pool
mysql_config = {
    #'jm' : {'host':'192.168.1.113', 'user':'jm', 'passwd':'123456',  'db':'jm'},
    # test
    'jm' : {'host':'127.0.0.1', 'user':'root', 'passwd':'zhouxin',  'db':'jm'},
}

g_jmDbPool = MysqlPool(mysql_config['jm'])

