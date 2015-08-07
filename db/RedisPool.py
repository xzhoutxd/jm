#-*- coding:utf-8 -*-
#!/usr/bin/env python

from sys import path

import threading
import redis
import traceback
path.append(r'../base')
import Environ as Environ
import Config  as Config
import Common  as Common

class RedisPool:
    '''A class of connect pool to Redis Database'''
    def __init__(self, redis_config=Environ.redis_config):
        # thread lock
        self.mutex         = threading.Lock()
        
        # 数据库连接池
        self.redis_pools   = {}

        # redis数据库
        self.redis_config  = redis_config

    def __del__(self):
        for pool in self.redis_pools.values():
            pool.disconnect()

    def createPool(self, _db=0):
        # 数据库ID不存在, 则直接返回空
        if not self.redis_config.has_key(_db):
            return None

        _host, _port, _passwd = self.redis_config[_db]

        _pool = redis.ConnectionPool(host=_host, port=_port, db=_db, password=_passwd)

        if self.mutex.acquire(1):
            self.redis_pools[_db] = _pool
            self.mutex.release()

    def getPool(self, _db=0):
        if not self.redis_pools.has_key(_db):
            self.createPool(_db)

        return self.redis_pools[_db]

    def write(self, keys, val, _db=0):
        try:
            _key  = Config.delim.join(keys)
            _val  = val
            _pool = self.getPool(_db)
            r = redis.Redis(connection_pool=_pool)
            r.set(_key, _val)
        except Exception, e:
            Common.log('# RedisPool write exception: %s' % e)

    def read(self, keys, _db=0):
        try:
            _key  = Config.delim.join(keys)
            _pool = self.getPool(_db)
            if _pool:
                r    = redis.Redis(connection_pool=_pool)            
                _val = r.get(_key)
                return _val
        except Exception, e:
            Common.log('# RedisPool read exception: %s' % e)

    # 扫描数据库内容
    def scan_db(self, _db=0):
        try:
            _vals = []
            _pool= self.getPool(_db)
            r    = redis.Redis(connection_pool=_pool)       
            for key in r.scan_iter():
                val = r.get(key)
                if not val: continue
                _vals.append((key, val))
            return _vals

        except Exception, e:
            Common.log('# RedisPool scan db exception: %s' % e)
            return []

    # 数据库计数
    def count_db(self, _db=0):
        try:
            _pool= self.getPool(_db)
            r    = redis.Redis(connection_pool=_pool)
            _size= r.dbsize()
            return _size            
        except Exception, e:
            Common.log('# RedisPool count db exception: %s' % e)

    # 清空数据库
    def flush_db(self, _db=0):
        try:
            _pool= self.getPool(_db)
            r    = redis.Redis(connection_pool=_pool)
            r.flushdb()
        except Exception, e:
            Common.log('# RedisPool flush db exception: %s' % e)

    def exists(self, keys, _db=0):
        try:
            _pool = self.getPool(_db)
            r     = redis.Redis(connection_pool=_pool)            
            _key  = Config.delim.join(keys)
            _ret  = r.exists(_key)
            return _ret
        except Exception, e:
            Common.log('# RedisPool exists exception: %s' % e)

    def remove(self, keys, _db=0):
        try:
            _pool = self.getPool(_db)
            r     = redis.Redis(connection_pool=_pool)            
            _key  = Config.delim.join(keys)
            r.delete(_key)

        except Exception, e:
            Common.log('# RedisPool remove exception: %s' % e)

if __name__ == '__main__':
    pass

