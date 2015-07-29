#-*- coding:utf-8 -*-
#!/usr/bin/env python

from sys import path

import threading
import redis
import traceback
path.append(r'../base')
import Environ as Environ
import Config  as Config

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
            print '# RedisPool write exception:', e
            traceback.print_exc()

    def read(self, keys, _db=0):
        try:
            _key  = Config.delim.join(keys)
            _pool = self.getPool(_db)
            if _pool:
                r    = redis.Redis(connection_pool=_pool)            
                _val = r.get(_key)
                return _val
        except Exception, e:
            print '# RedisPool read exception:', e
            traceback.print_exc()

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
            print '# RedisPool scan db exception:', e
            traceback.print_exc()
            return []

    # 数据库计数
    def count_db(self, _db=0):
        try:
            _pool= self.getPool(_db)
            r    = redis.Redis(connection_pool=_pool)
            _size= r.dbsize()
            return _size            
        except Exception, e:
            print '# RedisPool count db exception:', e
            traceback.print_exc()

    # 清空数据库
    def flush_db(self, _db=0):
        try:
            _pool= self.getPool(_db)
            r    = redis.Redis(connection_pool=_pool)
            r.flushdb()
        except Exception, e:
            print '# RedisPool flush db exception:', e
            traceback.print_exc()

    def exists(self, keys, _db=0):
        try:
            _pool = self.getPool(_db)
            r     = redis.Redis(connection_pool=_pool)            
            _key  = Config.delim.join(keys)
            _ret  = r.exists(_key)
            return _ret
        except Exception, e:
            print '# RedisPool exists exception:', e
            traceback.print_exc()

    def remove(self, keys, _db=0):
        try:
            _pool = self.getPool(_db)
            r     = redis.Redis(connection_pool=_pool)            
            _key  = Config.delim.join(keys)
            r.delete(_key)

        except Exception, e:
            print '# RedisPool remove exception:', e
            traceback.print_exc()

if __name__ == '__main__1':
    pool1 = RedisPool()
    pool2 = RedisPool()
    print pool1 is pool2
    
if __name__ == '__main__':
    pool = RedisPool()
    for i in [3,4,5]: pool.flush_db(i)

