#-*- coding:utf-8 -*-
#!/usr/bin/env python

import socket
import threading

# 是否只采用IPV4查询
CON_ONLY_CHECK_IPV4 = True

# getaddrinfo函数原型
#def getaddrinfo(host, port, family=None, socktype=None, proto=None, flags=None):
def _getaddrinfo(host, port, *plist, **pdict): # 代理函数
    dns_key = str((host, port, plist, pdict))
    dns_value = None
    try:
        dns_lock.acquire()
        dns_value = dns_cache.get(dns_key, None)
    finally:
        dns_lock.release()

    if dns_value:
        return dns_value
    if CON_ONLY_CHECK_IPV4:
        if plist:
            plist = list(plist)
            plist[0] = 2
        else:
            pdict['family'] = 2
    dns_value = t_getaddrinfo(host, port, *plist, **pdict)
    try:
        dns_lock.acquire()
        dns_cache[dns_key] = dns_value
    finally:
        dns_lock.release()
    return dns_value

# 清空缓存
def Clear_DNS_Cache():
    try:
        dns_lock.acquire()
        dns_cache.clear()
    finally:
        dns_lock.release()

# 初始化
if not hasattr(socket, 'dns_cached'):
    dns_cache = {}
    dns_lock = threading.Lock()
    t_getaddrinfo = socket.getaddrinfo
    socket.getaddrinfo = _getaddrinfo
    socket.dns_cached = True

# 清空dns
Clear_DNS_Cache()
