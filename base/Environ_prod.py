#-*- coding:utf-8 -*-
#!/usr/bin/env python

import Common

######################## 拨号服务器  #####################
dial_ip     = '192.168.7.214'
dial_port   = 9075
magic_num   = '%xiaoshu-dialing-9999%'

######################## Redis配置  #####################
redis_ip, redis_port, redis_passwd = '192.168.7.211', 6380, 'bigdata1234'  # 919测试
redis_config = {
    0  : (redis_ip, redis_port, redis_passwd),    # default      db
    1  : (redis_ip, redis_port, redis_passwd),    # tm/tb shop   db
    2  : (redis_ip, redis_port, redis_passwd),    # tm/tb item   db
    3  : (redis_ip, redis_port, redis_passwd),    # vip   act    db
    4  : (redis_ip, redis_port, redis_passwd),    # vip   item   db
    5  : (redis_ip, redis_port, redis_passwd),    # vip   item   db
    6  : (redis_ip, redis_port, redis_passwd),    # vip   actmap db    
    9  : (redis_ip, redis_port, redis_passwd),    # cookie       db
    10 : (redis_ip, redis_port, redis_passwd),    # queue        db
    20 : (redis_ip, redis_port, redis_passwd),    # jhs   act    db
    21 : (redis_ip, redis_port, redis_passwd),    # jhs   item   db
    30 : (redis_ip, redis_port, redis_passwd),    # jm    act  db
    31 : (redis_ip, redis_port, redis_passwd),    # jm    item db
    32 : (redis_ip, redis_port, redis_passwd),    # jm    sku  db
    100: (redis_ip, redis_port, redis_passwd)     # queue  db
}

redis_ip, redis_port, redis_passwd = '127.0.0.1', 6379, 'bigdata1234'  # 919测试
redis_config_dev = {
    0  : (redis_ip, redis_port, redis_passwd),    # default    db
    1  : (redis_ip, redis_port, redis_passwd),    # tm/tb shop db
    2  : (redis_ip, redis_port, redis_passwd),    # tm/tb item db
    3  : (redis_ip, redis_port, redis_passwd),    # vip   act  db
    4  : (redis_ip, redis_port, redis_passwd),    # vip   item db
    5  : (redis_ip, redis_port, redis_passwd),    # vip   item db
    6  : (redis_ip, redis_port, redis_passwd),    # vip   actmap db    
    9  : (redis_ip, redis_port, redis_passwd),    # cookie     db
    10 : (redis_ip, redis_port, redis_passwd)     # queue      db
}

######################## Mysql配置  ######################
# # 919测试
mysql_config = {
    'jm'   : {'host':'192.168.7.215', 'user':'jm',    'passwd':'123456', 'db':'jm'}
}

######################## Mongodb配置  #####################
# 919测试
mongodb_config = {'host':'192.168.7.211', 'port':9073}

# mongodb gridfs collection名
mongodb_fs = 'fs'

# mongodb bson字段的最大长度16MB = 16777216，预留40%用作bson结构
mongodb_maxsize= int(16777216*0.5)

# 截断超长网页字符串
def truncatePage(s):
    # 截断网页字符串，以符合mongodb字段长度限制
    return s if len(s) < mongodb_maxsize else s[0:mongodb_maxsize]

