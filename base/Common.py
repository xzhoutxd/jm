#-*- coding:utf-8 -*-
#!/usr/bin/env python

import sys
import re
import os
import random
import time
import datetime
import calendar
import urllib
import traceback
import Logger as Logger

# defined exception
class InvalidParameterException(Exception):
    pass

class DenypageException(Exception):
    pass

class NoPageException(Exception):
    pass

class InvalidPageException(Exception):
    pass

# network fails exception
class NetworkFailureException(Exception):
    pass

# not taobao login exception
class NoTBLoginException(Exception):
    pass

# taobao check code exception
class TBCheckCodeException(Exception):
    pass

# not activity exception
class NoActivityException(Exception):
    pass

# not activity item exception
class NoActivityItemException(Exception):
    pass

# offshelf item exception
class OffshelfItemException(Exception):
    pass

# postage/delivery/refund item exception
class NotStatItemException(Exception):
    pass

# not shop exception
class NoShopException(Exception):
    pass

# not item exception
class NoItemException(Exception):
    pass

# not shop item exception
class NoShopItemException(Exception):
    pass

# system busy exception
class SystemBusyException(Exception):
    pass

# retry exception
class RetryException(Exception):
    pass

def traceback_log():
    if Logger.logger:
        Logger.logger.info('#####--Traceback Start--#####')
        tp,val,td = sys.exc_info()
        for file, lineno, function, text in traceback.extract_tb(td):
            Logger.logger.info('exception traceback err:%s,line:%s,in:%s'%(file, lineno, function))
            Logger.logger.info(text)
        Logger.logger.info('exception traceback err:%s,%s,%s'%(tp,val,td))
        Logger.logger.info('#####--Traceback End--#####')
    else:
        print '#####--Traceback Start--#####'
        tp,val,td = sys.exc_info()
        for file, lineno, function, text in traceback.extract_tb(td):
            print "exception traceback err:%s,line:%s,in:%s"%(file, lineno, function)
            print text
        print "exception traceback err:%s,%s,%s"%(tp,val,td)
        print '#####--Traceback End--#####'

def log(e, level='INFO'):
    if Logger.logger:
        if level.upper() == 'INFO':
            Logger.logger.info(e)
        elif level.upper() == 'DEBUG':
            Logger.logger.debug(e)
        elif level.upper() == 'WARNING':
            Logger.logger.warn(e)
        elif level.upper() == 'ERROR':
            Logger.logger.error(e)
        elif level.upper() == 'CRITICAL':
            Logger.logger.critical(e)
        else:
            Logger.logger.info(e)
    else:
        print e


# 单体模式
def singleton(cls, *args, **kw):  
    instances = {}  
    def _singleton():  
        if cls not in instances:  
            instances[cls] = cls(*args, **kw)  
        return instances[cls]  
    return _singleton

# 创建目录
def createPath(p):
    if not os.path.exists(p): os.makedirs(p)

# 全局变量
template_str = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
template_low = 'abcdefghijklmnopqrstuvwxyz0123456789'
template_tag = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789&='
template_num = '0123456789'


### time functions ###
def date2timestamp(d):
    return float(time.mktime(d.timetuple()))

def nowhour_s(fmt='%H'):
    return time.strftime(fmt, time.localtime(time.time()))

# 取当前时间
def now():
    return time.time()

# 当前时间字符串
def now_s(fmt='%Y-%m-%d %H:%M:%S'):
    return time.strftime(fmt, time.localtime(time.time()))

# 当前时间字符串
def now_ss():
    return now_s('%Y%m%d%H%M%S')

def time_s(t, fmt='%Y-%m-%d %H:%M:%S'):
    try:
        t = float(t)
        return time.strftime(fmt, time.localtime(t))
    except:
        return ''

def time_ss(t):
    return time_s(t, '%Y%m%d%H%M%S')

def timestamp(_ms = 0):
    return time.time() * 1000 + _ms

def today_s(fmt='%Y-%m-%d'):
    return time.strftime(fmt, time.localtime(time.time()))

def today_ss():
    return today_s('%Y%m%d')

def today_sss():
    return '{dt.year}{dt.month}{dt.day}'.format(dt=datetime.datetime.now())

def day_s(t, fmt='%Y-%m-%d'):
    return '' if t == '' else time.strftime(fmt, time.localtime(t))

def day_ss(t):
    return day_s(t, '%Y%m%d')

def dt_to_t(dt):
    return float(time.mktime(dt.timetuple()))

def str2timestamp(s, fmt='%Y-%m-%d %H:%M:%S'):
    try:
        dt = datetime.datetime.strptime(s.strip(), fmt)
        return dt_to_t(dt)
    except:
        return 0.0

def date_s(t, fmt='%Y-%m-%d'):
    s = ''
    if type(t) is float or type(t) is int:
        s = time.strftime(fmt, time.localtime(t))
    return s


# To compute time delta
def timeDelta(t, h='00:00:00'):
    return str2timestamp(day_s(t) + ' ' + h, '%Y-%m-%d %H:%M:%S')

def yyyymmdd(t, fmt='%Y-%m-%d'):
    return str2timestamp(time_s(t, fmt), fmt)

def yyyymmddhh24(t, fmt='%Y-%m-%d %H:00:00'):
    return str2timestamp(time_s(t, fmt), fmt)

### datetime functions ###
def add_days(n = 0, fmt='%Y-%m-%d'):
    dt = datetime.datetime.now()
    nDays = datetime.timedelta(days=n)
    dt = dt + nDays
    return dt.strftime(fmt)

def dt_to_s(dt, fmt='%Y-%m-%d'):
    return datetime.datetime.strftime(dt, fmt)    

def s_to_dt(s, fmt='%Y-%m-%d'):
    return datetime.datetime.fromtimestamp(time.mktime(time.strptime(s, fmt))) 

def t_add_days(t, n = 0):
    s   = time_s(t, "%Y-%m-%d")
    dt  = s_to_dt(s)
    dt += datetime.timedelta(days=n)
    return dt

def s_add_days(s, n = 0):
    dt  = s_to_dt(s)
    dt += datetime.timedelta(days=n)
    return dt

def s_add_months(s, n = 0):
    dt    = s_to_dt(s)
    month = dt.month - 1 + n
    year  = dt.year + month / 12
    month = month % 12 + 1
    day = min(dt.day,calendar.monthrange(year,month)[1])
    dt  = dt.replace(year=year, month=month, day=day)
    return dt
    
def s_add_years(s, n = 0):
    dt   = s_to_dt(s)
    year = dt.year + n
    dt   = dt.replace(year=year)
    return dt

def add_hours(ts, n=0, fmt='%Y-%m-%d %H:%M:%S'):
    dt = datetime.datetime.fromtimestamp(ts)
    nHours = datetime.timedelta(hours=n)
    dt = dt + nHours
    return dt.strftime(fmt)

def add_hours_D(ts, n=0, fmt='%Y-%m-%d'):
    dt = datetime.datetime.fromtimestamp(ts)
    nHours = datetime.timedelta(hours=n)
    dt = dt + nHours
    return dt.strftime(fmt)

def subTS_hours(ts1, ts2):
    return (ts1 - ts2)/3600


##################
# 随机IP地址
def randIp():
    ips = []
    ips.append(str(random.randint(111, 251)))
    ips.append(str(random.randint(121, 240)))
    ips.append(str(random.randint( 40, 251)))
    ips.append(str(random.randint(150, 253)))
    return '.'.join(ips)

# 随机用户名
def rand_user(pfx = 'tb'):
    s = '%s%06d' %(pfx, random.randint(1, 999999))
    return s

# 随机字符串
def rand_s(template, length):
    ns = []
    for i in range(0, length):
        ns.append(random.choice(template))
    return ''.join(ns)

def rand_n(n=4):
    return rand_s(template_num, n)

# 计算差集
def diffSet(A, B):
    return list(set(A).difference(set(B)))

# 计算并集
def unionSet(A, B):
    return list(set(A).union(set(B)))

import HTMLParser
gHtmlParser = HTMLParser.HTMLParser()

def htmlDecode(data):
    return gHtmlParser.unescape(data)

def jsonDecode(data):
    return data.decode("unicode-escape")

def urlDecode(data):
    return urllib.unquote_plus(data)

def urlCode(data):
    return urllib.quote_plus(data)

def urlEncode(data,from_cs='utf-8',to_cs='gbk'):
    if from_cs != to_cs:
        data = data.decode(from_cs).encode(to_cs)
    return urllib.quote(data)

def htmlContent(s, c=''):
    return re.sub('<(.+?)>', c, s, flags=re.S)

def trim_s(s):
    if s and len(s) > 0:
        s = re.sub('\s|　','', s)
    return s

def trim_ch(s):
    if s and len(s) > 0:
        s = re.sub('\n|\r','', s)
    return s

def decode(s):
    return s.decode('utf-8','ignore').encode('gbk','ignore')

def decode_r(s):
    return s.decode('gbk','ignore').encode('utf-8','ignore')

def quotes_s(s):
    return re.sub(r'\'', '\\\'', s)

def htmlDecode_s(s):
    return s if s.find(r'&#') == -1 else htmlDecode(s)


# 计算中位数
def median(numbers):
    n = len(numbers)
    if n == 0: return None

    copy = numbers[:]
    copy.sort()
    if n & 1:
        m_val = copy[n/2]
    else:
        # 改进中位数算法：数值列表长度为偶数时，取中间小的数值
        m_val = copy[n/2-1]
        # 正常中位数算法：数值列表长度为偶数时，取中间2个数值的平均
        #m_val = (copy[n/2-1] + copy[n/2])/2
    return m_val

# To get url template
def urlTemplate(_type):
    template = None
    if _type == '1':
        template = 'http://detail.tmall.com/item.htm?id=%s'
    elif _type == '2':
        template = 'http://item.taobao.com/item.htm?id=%s'
    return template

# 随机json回调名称
def jsonCallback(n=4):
    return 'jsonp%s' %rand_s(template_num, n)

def cookieJar2Dict(cj):
    cj_d = {}
    for c in cj:
        cj_d[c.name] = c.value
    return cj_d

def charset(data):
    coder = ''
    if data and data != '':
        data = re.sub('"|\'| ', '', data.lower())
        if re.search(r'charset=utf-8', data) or re.search(r'charset="utf-8"', data, flags=re.S):
            coder = 'utf-8'
    return coder

# fix ju url 
def fix_url(url):
    if url:
        m = re.search(r'^/+',str(url))
        if m:
            url = re.sub(r'^/+','',url)

        if type(url) is str and url != '':
            if url.find('http://') == -1:
                url = 'http://' + url
        else:
            if str(url).find('http://') == -1:
                url = 'http://' + url

    return url


# local ip
import socket
def local_ip():
    #host = socket.gethostname()
    #ip = socket.gethostbyname(host)
    #return ip
    return '192.168.5.23'

def agg(num,s='%s'):
    return ','.join([ s for i in xrange(num)])
