#-*- coding:utf-8 -*-
#!/usr/bin/env python

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import re
import time
import random
import Common
from Crawler import Crawler

class JMCrawler(Crawler):
    def __init__(self):
        # parent construct
        Crawler.__init__(self)

        self.crawl_cookie = {}
        self.status_code = ''
        self.history = ''

    def getData(self, url, refers='', decode=True, terminal='1'):
        url = Common.fix_url(url)
        if refers and refers != '':
            refers = Common.fix_url(refers)
        # when null url, exit function
        if not url or not re.search(r'http://', url):
            return None

        # To build header
        _header = self.buildHeader(refers, terminal)

        ## need fix
        # 天猫/淘宝店铺搜索页延时3-10秒
        if re.search(r'htm?search=y', url):
            time.sleep(random.uniform(3, 10))
        # 天猫成交页面延时5-10秒
        elif re.search(r'http://ext.mdskip.taobao.com/\w+/dealRecords.htm?', url):
            time.sleep(random.uniform(5, 10))
        # 淘宝成交页面延时5-10秒
        elif re.search(r'http://detailskip.taobao.com/\w+/show_buyer_list.htm?', url) :
            time.sleep(random.uniform(5, 10))
        # 聚划算商品详情页延时3-5秒
        elif re.search(r'http://detail.ju.taobao.com/home.htm', url) :
            time.sleep(random.uniform(3, 5))
        # 聚划算品牌团页面延迟1-3秒
        elif re.search(r'http://ju.taobao.com/tg/brand_items.htm',url):
            time.sleep(random.uniform(1, 3))
        # 其他页面延时0.1-1秒
        else:
            time.sleep(random.uniform(0.1, 1))

        _cookie = self.session_cookie if self.use_cookie else self.crawl_cookie

        # http Get请求
        if self.use_stream:
            r = self.session.get(url, headers=_header, cookies=_cookie, timeout=self.timeout, stream=True)
        else:
            r = self.session.get(url, headers=_header, cookies=_cookie, timeout=self.timeout)

        # 网页返回码
        self.status_code = r.status_code
        self.history = r.history

        # 网页内容
        data = ''
        if self.use_stream:
            for line in r.iter_lines(): data += (line if line else '')
        else: data = r.content

        # 跟踪cookie
        if not self.use_cookie and len(r.cookies) > 0: self.crawl_cookie = Common.cookieJar2Dict(r.cookies)

        # 网页编码
        f_coder = Common.charset(r.headers.get('content-type'))
        if f_coder == '':
            m = re.search(r'<head>(.+?)</head>', data, flags=re.S)
            if m:
                f_coder = Common.charset(m.group(1))
        self.f_coder = f_coder

        # 关闭结果
        r.close()

        # 网页编码归一化
        if decode and self.f_coder != '' and self.f_coder != self.t_coder: data = data.decode(self.f_coder,'ignore').encode(self.t_coder,'ignore')

        # pc/wap网页异常
        if terminal in ['1', '2']:
            # need fix
            # 异常处理1: 网站deny页
            m = re.search(r'<title>亲，访问受限了</title>', data)
            if m: raise Common.DenypageException("Deny page occurs!")

            m = re.search(r'<title>很抱歉，现在暂时无法处理您的请求-.+?</title>', data)
            if m: raise Common.DenypageException("Deny page occurs!")

            # 异常处理2: 出现淘宝登录页面
            m = re.search(r'<title>淘宝网 - 淘！我喜欢</title>.+?<li class="current"><h2>登录</h2></li>', data, flags=re.S)
            if m: raise Common.NoTBLoginException("Not Taobao login exception occurs!")

            # 异常处理3: 出现验证码页面
            m = re.search(r'<label for="checkcodeInput">验证码：</label>', data)
            if m: raise Common.TBCheckCodeException("Taobao Check code occurs!")

            # 异常处理4: 网页不存在
            m = re.search(r'<title>对不起，您访问的页面不存在！</title>', data)
            if m: raise Common.NoPageException("No page occurs!")

            # 异常处理5: 系统繁忙
            m = re.search(r'<title>【聚划算】聚划算 - 系统繁忙</title>', data)
            if m: raise Common.SystemBusyException("System busy occurs!")

            # 当前的存取控制设定禁止您的请求被接受
            m = re.search(r'<H2>您所请求的网址（URL）无法获取</H2>', data)
            if m: raise Common.DenypageException("Deny page occurs! Access Denied.")

        # 返回抓取结果
        return data
