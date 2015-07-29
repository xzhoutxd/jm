#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import os
import re
import random
import json
import time
import threading
sys.path.append('../base')
import Common as Common
import Config as Config
from JMCrawler import JMCrawler

class Channel():
    '''A class of JM channel'''
    def __init__(self):
        # 抓取设置
        self.crawler = JMCrawler()
        self.crawling_time = Common.now() # 当前爬取时间
        self.crawling_time_s = Common.time_s(self.crawling_time)
        self.crawling_begintime = '' # 本次抓取开始时间
        self.crawling_beginDate = '' # 本次爬取日期
        self.crawling_beginHour = '' # 本次爬取小时

        # 频道信息
        self.platform = '聚美-pc' # 品牌团所在平台
        self.channel_id = '' # 频道id
        self.channel_url = '' # 频道链接
        self.channel_name = '' # 频道name

        # 原数据信息
        self.channel_page = '' # 频道页面html内容
        self.channel_pages = {} # 频道页面内请求数据列表

        # channel acts
        self.channel_sale_acts = []
        self.channel_coming_acts = []

    # 频道页初始化
    def init(self, channel_id, channel_name, channel_url, begin_time):
        self.channel_id = channel_id
        self.channel_name = channel_name
        self.channel_url = channel_url
        self.crawling_begintime = begin_time
        self.crawling_beginDate = time.strftime("%Y-%m-%d", time.localtime(self.crawling_begintime))
        self.crawling_beginHour = time.strftime("%H", time.localtime(self.crawling_begintime))

    def config(self):
        self.channelPage()
        if self.channel_page and self.channel_page != '':
            print '# onsale'
            # onsale
            m = re.search(r'<div id="container">.+?<div id="special_today" class="one_list">(.+?)<div class="zhuang_title special_will_title">', self.channel_page, flags=re.S)
            if m:
                s_p = 1
                acts_info = m.group(1)
                p = re.compile(r'<div class="today_list_item">\s+<div class="item_container">\s+<a class="item_text" href="(.+?)".*?>\s+<div class="item_intro">(.+?)</div>\s+<div class="item_img">.+?<img src="(.+?)".*?/>.+?</a>\s+</div>\s+</div>', flags=re.S)
                for act_info in p.finditer(acts_info):
                    #print act_info.group(0)
                    act_url, act_intro, act_pic_url, act_logo_url, act_name, act_desc, act_discounts, act_times = '', '', '', '', '', '', '', ''
                    act_url, act_intro, act_pic_url = act_info.group(1), act_info.group(2), act_info.group(3)
                    m = re.search(r'<div class="item_logo">\s+<img src="(.+?)".*?>\s+</div>', act_intro, flags=re.S)
                    if m:
                        act_logo_url = m.group(1)
                    m = re.search(r'<p>(.+?)</p>', act_intro, flags=re.S)
                    if m:
                        act_name = m.group(1)
                    m = re.search(r'</p>\s+<span>(.+?)</span>', act_intro, flags=re.S)
                    if m:
                        act_desc = m.group(1)
                    m = re.search(r'</span>\s+<em>(.+?)</em>', act_intro, flags=re.S)
                    if m:
                        act_discounts = re.sub(r'<.+?>', '', m.group(1))
                    m = re.search(r'<div class="zhuang_time">(.+?)</div>', act_intro, flags=re.S)
                    if m:
                        act_times = m.group(1)
                    #m = re.search(r'<div class="item_logo">\s+<img src="(.+?)">\s+</div>\s+<p>(.+?)</p>\s+<span>(.+?)</span>\s+<em>(.+?)</em>\s+<div class="zhuang_time">(.+?)</div>\s+</div>', act_intro, flags=re.S)
                    #if m:
                    #    act_logo_url, act_name, act_desc, act_discounts, act_times = m.group(1), m.group(2), m.group(3), re.sub(r'<.+?>','',m.group(4)), m.group(5)
                    act_id = 0
                    m = re.search(r'from=.+?_(\d+)_pos\d+', act_url)
                    if m:
                        act_id = int(m.group(1))
                    #print self.channel_id, self.channel_name, self.channel_url, s_p, act_id, act_url, act_name, act_desc, act_logo_url, act_pic_url, act_times, act_discounts, self.crawling_begintime
                    a_val = (self.channel_id, self.channel_name, self.channel_url, s_p, act_id, act_url, act_name, act_desc, act_logo_url, act_pic_url, act_times, act_discounts, self.crawling_begintime)
                    print a_val
                    self.channel_sale_acts.append(a_val)
                    s_p += 1
                    
            print '# will coming soon'
            # will
            m = re.search(r'<div id="container">.+?<div id="special_will">(.+?)</div>\s+</div>\s+<div class="lineblank"></div>', self.channel_page, flags=re.S)
            if m:
                w_p = 1
                will_acts_info = m.group(1)
                #print will_acts_info
                p = re.compile(r'<div class="two_imgbox">\s+<a href="(.+?)".*?>\s*<img src="(.+?)".*?/>\s*</a>.+?</div>\s+<a.+?>(.+?)</a>', flags=re.S)
                for act_info in p.finditer(will_acts_info):
                    act_url, act_intro, act_pic_url, act_logo_url, act_name, act_desc, act_discounts, act_times = '', '', '', '', '', '', '', ''
                    act_url, act_pic_url, act_intro = act_info.group(1), act_info.group(2), act_info.group(3)
                    m = re.search(r'<p class="two_title">(.+?)</p>', act_intro, flags=re.S)
                    if m:
                        act_name = m.group(1)
                    m = re.search(r'<p class="two_intro">(.+?)</p>', act_intro, flags=re.S)
                    if m:
                        act_desc = m.group(1)
                    m = re.search(r'<p class="two_zhe">(.+?)</p>', act_intro, flags=re.S)
                    if m:
                        act_discounts = re.sub(r'<.+?>', '', m.group(1).strip())
                    m = re.search(r'<div class="two_logo">\s+<img src="(.+?)".*?>\s+<span class="time_section">(.+?)</span>\s+</div>', act_intro, flags=re.S)
                    if m:
                        act_logo_url, act_times = m.group(1), m.group(2)
                    act_id = 0
                    m = re.search(r'from=.+?_(\d+)_pos\d+', act_url)
                    if m:
                        act_id = int(m.group(1))
                    #print self.channel_id, self.channel_name, self.channel_url, w_p, act_id, act_url, act_name, act_desc, act_logo_url, act_pic_url, act_times, act_discounts, self.crawling_begintime
                    a_val = (self.channel_id, self.channel_name, self.channel_url, w_p, act_id, act_url, act_name, act_desc, act_logo_url, act_pic_url, act_times, act_discounts, self.crawling_begintime)
                    print a_val
                    self.channel_coming_acts.append(a_val)
                    w_p += 1

            
    def channelPage(self):
        if self.channel_url and self.channel_url != '':
            data = self.crawler.getData(self.channel_url, Config.jm_home)
            if not data and data == '': raise Common.InvalidPageException("# channelPage:not find channel page,channel_id:%s,channel_name:%s,channel_url:%s"%(str(self.channel_id), self.channel_name, self.channel_url))
            if data and data != '':
                self.channel_page = data
                self.channel_pages['channel-home'] = (self.channel_url, data)


    def antPage(self, val):
        channel_id, channel_name, channel_url, begin_time = val
        self.init(channel_id, channel_name, channel_url, begin_time)
        self.config()


if __name__ == '__main__':
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    c = Channel()
    val = (1,'美妆','http://beauty.jumei.com/?from=all_null_index_top_nav_cosmetics&lo=3481&mat=30573',Common.now())
    c.antPage(val)
    time.sleep(1)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

