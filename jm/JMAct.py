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

class Act():
    '''A class of JM activity'''
    def __init__(self):
        # 品牌团抓取设置
        self.crawler = JMCrawler()
        self.crawling_time = Common.now() # 当前爬取时间
        self.crawling_time_s = Common.time_s(self.crawling_time)
        self.crawling_begintime = '' # 本次抓取开始时间
        self.crawling_beginDate = '' # 本次爬取日期
        self.crawling_beginHour = '' # 本次爬取小时
        self.crawling_confirm = 1 # 本活动是否需要爬取 1:没有开团需要抓取 2:已经开团 0:只需要更新商品位置

        # 类别
        self.act_platform = 1 # 品牌团所在平台 1:聚美-pc
        self.channel_id = 0 # 品牌团所在频道
        self.channel_name = '' # 品牌团所在频道
        self.channel_url = '' # 品牌团所在频道url
        self.category_id = 0 # 品牌团所在类别Id
        self.category_name = '' # 品牌团所在类别Name
        self.act_position = 0 # 品牌团所在类别位置
        self.front_category_id = 0 # 品牌团所在前端类别Id
        self.category_type = '0' # 品牌团分类的类型,'0':默认 '1': '2':
        self.sub_nav_name = '' # 活动所在分类下子导航Name

        # 是否在首页展示
        self.home_acts = {} # 首页品牌团信息
        self.act_home = 0 # 是否在首页展示,0:不在,1:存在
        self.act_home_position = '' # 在首页展示的位置
        self.act_home_dataType = '' # 在首页展示的所属栏目

        # 品牌团信息
        self.act_id = '' # 品牌团Id
        self.act_url = '' # 品牌团链接
        self.act_url_tail = '' # 品牌团链接后缀
        self.act_name = '' # 品牌团Name
        self.act_desc = '' # 品牌团描述
        self.act_logopic_url = '' # 品牌团Logo图片链接
        self.act_enterpic_url = '' # 品牌团展示图片链接
        self.act_times = '' # 品牌团时间区间
        self.act_discounts = '' # 品牌团折扣信息
        self.act_start_time = 0.0 # 品牌团开团时间
        self.act_start_time_s = '' # 品牌团开团时间字符串形式
        self.act_start_date = '' # 品牌团开团日期
        self.act_end_time = 0.0 # 品牌团结束时间
        self.act_end_time_s = '' # 品牌团结束时间字符串形式
        self.act_status = '' # 品牌团状态
        self.act_sign = 1 # 品牌团标识 1:普通品牌团,2:拼团,3:
        self.act_other_ids = '' # 如果是拼团, 其他团的ID
        self.act_brand = '' # 品牌团品牌信息
        self.act_brand_id = '' # 品牌团品牌Id

        # 店铺信息
        self.act_seller_id = '' # 品牌团卖家Id
        self.act_seller_name = '' # 品牌团卖家Name (回填)
        self.act_shop_id = '' # 品牌团店铺Id (回填)
        self.act_shop_name = '' # 品牌团店铺Name (回填)

        # 品牌团交易信息
        self.act_soldcount = 0 # 品牌团成交数
        self.act_remindnum = 0 # 品牌团关注人数
        self.act_discount = '' # 品牌团打折
        self.act_coupon = 0 # 品牌团优惠券, 默认0没有
        self.act_coupons = [] # 优惠券内容list

        # 品牌团商品
        self.act_itemids = []
        self.act_itemval_list = []

        # 原数据信息
        self.act_pagedata = '' # 品牌团所在数据项所有内容
        self.act_page = '' # 品牌团页面html内容
        self.act_pages = {} # 品牌页面内请求数据列表

    # 品牌团信息
    def itemConfig(self):
        page = self.act_page
        m = re.search(r'JM\.brand_id\s*=\s*(\d+);', page, flags=re.S)
        if m:
            self.act_brand_id = m.group(1)
        m = re.search(r'JM\.special_start_time\s*=\s*(\d+);', page, flags=re.S)
        if m:
            self.act_start_time = m.group(1)

        self.actUrltail(page)

    def actUrltail(self, page):
        m = re.search(r'JM\.specialSym\s*=\s*"(.+?)";', page, flags=re.S)
        if m:
            self.act_url_tail = m.group(1)
        else:
            m = re.search(r'/(\w+)\.html', self.act_url)
            if m:
                self.act_url_tail = m.group(1)
            else:
                m = re.search(r'/.+?-(\w+)\.html', self.act_url)
                if m:
                    self.act_url_tail = m.group(1)

    # 从品牌团页获取商品数据
    def actItems(self):
        page = self.act_page
        if self.act_url_tail == '':
            self.actUrltail(page)

        i_p = 0
        p = re.compile(r'<.+? href="(http://www.jumei.com/i/deal/.+?)".+?>', flags=re.S)
        i_p = self.findItemsByUrl(p, page, i_p)

        p = re.compile(r'<.+? href="(http://www.jumeiglobal.com/deal/.+?)".+?>', flags=re.S)
        i_p = self.findItemsByUrl(p, page, i_p)

        p = re.compile(r'<.+? href="(http://item.jumei.com/.+?)".+?>', flags=re.S)
        i_p = self.findItemsByUrl(p, page, i_p)

        p = re.compile(r'<.+? href="(http://item.jumeiglobal.com/.+?)".+?>', flags=re.S)
        i_p = self.findItemsByUrl(p, page, i_p)

        # ajax
        i_p = self.actAjax(page, i_p)

    def findItemsByUrl(self, p, page, i_p):
        for url_str in p.finditer(page):
            i_p += 1
            i_url = url_str.group(1)
            #print i_url, i_p
            self.itemVal((i_url,i_p))
        return i_p

    # 品牌团ajax
    def actAjax(self, page, i_p):
        p = re.compile(r'<div class="act_container" data-model-id\s*=\s*"(.+?)" data-floor-id="(.+?)" data-preview-id="(.+?)".+?>', flags=re.S)
        for a_container in p.finditer(page):
            a_model, f_id, p_id = a_container.group(1), a_container.group(2), a_container.group(3)
            if int(a_model) == 3:
                i_p = self.actAjax1(a_model, f_id, p_id, i_p)

        return i_p

    # ajax type 1
    def actAjax1(self, a_model, f_id, p_id, i_p):
        p_url = 'http://hd.jumei.com/ajax/get_shelfmodel%s/%s_%s_%s_%s_%s_%s_ShellCallbackDataShow%s.json?callback=ShellCallbackDataShow%s'
        p_size = 30
        p_index = 0
        a_url = p_url % (str(a_model), str(self.act_id), str(a_model), str(p_id), str(p_size), str(p_index), str(self.act_start_time), str(f_id), str(f_id)) 
        #print a_url
        result = self.get_itemjson(a_url, self.act_url, 'ShellCallbackDataShow%s' % (str(f_id)))
        i_p = self.parse_item(result, i_p, p_id, p_index)
        # 分页接口中获取数据
        t, r_data = result
        totalPage = 1
        if t == 'd' and r_data.has_key('data') and r_data['data'].has_key('count_num'):
            totalPage = int(r_data['data']['count_num']) /  p_size + 1
        elif t == 's':
            m = re.search(r'"count_num":(\d+),', r_data, flags=re.S)
            if m:
                totalPage = int(m.group(1)) / p_size + 1
        if totalPage > 1:
            for page_i in range(2, totalPage+1):
                p_index = (page_i - 1) * p_size
                a_url = p_url % (str(a_model), str(self.act_id), str(a_model), str(p_id), str(p_size), str(p_index), str(self.act_start_time), str(f_id), str(f_id))
                #print a_url
                result = self.get_itemjson(a_url, self.act_url, 'ShellCallbackDataShow%s' % (str(f_id)))
                i_p = self.parse_item(result, i_p, p_id, p_index)

        return i_p

    def get_itemjson(self, a_url, refers, a_back):
        result_data = None
        r_page = self.crawler.getData(a_url, refers)
        if not r_page or r_page == '': raise Common.InvalidPageException("# get_itemjson: get item json data empty, url:%s."%(a_url))
        m = re.search(r'%s\((.+?)\);$' % a_back, r_page, flags=re.S)
        if m:
            result = m.group(1)
        else:
            raise Common.InvalidPageException("# get_itemjson: not get item json data, url:%s, result:%s."%(a_url, str(r_page)))
        try:
            result_data = json.loads(result)
            return ('d', result_data)
        except Exception as e:
            print '# exception err in get_jsonData load json:',e
            print '# return string:',result
            return ('s', result)

    def parse_item(self, result, i_p, p_id, p_index):
        t, r_data = result
        if t == 'd':
            if r_data.has_key('data') and r_data['data'].has_key('products'):
                for product in r_data['data']['products']:
                    i_p += 1
                    p_index += 1
                    from_s = None
                    if self.act_url_tail != '':
                        from_s = '%s_pos_%s_%s1' % (self.act_url_tail, str(p_index), str(p_id))
                    self.parse_dictitem(product, i_p, from_s)
        else:
            m = re.search(r'"data":{"products":(\[{.+?}\]),.+?}}', r_data, flags=re.S)
            if m:
                p = re.compile(r'({"hash_id":.+?"pic_url":".+?"})', flags=re.S)
                for data in p.finditer(r_data):
                    i_p += 1
                    p_index += 1
                    from_s = None
                    if self.act_url_tail != '':
                        from_s = '%s_pos_%s_%s1' % (self.act_url_tail, str(p_index), str(p_id))
                    self.parse_stritem(data.group(1), i_p, from_s)
        return i_p

    def parse_dictitem(self, product, i_p, from_s=None):
        i_id = ''
        if product.has_key('hash_id'):
            i_id = product['hash_id']

        i_name = ''
        if product.has_key('medium_name'):
            i_name = product['medium_name']
        else:
            if product.has_key('short_name'):
                i_name = product['short_name']

        u_status = 'zs'
        if product.has_key('sellable'):
            if int(product['sellable']) == 0:
                u_status = 'end'

        p_url = 'http://item.jumei.com'
        if product.has_key('category'):
            if product['category'].find('global') != -1:
                p_url = 'http://item.jumeiglobal.com'

        if from_s:
            i_url = '%s/%s.html?from=%s&status=%s' % (p_url, i_id, from_s, u_status)
        else:
            i_url = '%s/%s.html' % (p_url, i_id)
            
        val = (product, i_id, i_name, i_url, i_p)
        self.itemVal(val)

    def parse_stritem(self, product, i_p, from_s=None):
        i_id = ''
        m = re.search(r'"hash_id":"(.+?)",', product, flags=re.S)
        if m:
            i_id = m.group(1)

        i_name = ''
        m = re.search(r'"short_name":"(.+?)",', product, flags=re.S)
        if m:
            i_name = m.group(1)
        else:
            m = re.search(r'"medium_name":"(.+?)",', product, flags=re.S)
            if m:
                i_name = m.group(1)

        u_status = 'zs'
        m = re.search(r'"sellable":\s*(\d+)', product, flags=re.S)
        if m:
            i_status = m.group(1)
            if int(i_status) == 0:
                u_status = 'end'

        p_url = 'http://item.jumei.com'
        m = re.search(r'"category":"(.+?)",', product, flags=re.S)
        if m:
            p_cate = m.group(1)
            if p_cate.find('global') != -1:
                p_url = 'http://item.jumeiglobal.com'
        if from_s:
            i_url = '%s/%s.html?from=%s&status=%s' % (p_url, i_id, from_s, u_status)
        else:
            i_url = '%s/%s.html' % (p_url, i_id)
            
        val = (product, i_id, i_name, i_url, i_p)
        self.itemVal(val)

    # 返回商品信息
    def itemVal(self, val):
        data, i_id, i_name, i_url, i_position = '', '', '', '', 0
        v_l = len(val)
        if val and len(val) > 0:
            v_l = len(val)
            i_val = None
            if v_l == 2:
                i_url, i_position = val
                if i_url and i_url != '':
                    m = re.search(r'/(\w+)\.html', i_url)
                    if m:
                        i_id = m.group(1)
                        if i_url.find('jumeiglobal') != -1:
                            i_url = 'http://item.jumeiglobal.com/%s.html?%s' % (i_id, i_url.split('?')[1])
                        else:
                            i_url = 'http://item.jumei.com/%s.html?%s' % (i_id, i_url.split('?')[1])
            else:
                data, i_id, i_name, i_url, i_position = val
            if i_url != '':
                i_val = (self.act_id, self.act_name, self.act_url, data, i_id, i_name, i_url, i_position)
                #print i_val
                self.act_itemval_list.append(i_val)
                if i_id and i_id != '':
                    self.act_itemids.append(i_id)

    # 品牌团页面
    def actPage(self):
        if self.act_url and self.act_url != '':
            data = self.crawler.getData(self.act_url, self.channel_url)
            if not data and data == '': raise Common.InvalidPageException("# actPage:not find act page,act_id:%s,act_name:%s,act_url:%s"%(str(self.act_id), self.act_name, self.act_url))
            if data and data != '':
                self.act_page = data
                self.act_pages['act-home'] = (self.act_url, data)

    # 品牌团信息和其中商品基本信息
    def antPage(self, val):
        self.channel_id, self.channel_name, self.channel_url, self.act_position, self.act_id, self.act_url, self.act_name, self.act_desc, self.act_logopic_url, self.act_enterpic_url, self.act_times, self.act_discounts, self.crawling_begintime = val
        # 本次抓取开始日期
        self.crawling_beginDate = time.strftime("%Y-%m-%d", time.localtime(self.crawling_begintime))
        # 本次抓取开始小时
        self.crawling_beginHour = time.strftime("%H", time.localtime(self.crawling_begintime))
        self.actPage()
        self.itemConfig()
        self.actItems()


    # 输出活动的网页
    def outItemPage(self,crawl_type):
        if self.crawling_begintime != '':
            time_s = time.strftime("%Y%m%d%H", time.localtime(self.crawling_begintime))
        else:
            time_s = time.strftime("%Y%m%d%H", time.localtime(self.crawling_time))
        # timeStr_jmtype_webtype_act_crawltype_actid
        key = '%s_%s_%s_%s_%s_%s' % (time_s,Config.JM_TYPE,'1','act',crawl_type,str(self.act_id))
        pages = {}
        for p_tag in self.act_pages.keys():
            p_url, p_content = self.act_pages[p_tag]
            f_content = '<!-- url=%s --> %s' %(p_url, p_content)
            pages[p_tag] = f_content.strip()
        return (key,pages)

    # 写html文件
    def writeLog(self, time_path):
        try:
            return None
            pages = self.outItemLog()
            for page in pages:
                filepath = Config.pagePath + time_path + page[2]
                Config.createPath(filepath)
                filename = filepath + page[0]
                fout = open(filename, 'w')
                fout.write(page[3])
                fout.close()
        except Exception as e:
            print '# exception err in writeLog info:',e

    # 输出抓取的网页log
    def outItemLog(self):
        pages = []
        for p_tag in self.act_pages.keys():
            p_url, p_content = self.act_pages[p_tag]

            # 网页文件名
            f_path = '%s_act/' %(self.act_id)
            f_name = '%s-%s_%d.htm' %(self.act_id, p_tag, self.crawling_time)

            # 网页文件内容
            f_content = '<!-- url=%s -->\n%s\n' %(p_url, p_content)
            pages.append((f_name, p_tag, f_path, f_content))

        return pages

    def outSql(self):
        act_start_time = ''
        if self.act_start_time and float(self.act_start_time) != 0.0 and int(self.act_start_time) > 0:
            act_start_time = Common.time_s(float(self.act_start_time))
        act_end_time = ''
        if self.act_end_time and float(self.act_end_time) != 0.0 and int(self.act_end_time) > 0:
            act_end_time = Common.time_s(float(self.act_end_time))
        return (Common.time_s(self.crawling_time),self.channel_id,self.channel_name,self.act_id,self.act_name,self.act_desc,self.act_platform,self.act_position,self.act_url,self.act_logopic_url,self.act_enterpic_url,self.act_brand_id,act_start_time,act_end_time,self.crawling_beginDate,self.crawling_beginHour)

if __name__ == '__main__':
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    a = Act()
    val = (1, '\xe7\xbe\x8e\xe5\xa6\x86', 'http://beauty.jumei.com/?from=all_null_index_top_nav_cosmetics&lo=3481&mat=30573', 4, 8830, 'http://hd.jumei.com/act/plt_ribenkouqiang_150730.html?from=beauty_coming_8830_pos4', '\xe6\x97\xa5\xe6\x9c\xac\xe5\x8f\xa3\xe8\x85\x94\xe5\x9b\xa2', '\xe6\x97\xa5\xe6\x9c\xac\xe5\x8f\xa3\xe8\x85\x94\xe5\x9b\xa2', 'http://p0.jmstatic.com/brand/logo_180/3428.jpg', 'http://p0.jmstatic.com/jmstore/image/000/001/1520_std/55b7140b6c894_450_240.jpg?1438065505', '7\xe6\x9c\x8830\xe6\x97\xa5-7\xe6\x9c\x8831\xe6\x97\xa5', '\xe5\x85\xa8\xe5\x9c\xba39\xe5\x85\x83\xe8\xb5\xb7', Common.now())
    a.antPage(val)
    time.sleep(1)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
 
