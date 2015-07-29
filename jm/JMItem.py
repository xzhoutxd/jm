#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import re
import random
import json
import time
import traceback
import threading
import hashlib
sys.path.append('../base')
import Common as Common
import Config as Config
from JMCrawler import JMCrawler

class Item():
    '''A class of JM act's Item'''
    def __init__(self):
        # 商品页面抓取设置
        self.crawler = JMCrawler()
        self.crawling_time = Common.now() # 当前爬取时间
        self.crawling_begintime = '' # 本次抓取开始时间
        self.crawling_beginDate = '' # 本次爬取日期
        self.crawling_beginHour = '' # 本次爬取小时

        # 商品所在活动
        self.act_id = '' # 商品所属活动Id
        self.act_name = '' # 商品所属活动Name
        self.act_url = '' # 商品所属活动Url
        self.item_position = 0 # 商品所在活动位置
        self.act_start_time = 0 # 商品所在活动开团时间
        self.act_end_time = 0 # 商品所在活动结束时间

        # 商品信息
        self.item_id = '' # 商品Id
        self.item_url = '' # 商品链接
        self.item_pic_url = '' # 商品展示图片链接
        self.item_name = '' # 商品Name
        self.item_desc = '' # 商品说明
        self.item_cat_id = '' # 商品叶子类目id
        self.item_cat_name = '' # 商品叶子类目name
        self.item_brand = 0 # 商品品牌id
        self.item_brand_name = '' # 商品品牌name
        self.item_brand_friendly_name = '' # 商品品牌近似name
        self.item_sell_status = -1 # 商品是否售卖 0:不售,1:在售 售罄和结束为0
        self.item_isLock = 1 # 商品是否锁定 0:锁定,1:没有锁定 售罄和结束为0
        self.item_isLock_time = None # 抓到锁定的时间
        self.item_type_s = '' # 商品抓取到的属性 'retail_global' 'global' 'product' 
        self.item_type = '' # 商品属性是否是'global' 'product'
        self.item_category_id = 0 # 商品所属分类id
        self.item_category_name = '' # 商品所属分类name
        self.item_category_v3_1 = 0 # 
        self.item_mname = '' # 商品长名称
        self.item_sname = '' # 商品短名称
        self.item_product_id = '' # 商品产品id

        # 商品时间信息
        self.item_start_time = 0 # 商品开团时间
        self.item_end_time = 0 # 商品结束时间

        # 商品店铺
        self.item_seller_id = '' # 商品卖家id
        self.item_seller_name = '' # 商品卖家name
        self.item_shop_id = '' # 商品店铺id
        self.item_shop_name = '' # 商品店铺name
        self.item_shop_type = 0 # 商品店铺类型 0:默认

        # 商品交易
        self.item_oriprice = '' # 商品原价
        self.item_disprice = '' # 商品折扣价
        self.item_discount = '' # 商品打折
        self.item_soldCount = '' # 商品销售数量
        self.item_stock = '' # 商品库存
        self.item_coupons = [] # 商品优惠券
        self.item_promotions = [] # 商品其他优惠
        self.item_prepare = 0 # 商品活动前备货数
        self.item_fav_num = 0 # 商品收藏数
        self.item_buyer_num = '' # 购买人数?
        self.item_real_buyer_num = '' # 购买人数?
        self.item_ending_buyer_number = '' # ???
        self.item_wish_num = '' # 商品想买人数
        self.item_sku_min_price = '' # sku 最小价格
        self.item_sku_max_mprice = '' # sku 最大市场价格
        self.item_min_discount = '' # 最小折扣
        self.item_deal_tax = '' # 税率

        # SKU
        self.item_skus = []

        # 原数据信息
        self.item_pageData = '' # 商品所属数据项内容
        self.item_page = '' # 商品页面html内容
        self.item_pages = {} # 商品页面内请求数据列表

        # 每小时
        self.hour_index = 0 # 每小时的时刻

        # 商品状态类型
        self.item_status_type = 0 # 0:预热 1:售卖 2:售罄

    # 商品初始化
    def initItem(self, act_id, act_name, act_url, item_data, item_id, item_name, item_url, item_position, begin_time):
        # 商品所属数据项内容
        self.item_pageData = item_data
        self.item_pages['item-init'] = ('',item_data)
        # 商品所属活动id
        self.act_id = act_id
        # 商品所属活动name
        self.act_name = act_name
        # 商品所属活动url
        self.act_url = Common.fix_url(act_url)
        # 商品所在活动位置
        self.item_position = item_position
        # 商品聚划算链接
        self.item_url = Common.fix_url(item_url)
        # 商品id
        self.item_id = item_id
        # # 商品name
        self.item_sname = item_name
        # 本次抓取开始时间
        self.crawling_begintime = begin_time
        # 本次抓取开始日期
        self.crawling_beginDate = time.strftime("%Y-%m-%d", time.localtime(self.crawling_begintime))
        # 本次抓取开始小时
        self.crawling_beginHour = time.strftime("%H", time.localtime(self.crawling_begintime))


    # 聚划算商品页信息
    def itemConfig(self):
        # 聚划算商品页信息
        self.itemPage()
        page = self.item_page

        self.itemType()

        if self.item_type == 'global':
            self.itemGlobal()
        elif self.item_type == 'product':
            self.itemProduct()

    def itemType(self):
        page = self.item_page
        if self.item_type == '': 
            m = re.search(r'<input type="hidden" id="deal_category" value="(.+?)">', page, flags=re.S)
            if m:
                value = m.group(1)
                if value.find('global') != -1:
                    self.item_type = 'global'
            if self.item_type == '':
                m = re.search(r'document.domain\s*=\s*\'jumeiglobal.com\';', page, flags=re.S)
                if m:
                    self.item_type = 'global'
                else:
                    m = re.search(r'SITE_ITEM_WEBBASEURL=\'http://item.jumeiglobal.com/\';', page, flags=re.S)
                    if m:
                        self.item_type = 'global'
                    else:
                        m = re.search(r'CURRENT_SITE_MAIN_WEBBASEURL=\'jumeiglobal.com\';', page, flags=re.S)
                        if m:
                            self.item_type = 'global'
                        else:
                            self.item_type = 'product'

    def itemGlobal(self):
        page = self.item_page
        m = re.search(r'(<input type="hidden" id="stream_id".+?>)', page, flags=re.S)
        if m:
            stream_s = m.group(1)
        else:
            stream_s = page
        m = re.search(r'<input type="hidden" id="stream_id".+?search_product_id="(.+?)".+?>', stream_s, flags=re.S)
        if m:
            self.item_product_id = m.group(1)
        m = re.search(r'<input type="hidden" id="stream_id".+?search_category_id="(.+?)".+?>', stream_s, flags=re.S)
        if m:
            item_search_category_id = m.group(1)
        m = re.search(r'<input type="hidden" id="stream_id".+?search_brand_id="(.+?)".+?>', stream_s, flags=re.S)
        if m:
            self.item_brand_id = m.group(1)
        m = re.search(r'<input type="hidden" id="stream_id".+?search_product_type="(.+?)".+?>', stream_s, flags=re.S)
        if m:
            self.item_type_s = m.group(1)
        m = re.search(r'<input type="hidden" id="stream_id".+?search_sku_id="(.+?)".+?>', stream_s, flags=re.S)
        if m:
            item_search_sku_id = m.group(1)
        m = re.search(r'<input type="hidden" id="stream_id".+?price="(.+?)".+?>', stream_s, flags=re.S)
        if m:
            self.item_disprice = m.group(1)
        m = re.search(r'<input type="hidden" id="stream_id".+?search_short_name="(.+?)">', stream_s, flags=re.S)
        if m:
            self.item_sname = m.group(1)

        m = re.search(r'<input type="hidden" id="category_id" value="(.+?)">', page, flags=re.S)
        if m:
            self.item_category_v3_1 = m.group(1)

        m = re.search(r'<p class="introduce_cd">\s+<strong>.+?</strong>(.+?)</p>', page, flags=re.S)
        if m:
            self.item_desc = re.sub(r'<.+?>','',m.group(1).strip())
        else:
            m = re.search(r'<p class="long_title">\s+<strong>.+?</strong>(.+?)</p>', page, flags=re.S)
            if m:
                self.item_desc = re.sub(r'<.+?>','',m.group(1).strip())

        self.itemGlobalDealinfo()

    def itemGlobalDealinfo(self):
        a_url = 'http://www.jumeiglobal.com/ajax_new/dealinfo?hash_id=%s&callback=static_callback' % self.item_id
        page = self.crawler.getData(a_url, self.item_url)
        m = re.search(r'\(({.+?})\)', page, flags=re.S)
        if m:
            result = m.group(1)
            try:
                i_val = json.loads(result)
                if i_val.has_key('category'):
                    self.item_type_s = i_val['category']
                if i_val.has_key('stocks'):
                    item_stocks = i_val['stocks']
                if i_val.has_key('ending_buyer_number'):
                    self.item_ending_buyer_number = i_val['ending_buyer_number']
                if i_val.has_key('kucun'):
                    item_kucun = i_val['kucun']
                if i_val.has_key('jumei_price'):
                    self.item_disprice = i_val['jumei_price']
                if i_val.has_key('market_price'):
                    self.item_oriprice = i_val['market_price']
                if i_val.has_key('discount'):
                    self.item_discount = i_val['discount']
                if i_val.has_key('deal_tax'):
                    self.item_deal_tax = i_val['deal_tax']
                if i_val.has_key('start_time'):
                    self.item_start_time = i_val['start_time']
                if i_val.has_key('end_time'):
                    self.item_end_time = i_val['end_time']
                if i_val.has_key('buyer_number'):
                    self.item_buyer_num  = i_val['buyer_number']
                if i_val.has_key('wish_number'):
                    self.item_wish_num = i_val['wish_number']
                if i_val.has_key('sku_list'):
                    if len(i_val['sku_list']) == 1:
                        self.item_skus = i_val['sku_list'][0]
                    else:
                        self.item_skus = i_val['sku_list']
                if i_val.has_key('fav_number'):
                    if i_val['fav_number'].has_key('fav_number'):
                        self.item_fav_num = i_val['fav_number']['fav_number']
                if i_val.has_key('promo_sale_text'):
                    if i_val['promo_sale_text'].has_key('reduce'):
                        i_reduces = i_val['promo_sale_text']['reduce']
                        for i_reduce in i_reduces:
                            if i_reduce.has_key('show_name'):
                                self.item_promotions.append(i_reduce['show_name'])
                    if i_val['promo_sale_text'].has_key('gift'):
                        i_gifts = i_val['promo_sale_text']['gift']
                        for i_gift in i_gifts:
                            if i_gift.has_key('show_name'):
                                self.item_promotions.append(i_gift['show_name'])
            except Exception as e:
                print '# itemGlobalDealinfo,exception err in load json:',e
        
    def itemProduct(self):
        page = self.item_page
        detail_info = ''
        m = re.search(r'DealDetail.global\s*=\s*({.+?});', page, flags=re.S)
        if m:
            detail_info = m.group(1)
            m = re.search(r'"hashId":"(.+?)"', detail_info, flags=re.S)
            if m:
                self.item_id = m.group(1)
            m = re.search(r'"productId":"(.+?)"', detail_info, flags=re.S)
            if m:
                self.item_product_id = m.group(1)
            m = re.search(r'"brandId":"(.+?)"', detail_info, flags=re.S)
            if m:
                self.item_brand_id = m.group(1)
            m = re.search(r'"categoryId":"(.+?)"', detail_info, flags=re.S)
            if m:
                self.item_category_id = m.group(1)
            m = re.search(r'"startTime":"(.+?)"', detail_info, flags=re.S)
            if m:
                self.item_start_time = m.group(1)
            m = re.search(r'"endTime":"(.+?)"', detail_info, flags=re.S)
            if m:
                self.item_end_time = m.group(1)
            m = re.search(r'scountedPrice":(.+?),', detail_info, flags=re.S)
            if m:
                self.item_disprice = re.sub('"','',m.group(1))
            m = re.search(r'"originalPrice":(.+?),', detail_info, flags=re.S)
            if m:
                self.item_oriprice = re.sub('"','',m.group(1))
            m = re.search(r'"discount":"(.+?)"', detail_info, flags=re.S)
            if m:
                self.item_discount = m.group(1)
            m = re.search(r'"imgDir":"(.+?)"', detail_info, flags=re.S)
            if m:
                item_img_dir = re.sub(r'\\','',m.group(1))
            m = re.search(r'"endingBuyerNumber":"(.+?)",', page, flags=re.S)
            if m:
                self.item_ending_buyer_number = m.group(1)

        item_ping = ''
        m = re.search(r'window.PingY\s*=\s*({.+?});', page, flags=re.S)
        if m:
            item_ping = m.group(1)
            m = re.search(r'id:"(.+?)"', item_ping, flags=re.S)
            if m:
                self.item_id = m.group(1)
            m = re.search(r'soldOut:"(.+?)"', item_ping, flags=re.S)
            if m:
                item_soldout = m.group(1)
            m = re.search(r'category:"(.+?)"', item_ping, flags=re.S)
            if m:
                self.item_category_name = m.group(1)
            m = re.search(r'categoryId:"(.+?)"', item_ping, flags=re.S)
            if m:
                self.item_category_id = m.group(1)
            m = re.search(r'name:"(.+?)"', item_ping, flags=re.S)
            if m:
                self.item_sname = m.group(1)
            m = re.search(r'price:"(.+?)"', item_ping, flags=re.S)
            if m:
                self.item_disprice = m.group(1)
            m = re.search(r'imgUrl:"(.+?)"', item_ping, flags=re.S)
            if m:
                self.item_pic_url = m.group(1)
            m = re.search(r'productUrl:"(.+?)"', item_ping, flags=re.S)
            if m:
                self.item_product_url = m.group(1)
            m = re.search(r'brand:"(.+?)"', item_ping, flags=re.S)
            if m:
                self.item_brand_name = m.group(1)
            #m = re.search(r'promotion:"(.+?)"', item_ping, flags=re.S)
            #if m:
            #    self.item_promotions = m.group(1)
            m = re.search(r'discount:"(.+?)"', item_ping, flags=re.S)
            if m:
                self.item_discount = m.group(1)
            m = re.search(r'origPrice:"(.+?)"', item_ping, flags=re.S)
            if m:
                self.item_oriprice = m.group(1)

        m = re.search(r'<div class="deal_detail_title">\s+<div class="deal_title_detail">.+?<span class="share_title">(.+?)</span>\s+</div>', page, flags=re.S)
        if m:
            self.item_desc = m.group(1).strip()
        else:
            m = re.search(r'<meta name="description" content="(.+?)".+?>', page, flags=re.S)
            if m:
                self.item_desc = m.group(1).strip()

        self.itemProductDealinfo()
        self.itemProductKoubei()

    def callback_s(self, p_s, v_s, t):
        return p_s + v_s + re.sub('\.','',str('%17.16f' % random.random())) + '_' + str(t+1)

    def itemProductDealinfo(self):
        time_n = int(float(Common.now())*1000)
        #callback_s = '1112' + re.sub('\.','',str('%17.16f' % random.random())) + '_' + str(time_n+1)
        callback_s = self.callback_s('jQuery', '1112', time_n)
        a_url = 'http://www.jumei.com/i/static/getDealInfoByHashId?hash_id=%s&brand_id=%s&callback=%s&_=%s' % (self.item_id,self.item_brand_id,callback_s,str(time_n))
        #print a_url
        page = self.crawler.getData(a_url, self.item_url)
        m = re.search(r'\(({.+?})\)', page, flags=re.S)
        if m:
            result = m.group(1)
            try:
                i_val = json.loads(result)
                if i_val.has_key('start_time'):
                    self.item_start_time = i_val['start_time']
                if i_val.has_key('end_time'):
                    self.item_end_time = i_val['end_time']
                if i_val.has_key('buyer_number'):
                    self.item_buyer_num  = i_val['buyer_number']
                if i_val.has_key('wish_number'):
                    self.item_wish_num = i_val['wish_number']
                if i_val.has_key('sku_info') and i_val['sku_info'].has_key('skus'):
                    self.item_skus = i_val['sku_info']['skus']
                if i_val.has_key('fav_number'):
                    if i_val['fav_number'].has_key('fav_number'):
                        self.item_fav_num = i_val['fav_number']['fav_number']
                if i_val.has_key('promo_sale_text'):
                    if type(i_val['promo_sale_text']) is dict:
                        if i_val['promo_sale_text'].has_key('reduce'):
                            i_reduces = i_val['promo_sale_text']['reduce']
                            for i_reduce in i_reduces:
                                if i_reduce.has_key('show_name'):
                                    self.item_promotions.append(i_reduce['show_name'])
                        if i_val['promo_sale_text'].has_key('gift'):
                            i_gifts = i_val['promo_sale_text']['gift']
                            for i_gift in i_gifts:
                                if i_gift.has_key('show_name'):
                                    self.item_promotions.append(i_gift['show_name'])
                    else:
                        self.item_promotions = i_val['promo_sale_text']
            except Exception as e:
                print '# itemProductDealinfo,exception err in load json:',e


    def itemProductKoubei(self):
        page = self.item_page

        keyVal = "2e71b087c883dd2fa7b3d405b5db808fc372f49703dd51c5b802e381031b522e"
        m = re.search(r'<script type="text/javascript" src="(http:.+?/deal_main.js)">', page, flags=re.S)
        if m:
            main_js = m.group(1)
            main_js_r = self.crawler.getData(main_js, self.item_url)
            m = re.search(r'keyVal:"(.+?)",', main_js_r, flags=re.S)
            if m:
                keyVal = m.group(1)
        time_s = str(int(float(Common.now())*1000))
        verify_code = self.getMD5Val(keyVal, self.item_product_id, time_s)
        time_n = int(float(Common.now())*1000)
        callback_s = self.callback_s('jQuery', '1112', time_n)
        a_url = 'http://koubei.jumei.com/Ajax/getDealDatasByProductId?product_id=%s&verify_code=%s&time=%s&callback=%s&_=%s' % (self.item_product_id,verify_code,time_s,callback_s,str(time_n))
        #print a_url
        result = self.crawler.getData(a_url, self.item_url)
        if result and result != '':
            m = re.search(r'"dealCommentNumber":"(.+?)",', result, flags=re.S)
            if m:
                item_comment_num = m.group(1)
            m = re.search(r'"rating":"(.+?)"', result, flags=re.S)
            if m:
                item_rating = m.group(1)
            m = re.search(r'"averageRating":"(.+?)"', result, flags=re.S)
            if m:
                item_average_rating = m.group(1)

    def getMD5Val(self, keyVal, productid, time_s):
        #return window.md5(productid + keyVal + time_s)
        return hashlib.md5(productid + keyVal + time_s).hexdigest()


    # 商品详情页html
    def itemPage(self):
        if self.item_url != '':
            refer_url = ''
            if self.act_url != '':
                refer_url = self.act_url
            page = self.crawler.getData(self.item_url, refer_url)

            if type(self.crawler.history) is list and len(self.crawler.history) != 0 and re.search(r'302',str(self.crawler.history[0])):
                if not self.itempage_judge(page):
                    print '#crawler history:',self.crawler.history
                    raise Common.NoPageException("# itemPage: not find item page, redirecting to other page,id:%s,item_url:%s"%(str(self.item_id), self.item_url))

            if not page or page == '': 
                print '#crawler history:',self.crawler.history
                raise Common.InvalidPageException("# itemPage: find item page empty,id:%s,item_url:%s"%(str(self.item_id), self.item_url))
            self.item_page = page
        else:
            raise Common.NoPageException("# itemPage: not find item page, url is null,id:%s,item_url:%s"%(str(self.item_id), self.item_url))

    def itempage_judge(self, page):
        m = re.search(r'JM.CONTROL\s*=\s*\'Deal\';', page, flags=re.S)
        if m:
            return True
        else:
            m = re.search(r'JM.SITE_ITEM_WEBBASEURL=\'http://item.jumeiglobal.com/\';', page, flags=re.S)
            if m:
                return True
            else:
                return False

    def itemParser(self):
        # 基本信息
        if type(self.item_pageData) is str:
            if self.item_pageData != '': 
                try:
                    self.item_pageData = json.loads(self.item_pageData)
                    self.itemDict()
                except Exception as e:
                    print '# item itemParser json loads error:',self.item_pageData
                    self.itemString()
            #else:
            #    print '# item itemParser item_pageData is empty...'
        else:
            self.itemDict()
        self.item_pages['item-init'] = ('',self.item_pageData)

    # json string
    def itemString(self):
        if self.item_pageData and self.item_pageData != '':
            m = re.search(r'"hash_id":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_id = m.group(1)
            m = re.search(r'"medium_name":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_mname = m.group(1)
            m = re.search(r'"short_name":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_sname = m.group(1)
            m = re.search(r'"buyer_number":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_sname = m.group(1)
            m = re.search(r'"real_buyer_number":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_real_buyer_num = m.group(1)
            m = re.search(r'"discounted_price":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_disprice = m.group(1)
            m = re.search(r'"original_price":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_oriprice = m.group(1)
            m = re.search(r'"discount":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_discount = m.group(1)
            m = re.search(r'"wish_number":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_wish_num = m.group(1)
            m = re.search(r'"category":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_type_s = m.group(1)
                if self.item_type_s.find('global') != -1:
                    self.item_type = 'global'
                elif self.item_type_s.find('product') != -1:
                    self.item_type = 'product'
            m = re.search(r'"product_id":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_product_id = m.group(1)
            m = re.search(r'"start_time":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_start_time = m.group(1)
            m = re.search(r'"end_time":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_end_time = m.group(1)
            m = re.search(r'"brand_id":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_brand_id = m.group(1)
            m = re.search(r'"category_id":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_category_id = m.group(1)
            m = re.search(r'"category_v3_1":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_category_v3_1 = m.group(1)
            m = re.search(r'"sku_min_price":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_sku_min_price = m.group(1)
            m = re.search(r'"sku_max_market_price":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_sku_max_mprice = m.group(1)
            m = re.search(r'"min_discount":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_min_discount = m.group(1)
            m = re.search(r'"chinese_name":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_brand_name = m.group(1)
            m = re.search(r'"friendly_name":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_brand_friendly_name = m.group(1)
            m = re.search(r'"sellable":"(.+?)",', self.item_pageData, flags=re.S)
            if m:
                self.item_sell_status = m.group(1)
            m = re.search(r'"pic_url":"(.+?)"', self.item_pageData, flags=re.S)
            if m:
                self.item_pic_url = m.group(1)

    # Json dict
    def itemDict(self):
        if self.item_pageData:
            if self.item_pageData.has_key('hash_id'):
                self.item_id = self.item_pageData['hash_id']
            if self.item_pageData.has_key('medium_name'):
                self.item_mname = self.item_pageData['medium_name']
            if self.item_pageData.has_key('short_name'):
                self.item_sname = self.item_pageData['short_name']
            if self.item_pageData.has_key('buyer_number'):
                self.item_buyer_num = self.item_pageData['buyer_number']
            if self.item_pageData.has_key('real_buyer_number'):
                self.item_real_buyer_num = self.item_pageData['real_buyer_number']
            if self.item_pageData.has_key('discounted_price'):
                self.item_disprice = self.item_pageData['discounted_price']
            if self.item_pageData.has_key('original_price'):
                self.item_oriprice = self.item_pageData['original_price']
            if self.item_pageData.has_key('discount'):
                self.item_discount = self.item_pageData['discount']
            if self.item_pageData.has_key('wish_number'):
                self.item_wish_num = self.item_pageData['wish_number']
            if self.item_pageData.has_key('category'):
                self.item_type_s = self.item_pageData['category']
                if self.item_pageData['category'].find('global') != -1:
                    self.item_type = 'global'
                elif self.item_pageData['category'].find('product') != -1:
                    self.item_type = 'product'
            if self.item_pageData.has_key('product_id'):
                self.item_product_id = self.item_pageData['product_id']
            if self.item_pageData.has_key('start_time'):
                self.item_start_time = self.item_pageData['start_time']
            if self.item_pageData.has_key('end_time'):
                self.item_end_time = self.item_pageData['end_time']
            if self.item_pageData.has_key('brand_id'):
                self.item_brand_id = self.item_pageData['brand_id']
            if self.item_pageData.has_key('category_id'):
                self.item_category_id = self.item_pageData['category_id']
            if self.item_pageData.has_key('category_v3_1'):
                self.item_category_v3_1 = self.item_pageData['category_v3_1']
            if self.item_pageData.has_key('sku_min_price'):
                self.item_sku_min_price = self.item_pageData['sku_min_price']
            if self.item_pageData.has_key('sku_max_market_price'):
                self.item_sku_max_mprice = self.item_pageData['sku_max_market_price']
            if self.item_pageData.has_key('min_discount'):
                self.item_min_discount = self.item_pageData['min_discount']
            if self.item_pageData.has_key('chinese_name'):
                self.item_brand_name = self.item_pageData['chinese_name']
            if self.item_pageData.has_key('friendly_name'):
                self.item_brand_friendly_name = self.item_pageData['friendly_name']
            if self.item_pageData.has_key('sellable'):
                self.item_sell_status = self.item_pageData['sellable']
            if self.item_pageData.has_key('pic_url'):
                self.item_pic_url = self.item_pageData['pic_url']

    # 执行
    def antPage(self, val):
        act_id, act_name, act_url, data, item_id, item_name, item_url, item_position, begin_time = val
        self.initItem(act_id, act_name, act_url, data, item_id, item_name, item_url, item_position, begin_time)
        self.itemParser()
        self.itemConfig()
        self.item_pages['item-home'] = (self.item_url, self.item_page)

    # 输出商品的网页
    def outItemPage(self,crawl_type):
        if self.crawling_begintime != '':
            time_s = time.strftime("%Y%m%d%H", time.localtime(self.crawling_begintime))
        else:
            time_s = time.strftime("%Y%m%d%H", time.localtime(self.crawling_time))
        # timeStr_jmtype_webtype_item_crawltype_itemid
        key = '%s_%s_%s_%s_%s_%s' % (time_s,Config.JM_TYPE,'1','item',crawl_type,str(self.item_id))
        pages = {}
        for p_tag in self.item_pages.keys():
            p_url, p_content = self.item_pages[p_tag]
            f_content = '<!-- url=%s --> %s' %(p_url, p_content)
            pages[p_tag] = f_content.strip()
        return (key,pages)

    # 写html文件
    def writeLog(self,time_path):
        try:
            return None
            pages = self.outItemLog()
            for page in pages:
                filepath = Config.pagePath + time_path + page[2]
                Config.createPath(filepath)
                #if not os.path.exists(filepath):
                #    os.mkdir(filepath)
                filename = filepath + page[0]
                fout = open(filename, 'w')
                fout.write(page[3])
                fout.close()
        except Exception as e:
            print '# exception err in writeLog info:',e

    # 输出抓取的网页log
    def outItemLog(self):
        pages = []
        for p_tag in self.item_pages.keys():
            p_url, p_content = self.item_pages[p_tag]

            # 网页文件名
            f_path = '%s_item/%s/' %(self.act_id, self.item_id)
            f_name = '%s-%s_%s_%d.htm' %(self.act_id, self.item_id, p_tag, self.crawling_time)

            # 网页文件内容
            f_content = '<!-- url=%s -->\n%s\n' %(p_url, p_content)
            pages.append((f_name, p_tag, f_path, f_content))

        return pages

    def outTuple(self):
        return (self.item_id,self.item_sname,self.item_mname,self.item_sname,self.item_desc,self.item_url,self.item_pic_url,self.item_disprice,self.item_oriprice,self.item_discount,self.item_min_discount,self.item_sku_min_price,self.item_sku_max_mprice,self.item_buyer_num,self.item_real_buyer_num,self.item_ending_buyer_number,self.item_wish_num,self.item_product_id,self.item_category_id,self.item_category_name,self.item_category_v3_1,self.item_brand_id,self.item_brand_name,self.item_brand_friendly_name,self.item_sell_status,self.item_type_s,self.item_type,self.item_deal_tax,self.item_promotions,self.item_skus,Common.time_s(float(self.item_start_time)),Common.time_s(float(self.item_end_time)),self.act_id,self.item_position)

    def outSql(self):
        item_start_time = ''
        if self.item_start_time and float(self.item_start_time) != 0.0 and int(self.item_start_time) > 0:
            item_start_time = Common.time_s(float(self.item_start_time))
        item_end_time = ''
        if self.item_end_time and float(self.item_end_time) != 0.0 and int(self.item_end_time) > 0:
            item_end_time = Common.time_s(float(self.item_end_time))
        return (Common.time_s(float(self.crawling_time)),self.item_id,self.item_sname,self.item_url,self.item_pic_url,self.item_disprice,self.item_oriprice,self.item_discount,self.item_buyer_num,self.item_real_buyer_num,self.item_ending_buyer_number,self.item_wish_num,self.item_product_id,self.item_category_id,self.item_category_name,self.item_category_v3_1,self.item_brand_id,self.item_brand_name,self.item_brand_friendly_name,self.item_type,item_start_time,item_end_time,self.act_id,self.item_position,self.crawling_beginDate,self.crawling_beginHour)

def test():
    pass

if __name__ == '__main__':
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    i = Item()
    val = (7970, '\xe6\x97\xa5\xe7\xb3\xbb\xe4\xb8\xaa\xe6\x8a\xa4\xe5\x9b\xa2', 'http://hd.jumei.com/act/ppt_tsubaki_150722.html?from=beauty_onsale_n2_7970_pos1', {'original_price': '148', 'discounted_price': '148', 'deposit': '0.00', 'payment_start_time': '0', 'is_new': 0, 'promo_sale_text': {'reduce': [{'show_name': u'\u8d44\u751f\u5802\u83f2\u5a77\u54c1\u724c\u56e2\u6ee1199\u51cf100 \u9650\u9996\u5355', 'site': 'all'}]}, 'product_report_rating': '0.0', 'brand_id': '1445', 'sellable': 1, 'pic_url': 'http://p4.jmstatic.com/product/000/505/505454_std/505454_350_350.jpg', 'category': 'product', 'commission_rate': '0.0000', 'chinese_name': u'\u8d44\u751f\u5802\u83f2\u5a77', 'category_v3_1': '107', 'promo': 'new', 'medium_name': u'\u4e1d\u84d3\u7eee\u5962\u8000\u67d4\u8273\u6d17\u53d1\u9732750ml\xd72\uff0c\u6fc0\u53d1\u7f8e\u4e3d\u7684\u6f5c\u80fd\uff0c\u4ee4\u79c0\u53d1\u67d4\u987a\u5149\u8273\u3002', 'hash_id': 'd150722p505454zc', 'status': '1', 'sku_min_price': '0.00', 'payment_end_time': '0', 'real_buyer_number': '148', 'short_name': u'\u4e1d\u84d3\u7eee\u5962\u8000\u67d4\u8273\u6d17\u53d1\u9732750ml\xd72', 'wish_number': '396', 'start_time': '1437530400', 'discount': '10', 'sale_forms': 'normal', 'aca': 0, 'is_published_price': '1', 'product_id': '505454', 'product_reports_number': '0', 'friendly_name': u'\u8d44\u751f\u5802\u83f2\u5a77', 'is_exist_225': 0, 'end_time': '1437789599', 'category_id': '13', 'is_stock_split': '1', 'buyer_number': '8313'}, 'd150722p505454zc', u'\u4e1d\u84d3\u7eee\u5962\u8000\u67d4\u8273\u6d17\u53d1\u9732750ml\xd72\uff0c\u6fc0\u53d1\u7f8e\u4e3d\u7684\u6f5c\u80fd\uff0c\u4ee4\u79c0\u53d1\u67d4\u987a\u5149\u8273\u3002', 'http://item.jumei.com/d150722p505454zc.html?from=ppt_tsubaki_150722_pos_1_101&status=zs', 22, Common.now())
    val = (8318, '\xe9\x98\xb2\xe6\x99\x92\xe5\x9b\xa2', 'http://hd.jumei.com/act/plt_fangshai_150721.html?from=beauty_onsale_n2_8318_pos1', {'original_price': '0', 'discounted_price': '199', 'deposit': '0.00', 'payment_start_time': '0', 'is_new': 1, 'promo_sale_text': [], 'product_report_rating': '5.0', 'brand_id': '7744', 'is_stock_split': '0', 'pic_url': 'http://p0.jmstatic.com/product/001/577/1577810_std/1577810_350_350.jpg', 'category': 'retail_global', 'commission_rate': '0.0000', 'chinese_name': u'\u5b89\u8010\u6652', 'category_v3_1': '107', 'sku_max_market_price': '289', 'min_discount': 6.9, 'medium_name': u'Anessa \u9ec4\u91d1\u6c34\u94bb\u9632\u6652\u4e73\u5957\u88c5\uff082015\u9650\u5b9a\u7248\uff09\uff0c\u4eba\u6c14\u5927\u7206\u6b3e\u7ec4\u5408\u6765\u88ad\u3002', 'hash_id': 'ht150721p1577810t2', 'status': '1', 'sku_min_price': '199.00', 'payment_end_time': '0', 'real_buyer_number': '5276', 'short_name': u'Anessa \u9ec4\u91d1\u6c34\u94bb\u9632\u6652\u4e73\u5957\u88c5', 'wish_number': '12333', 'start_time': '1437444000', 'baoyou': 0, 'discount': '0', 'sale_forms': 'normal', 'sellable': 1, 'spu_area_code': '19', 'aca': 0, 'is_published_price': '1', 'promo': 'new', 'product_id': '1577810', 'product_reports_number': '2', 'friendly_name': u'\u5b89\u8010\u6652(ANESSA)', 'is_exist_225': 0, 'end_time': '1438099199', 'category_id': '33', 'spu_abroad_price': '1.00', 'buyer_number': '38526'}, 'ht150721p1577810t2', u'Anessa \u9ec4\u91d1\u6c34\u94bb\u9632\u6652\u4e73\u5957\u88c5\uff082015\u9650\u5b9a\u7248\uff09\uff0c\u4eba\u6c14\u5927\u7206\u6b3e\u7ec4\u5408\u6765\u88ad\u3002', 'http://item.jumeiglobal.com/ht150721p1577810t2.html?from=plt_fangshai_150721_pos_1_111&status=zs', 21, Common.now())
    val = (8540, 'SK-\xe2\x85\xa1\xe5\x93\x81\xe7\x89\x8c\xe5\x9b\xa2', 'http://hd.jumei.com/act/ppt_SKII_150725.html?from=beauty_onsale_n2_8540_pos2', {'original_price': '2450', 'discounted_price': '1399', 'deposit': '0.00', 'payment_start_time': '0', 'is_new': 0, 'promo_sale_text': [], 'product_report_rating': '5.0', 'brand_id': '596', 'sellable': 1, 'pic_url': 'http://p0.jmstatic.com/product/000/517/517910_std/517910_350_350.jpg', 'category': 'product', 'commission_rate': '0.0000', 'chinese_name': 'SK-II', 'category_v3_1': '107', 'promo': 'new', 'medium_name': u'SK-II\u808c\u6e90\u4fee\u62a4\u5957\u88c5\uff0c\u9006\u8f6c\u65f6\u5149\uff0c\u5e26\u52a8\u5168\u7403\u5973\u6027\u8fc8\u5165\u6297\u8001\u65b0\u7eaa\u5143\uff01', 'hash_id': 'd150725p517910zc', 'status': '1', 'sku_min_price': '0.00', 'payment_end_time': '0', 'real_buyer_number': '10', 'short_name': u'SK-II\u808c\u6e90\u4fee\u62a4\u5957\u88c5', 'wish_number': '785', 'start_time': '1437789600', 'discount': '5.7', 'sale_forms': 'normal', 'aca': 0, 'is_published_price': '1', 'product_id': '517910', 'product_reports_number': '1', 'friendly_name': 'SK-II', 'is_exist_225': 0, 'end_time': '1438048799', 'category_id': '23', 'is_stock_split': '1', 'buyer_number': '1739'}, 'd150725p517910zc', u'SK-II\u808c\u6e90\u4fee\u62a4\u5957\u88c5\uff0c\u9006\u8f6c\u65f6\u5149\uff0c\u5e26\u52a8\u5168\u7403\u5973\u6027\u8fc8\u5165\u6297\u8001\u65b0\u7eaa\u5143\uff01', 'http://item.jumei.com/d150725p517910zc.html?from=ppt_SKII_150725_pos_2_31&status=zs', 14, Common.now())

    i.antPage(val)
    i_val = i.outTuple()
    for s in i_val:
        print s
    time.sleep(1)
    print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

