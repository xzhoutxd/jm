#-*- coding:utf-8 -*-
#!/usr/bin/env python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import traceback
import MysqlPool
sys.path.append('../base')
import Config as Config
import Common as Common

class MysqlAccess():
    '''A class of mysql db access'''
    def __init__(self):
        # 聚划算
        self.jm_db = MysqlPool.g_jmDbPool

    def __del__(self):
        # 聚划算
        self.jm_db = None

    # 新加活动
    def insertJMActHour(self, args):
        try:
            sql = 'replace into nd_jm_parser_activity_h(crawl_time,channel_id,channel_name,activity_id,activity_name,activity_desc,platform,position,activity_url,activity_logo_url,activity_pic_url,brand_id,start_time,end_time,c_begindate,c_beginhour) values(%s)' % Common.agg(16)
            self.jm_db.execute(sql, args)
        except Exception, e:
            print '# insert jm brand Act hour exception:', e

    # 新加商品信息
    def insertJMItemHour(self, args_list):
        try:
            sql = 'call sp_jm_parser_item_h(%s)' % Common.agg(26)
            self.jm_db.executemany(sql, args_list)
        except Exception, e:
            print '# insert jm Item hour info exception:', e

if __name__ == '__main__':
    pass
    #my = MysqlAccess()
