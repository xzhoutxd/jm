#-*- coding:utf-8 -*-
#!/usr/bin/env python

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import re
import time
import random
import Common
from JMCrawler import JMCrawler
sys.path.append('../dial')
from DialClient import DialClient

class RetryCrawler():
    '''A class of retry crawl data'''
    def __init__(self):
        # 抓取设置
        self.crawler = JMCrawler()
        # dial client
        self.dial_client = DialClient()
        # local ip
        self._ip = Common.local_ip()
        # router tag
        self._tag = 'ikuai'

        # wait time
        self.w_time = 1

    # To dial router
    def dialRouter(self, _type, _obj):
        try:
            _module = '%s_%s' %(_type, _obj)
            self.dial_client.send((_module, self._ip, self._tag))
        except Exception as e:
            print '# To dial router exception :', e

    def getData(self, url, refers='', max_retry=20):
        page = ''
        retry = 1
        while True:
            try:
                page = self.crawler.getData(url, refers)
                break
            except Common.InvalidPageException as e:
                if retry >= max_retry:
                    break
                retry += 1
                print '# Invalid page exception:',e
                time.sleep(self.w_time*retry)
            except Common.DenypageException as e:
                if retry >= max_retry:
                    break
                retry += 1
                print '# Deny page exception:',e
                # 重新拨号
                try:
                    self.dialRouter(4, 'chn')
                except Exception as e:
                    print '# DailClient Exception err:', e
                time.sleep(random.uniform(10,30))

            except Common.SystemBusyException as e:
                if retry >= max_retry:
                    break
                retry += 1
                print '# System busy exception:',e
                time.sleep(self.w_time*retry)
            except Exception as e:
                print '# exception err in retry crawler:',e
                if str(e).find('Read timed out') != -1:
                    if retry >= max_retry:
                        break
                    retry += 1
                    time.sleep(random.uniform(1,3))
                elif str(e).find('Name or service not known') != -1 or str(e).find('Temporary failure in name resolution'):
                    if retry >= max_retry:
                        break
                    retry += 1
                    
                    # 重新拨号
                    try:
                        self.dialRouter(4, 'chn')
                    except Exception as e:
                        print '# DailClient Exception err:', e
                    time.sleep(random.uniform(10,30))
                else:
                    break

        return page
