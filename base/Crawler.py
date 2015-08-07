#-*- coding:utf-8 -*-
#!/usr/bin/env python
import random
import socket
import Config
import requests
import DnsCache
import warnings
warnings.filterwarnings("ignore")

class Crawler:
    '''A class to crawl web pages'''
    def __init__(self):
        # Socket超时时间
        socket.setdefaulttimeout(35)

        # http get超时时间
        self.timeout     = (10, 30)

        # 网页编码
        self.f_coder     = 'gbk'
        self.t_coder     = 'utf-8'

        # 报文头
        self.header      = Config.g_httpHeader
        self.pc_agent    = random.choice(Config.g_pcAgents)
        self.wap_agent   = random.choice(Config.g_wapAgents)
        self.pad_agent   = random.choice(Config.g_padAppAgents)

        # cookie
        self.session_cookie= {}
        self.use_cookie    = False

        # stream
        self.use_stream    = False

        # session
        self.session       = requests.Session()
        self.session.verify= False

    def __del__(self):
        self.session.close()
        self.session = None

    def buildHeader(self, refers='', terminal='1'):
        # base header
        _header = self.header

        if terminal == '1':    # PC端
            self.header['User-agent'] = self.pc_agent
        elif terminal == '2':  # wap端
            self.header['User-agent'] = self.wap_agent
        elif terminal == '3':  # pad端
            self.header['User-agent'] = self.pad_agent
        else:
            self.header['User-agent'] = self.pc_agent

        # refers
        if refers and refers != '': _header['Referer'] = refers
        return _header

    def setCookie(self, _cookie):
        self.session_cookie = _cookie

    def useCookie(self, flag=False):
        self.use_cookie = flag
