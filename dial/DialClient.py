#-*- coding:utf-8 -*-
#!/usr/bin/env python

import sys
import json
from socket import AF_INET, SOCK_STREAM, socket
sys.path.append('../base')
import Common as Common
import Config as Config

class DialClient():
    ''' A class of Dial client, to send dial request '''
    def __init__(self):
        self.magic_num = Config.magic_num
        self.bufsize   = 1024
        self.dial_ip   = Config.dial_ip
        self.dial_port = Config.dial_port
        self.dial_addr = (self.dial_ip, self.dial_port)

        self.initClient()

    def __del__(self):
        self.closeClient()

    def closeClient(self):
        self.client.close()

    def initClient(self):
        self.client = socket(AF_INET, SOCK_STREAM)
        self.client.settimeout(60)
        self.setConnect(False)

    def connectClient(self):
        try:
            self.client.connect(self.dial_addr)
            self.setConnect(True)
        except:
            Common.log('# connectClient: %s' % str(self.dial_addr))

    def isConnect(self):
        return self.is_connect

    def setConnect(self, flag=True):
        self.is_connect = flag

    def buildMsg(self, _content):
        _module, _ip, _tag = _content
        msg_d = {}
        msg_d['magic']  = self.magic_num
        msg_d['module'] = _module
        msg_d['ip']     = _ip
        msg_d['tag']    = _tag
        s = json.dumps(msg_d)
        return s
 
    def send(self, _content):
        s = self.buildMsg(_content)
        try:
            if not self.isConnect():
                self.connectClient()
            self.client.sendall(s)

        except Exception as e:
            Common.log('# DailClient send exception: %s' % e)
            self.connectClient()

    def recv(self):
        return self.client.recv(self.bufsize)

if __name__ == "__main__":
    args = sys.argv
    args = ['DialClient', '3_act', '192.168.7.1', 'ikuai']
    if len(args) < 3:
        Common.log('python DialClient module ip tag')
        exit()

    # 处理输入参数
    _module, _ip, _tag = args[1], args[2], args[3]
    Common.log('# DialClient: %s %s %s' % (str(_module), str(_ip), str(_tag)))

    c = DialClient()
    c_time = Common.now_ss() 
    c.send((_module, _ip, _tag))
    c = None

