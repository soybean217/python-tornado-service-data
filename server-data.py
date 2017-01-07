# coding: utf-8

import tornado.ioloop
import tornado.web
import struct
import torndb
import time
import geoip2.database

import config
from Bastion import _test

TEST_CONTENT =  "<datas><cfg><durl></durl><vno></vno><stats>1</stats></cfg><da><data><kno>135</kno><kw>验证码*中国铁路</kw><apid>100</apid></data></da></datas>";

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("error")
    def post(self, *args, **kwargs):
        self.write("error")  

        
class SmsHandler(tornado.web.RequestHandler):
    def get(self,sms):
        self.write("ok")
        print sms
        _sms = {'spnumber':self.get_argument('spnumber'),'mobile':self.get_argument('mobile'),'linkid':self.get_argument('linkid'),'msg':self.get_argument('msg'),'status':self.get_argument('status')} 

class IvrHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("ok")

class MonthHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("ok")

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/sms/([0-9a-zA-Z\-\_]+)", SmsHandler),
        (r"/ivr/([0-9a-zA-Z\-\_]+)", IvrHandler),
        (r"/month/([0-9a-zA-Z\-\_]+)", MonthHandler),
    ])

if __name__ == "__main__":
    print "begin..."
    app = make_app()
    app.listen(config.GLOBAL_SETTINGS['port'])
    tornado.ioloop.IOLoop.current().start()