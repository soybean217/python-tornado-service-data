# coding: utf-8

import tornado.ioloop
import tornado.web
import struct
import torndb
import time
import threading

import config
from Bastion import _test

TEST_CONTENT =  "<datas><cfg><durl></durl><vno></vno><stats>1</stats></cfg><da><data><kno>135</kno><kw>验证码*中国铁路</kw><apid>100</apid></data></da></datas>";

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("error")
    def post(self, *args, **kwargs):
        self.write("error")  


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

class SmsHandler(tornado.web.RequestHandler):
    def get(self,sms):
        self.write("ok")
        _ip = self.request.headers["X-Real-IP"] if self.request.headers["X-Real-IP"] != None and len(self.request.headers["X-Real-IP"])>0 else self.request.remote_ip
        _sms_info = {'spcode':sms,'spnumber':self.get_argument('spnumber'),'mobile':self.get_argument('mobile'),'linkid':self.get_argument('linkid'),'msg':self.get_argument('msg'),'status':self.get_argument('status'),'ip':_ip}
        _t = threading.Thread(target=insert_sms_log(_sms_info))
        _t.start() 
        _t1 = threading.Thread(target=proc_sms(_sms_info))
        _t1.start() 

def insert_sms_log(_sms_info):
    _log_id = 101
    dbLog=torndb.Connection(config.GLOBAL_SETTINGS['log_db']['host'],config.GLOBAL_SETTINGS['log_db']['name'],config.GLOBAL_SETTINGS['log_db']['user'],config.GLOBAL_SETTINGS['log_db']['psw'])
    _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`,`para07`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    dbLog.insert(_sql,int(round(time.time() * 1000)),_log_id,_sms_info["ip"],_sms_info["spcode"],_sms_info["spnumber"],_sms_info["mobile"],_sms_info["linkid"],_sms_info["msg"],_sms_info["status"])
    return

def proc_sms(_sms_info):
    try:
        _sms_cmd = get_cmd(_sms_info)
        _user = get_user_by_mobile(_sms_info['mobile'])
        if _user == None :
            print "can not match user by mobile:" + _sms_info['mobile']
        else:
            return
    except "ParameterError",_argument:
        print "ParameterError:", _argument
    else:
        return

def get_cmd(_sms_info):
    dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])
    _sql = 'SELECT spNumber as spnumber,msg,price FROM `sms_cmd_configs` WHERE spNumber = %s and msg = %s'
    _record = dbConfig.get(_sql, _sms_info['spnumber'], _sms_info['msg']) 
    if _record==None:
        raise Exception("ParameterError", "can not match cmd,spcode:"+_sms_info['spcode']+",spnumber:"+_sms_info['spnumber']+",msg:"+_sms_info['msg'])
        return
    else:
        return _record

def get_user_by_mobile(_mobile):
    dbConfig=torndb.Connection(config.GLOBAL_SETTINGS['config_db']['host'],config.GLOBAL_SETTINGS['config_db']['name'],config.GLOBAL_SETTINGS['config_db']['user'],config.GLOBAL_SETTINGS['config_db']['psw'])
    _sql = 'SELECT * FROM `imsi_users` WHERE mobile = %s'
    _record = None
    if len(_mobile)==11:
        _record = dbConfig.get(_sql, '86'+_mobile) 
    if _record == None:
        _record = dbConfig.get(_sql, _mobile) 
    return _record

if __name__ == "__main__":
    print "begin..."
    app = make_app()
    app.listen(config.GLOBAL_SETTINGS['port'])
    tornado.ioloop.IOLoop.current().start()