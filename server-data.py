# coding: utf-8

import tornado.ioloop
import tornado.web
import struct
import torndb
import time
import threading
import random
import json

import config
import public
from Bastion import _test
import MySQLdb
from DBUtils.PooledDB import PooledDB
poolConfig = PooledDB(MySQLdb, 5, host=config.GLOBAL_SETTINGS['config_db']['host'], user=config.GLOBAL_SETTINGS['config_db']['user'], passwd=config.GLOBAL_SETTINGS[
                      'config_db']['psw'], db=config.GLOBAL_SETTINGS['config_db']['name'], port=config.GLOBAL_SETTINGS['config_db']['port'], setsession=['SET AUTOCOMMIT = 1'], cursorclass=MySQLdb.cursors.DictCursor, charset="utf8")
poolLog = PooledDB(MySQLdb, 5, host=config.GLOBAL_SETTINGS['log_db']['host'], user=config.GLOBAL_SETTINGS['log_db']['user'], passwd=config.GLOBAL_SETTINGS[
    'log_db']['psw'], db=config.GLOBAL_SETTINGS['log_db']['name'], port=config.GLOBAL_SETTINGS['log_db']['port'], setsession=['SET AUTOCOMMIT = 1'], cursorclass=MySQLdb.cursors.DictCursor, charset="utf8")

TEST_CONTENT = "<datas><cfg><durl></durl><vno></vno><stats>1</stats></cfg><da><data><kno>135</kno><kw>验证码*中国铁路</kw><apid>100</apid></data></da></datas>"

systemConfigs = {}
channelConfigs = {}
targetConfigs = {}


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
        (r"/register/([0-9a-zA-Z\-\_]+)", RegisterHandler),
        (r"/getmobi", GetMobiHandler),
    ])


class GetMobiHandler(tornado.web.RequestHandler):

    def get(self):
        _result = {}
        threads = []
        if self.checkParameter():
            _dbConfig = poolConfig.connection()
            _cur = _dbConfig.cursor()
            _sql = 'SELECT mobile,imsi FROM `imsi_users` WHERE imsi = ( SELECT imsi FROM `register_user_relations` WHERE apid = %s and getTime > (%s-80000) and ifnull(registerChannelId,1)=1 limit 1)'
            _cur.execute(_sql, (self.get_argument('apid'), time.time()))
            _record = _cur.fetchone()
            if _record == None:
                _result['result'] = 'no valid mobile'
            else:
                if len(_record['mobile']) == 13:
                    _result['no'] = _record['mobile'][2:13]
                else:
                    _result['no'] = _record['mobile']
                threads.append(threading.Thread(
                    target=update_relation(_record['imsi'], self.get_argument('apid'), self.get_argument('aid'))))
                _info = {'ip': self.request.remote_ip, 'mobile': _result['no'],
                         'query': self.request.query, 'rsp': json.dumps(_result)}
                threads.append(threading.Thread(
                    target=insert_fetch_log(_info)))
            _cur.close()
            _dbConfig.close()
        else:
            _result['result'] = 'invalid data'
        self.write(json.dumps(_result))
        self.finish()
        for t in threads:
            t.start()

    def checkParameter(self):
        result = False
        if self.get_argument('aid') in channelConfigs.keys() and channelConfigs[self.get_argument('aid')]['state'] == 'open' and channelConfigs[self.get_argument('aid')]['authKey'] == self.get_argument('authkey') and int(self.get_argument('apid')) in targetConfigs.keys():
            result = True
        return result


class RegisterHandler(tornado.web.RequestHandler):

    def get(self, spCode):
        self.write("ok")
        self.finish()
        _ip = self.request.remote_ip
        if spCode == 'dexing':
            info = {'spcode': spCode, 'spnumber': self.get_argument('spnumber'), 'mobile': self.get_argument('mobile'), 'linkid': self.get_argument(
                'linkid'), 'msg': self.get_argument('msg'), 'status': self.get_argument('delivrd'), 'ip': _ip, 'para': self.get_argument('ccpara')}
        else:
            print('error : no interface')
            return
        threads = []
        threads.append(threading.Thread(target=insert_register_log(info)))
        for t in threads:
            t.start()


class SmsHandler(tornado.web.RequestHandler):

    def get(self, sms):
        self.write("ok")
        self.finish()
        _ip = self.request.remote_ip
        if sms == 'liyu':
            _sms_info = {'spcode': sms, 'spnumber': self.get_argument('spnumber'), 'mobile': self.get_argument(
                'mobile'), 'linkid': self.get_argument('linkid'), 'msg': self.get_argument('msg'), 'status': self.get_argument('status'), 'ip': _ip, 'feetime': ''}
        elif sms == 'xinsheng':
            _sms_info = {'spcode': sms, 'spnumber': self.get_argument('spnumber'), 'mobile': self.get_argument(
                'mobile'), 'linkid': self.get_argument('linkid'), 'msg': self.get_argument('momsg'), 'status': self.get_argument('flag'), 'ip': _ip, 'feetime': ''}
        elif sms == 'zhongketianlang':
            _sms_info = {'spcode': sms, 'spnumber': self.get_argument('spnum'), 'mobile': self.get_argument(
                'mobile'), 'linkid': self.get_argument('linkid'), 'msg': self.get_argument('mocontents'), 'status': self.get_argument('status'), 'ip': _ip, 'feetime': self.get_argument('feetime')}
        elif sms == 'youle':
            _sms_info = {'spcode': sms, 'spnumber': self.get_argument('spnumber'), 'mobile': self.get_argument(
                'mobile'), 'linkid': self.get_argument('linkid'), 'msg': self.get_argument('momsg'), 'status': '', 'ip': _ip, 'feetime': ''}
        else:
            print('error : no interface')
            return
        threads = []
        # 注意这里是顺序执行而不是并行的，好郁闷
        threads.append(threading.Thread(target=insert_sms_log(_sms_info)))
        threads.append(threading.Thread(target=proc_sms(_sms_info)))
        for t in threads:
            t.start()
       # _t = threading.Thread(target=insert_sms_log(_sms_info))
       #  _t.start()
       #  _t1 = threading.Thread(target=proc_sms(_sms_info))
       #  _t1.start()


def insert_sms_log(_sms_info):
    dbLog = poolLog.connection()
    _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`,`para07`,`para08`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    _paras = (long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 101,
              _sms_info["ip"], _sms_info["spcode"], _sms_info["spnumber"], _sms_info["mobile"], _sms_info["linkid"], _sms_info["msg"], _sms_info["status"], _sms_info["feetime"])
    dbLog.cursor().execute(_sql, _paras)
    dbLog.close()
    return


def insert_register_log(_info):
    _dbLog = poolLog.connection()
    _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`,`para07`,`para08`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    _paras = (long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 102,
              _info["mobile"], _info["spcode"], _info["ip"], _info["linkid"], _info["msg"], _info["spnumber"], _info["status"], _info["para"])
    _dbLog.cursor().execute(_sql, _paras)
    _dbLog.close()
    return


def insert_fetch_log(_info):
    _dbLog = poolLog.connection()
    _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`) values (%s,%s,%s,%s,%s,%s)'
    _paras = (long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 321,
              _info["mobile"], _info["ip"], _info["query"], _info["rsp"])
    _dbLog.cursor().execute(_sql, _paras)
    _dbLog.close()
    return


def proc_sms(_sms_info):
    try:
        _sms_cmd = get_cmd(_sms_info)
        _user = get_user_by_mobile(_sms_info['mobile'])
        if _user == None:
            print("can not match user by mobile:" + _sms_info['mobile'])
        else:
            update_user_by_fee_info(_sms_cmd, _user)
            return
    except "ParameterError", _argument:
        print "ParameterError:", _argument
    else:
        return


def update_relation(imsi, apid, aid):
    try:
        _dbConfig = poolConfig.connection()
        _sql = 'update register_user_relations set registerChannelId = %s ,  fetchTime = %s where `imsi`=%s and apid=%s'
        _paras = (aid, time.time(), imsi, apid)
        _dbConfig.cursor().execute(_sql, _paras)
        _dbConfig.close()
    except "ParameterError", _argument:
        print "ParameterError:", _argument
    else:
        return


def update_user_by_fee_info(_sms_cmd, _user):
    _time_current = time.time()
    if public.is_same_month(_time_current, _user['lastFeeTime']):
        _sql = 'update imsi_users set lastFeeTime = %s , feeSum = ifnull(feeSum,0)  + %s , feeSumMonth = ifnull(feeSumMonth,0) + %s where imsi = %s '
    else:
        _sql = 'update imsi_users set lastFeeTime = %s , feeSum = ifnull(feeSum,0) + %s , feeSumMonth = %s where imsi = %s '
    dbConfig = poolConfig.connection()
    _paras = (_time_current, _sms_cmd['price'],
              _sms_cmd['price'], _user['imsi'])
    dbConfig.cursor().execute(_sql, _paras)
    dbConfig.close()


def get_cmd(_sms_info):
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT spNumber as spnumber,msg,price FROM `sms_cmd_configs` WHERE spNumber = %s and msg = %s'
    # _record = dbConfig.get(_sql, _sms_info['spnumber'], _sms_info['msg'])
    _paras = (_sms_info['spnumber'], _sms_info['msg'])
    _cur.execute(_sql, _paras)
    _record = _cur.fetchone()
    _cur.close()
    _dbConfig.close()
    if _record == None:
        raise Exception("ParameterError",
                        "can not match cmd:" + str(_sms_info))
    else:
        if _record['price'] <= 0:
            raise Exception("ParameterError",
                            "cmd price less zero:" + str(_sms_info))
        else:
            return _record


def get_user_by_mobile(_mobile):
    dbConfig = poolConfig.connection()
    cur = dbConfig.cursor()
    _sql = 'SELECT * FROM `imsi_users` WHERE mobile = %s'
    _record = None
    if len(_mobile) == 11:
        cur.execute(_sql, ('86' + _mobile))
        _record = cur.fetchone()
        # _record = dbConfig.get(_sql, '86' + _mobile)
    if _record == None:
        cur.execute(_sql, (_mobile))
        _record = cur.fetchone()
        # _record = dbConfig.get(_sql, _mobile)
    cur.close()
    dbConfig.close()
    return _record


def cache_config():
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT * FROM `system_configs` '
    _cur.execute(_sql)
    _recordRsp = _cur.fetchall()
    for _t in _recordRsp:
        systemConfigs[_t['title']] = _t['detail']
    # print(systemConfigs)
    _sql = 'SELECT * FROM `register_channels` '
    _cur.execute(_sql)
    _recordRsp = _cur.fetchall()
    for _t in _recordRsp:
        channelConfigs[_t['aid']] = _t
    # print(channelConfigs)
    _sql = 'SELECT * FROM `register_targets` '
    _cur.execute(_sql)
    _recordRsp = _cur.fetchall()
    for _t in _recordRsp:
        targetConfigs[_t['apid']] = _t
    # print(targetConfigs)
    _cur.close()
    _dbConfig.close()
    return

if __name__ == "__main__":
    print("begin...")
    app = make_app()
    cache_config()
    tornado.ioloop.PeriodicCallback(cache_config, 6000).start()
    app.listen(config.GLOBAL_SETTINGS['port'], xheaders=True)
    tornado.ioloop.IOLoop.current().start()
