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
from log import logger
import public
import MySQLdb
from pymongo import MongoClient
from gevent.greenlet import Greenlet
from gevent import monkey
monkey.patch_all()
from DBUtils.PooledDB import PooledDB
poolConfig = PooledDB(MySQLdb, 5, host=config.GLOBAL_SETTINGS['config_db']['host'], user=config.GLOBAL_SETTINGS['config_db']['user'], passwd=config.GLOBAL_SETTINGS[
                      'config_db']['psw'], db=config.GLOBAL_SETTINGS['config_db']['name'], port=config.GLOBAL_SETTINGS['config_db']['port'], setsession=['SET AUTOCOMMIT = 1'], cursorclass=MySQLdb.cursors.DictCursor, charset="utf8")
poolLog = PooledDB(MySQLdb, 5, host=config.GLOBAL_SETTINGS['log_db']['host'], user=config.GLOBAL_SETTINGS['log_db']['user'], passwd=config.GLOBAL_SETTINGS[
    'log_db']['psw'], db=config.GLOBAL_SETTINGS['log_db']['name'], port=config.GLOBAL_SETTINGS['log_db']['port'], setsession=['SET AUTOCOMMIT = 1'], cursorclass=MySQLdb.cursors.DictCursor, charset="utf8")
gMongoCli = MongoClient(config.GLOBAL_SETTINGS['mongodb'])

systemConfigs = {}
channelConfigs = {}
targetConfigs = {}


class MainHandler(tornado.web.RequestHandler):

    def get(self):
        self.write("error")

    def post(self, *args, **kwargs):
        self.write("error")


class IvrHandler(tornado.web.RequestHandler):

    def get(self, spCode):
        self.write("ok")
        self.finish()
        _info = None
        _ip = self.request.remote_ip
        if spCode == 'zhongketianlang':
            _info = {'spcode': spCode, 'spnumber': self.get_argument('spnum'), 'mobile': self.get_argument(
                'mobile'), 'linkid': self.get_argument('linkid'), 'msg': self.get_argument('mocontents'), 'status': self.get_argument('status'), 'ip': _ip, 'feetime': self.get_argument('feetime'), 'query': str(self.request.query_arguments)}
        else:
            print('error : no interface')
            return
        if _info != None:
            _g_insert_ivr_log = insert_ivr_log(_info)
            _g_insert_ivr_log.start()


class insert_ivr_log(Greenlet):

    def __init__(self, info):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self.info = info

    def run(self):
        self.insertLog(self.info)

    def insertLog(self, _sms_info):
        _sms_info['province'] = get_province_from_mobile(
            _sms_info["mobile"][0:7])
        dbLog = poolLog.connection()
        _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`,`para07`,`para08`,`para09`,`para10`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        _paras = [long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 103,
                  _sms_info["mobile"], _sms_info["spcode"], _sms_info["spnumber"], _sms_info["ip"], _sms_info["linkid"], _sms_info["msg"], _sms_info["status"], _sms_info["feetime"], _sms_info["province"], _sms_info["query"]]
        dbLog.cursor().execute(_sql, _paras)
        dbLog.close()
        return


class MonthHandler(tornado.web.RequestHandler):

    def get(self):
        self.write("ok")


class GetMobiHandler(tornado.web.RequestHandler):

    def get(self):
        _result = {}
        threads = []
        # if self.get_argument('apid') != '105' and self.checkParameter():
        if self.checkParameter():
            # if False:
            province = self.get_argument("province", None, True)
            _dbConfig = poolConfig.connection()
            _cur = _dbConfig.cursor()
            if province == None:
                # _sql = 'SELECT mobile,imsi FROM `imsi_users` WHERE imsi = ( SELECT imsi FROM `register_user_relations` WHERE apid = %s and getTime > (%s-87400) and ifnull(registerChannelId,1)=1 limit 1)'
                _sql = 'SELECT mobile,imsi_users.imsi FROM `imsi_users`,register_user_relations WHERE imsi_users.imsi = register_user_relations.imsi AND LENGTH(mobile)>=11 AND apid = %s AND getTime > (%s-86400) AND IFNULL(registerChannelId,1)=1 AND isMoReady=1 and tryCount<%s LIMIT 1'
                _cur.execute(_sql, [self.get_argument(
                    'apid'), time.time(), systemConfigs['relationTryCountLimit']])
            else:
                _sql = 'SELECT mobile,imsi_users.imsi FROM `imsi_users`,register_user_relations,`mobile_areas` WHERE register_user_relations.imsi = `imsi_users`.`imsi` AND SUBSTR(IFNULL(imsi_users.mobile,\'8612345678901\'),3,7)=mobile_areas.`mobileNum` AND register_user_relations.apid = %s AND register_user_relations.getTime > (%s-87400) AND IFNULL(register_user_relations.registerChannelId,1)=1 AND mobile_areas.province=%s AND isMoReady=1 and tryCount<%s   LIMIT 1'
                _cur.execute(_sql, [self.get_argument('apid'),
                                    time.time(), province, systemConfigs['relationTryCountLimit']])
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
        elif spCode == 'dexingwx':
            info = {'spcode': spCode, 'spnumber': self.get_argument('spnumber'), 'mobile': self.get_argument('mobile'), 'linkid': self.get_argument(
                'linkid'), 'msg': self.get_argument('msg'), 'status': self.get_argument('delivrd'), 'ip': _ip, 'para': self.get_argument('ccpara')}
        elif spCode == 'kaixingyuan':
            # info = {'spcode': spCode, 'spnumber': self.get_argument('spnumber'), 'mobile': self.get_argument('mobile'), 'linkid': self.get_argument(
            #     'orderId'), 'msg': self.get_argument('cmd'), 'status': 'delivrd', 'ip': _ip, 'para': self.get_argument('cpparm')}
            info = {'spcode': spCode, 'spnumber': '12306', 'mobile': self.get_argument('mobile'), 'linkid': self.get_argument(
                'orderId'), 'msg': '999', 'status': 'delivrd', 'ip': _ip, 'para': self.get_argument('cpparm')}
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
                'mobile'), 'linkid': self.get_argument('linkid'), 'msg': self.get_argument('msg'), 'status': self.get_argument('status'), 'ip': _ip, 'feetime': '', 'query': str(self.request.query_arguments)}
        elif sms == 'xinsheng':
            _sms_info = {'spcode': sms, 'spnumber': self.get_argument('spnumber'), 'mobile': self.get_argument(
                'mobile'), 'linkid': self.get_argument('linkid'), 'msg': self.get_argument('momsg'), 'status': self.get_argument('flag'), 'ip': _ip, 'feetime': '', 'query': str(self.request.query_arguments)}
        elif sms == 'zhongketianlang':
            _sms_info = {'spcode': sms, 'spnumber': self.get_argument('spnum'), 'mobile': self.get_argument(
                'mobile'), 'linkid': self.get_argument('linkid'), 'msg': self.get_argument('mocontents'), 'status': self.get_argument('status'), 'ip': _ip, 'feetime': self.get_argument('feetime'), 'query': str(self.request.query_arguments)}
        elif sms == 'youle':
            _sms_info = {'spcode': sms, 'spnumber': self.get_argument('spnumber'), 'mobile': self.get_argument(
                'mobile'), 'linkid': self.get_argument('linkid'), 'msg': self.get_argument('momsg'), 'status': '', 'ip': _ip, 'feetime': '', 'query': str(self.request.query_arguments)}
        elif sms == 'kaixingyuan':
            _sms_info = {'spcode': sms, 'spnumber': self.get_argument('spnumber'), 'mobile': self.get_argument(
                'mobile'), 'linkid': self.get_argument('orderId'), 'msg': self.get_argument('cmd'), 'status': '', 'ip': _ip, 'feetime': '', 'province': self.get_argument('province'), 'query': str(self.request.query_arguments)}
        else:
            logger.info('error : no interface')
            return
        _g_insert_sms_log = insert_sms_log(_sms_info)
        _g_insert_sms_log.start()
        _g_proc_sms_log = proc_sms(_sms_info)
        _g_proc_sms_log.start()
        # threads = []
        # 注意这里是顺序执行而不是并行的，好郁闷
        # threads.append(threading.Thread(target=insert_sms_log(_sms_info)))
        # threads.append(threading.Thread(target=proc_sms(_sms_info)))
        # for t in threads:
        #     t.start()
       # _t = threading.Thread(target=insert_sms_log(_sms_info))
       #  _t.start()
       #  _t1 = threading.Thread(target=proc_sms(_sms_info))
       #  _t1.start()


class insert_sms_log(Greenlet):

    def __init__(self, info):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self.info = info

    def run(self):
        self.insertLog(self.info)

    def insertLog(self, _sms_info):
        if not 'province' in _sms_info:
            _sms_info['province'] = get_province_from_mobile(
                _sms_info["mobile"][0:7])
        dbLog = poolLog.connection()
        _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`,`para07`,`para08`,`para09`,`para10`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        _paras = [long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 101,
                  _sms_info["ip"], _sms_info["spcode"], _sms_info["spnumber"], _sms_info["mobile"], _sms_info["linkid"], _sms_info["msg"], _sms_info["status"], _sms_info["feetime"], _sms_info["province"], _sms_info["query"]]
        dbLog.cursor().execute(_sql, _paras)
        dbLog.close()
        return


# def insert_sms_log(_sms_info):
#     if not 'province' in _sms_info:
#         _sms_info['province'] = get_province_from_mobile(
#             _sms_info["mobile"][0:7])
#     dbLog = poolLog.connection()
#     _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`,`para07`,`para08`,`para09`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
#     _paras = [long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 101,
#               _sms_info["ip"], _sms_info["spcode"], _sms_info["spnumber"], _sms_info["mobile"], _sms_info["linkid"], _sms_info["msg"], _sms_info["status"], _sms_info["feetime"], _sms_info["province"]]
#     dbLog.cursor().execute(_sql, _paras)
#     dbLog.close()
#     return


def get_province_from_mobile(_prefix_mobile):
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT province FROM `mobile_areas` WHERE mobileNum = %s'
    # _record = dbConfig.get(_sql, _sms_info['spnumber'], _sms_info['msg'])
    _paras = [_prefix_mobile]
    _cur.execute(_sql, _paras)
    _record = _cur.fetchone()
    _result = None
    if _record != None:
        _result = _record['province']
    _cur.close()
    _dbConfig.close()
    return _result


def insert_register_log(_info):
    _info['province'] = get_province_from_mobile(_info["mobile"][0:7])
    _dbLog = poolLog.connection()
    _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`,`para06`,`para07`,`para08`,`para09`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    _paras = [long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 102,
              _info["mobile"], _info["spcode"], _info["ip"], _info["linkid"], _info["msg"], _info["spnumber"], _info["status"], _info["para"], _info["province"]]
    _dbLog.cursor().execute(_sql, _paras)
    _dbLog.close()
    return


def insert_weixinMo_log(_info):
    _dbLog = poolLog.connection()
    _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`,`para05`) values (%s,%s,%s,%s,%s,%s,%s)'
    _paras = [long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 333,
              _info["mobile"], _info["spcode"], _info["ip"], _info["spnumber"], _info["msg"]]
    _dbLog.cursor().execute(_sql, _paras)
    _dbLog.close()
    return

WXMO_CONTENT = '<?xml version="1.0" encoding="UTF-8"?><wml><card><Ccmd_cust>[cmd]</Ccmd_cust><Cnum_cust>[targetNum]</Cnum_cust><filter1_cust>腾讯科技|微信</filter1_cust><filter2_cust></filter2_cust><Creconfirm_cust></Creconfirm_cust><PortShield>1069</PortShield><fee></fee><autofee>1</autofee><feemode>11</feemode></card></wml>'


def proc_weixinMo(self, info):
    doc = gMongoCli.sms.wechat_mos.find_one({"_id": info['mobile']})
    if doc != None:
        dbConfig = poolConfig.connection()
        _cur = dbConfig.cursor()
        _sql = "select expiredTime from test_responses where imsi=%s and testStatus='wxmo'"
        _paras = [doc['imsi']]
        _cur.execute(_sql, _paras)
        _recordRsp = _cur.fetchone()
        wxmoRsp = WXMO_CONTENT.replace(
            '[cmd]', str(info['msg'])).replace('[targetNum]', str(info['spnumber']))
        if _recordRsp != None:
            expiredTime = round(time.time()) + 3600
            if expiredTime < int(_recordRsp['expiredTime']):
                expiredTime = int(_recordRsp['expiredTime'])
            _sql = "update `test_responses` set expiredTime=%s,response=%s where imsi = %s  and testStatus='wxmo'"
            _paras = [expiredTime, wxmoRsp, doc['imsi']]
            _cur.execute(_sql, _paras)
        else:
            _sql = "insert into test_responses (imsi,testStatus,expiredTime,response) values (%s,'wxmo',unix_timestamp(now())+3600,%s)"
            _paras = [doc['imsi'], wxmoRsp]
            _cur.execute(_sql, _paras)
        _sql = 'select testStatus,expiredTime from test_imsis where imsi=%s'
        _paras = [doc['imsi']]
        _cur.execute(_sql, _paras)
        _recordRsp = _cur.fetchone()
        if _recordRsp != None:
            expiredTime = round(time.time()) + 3600
            if expiredTime < int(_recordRsp['expiredTime']):
                expiredTime = int(_recordRsp['expiredTime'])
            if _recordRsp['testStatus'] == 'wxmo':
                _sql = "update `test_imsis` set expiredTime=%s,remark=%s,mobile=%s where imsi = %s"
            else:
                _sql = "update `test_imsis` set expiredTime=%s,remark=%s,mobile=%s,testStatus='wxmo' where imsi = %s"
            _paras = [expiredTime, info['msg'],
                      info['mobile'], doc['imsi']]
            _cur.execute(_sql, _paras)
        else:
            _sql = "insert into test_imsis (imsi,testStatus,expiredTime,remark,mobile) values (%s,'wxmo',unix_timestamp(now())+3600,%s,%s)"
            _paras = [doc['imsi'], info['msg'], info['mobile']]
            _cur.execute(_sql, _paras)
        _sql = "insert into wait_send_ads (targetMobile,msg,createTime,oriContent) values (%s,'ztldxtest',unix_timestamp(now()),%s)"
        _paras = [info['mobile'], info['msg']]
        _cur.execute(_sql, _paras)
        _cur.close()
        dbConfig.close()
        self.write("ok")
    else:
        self.write('{"err":"mobile have no record"}')


class WeiXinMoHandler(tornado.web.RequestHandler):

    def get(self, spCode):
        # self.write("ok")
        # self.finish()
        _ip = self.request.remote_ip
        if spCode == 'dexing':
            info = {'spcode': spCode, 'spnumber': self.get_argument('spnumber'), 'mobile': self.get_argument(
                'mobile'),  'msg': self.get_argument('replyinfo'), 'ip': _ip}
        else:
            logger.error('error : no interface')
            self.write("error : no interface")
            return
        proc_weixinMo(self, info)
        threads = []
        threads.append(threading.Thread(target=insert_weixinMo_log(info)))
        # threads.append(threading.Thread(target=proc_weixinMo(info)))
        for t in threads:
            t.start()


def insert_fetch_log(_info):
    _dbLog = poolLog.connection()
    _sql = 'insert into log_async_generals (`id`,`logId`,`para01`,`para02`,`para03`,`para04`) values (%s,%s,%s,%s,%s,%s)'
    _paras = [long(round(time.time() * 1000)) * 10000 + random.randint(0, 9999), 321,
              _info["mobile"], _info["ip"], _info["query"], _info["rsp"]]
    _dbLog.cursor().execute(_sql, _paras)
    _dbLog.close()
    return


class proc_sms(Greenlet):

    def __init__(self, info):
        # super(greenlet, self).__init__()
        Greenlet.__init__(self)
        self.info = info

    def run(self):
        self.proc(self.info)

    def proc(self, _sms_info):
        try:
            _sms_cmd = get_cmd(_sms_info)
            _user = get_user_by_mobile(_sms_info['mobile'])
            if _user == None:
                logger.info("can not match user by mobile:" +
                            _sms_info['mobile'])
            else:
                update_user_by_fee_info(_sms_cmd, _user)
            return
        except Exception as error:
            logger.error(error)
        else:
            return


# def proc_sms(_sms_info):
#     try:
#         _sms_cmd = get_cmd(_sms_info)
#         _user = get_user_by_mobile(_sms_info['mobile'])
#         if _user == None:
#             print("can not match user by mobile:" + _sms_info['mobile'])
#         else:
#             update_user_by_fee_info(_sms_cmd, _user)
#         return
#     except Exception as error:
#         print(error)
#     else:
#         return


def update_relation(imsi, apid, aid):
    try:
        _dbConfig = poolConfig.connection()
        _sql = 'update register_user_relations set registerChannelId = %s ,  fetchTime = %s , tryCount=tryCount+1 where `imsi`=%s and apid=%s'
        _paras = [aid, time.time(), imsi, apid]
        _dbConfig.cursor().execute(_sql, _paras)
        _dbConfig.close()
    except Exception as error:
        print(error)
    else:
        return


def update_user_by_fee_info(_sms_cmd, _user):
    _time_current = time.time()
    if public.is_same_month(_time_current, _user['lastFeeTime']):
        _sql = 'update imsi_users set lastFeeTime = %s , feeSum = ifnull(feeSum,0)  + %s , feeSumMonth = ifnull(feeSumMonth,0) + %s where imsi = %s '
    else:
        _sql = 'update imsi_users set lastFeeTime = %s , feeSum = ifnull(feeSum,0) + %s , feeSumMonth = %s where imsi = %s '
    dbConfig = poolConfig.connection()
    _paras = [_time_current, _sms_cmd['price'],
              _sms_cmd['price'], _user['imsi']]
    dbConfig.cursor().execute(_sql, _paras)
    dbConfig.close()


def get_cmd(_sms_info):
    _dbConfig = poolConfig.connection()
    _cur = _dbConfig.cursor()
    _sql = 'SELECT spNumber as spnumber,msg,price FROM `sms_cmd_configs` WHERE spNumber = %s and msg = %s'
    # _record = dbConfig.get(_sql, _sms_info['spnumber'], _sms_info['msg'])
    _paras = [_sms_info['spnumber'], _sms_info['msg']]
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
    _sql = 'SELECT * FROM `imsi_users` LEFT JOIN mobile_areas ON SUBSTR(imsi_users.mobile,3,7)=mobile_areas.`mobileNum` WHERE imsi_users.mobile = %s order by id desc limit 1'
    _record = None
    if len(_mobile) == 11:
        cur.execute(_sql, ['86' + _mobile])
        _record = cur.fetchone()
        # _record = dbConfig.get(_sql, '86' + _mobile)
    if _record == None:
        cur.execute(_sql, [_mobile])
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


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/sms/([0-9a-zA-Z\-\_]+)", SmsHandler),
        (r"/ivr/([0-9a-zA-Z\-\_]+)", IvrHandler),
        (r"/month/([0-9a-zA-Z\-\_]+)", MonthHandler),
        (r"/register/([0-9a-zA-Z\-\_]+)", RegisterHandler),
        (r"/wxmo/([0-9a-zA-Z\-\_]+)", WeiXinMoHandler),
        (r"/getmobi", GetMobiHandler),
    ])

if __name__ == "__main__":
    logger.info("begin... on port:" + str(config.GLOBAL_SETTINGS['port']))
    app = make_app()
    cache_config()
    tornado.ioloop.PeriodicCallback(cache_config, 6000).start()
    app.listen(config.GLOBAL_SETTINGS['port'], xheaders=True)
    tornado.ioloop.IOLoop.current().start()
