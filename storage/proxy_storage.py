# -*- coding:utf-8 -*-
import json
import logging
import traceback

import configuration
from storage.storager import StorageConsumer
import pymysql
logger = logging.getLogger(configuration.logger_name)

class ProxyStoreConsumer(StorageConsumer):
    def __init__(self,topic,IpAddress,g_id,session_timeout,request_timeout,producer):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, producer=producer,
                         session_timeout=session_timeout, request_timeout=request_timeout)
        self.connect = pymysql.connect(host=configuration.MYSQL_PROXY_HOST, port=configuration.MYSQL_PROXY_PORT, user=configuration.MYSQL_PROXY_USER,
                              passwd=configuration.MYSQL_PROXY_PASSWD, db=configuration.MYSQL_PROXY_DB)
        self.mysql = self.connect.cursor()
        self.mysql.execute(configuration.MYSQL_CREATE_PROXY_TABLE)
        self.connect.commit()

    def run(self):
        for msg in self.consumer:
            try:
                msg = msg.value.decode('utf-8')
                if msg == "0": continue
                # logger.debug("proxy storage: {}".format(msg))

                result = json.loads(msg)

                if not(len(result)==4):
                    if not ('server'in result):
                        result['server']=''
                    if not ('port'in result):
                        result['port'] = ''
                    if not ('secret'in result):
                        result['secret'] = ''
            except BaseException as e:
                logger.error(str(msg.value)+' reason:'+repr(e))
                continue

            #store to database，five fileds
            try:
                sql = 'INSERT INTO ' + configuration.MYSQL_TABLE_PROXY + ' (server,port,secret,date) VALUES (%s, %s, %s,%s)'
                self.connect.ping(reconnect=True)
                self.mysql.execute(sql, (result['server'], result['port'], result['secret'], result['date']))
                self.connect.commit()
                logger.debug('store to sql successfully,proxy:'+str(result))
            except BaseException as e:
                logger.warning(str(result) + traceback.format_exc())
                self.reput(result)
