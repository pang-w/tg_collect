# -*- coding:utf-8 -*-
import logging
import time
import traceback

import configuration
from storage.storager import StorageConsumer
from datetime import datetime
import pymysql

logger = logging.getLogger(configuration.logger_name)

class OnionStoreConsumer(StorageConsumer):
    def __init__(self,topic,IpAddress,g_id,session_timeout,request_timeout,producer):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, producer=producer,
                         session_timeout=session_timeout, request_timeout=request_timeout)
        self.connect = pymysql.connect(host=configuration.MYSQL_ONION_HOST, port=configuration.MYSQL_ONION_PORT, user=configuration.MYSQL_ONION_USER,
                              passwd=configuration.MYSQL_ONION_PASSWD, db=configuration.MYSQL_ONION_DB)
        self.mysql = self.connect.cursor()
        self.mysql.execute(configuration.MYSQL_CREATE_ONION_TABLE)
        self.connect.commit()

    def run(self):
        for msg in self.consumer:
            try:
                result = msg.value.decode('utf-8')
                if result == "0": continue
                # logger.debug("onion storage: {}".format(msg))

            except BaseException as e:
                logger.error(str(msg.value)+' reason:'+repr(e))
                continue

            onion_link='http://'+str(result)+'.onion/'

            # check is the onion exist
            try:
                check_sql='select * from '+configuration.MYSQL_TABLE_ONION+' where url=%s'
                self.connect.ping(reconnect=True)
                self.mysql.execute(check_sql,(onion_link))
                olink = self.mysql.fetchall()
                now_time=datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
                if(len(olink)>0):
                    # update this data
                    update_sql= 'UPDATE ' + configuration.MYSQL_TABLE_ONION + ' SET last_discovered_time= %s, last_discovered_method=%s where url=%s'
                    self.mysql.execute(update_sql,(now_time,'telegram',onion_link))
                else:
                    # store this new data
                    insert_sql = 'INSERT INTO ' + configuration.MYSQL_TABLE_ONION + ' (url,first_discovered_time,first_discovered_method,last_discovered_time,last_discovered_method,fail_count,is_checked,type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
                    self.mysql.execute(insert_sql, (onion_link,now_time,'telegram',now_time,'telegram','0','0','tor'))
                self.connect.commit()
                logger.debug('store to sql successfully,onion:'+str(onion_link))
            except BaseException:
                logger.warning(str(msg.value) + traceback.format_exc())
                self.producer.sendMsg(topic=self.topic, value=result)# 重新发送
                time.sleep(10)