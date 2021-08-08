# -*- coding:utf-8 -*-
import datetime
import time
import traceback

from storage.storager import StorageConsumer
import json
import logging
import configuration

import pymysql

logger=logging.getLogger(configuration.logger_name)

class FindGroupStoreConsumer(StorageConsumer):

    def __init__(self, topic, IpAddress, g_id, producer,session_timeout,request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, producer=producer,session_timeout=session_timeout,request_timeout=request_timeout)
        self.connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                                       user=configuration.MYSQL_USERNAME,
                                       passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
        self.mysql = self.connect.cursor()
        self.mysql.execute(configuration.MYSQL_CREATE_FIND_GROUP_TABLE)
        self.connect.commit()

    def run(self):
        for msg in self.consumer:
            try:
                msg = msg.value.decode('utf-8')
                if msg == "0": continue
                # logger.debug("find group storage: {}".format(msg))

                link_info=json.loads(msg)
            except BaseException as e:
                logger.error(str(msg.value) + ' reason:' + repr(e))
                continue
            # 保存到find_group表
            try:
                check_sql = 'select * from ' + configuration.MYSQL_TABLE_FIND_GROUP + ' where link=%s;'
                self.connect.ping(reconnect=True)
                self.mysql.execute(check_sql, (link_info["link"]))
                res = self.mysql.fetchall()
                if res.__len__() == 0:
                    insert_sql = 'INSERT INTO ' + configuration.MYSQL_TABLE_FIND_GROUP + ' (link,type,ui,chat_id,message_id,text,first_find_time,last_find_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
                    self.mysql.execute(insert_sql, (link_info["link"], link_info["type"], link_info["ui"], link_info["chat_id"],
                                                    link_info["message_id"], link_info["text"], link_info["find_time"], link_info["find_time"]))
                else:
                    # 做时间的比较是为了当采集历史消息时出现链接时间比数据库中存储的时间要早的情况
                    first_find_time = res[0][6]  # 数据库中保存的第一次发现时间
                    last_find_time = res[0][7]  # 数据库中保存的最后一次发现时间
                    find_time = datetime.datetime.strptime(link_info["find_time"], "%Y-%m-%d %H:%M:%S")  # 此链接的消息发送的时间，传进来的时间
                    if (find_time - last_find_time).total_seconds() > 0 : # 如果传进来的时间比数据库中最后一次发现时间晚，则更新最后一次发现时间
                        update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_FIND_GROUP + ' SET last_find_time= %s where link=%s'
                        self.mysql.execute(update_sql, (link_info["find_time"], link_info["link"]))
                    elif (find_time - first_find_time).total_seconds() < 0: # 如果比第一次发现时间早则第一次发现时间为传进来的时间
                        update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_FIND_GROUP + ' SET first_find_time=%s where link=%s'
                        self.mysql.execute(update_sql, (link_info["find_time"], link_info["link"]))
                    # 在两个时间中间则不处理
                self.connect.commit()
                logger.debug('store to mysql successfully,link:' + link_info["link"])
            # except pymysql.err.InterfaceError as e:
            #     logger.warning('the connect of onion sql is failed,store data failly,retry,sql data: '+str(link_info) +'  reason:'+repr(e))
            #     self.connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
            #                                    user=configuration.MYSQL_USERNAME,
            #                                    passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
            #     self.mysql = self.connect.cursor()
            #     self.reput(link_info)  # retry
            except BaseException:
                logger.error(str(msg) + traceback.format_exc())
                self.reput(link_info)
                time.sleep(10)


