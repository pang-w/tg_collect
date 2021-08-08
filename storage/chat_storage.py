# encoding: utf-8
import time
import traceback

import configuration
from elasticsearch.exceptions import ConnectionTimeout
from storage.storager import EsStorageConsumer
import json
import logging
import pymysql

logger = logging.getLogger(configuration.logger_name)

class ChatStoreConsumer(EsStorageConsumer):

    def __init__(self, topic, IpAddress, g_id, producer,es,es_index,session_timeout,request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, producer=producer,es=es,es_index=es_index,session_timeout=session_timeout,request_timeout=request_timeout)
        self.connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                                       user=configuration.MYSQL_USERNAME,
                                       passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
        self.mysql = self.connect.cursor()
        self.connect.commit()

    def run(self):
        for msg in self.consumer:
            if configuration.clean_topic:
                print("chatStorage：", msg.value.decode('utf-8'))
            else:
                logger.debug('partition='+str(msg.partition)+', offset='+str(msg.offset))
                try:
                    msg = msg.value.decode('utf-8')
                    if msg == "0": continue
                    # logger.debug("chat storage: {}".format(msg))

                    chat = json.loads(msg)

                    _id = str(chat["chat_id"])
                except BaseException as e:
                    logger.error(str(msg) + ' reason:' + repr(e))
                    continue

                # store to chat index
                try:

                    result = self.es[0].search(index=configuration.ES_INDEX_CHAT,
                                               body={"query": {"match": {"chat_id": chat["chat_id"]}}}, size=1)
                    if len(result["hits"]["hits"]) != 0:
                        res_chat = result["hits"]["hits"][0]["_source"]
                        if "migrate_to_chat_id" in res_chat:
                            chat["migrate_to_chat_id"] = res_chat["migrate_to_chat_id"]
                        if "migrate_from_chat_id" in res_chat:
                            chat["migrate_from_chat_id"] = res_chat["migrate_from_chat_id"]
                    for e in self.es:
                        e.index(index=self.es_index,id=_id,body=chat)
                    # 尽量保证mysql中记录正在采集群组的表中群组的数据也是新的
                    type = chat["type"]
                    title = chat["title"] if "title" in chat else None
                    username = chat["username"] if "username" in chat else None
                    members_count = chat["members_count"] if "members_count" in chat else 0
                    invite_link = chat["invite_link"] if "invite_link" in chat else None
                    description = chat["description"] if "description" in chat else None
                    check_sql = 'select * from ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' where group_id=%s;'
                    self.connect.ping(reconnect=True)
                    self.mysql.execute(check_sql, (chat["chat_id"]))
                    res = self.mysql.fetchall()
                    if res.__len__() != 0: # 有则更新，没有则可能是新增，也有可能是从客户端离开群组，status值不确定，不作处理
                        update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' SET type=%s, title=%s, username=%s, members_count=%s, invite_link=%s, description=%s, collect_account=%s where group_id=%s'
                        self.mysql.execute(update_sql,
                                           (type, title, username, members_count, invite_link, description, configuration.PHONENUM, chat["chat_id"]))
                    self.connect.commit()
                    logger.debug('store to ES successfully,chat_id='+_id)
                except BaseException:
                    logger.error(str(msg) + traceback.format_exc())
                    self.reput(chat)  # 保存失败则将此chat重新发送至es消费者存储
                    time.sleep(10)