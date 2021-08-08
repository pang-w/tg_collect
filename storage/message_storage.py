# -*- coding:utf-8 -*-
import time
import traceback
from datetime import datetime

import configuration
from elasticsearch.exceptions import ConnectionTimeout, ConnectionError
from elasticsearch.helpers import bulk
from storage.storager import EsStorageConsumer
import json
from utils import util
import logging
logger=logging.getLogger(configuration.logger_name)


class MessageStoreConsumer(EsStorageConsumer):

    def __init__(self, topic, IpAddress, g_id, producer,es,es_index,session_timeout,request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, producer=producer,es=es,es_index=es_index,session_timeout=session_timeout,request_timeout=request_timeout)
        self.actions = []

    def run(self):
        for msg in self.consumer:
            if configuration.clean_topic:
                print("messageStorage：", json.loads(msg.value.decode('utf-8')))
            else:
                logger.debug('topic='+self.topic+',partition='+str(msg.partition)+', offset='+str(msg.offset))
                try:
                    msg = msg.value.decode('utf-8')
                    if msg == "0": continue
                    # logger.debug("message storage: {}".format(msg))

                    message=json.loads(msg)
                    message["insert_date"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    _id = str(message['chat_id']) + '_' + str(message['message_id'])
                    if "edit_date" in message:  # 编辑日期（消息可重新编辑）
                        edit_date = datetime.strptime(message["edit_date"], '%Y-%m-%d %H:%M:%S')
                        _id = _id + "_" + edit_date.strftime('%Y%m%d%H%M%S')
                except BaseException as e:
                    logger.error(str(msg.value) + ' reason:' + repr(e))
                    continue

                # store to message index
                try:
                    self.actions.append({
                        "_id": _id,
                        "_source": message
                    })
                    if len(self.actions) >= 10:  # 10条消息提交一次
                        for e in self.es:
                            bulk(client=e, actions=self.actions, index=configuration.ES_INDEX_MESSAGE)
                        for action in self.actions:
                            logger.debug('store to ES successfully,message_union_id=' + action["_id"])
                        self.actions = []
                except BaseException:
                    logger.error(str(msg) + traceback.format_exc())
                    self.actions = []
                    for action in self.actions:
                        self.reput(action["_source"])
                    time.sleep(10)
