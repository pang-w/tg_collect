# encoding: utf-8
import traceback

import configuration
from elasticsearch.exceptions import ConnectionTimeout, ConnectionError
from elasticsearch.helpers import bulk
from storage.storager import EsStorageConsumer
import json
import time
import logging
logger = logging.getLogger(configuration.logger_name)


class ChatMemberStorageConsumer(EsStorageConsumer):

    def __init__(self, topic, IpAddress, g_id, producer, es, es_index, session_timeout, request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, producer=producer, es=es, es_index=es_index,
                         session_timeout=session_timeout, request_timeout=request_timeout)
        self.actions = []

    def run(self):
        for msg in self.consumer:
            if configuration.clean_topic:
                print("chatMemberStorage：", msg.value.decode('utf-8'))
            else:
                try:
                    msg = msg.value.decode('utf-8')
                    if msg == "0": continue
                    # logger.debug("chat_member storage: {}".format(msg))

                    chat_member=json.loads(msg)

                    _id = str(chat_member["chat_id"]) + "_" + str(chat_member["user_id"])
                except BaseException as e:
                    logger.error(str(msg) + ' reason:' + repr(e))
                    continue

                # store to chat_member index
                try:
                    # print("群组关系存储模块：", chat_member)
                    self.actions.append({
                        "_id": _id,
                        "_source": chat_member
                    })
                    if len(self.actions) >= 50:  # 10条消息提交一次
                        for e in self.es:
                            result = bulk(client=e, actions=self.actions, index=configuration.ES_INDEX_CHAT_MEMBER)
                            # print("返回结果为：", result)
                        for action in self.actions:
                            logger.debug('store to ES successfully,chat_member id:' + action["_id"])
                        self.actions = []
                except BaseException:
                    logger.error(str(msg) + traceback.format_exc())
                    self.actions = []
                    for action in self.actions:
                        self.reput(action["_source"])
                    time.sleep(10)