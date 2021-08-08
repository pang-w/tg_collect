# -*- coding:utf-8 -*-
import time
import traceback

import configuration
from elasticsearch.exceptions import ConnectionTimeout, ConnectionError
from elasticsearch.helpers import bulk
from storage.storager import EsStorageConsumer
import json
import logging
logger = logging.getLogger(configuration.logger_name)


class UserStoreConsumer(EsStorageConsumer):

    def __init__(self, topic, IpAddress, g_id, producer, es, es_index, session_timeout, request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, producer=producer, es=es, es_index=es_index,
                         session_timeout=session_timeout, request_timeout=request_timeout)
        self.actions = []

    def run(self):
        for msg in self.consumer:
            if configuration.clean_topic:
                print("userStorage：", msg.value.decode('utf-8'))
            else:
                logger.debug('topic='+self.topic+',partition='+str(msg.partition)+', offset='+str(msg.offset))
                try:
                    msg = msg.value.decode('utf-8')
                    if msg == "0": continue
                    # logger.debug("user storage: {}".format(msg))

                    user = json.loads(msg)
                    username = "" if not "username" in user else "_"+user["username"]
                    _id = str(user["user_id"]) + username
                except BaseException as e:
                    logger.error(str(msg.value) + ' reason:' + repr(e))
                    continue

                # store to user index
                try:
                    if "description" in user:
                        self.actions.append({"_id": _id, "_source": user})
                    else:
                        result = self.es[0].search(index=configuration.ES_INDEX_USER,
                                                   body={"query": {"match": {"user_id": user["user_id"]}}})
                        if len(result["hits"]["hits"]) == 0:  # 如果es中没有，则直接存
                            self.actions.append({"_id": _id, "_source": user})
                        else:  # 如果es中有，则遍历看看是否有description字段，如果有，则加上
                            for i in range(len(result["hits"]["hits"])):
                                res_user = result["hits"]["hits"][i]["_source"]
                                if "description" in res_user:
                                    user["description"] = res_user["description"]
                                    break
                            self.actions.append({"_id": _id, "_source": user})

                    if len(self.actions) >= 50:  # 50条消息提交一次
                        for e in self.es:
                            bulk(client=e, actions=self.actions, index=configuration.ES_INDEX_USER)
                        for action in self.actions:
                            logger.debug('store to ES successfully,user:' + action["_id"])
                        self.actions = []
                except BaseException:
                    logger.error(str(msg) + traceback.format_exc())
                    self.actions = []
                    for action in self.actions:
                        self.reput(action["_source"])
                    time.sleep(10)
