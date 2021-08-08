# encoding: utf-8

import configuration
from elasticsearch.exceptions import ConnectionTimeout
from storage.storager import EsStorageConsumer
import json
import logging
logger = logging.getLogger(configuration.logger_name)


class MediaStorageConsumer(EsStorageConsumer):

    def __init__(self, topic, IpAddress, g_id, producer, es, es_index, session_timeout, request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, producer=producer, es=es, es_index=es_index,
                         session_timeout=session_timeout, request_timeout=request_timeout)

    def run(self):
        for msg in self.consumer:
            if configuration.clean_topic:
                print("media storage：", msg.value.decode('utf-8'))
            else:
                try:
                    media=json.loads(msg.value.decode('utf-8'))
                except BaseException as e:
                    logger.error(str(msg.value) + ' reason:' + repr(e))
                    continue

                print("media 存储模块接到数据：", media)

                if media["type"] == "chat_avatar":
                    _id = str(media["chat_id"]) + "_" + media["file_id"]
                elif media["type"] == "user_avatar":
                    _id = str(media["user_id"]) + "_" + media["file_id"]
                elif media["type"] in ("photo", "document", "audio", "video", "voice"):
                    _id = str(media["chat_id"]) + "_" + str(media["message_id"]) + "_" + media["file_id"]
                else:
                    logger.warning("check the media type!")
                    continue

                #store to chat_member index
                try:
                    for e in self.es:
                        e.index(index=self.es_index,id=_id,body=media)
                    logger.debug('store to ES successfully,media id:' + _id)
                except (ConnectionTimeout,BaseException) as e :
                    logger.warning('store data failly,retry,media id:' + _id+'   ,reason:'+repr(e))
                    self.reput(media) # 保存失败则将此chat_member重新发送至es消费者存储