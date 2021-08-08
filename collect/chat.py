# encoding: utf-8

import json
import time
import traceback
import logging
from filter.filter import filter_chat

from pyrogram.errors import FloodWait, ChannelPrivate

import configuration
from kafkaConsumer import KafkaConsumer

logger=logging.getLogger(configuration.logger_name)

# 采集一个chat的信息
# topic name: chat
# topic data format: str(chat_id)
class Chat(KafkaConsumer):
    def __init__(self,topic,IpAddress,g_id,producer,client,session_timeout,request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, session_timeout=session_timeout,
                         request_timeout=request_timeout)
        self.producer = producer
        self.client = client

    def run(self):  # 消费者获取群组id，采集更新群组信息
        for msg in self.consumer:
            try:
                chat_id = msg.value.decode('utf-8')
                if chat_id == "0": continue
                logger.debug("chat collector: {}".format(chat_id))
                chat_id = int(chat_id)
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())
                continue

            try:
                chat = self.client.get_chat(chat_id)
                save_chat = filter_chat(json.loads(str(chat)), self.producer)
                # 发送至保存模块保存过滤后的群组信息
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_STORE, value=str(json.dumps(save_chat)))
            except FloodWait as e:
                logger.warning("Sleeping for {}s".format(e.x))
                time.sleep(e.x)
            except ChannelPrivate:
                logger.warning("The channel/supergroup [{}] is not accessible".format(chat_id))
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())




