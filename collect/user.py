# encoding: utf-8

import json
import time
import traceback
import logging
from pyrogram.errors import FloodWait

import configuration
from kafkaConsumer import KafkaConsumer
from filter.filter import filter_user

logger=logging.getLogger(configuration.logger_name)

# 采集一个user的信息
# topic name: user
# topic data format: str(user_id)
class User(KafkaConsumer):
    def __init__(self,topic,IpAddress,g_id,producer,client,es,session_timeout,request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, session_timeout=session_timeout,
                         request_timeout=request_timeout)
        self.producer = producer
        self.client = client
        self.es = es

    def run(self): # 消费者获取群组id，采集更新群组信息
        for msg in self.consumer:
            if configuration.clean_topic:
                print("user.py：", msg.value.decode('utf-8'))
            else:
                try:
                    user_id = int(msg.value.decode('utf-8'))
                    print("获取用户信息：", user_id)
                    user = self.client.get_users(user_id)
                    save_user = filter_user(json.loads(str(user)), self.producer, self.es)
                    # 发送至保存模块保存过滤后的群组信息
                    self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_USER_STORE, value=str(json.dumps(save_user)))
                except FloodWait as e:
                    logger.warning("Sleeping for {}s".format(e.x))
                    time.sleep(e.x)
                except BaseException:
                    logger.error(str(msg.value) + traceback.format_exc())