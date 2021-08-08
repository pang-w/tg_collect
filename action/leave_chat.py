# encoding: utf-8

import time
import pymysql
import traceback

from pyrogram.errors import FloodWait, PeerIdInvalid, UserNotParticipant

import configuration
from kafkaConsumer import KafkaConsumer
from utils import util

logger=util.init_other_logger('leave_chat', 'leave_chat')


# 离开一个群组
# topic name: leave
# topic data format: str(chat_id)
class LeaveChatConsumer(KafkaConsumer):
    def __init__(self, topic, IpAddress, g_id, producer, client, session_timeout, request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, session_timeout=session_timeout,
                         request_timeout=request_timeout)
        self.producer = producer
        self.client = client
        self.connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                                       user=configuration.MYSQL_USERNAME,
                                       passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
        self.mysql = self.connect.cursor()
        self.connect.commit()

    def run(self):  # 消费者获取群组id，采集更新群组信息
        for msg in self.consumer:
            try:
                chat_id = msg.value.decode('utf-8')
                if chat_id == "0": continue
                logger.debug("leave chat: {}".format(chat_id))

                chat_id = int(chat_id)
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())
                continue

            try:
                self.client.leave_chat(chat_id)

                logger.debug("chat_id [{}] left successfully.".format(chat_id))

                # 更新账号采集群组情况
                self.update_collected_group(chat_id)
            except FloodWait as e:
                logger.warning("Sleeping for {}s".format(e.x))
                time.sleep(e.x)
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_LEAVE, value=str(chat_id))
                continue
            except (ValueError, PeerIdInvalid, UserNotParticipant):
                logger.warning("The the chat [{}] is not collecting!".format(chat_id))
                self.update_collected_group(chat_id)
                continue
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())

    def update_collected_group(self, chat_id):
        check_sql = 'select * from ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' where group_id=%s;'
        self.connect.ping(reconnect=True)
        self.mysql.execute(check_sql, (chat_id))
        res = self.mysql.fetchall()
        if res.__len__() != 0:  # 如果有，则将采集状态改为停止采集
            update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' SET status=%s where group_id=%s'
            self.mysql.execute(update_sql, (0, chat_id))
        self.connect.commit()



