# encoding: utf-8

import pymysql
import traceback

import configuration
from kafkaConsumer import KafkaConsumer
from utils import util

logger=util.init_other_logger('manage_leave_chat', 'manage_leave_chat')


# 管理离开一个群组
# topic name: leave
# topic data format: str(chat_id)
class ManageLeaveChatConsumer(KafkaConsumer):
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
                logger.debug("manage leave chat: {}".format(chat_id))

                chat_id = int(chat_id)
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())
                continue
            try:
                res = self.get_to_send_account(chat_id)
                if len(res) == 0:
                    logger.debug("the chat_id [{}] to leave is invalid.".format(chat_id))
                else:
                    account = res[0][0]
                    self.producer.sendMsg(topic=account + "_leave", value=str(chat_id))
                    logger.debug("the chat_id [{}] to leave has been sent to collect_account [{}].".format(chat_id, account))
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())

    def get_to_send_account(self, chat_id):
        sql = "SELECT cg.collect_account FROM collected_group cg WHERE cg.group_id=%s;"
        self.connect.ping(reconnect=True)
        self.mysql.execute(sql, (chat_id))
        res = self.mysql.fetchall()
        return res




