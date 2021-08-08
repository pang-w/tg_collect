# encoding: utf-8

import json
import re
import time
import pymysql
import traceback

from pyrogram.errors import UsernameNotOccupied, UsernameInvalid, PeerIdInvalid

import configuration
from kafkaConsumer import KafkaConsumer

from utils import util

logger=util.init_other_logger('manage_join_chat', 'manage_join_chat')

# 管理加入一个群组
# topic name: join
# topic data format: supergroup or channel: username
#                                    group: invite_link
class ManageJoinChatConsumer(KafkaConsumer):
    def __init__(self,topic,IpAddress,g_id,producer,client,es,session_timeout,request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, session_timeout=session_timeout,
                         request_timeout=request_timeout)
        self.producer = producer
        self.client = client
        self.es = es
        self.connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                                       user=configuration.MYSQL_USERNAME,
                                       passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
        self.mysql = self.connect.cursor()
        self.connect.commit()

    def run(self):  # 消费者获取群组id，采集更新群组信息
        for msg in self.consumer:
            try:
                username_or_link = msg.value.decode('utf-8')
                if username_or_link == '0': continue
                logger.debug("manage join chat: {}".format(username_or_link))
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())
                continue

            try:
                self.connect.ping(reconnect=True)
                self.connect.commit()
                # 先判断是否曾经有账号采集过此群组，如果采集过则依然发送至那个账号进行采集
                if self.is_private_group(username_or_link): # 如果是私有群组
                    # print("是私有群组链接！", username_or_link)
                    logger.debug("private group link received [{}].".format(username_or_link))
                    sql = "SELECT cg.collect_account FROM collected_group cg WHERE cg.invite_link=%s or cg.join_by=%s;"
                    self.mysql.execute(sql, (username_or_link, username_or_link))
                    res = self.mysql.fetchall()
                    if len(res) != 0:
                        self.producer.sendMsg(topic=res[0][0] + "_join", value=str(username_or_link))
                        logger.debug("private group link [{}] sent to collect account [{}]".format(username_or_link, res[0][0]))
                        continue
                else:  # 如果是超级群组或频道
                    logger.debug("supergroup/channel username received ['{}'].".format(username_or_link))
                    try:
                        chat = json.loads(str(self.client.get_chat(username_or_link)))
                        logger.debug("new chat info: id[{}], username[{}], title[{}]".format(chat["id"], chat["username"], chat["title"]))
                        sql = "SELECT cg.collect_account FROM collected_group cg WHERE cg.group_id=%s;"
                        self.mysql.execute(sql, (chat["id"]))
                        res = self.mysql.fetchall()
                        if len(res) != 0:
                            self.producer.sendMsg(topic=res[0][0] + "_join", value=str(username_or_link))
                            logger.debug("supergroup/channel username [{}] sent to collect account [{}]".format(username_or_link, res[0][0]))
                            continue
                    except (UsernameNotOccupied, UsernameInvalid, PeerIdInvalid):
                        logger.warning("the supergroup/channel username [{}] is invalid".format(username_or_link))
                        continue

                # 执行到此的都是从未采集过的群组，或是邀请链接改变了的私有群组
                # 如果是master节点且正在采集的群组小于最大采集个数，则发送至master节点，否则从slave节点加入
                if configuration.ROLE == "master" and self.get_collected_group_num_in_master_node() < configuration.max_chat_num_of_master_can_collect:
                    logger.debug("send to node [master] to join.")
                    self.producer.sendMsg(topic=configuration.PHONENUM + "_join", value=str(username_or_link))
                else:
                    account = self.get_to_send_account()
                    logger.debug("send to node [{}}] to join.".format(account))
                    if account == "0":
                        logger.error("不存在正在工作的slave节点，请新增slave节点！")
                    else:
                        self.producer.sendMsg(topic=str(account) + "_join", value=str(username_or_link))
                        logger.debug("the username/link [{}] sent to collect account [{}]".format(username_or_link, account))
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())

    # 判断传进来的是否是私有群组，即带joinchat的链接
    def is_private_group(self, username_or_link):
        private_group_type = re.compile(r"(?:http|https)://(?:t\.me|telegram\.me)/joinchat/")
        m = private_group_type.search(username_or_link)
        if m:
            return True
        return False

    def get_account_and_collected_group_num(self):
        sql = "SELECT cg.collect_account, COUNT(cg.collect_account) FROM collected_group cg LEFT JOIN collect_account ca ON cg.collect_account=ca.phone_num WHERE cg.`status`=1 and ca.working=1 and ca.role='master' or ca.role='slave' GROUP BY cg.collect_account ORDER BY COUNT(cg.collect_account) ASC;"
        self.mysql.execute(sql)
        res = self.mysql.fetchall()
        return res

    def get_collected_group_num_in_master_node(self):
        sql = "SELECT cg.collect_account FROM collected_group cg LEFT JOIN collect_account ca ON cg.collect_account=ca.phone_num WHERE cg.`status`=1 and ca.working=1 and ca.role='master';"
        self.mysql.execute(sql)
        res = self.mysql.fetchall()
        return len(res)

    # 获取要发送给哪个节点（账号、手机号）
    def get_to_send_account(self):
        msg_nums = []
        # 获取当天每个账号正在采集的群组的消息量
        now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        start =time.strftime('%Y-%m-%d 00:00:00', time.localtime(time.time()))
        phones = self.get_working_slave_account()
        if len(phones) == 0: # 如果没有其他
            return "0"
        for phone in phones:
            msg_num = 0
            groups = self.get_collecting_groups(phone)
            for group_id in groups:
                result = self.es.search(index=configuration.ES_INDEX_MESSAGE,body={"track_total_hits": True,"query": {"bool": {"must": [{"term": {"chat_id": {"value": group_id[0]}}},{"range": {"date": {"gte": str(start),"lte": str(now)}}}]}},"size":1})
                msg_num = msg_num + result["hits"]["total"]["value"]
            msg_nums.append(msg_num)
        i = 0
        index = 0
        min = msg_nums[0]
        for msg_num in msg_nums:
            if msg_num < min:
                index = i
            i = i+1
        return phones[index][0]

    # 获取正在正常工作的采集账号
    def get_working_slave_account(self):
        self.connect.commit()
        sql = "SELECT ca.phone_num FROM collect_account ca WHERE ca.working=1 and ca.role='slave';"
        self.mysql.execute(sql)
        res = self.mysql.fetchall()
        return res

    # 根据采集账号获取正在采集的群组
    def get_collecting_groups(self, phone):
        sql = "SELECT cg.group_id FROM collected_group cg WHERE cg.collect_account=%s;"
        self.mysql.execute(sql, (phone))
        res = self.mysql.fetchall()
        return res




