# encoding: utf-8

import json
import time

import traceback
import logging

import pymysql
from pyrogram.errors import FloodWait, ChatAdminRequired, PeerIdInvalid, UserNotParticipant, ChannelPrivate

import configuration
from kafkaConsumer import KafkaConsumer
from filter.filter import filter_chat_member, filter_user

logger=logging.getLogger(configuration.logger_name)

# 采集一个群组的单独一条chat-user关系数据
# topic name: chat_member
# topic data format:
# {
#     "chat_id": -1001448532229,
#     "user_id": 1389853140
# }
class ChatMember(KafkaConsumer):
    def __init__(self,topic,IpAddress,g_id,producer,client,es,es_index,session_timeout,request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, session_timeout=session_timeout,
                         request_timeout=request_timeout)
        self.producer = producer
        self.client = client
        self.es = es
        self.es_index = es_index
        self.connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                                       user=configuration.MYSQL_USERNAME,
                                       passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
        self.mysql = self.connect.cursor()

    def run(self): # 消费者获取群组id，采集更新群组成员信息
        for msg in self.consumer:
            logger.debug('topic=' + self.topic + ',partition=' + str(msg.partition) + ', offset=' + str(msg.offset))
            try:
                chat_member_info = msg.value.decode('utf-8')
                if chat_member_info == "0": continue
                logger.debug("chat_member collector: {}".format(chat_member_info))
                chat_member_info = json.loads(chat_member_info)
            except BaseException:
                logger.error(msg + traceback.format_exc())
                continue

            try:
                if "user_id" in chat_member_info:  # 如果有user_id，更新单条群组成员关系
                    chat_id = chat_member_info["chat_id"]
                    user_id = chat_member_info["user_id"]
                    member = self.client.get_chat_member(chat_id, user_id)
                    save_user = filter_user(json.loads(str(member))["user"], self.producer, self.es)
                    save_chat_member = filter_chat_member(json.loads(str(member)), chat_id)
                    self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_USER_STORE,
                                          value=str(json.dumps(save_user)))
                    self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_MEMBER_STORE,
                                          value=str(json.dumps(save_chat_member)))
            except FloodWait as e:
                logger.warning("Sleeping for {}s".format(e.x))
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_MEMBER,
                                      value=str(json.dumps(chat_member_info)))
                time.sleep(e.x)
            except (PeerIdInvalid, UserNotParticipant):  # 当从chat_id中获取不到user_id用户时报着个错误
                print("报错，群内没有此ID：", chat_member_info["chat_id"], chat_member_info["user_id"])
                # 因为是监听到该用户进来了，所以直接确定用户已进入该群组
                # todo user表中不一定存在此用户
                if not self.has_chat_member(chat_member_info["chat_id"], chat_member_info["user_id"]):
                    self.insert_customer_chat_member(chat_member_info["chat_id"], chat_member_info["user_id"])
            except (ValueError, AttributeError, ChatAdminRequired):  # 错误分别为chat_id数据错误(个人聊天)、chat里面无人、请求频道成员时需要管理员权限
                logger.warning(str(msg.value) + traceback.format_exc())
                continue
            except ChannelPrivate:
                logger.warning("The channel/supergroup [{}] is not accessible".format(chat_member_info["chat_id"]))
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_MEMBER, value=str(json.dumps(chat_member_info)))

            # self.consumer.commit()

    # 表中是否存在这个关系
    def has_chat_member(self, chat_id, user_id):
        try:
            for e in self.es:
                e.get(index=self.es_index, id=str(chat_id) + "_" + str(user_id))
        except:
            return False
        return True

    # 插入一条自定义群组关系
    def insert_customer_chat_member(self, chat_id, user_id):
        chat_member = {}
        chat_member["chat_id"] = chat_id
        chat_member["user_id"] = user_id
        chat_member["joined_date"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        chat_member["status"] = "member"
        chat_member["left"] = False
        chat_member["update_time"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        _id = str(chat_member["chat_id"]) + "_" + str(chat_member["user_id"])
        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_MEMBER_STORE, value=json.dumps(chat_member))

    # 获取一个群组的所有成员信息，需从数据库中读取记录并处理
    def get_all_chat_member(self):
        while True:
            logger.debug("chat_member collector is running.")
            self.connect.ping(reconnect=True)
            res = self.get_need_update_chat_member()
            if len(res) == 0:
                time.sleep(60)
            else:
                id = res[0][0]
                chat_id = res[0][1]
                logger.debug("collecting chat_member [{}]...".format(chat_id))
                try:
                    chat = json.loads(str(self.client.get_chat(chat_id)))
                    filter_kind = "recent" if chat["members_count"] < 10000 else "all"
                    for member in self.client.iter_chat_members(chat["id"], filter=filter_kind):
                        member = json.loads(str(member))
                        save_user = filter_user(member["user"], self.producer, self.es)
                        save_chat_member = filter_chat_member(member, chat_id)
                        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_USER_STORE, value=str(json.dumps(save_user)))
                        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_MEMBER_STORE,
                                         value=str(json.dumps(save_chat_member)))
                except BaseException as e:
                    logger.warning(str(e) + traceback.format_exc())
                    time.sleep(300)  # 停止五分钟
                    continue
                except FloodWait as e:
                    logger.warning("Sleeping for {}s".format(e.x))
                    time.sleep(e.x + 900)
                    continue
                try:
                    self.update_table(id)
                except BaseException as e:
                    logger.warning(str(e) + traceback.format_exc())
                logger.debug("chat_member [{}] has been collected.".format(chat_id))
                time.sleep(300)

    def get_need_update_chat_member(self):
        sql = 'select * from ' + configuration.MYSQL_TABLE_NEED_UPDATE_CHAT_MEMBER + ' where collect_account=%s and updated=0 limit 1;'
        self.connect.ping(reconnect=True)
        self.mysql.execute(sql, (configuration.PHONENUM))
        res = self.mysql.fetchall()
        self.connect.commit()
        return res

    def update_table(self, id):
        update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_NEED_UPDATE_CHAT_MEMBER + ' SET updated=%s,update_time=%s where id=%s;'
        self.connect.ping(reconnect=True)
        self.mysql.execute(update_sql, (1, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), id))
        self.connect.commit()