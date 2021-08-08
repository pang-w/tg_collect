# encoding: utf-8

import json
import time
import pymysql
import traceback

from pymysql import IntegrityError, OperationalError

from filter.filter import filter_chat

from pyrogram.errors import FloodWait, UserAlreadyParticipant, UsernameNotOccupied, InviteHashExpired, \
    InviteHashInvalid, UsernameInvalid, ChannelPrivate

import configuration
from kafkaConsumer import KafkaConsumer
from utils import util

logger=util.init_other_logger('join_chat', 'join_chat')

# 加入一个群组
# topic name: join
# topic data format: supergroup or channel: username
#                                    group: invite_link
class JoinChatConsumer(KafkaConsumer):
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
                logger.debug("join chat: {}".format(username_or_link))
            except BaseException:
                logger.warning(str(msg.value) + traceback.format_exc())
                continue

            try:
                chat = self.client.join_chat(username_or_link)
                chat = self.client.get_chat(chat.id)  # get_chat()请求到的chat信息更全
                save_chat = filter_chat(json.loads(str(chat)), self.producer)
                logger.debug("username_or_link [{}] joined successfully.".format(username_or_link))
                logger.debug("chat info: {}.".format(save_chat))

                # 更新账号采集群组情况
                self.update_collected_group(save_chat, username_or_link)

                # 新增更新es中该群组的数据
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_STORE, value=str(json.dumps(save_chat)))

                if save_chat["type"] == "channel": # 只有频道才需要从这里获取历史消息，加入若是私有群组或超级群组的话，chat_action_lintener会监听到并处理，频道的加入监听不到
                    self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_HISTORY_INFO_GETTER, value=str(save_chat["chat_id"]))

            except FloodWait as e:
                logger.warning("Sleeping for {}s".format(e.x))
                logger.warning("username_or_link [{}] joined failed.".format(username_or_link))
                time.sleep(e.x)
            except UserAlreadyParticipant:  # 当所添加群组是私有群组且用户在群组中时报这个错
                logger.warning("the chat [{}] has been joined.".format(username_or_link))
                continue
            except (UsernameNotOccupied, UsernameInvalid):  # 通过username添加超级群组或频道时找不到这个username
                logger.warning("The username [{}] is invalid.".format(username_or_link))
                continue
            except (InviteHashExpired, InviteHashInvalid, ChannelPrivate):
                logger.warning("The chat invite link [{}] is no longer valid.".format(username_or_link))
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())

    def update_collected_group(self, save_chat, username_or_link):
        type = save_chat["type"]
        title = save_chat["title"] if "title" in save_chat else None
        username = save_chat["username"] if "username" in save_chat else None
        members_count = save_chat["members_count"] if "members_count" in save_chat else 0
        invite_link = save_chat["invite_link"] if "invite_link" in save_chat else None
        description = save_chat["description"] if "description" in save_chat else None
        check_sql = 'select * from ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' where group_id=%s;'
        try:
            self.connect.ping(reconnect=True)
            self.connect.commit()
            self.mysql.execute(check_sql, (save_chat["chat_id"]))
        except BaseException as e:
            logger.error("mysql error, Reason: {}".format(e))
            return
        res = self.mysql.fetchall()
        if res.__len__() != 0:  # 改变
            update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' SET type=%s, title=%s, username=%s, members_count=%s, invite_link=%s, description=%s, collect_account=%s, status=%s, join_by=%s where group_id=%s'
            self.mysql.execute(update_sql, (type, title, username, members_count, invite_link,
                                            description, configuration.PHONENUM, 1, username_or_link, save_chat["chat_id"]))
            self.connect.commit()
        else:  # 新增
            insert_sql = 'INSERT INTO ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' (group_id,type,title,username,members_count,invite_link,description,collect_account,status,join_by) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            try:
                self.mysql.execute(insert_sql, (save_chat["chat_id"], type, title, username, members_count, invite_link,
                                                description, configuration.PHONENUM, 1, username_or_link))
                self.connect.commit()
            except (IntegrityError, OperationalError):
                time.sleep(5)
                update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' SET type=%s, title=%s, username=%s, members_count=%s, invite_link=%s, description=%s, collect_account=%s, status=%s, join_by=%s where group_id=%s'
                try:
                    self.mysql.execute(update_sql, (type, title, username, members_count, invite_link,description, configuration.PHONENUM,
                                                    1, username_or_link, save_chat["chat_id"]))
                    self.connect.commit()
                    logger.debug(" save chat '{}' successfully)".format(save_chat["chat_id"]))
                except BaseException as e:
                    logger.error(e)