# encoding: utf-8

import json
import logging

import pymysql
from pymysql import IntegrityError, OperationalError
from pyrogram.handlers import RawUpdateHandler

import configuration
from filter.filter import filter_chat

from init import init

logger=logging.getLogger(configuration.logger_name)
logger_join=logging.getLogger("join_chat")

# 用来更新群组信息以及群组——成员关系信息
class ChatActionListener():

    def __init__(self, client, producer, es, self_id):
        self.client = client
        self.producer = producer
        self.es = es
        self.self_id = self_id
        self.connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                                       user=configuration.MYSQL_USERNAME,
                                       passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
        self.mysql = self.connect.cursor()
        self.connect.commit()

    def run(self):
        chat_action_handler = RawUpdateHandler(self.handler)
        self.client.add_handler(chat_action_handler)
        logger.info('start listening chat action')

    def handler(self, client, update, users, chats):
        # 直接对chat action进行处理
        update_json = json.loads(str(update))
        # 如果是服务类消息
        if update_json["_"] == ("types.UpdateNewChannelMessage" or update_json["_"] == "types.UpdateNewMessage") and update_json["message"]["_"] == "types.MessageService":

            logger.debug("listened chat action: {}".format(update_json))

            chat_id = int(self.get_chat_id_from_new_message_update(update_json))

            # 成员加入到超级群组或私有群组，或通过链接加入到私有群组
            if update_json["message"]["action"]["_"] == "types.MessageActionChatAddUser" or update_json["message"]["action"]["_"] == "types.MessageActionChatJoinedByLink":
                user_id = update_json["message"]["from_id"]
                if user_id == self.self_id:  # 如果是自己加入某个超级群组
                    res = self.get_collect_account_by_chat_id(chat_id)
                    if len(res) == 0 or (len(res) == 1 and res[0][0] == configuration.PHONENUM):  # 如果没有采集或者之前是此账号采集
                        logger_join.debug("joined to chat [{}] successfully.".format(chat_id))
                        # 更新成员列表
                        init.insert_to_need_update_chat_member(int(chat_id))
                        # 初始化此群组的历史消息
                        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_HISTORY_INFO_GETTER, value=str(chat_id))
                        # 上报更新已采集群组列表
                        chat = self.client.get_chat(chat_id)
                        save_chat = filter_chat(json.loads(str(chat)), self.producer)
                        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_STORE, value=json.dumps(save_chat))
                        self.report_chat(save_chat)
                    elif len(res) == 1 and res[0][0] != configuration.PHONENUM:  # 之前被其他账号采集
                        self.client.leave_chat(chat_id)
                    else:
                        logger.warning("群组[{}]重复采集".format(chat_id))
                else:  # 如果是别人加入，则更新群组成员列表以及群组信息
                    chat_member_info = {"chat_id": int(chat_id), "user_id": user_id}
                    self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT, value=str(chat_id))
                    self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_MEMBER, value=str(json.dumps(chat_member_info)))

            # 私有群组创建的时候被拉入私有群组，直接采集该群组
            elif update_json["message"]["action"]["_"] == "types.types.MessageActionChatCreate":
                logger_join.debug("chat [{}] was created.".format(chat_id))
                init.insert_to_need_update_chat_member(int(chat_id))  # 获取新群组的成员列表
                # 上报更新已采集群组列表
                chat = self.client.get_chat(chat_id)
                save_chat = filter_chat(json.loads(str(chat)), self.producer)
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_STORE, value=json.dumps(save_chat))
                self.report_chat(save_chat)

            # 从私有群组迁移到超级群组
            elif update_json["message"]["action"]["_"] == "types.MessageActionChannelMigrateFrom":  # 私有群组迁移到超级群组，默认直接采集该超级群组
                old_chat_id = -update_json["message"]["action"]["chat_id"]
                new_chat_id = chat_id
                logger_join.debug("private chat [{}] migrated to supergroup [{}].".format(old_chat_id, chat_id))
                init.insert_to_need_update_chat_member(new_chat_id)  # 获取新群组的成员列表(成员列表和旧的一样，需要更新群组成员关系)
                # 上报更新已采集群组列表
                chat = self.client.get_chat(chat_id)
                save_chat = filter_chat(json.loads(str(chat)), self.producer)
                save_chat["migrate_to_chat_id"] = new_chat_id
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_STORE, value=json.dumps(save_chat))
                self.report_chat(save_chat)

            # 成员离开超级群组或私有群组
            elif update_json["message"]["action"]["_"] == "types.MessageActionChatDeleteUser":  # 成员离开超级群组
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT, value=str(chat_id))
                user_id = update_json["message"]["from_id"]
                if self.has_chat_member(chat_id, user_id):
                    chat_member_es = self.es[0].get(index=configuration.ES_INDEX_CHAT_MEMBER, id=str(chat_id) + "_" + str(user_id))
                    chat_member = chat_member_es["_source"]
                    chat_member["left"] = True
                    self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_MEMBER_STORE, value=json.dumps(chat_member))

            # 群组信息（title、photo）更新
            elif update_json["message"]["action"]["_"] == "types.MessageActionChatEditTitle":
                logger.debug("listened chat title changed: {}".format(update_json))
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT, value=str(chat_id))
            elif update_json["message"]["action"]["_"] == "types.MessageActionChatEditPhoto":
                logger.debug("listened chat photo changed: {}".format(update_json))
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT, value=str(chat_id))

    def has_chat_member(self, chat_id, user_id):
        try:
            for e in self.es:
                e.get(index=configuration.ES_INDEX_CHAT_MEMBER, id=str(chat_id) + "_" + str(user_id))
        except:
            return False
        return True

    def get_chat_id_from_new_message_update(self, update):
        to_id = update["message"]["to_id"]
        chat_id = 0
        if to_id["_"] == "types.PeerChannel":
            chat_id = to_id["channel_id"]
        elif to_id["_"] == "types.PeerChat":
            chat_id = to_id["chat_id"]
        # 如果是普通群组，id为9位数，id前加上"-"；如果是超级群组或频道，则是1开头的10位数，在id前加上"-100"
        if len(str(chat_id)) == 9:
            return "-" + str(chat_id)
        return "-100" + str(chat_id)

    # 根据chat_id获取是哪一个账号在采集
    def get_collect_account_by_chat_id(self, chat_id):
        sql = "SELECT collect_account FROM " + configuration.MYSQL_TABLE_COLLECTED_GROUP + " WHERE group_id=%s;"
        self.connect.ping(reconnect=True)
        self.mysql.execute(sql, (chat_id))
        res = self.mysql.fetchall()
        return res

    # 上报自己信息
    def report_chat(self, chat):
        phone_num = configuration.PHONENUM
        type = chat["type"]
        title = chat["title"] if "title" in chat else None
        username = chat["username"] if "username" in chat else None
        members_count = chat["members_count"] if "members_count" in chat else 0
        invite_link = chat["invite_link"] if "invite_link" in chat else None
        description = chat["description"] if "description" in chat else None
        if len(self.get_collect_account_by_chat_id(chat["chat_id"])) > 0:
            update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' SET type=%s, title=%s, username=%s, members_count=%s, invite_link=%s, description=%s, collect_account=%s, status=%s where group_id=%s'
            self.mysql.execute(update_sql, (type, title, username, members_count, invite_link, description, phone_num, 1, chat["chat_id"]))
            self.connect.commit()
        else:
            try:
                insert_sql = 'INSERT INTO ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' (group_id,type,title,username,members_count,invite_link,description,collect_account,status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                self.mysql.execute(insert_sql, (chat["chat_id"], type, title, username, members_count, invite_link, description, phone_num, 1))
                self.connect.commit()
            except (IntegrityError, OperationalError):  # 如果已存在或上锁了，是刚插入的数据，就不做处理了
                logger.debug(" save chat '{}' successfully)".format(chat["chat_id"]))
