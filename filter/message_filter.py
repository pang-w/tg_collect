# encoding: utf-8

import json
import re
import time
import traceback
import logging

import configuration
from kafkaConsumer import KafkaConsumer
from filter.filter import need_to_download, filter_user

from utils import util

# logger=logging.getLogger(configuration.logger_name)
logger = util.init_other_logger('message_filter', 'message_filter')

class NewMessageFilterConsumer(KafkaConsumer):

    def __init__(self, topic, IpAddress, g_id, producer, es, session_timeout, request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, session_timeout=session_timeout,
                         request_timeout=request_timeout)
        self.producer = producer
        self.es = es

    def run(self):
        for msg in self.consumer:
            logger.debug('topic=' + self.topic + ',partition=' + str(msg.partition) + ', offset=' + str(msg.offset))
            try:
                msg = msg.value.decode('utf-8')
                if msg == "0": continue
                logger.debug("message filter: {}".format(msg))
                message = json.loads(msg)
                self.filter_message(message)
            except BaseException:
                logger.error(str(msg) + traceback.format_exc())

    def filter_message(self, message):
        # 如果消息为空不处理
        if "empty" in message and message["empty"] is True: # 消息被撤回可能为空，采集历史消息时可能有空的消息
            return
        # 先判断是否是service类型的消息,处理群组转移关系
        if "service" in message and message["service"] is True:
            self.handle_service_message(message)
            return

        msg = {}
        msg["message_id"] = message["message_id"]
        if "chat" in message:
            msg["chat_id"] = message["chat"]["id"]
        if "from_user" in message:
            msg["from_user_id"] = message["from_user"]["id"]
            save_user = filter_user(message["from_user"], self.producer, self.es)
            self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_USER_STORE, value=str(json.dumps(save_user)))
            # 如果是超级群组和私有群组
            if "chat" in message and (message["chat"]["type"]=="supergroup" or message["chat"]["type"]=="group"):
                # 如果没有群组关系，则请求群组关系和用户信息
                if not self.has_chat_member(message["chat"]["id"], message["from_user"]["id"]):
                    self.insert_customer_chat_member(message["chat"]["id"], message["from_user"]["id"])
                    # self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_MEMBER, value=json.dumps({"chat_id": message["chat"]["id"], "user_id": message["from_user"]["id"]}))
        if "sender_chat" in message:
            msg["sender_chat_id"] = message["sender_chat"]["id"]
        if "date" in message:
            msg["date"] = message["date"]
        # 转发相关
        if "forward_from" in message: # 原始消息的发送者
            msg["forward_from_id"] = message["forward_from"]["id"]
        if "forward_sender_name" in message: # 从隐藏了自己账号的用户发送的消息，此字段表示此用户的name
            msg["forward_sender_name"] = message["forward_sender_name"]
        if "forward_from_chat" in message: # 用于从频道转发的消息，原始频道的id
            msg["forward_from_chat_id"] = message["forward_from_chat"]["id"]
        if "forward_from_message_id" in message: # 用于从频道转发的消息，频道里此消息的id
            msg["forward_from_message_id"] = message["forward_from_message_id"]
        if "forward_signature" in message: # 对于从频道转发的消息，帖子作者的签名（如果有）
            msg["forward_signature"] = message["forward_signature"]
        if "forward_date" in message: # 原始消息被发送的日期
            msg["forward_date"] = message["forward_date"]
        # 回复
        if "reply_to_message" in message:
            msg["reply_to_message_id"] = message["reply_to_message"]["message_id"]

        if "mentioned" in message:
            msg["mentioned"] = message["mentioned"]
        if "edit_date" in message: # 编辑日期（消息可重新编辑）
            msg["edit_date"] = message["edit_date"]
        if "media_group_id" in message: # The unique identifier of a media message group this message belongs to.
            msg["media_group_id"] = message["media_group_id"]
        if "author_signature" in message: # 频道中消息的帖子作者的签名，或匿名组管理员的自定义标题
            msg["author_signature"] = message["author_signature"]
        if "text" in message:
            msg["text"] = message["text"]
        if "caption" in message:
            msg["caption"] = message["caption"]
        if "via_bot" in message:
            msg["via_bot_id"] = message["via_bot"]["id"]
        if "outgoing" in message:
            msg["outgoing"] = message["outgoing"]
        # 链接提取
        links = set()
        if "entities" in message:
            links = self.filter_link_from_entities(message["entities"], message["text"])
            for link in links:
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_LINK_FILTER, value=str(json.dumps(
                    {
                        "link": link,
                        "chat_id": message["chat"]["id"],
                        "message_id": message["message_id"],
                        "date": message["date"],
                        "text": message["text"]
                    })))
        caption_links = set()
        if "caption_entities" in message:
            caption_links = self.filter_link_from_entities(message["caption_entities"], message["caption"])
            for link in caption_links:
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_LINK_FILTER, value=str(json.dumps(
                    {
                        "link": link,
                        "chat_id": message["chat"]["id"],
                        "message_id": message["message_id"],
                        "date": message["date"],
                        "text": message["caption"]
                    })))

        reply_markup_links = set()
        if "reply_markup" in message:
            reply_markup_links = self.filter_link_from_reply_markup(message["reply_markup"])
            for link in reply_markup_links:
                self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_LINK_FILTER, value=str(json.dumps(
                    {
                        "link": link,
                        "chat_id": message["chat"]["id"],
                        "message_id": message["message_id"],
                        "date": message["date"],
                        "text": "" if (not ("text" in message)) else message["text"]
                    })))

        msg["links"] = list(links | caption_links | reply_markup_links)

        # 文件相关(message实体中只记录是否有文件，具体文件信息在另一张表中)
        msg["media"] = False
        if need_to_download(message):
            msg["media"] = True
            file_info = self.handle_need_download_message(message)
            # msg["file_id"] = file_info["file_id"]

        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_MESSAGE_STORE, value=str(json.dumps(msg)))

    def filter_link_from_entities(self, entities, text):
        pattern = re.compile(r'(?:http[s]?|tg)://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
        links = re.findall(pattern, text)  # 先从明文中提取链接
        for entity in entities:  # 提取文本链接(非明文)
            if entity["type"] == "text_link" and "url" in entity:
                link = entity["url"]
                links.append(link)
        return_links = set()
        for link in links:
            return_links.add(link)
        return return_links

    def filter_link_from_reply_markup(self, reply_markup):
        reply_markup_links = set()
        if reply_markup["_"] == "InlineKeyboardMarkup":
            for list in reply_markup["inline_keyboard"]:
                for data in list:
                    if data != None and "url" in data:
                        reply_markup_links.add(data["url"])
        return reply_markup_links

    def handle_service_message(self, message):
        print("处理服务类消息")
        if "migrate_to_chat_id" in message:
            chat_id = message["chat"]["id"]
            self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT, value=str(chat_id))  # 获取迁移的新群组的信息
            # 消息直接采集，无需获取历史消息
            result = self.es[0].search(index=configuration.ES_INDEX_CHAT, body={"query": {"match": {"chat_id": chat_id}}}, size=1)
            if len(result["hits"]["hits"]) ==1:
                res_chat = result["hits"]["hits"][0]["_source"]
                res_chat["migrate_to_chat_id"] = message["migrate_to_chat_id"]
                for e in self.es:
                    e.index(index=configuration.ES_INDEX_CHAT, id=str(res_chat["chat_id"]), body=res_chat)
        if "migrate_from_chat_id" in message:
            chat_id = message["chat"]["id"]
            self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT, value=str(chat_id))  # 获取迁移的新群组的信息
            result = self.es[0].search(index=configuration.ES_INDEX_CHAT, body={"query": {"match": {"chat_id": chat_id}}}, size=1)
            if len(result["hits"]["hits"]) == 1:
                res_chat = result["hits"]["hits"][0]["_source"]
                res_chat["migrate_from_chat_id"] = message["migrate_from_chat_id"]
                for e in self.es:
                    e.index(index=configuration.ES_INDEX_CHAT, id=str(res_chat["chat_id"]), body=res_chat)

    def handle_need_download_message(self, message):
        file_info = {}
        file_info["chat_id"] = message["chat"]["id"]
        file_info["message_id"] = message["message_id"]
        if "from_user" in message:
            file_info["user_id"] = message["from_user"]["id"]
        if "document" in message:
            file_info["type"] = "document"
            file_info["file_id"] = message["document"]["file_id"]
            file_info["file_ref"] = message["document"]["file_ref"]
            if "file_size" in message["document"]:
                file_info["file_size"] = message["document"]["file_size"]
            if "mime_type" in message["document"]:
                file_info["mime_type"] = message["document"]["mime_type"]
            if "file_name" in message["document"]:
                file_info["file_name"] = message["document"]["file_name"]
        elif "photo" in message:
            file_info["type"] = "photo"
            file_info["file_id"] = message["photo"]["file_id"]
            file_info["file_ref"] = message["photo"]["file_ref"]
            if "file_size" in message["photo"]:
                file_info["file_size"] = message["photo"]["file_size"]
            if "mime_type" in message["photo"]:
                file_info["mime_type"] = message["photo"]["mime_type"]
        elif "audio" in message:
            file_info["type"] = "audio"
            file_info["file_id"] = message["audio"]["file_id"]
            file_info["file_ref"] = message["audio"]["file_ref"]
            if "file_size" in message["audio"]:
                file_info["file_size"] = message["audio"]["file_size"]
            if "mime_type" in message["audio"]:
                file_info["mime_type"] = message["audio"]["mime_type"]
            if "file_name" in message["audio"]:
                file_info["file_name"] = message["audio"]["file_name"]
        elif "video" in message:
            file_info["type"] = "video"
            file_info["file_id"] = message["video"]["file_id"]
            file_info["file_ref"] = message["video"]["file_ref"]
            if "file_size" in message["video"]:
                file_info["file_size"] = message["video"]["file_size"]
            if "mime_type" in message["video"]:
                file_info["mime_type"] = message["video"]["mime_type"]
            if "file_name" in message["video"]:
                file_info["file_name"] = message["video"]["file_name"]
        elif "voice" in message:
            file_info["type"] = "voice"
            file_info["file_id"] = message["voice"]["file_id"]
            file_info["file_ref"] = message["voice"]["file_ref"]
            if "file_size" in message["voice"]:
                file_info["file_size"] = message["voice"]["file_size"]
            if "mime_type" in message["voice"]:
                file_info["mime_type"] = message["voice"]["mime_type"]
        file_info["source"] = message["type"]
        file_info["collect_account"] = configuration.PHONENUM
        if "date" in message:
            file_info["date"] = message["date"]
            if "edit_date" in message:
                file_info["date"] = message["edit_date"]
        file_info["status"] = "Not downloaded"
        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_NEED_DOWNLOAD_FILE_STORE,
                              value=str(json.dumps(file_info)))
        return file_info

    # 查询群组关系是否存在
    def has_chat_member(self, chat_id, user_id):
        try:
            result = self.es[0].search(index=configuration.ES_INDEX_CHAT_MEMBER,
                                       body={"query": {"bool": {"must": [{"match": {"chat_id": {"value": chat_id}}},
                                                             {"match": {"user_id": {"value": user_id}}},
                                                             {"match": {"left": {"value": False}}}]}}})
        except:
            return False
        if result["hits"]["total"]["value"] == 0:
            return False
        return True

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