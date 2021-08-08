# encoding: utf-8

import json
import time

import traceback

import pymysql
from pyrogram.errors import ChannelInvalid, FloodWait, ChannelPrivate

import configuration
from kafkaConsumer import KafkaConsumer
from utils import util

logger = util.init_other_logger('history', 'history')


# 历史消息采集任务的处理
class HistoryCollector():

    def __init__(self, producer, client, es):
        self.producer = producer
        self.client = client
        self.es = es
        self.connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                                       user=configuration.MYSQL_USERNAME,
                                       passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
        self.mysql = self.connect.cursor()

    def run(self):

        NUM_OF_HISTORY_MSG_COLLECT_AT_ONE_TIME = 300

        while True:

            logger.debug("historical message collector is running")

            try:
                query_sql = 'SELECT * FROM ' + configuration.MYSQL_TABLE_HISTORY_MSG_INFO + ' WHERE collect_account=%s AND (collect_to_msg_id - now_msg_id)>0 ORDER BY (collect_to_msg_id - now_msg_id) asc;'
                self.mysql.execute(query_sql, (configuration.PHONENUM))
                result = self.mysql.fetchall()
                self.connect.commit()
            except BaseException as e:
                logger.error(e)
                time.sleep(60)
                continue
            if len(result) == 0:
                time.sleep(60)
                continue
            try:
                i = 0
                # 一次最多处理20条历史采集任务
                while i < len(result) and i < 20:
                    res = result[i]
                    logger.debug("historical msg collector: {}".format(res))
                    id = res[0]
                    chat_id = res[1]
                    now_msg_id = res[2]
                    collect_to_msg_id = res[5]

                    msg_id_flag = collect_to_msg_id
                    while msg_id_flag > now_msg_id and collect_to_msg_id - msg_id_flag < NUM_OF_HISTORY_MSG_COLLECT_AT_ONE_TIME:
                        union_chat_message_id = str(chat_id) + "_" + str(msg_id_flag)
                        logger.debug("historical msg collector: {}".format(union_chat_message_id))
                        if self.has_message(union_chat_message_id):  # 如果此消息已存在，则不采集
                            logger.debug("message {} has been collected".format(union_chat_message_id))
                            msg_id_flag = msg_id_flag - 1
                            continue
                        try:
                            message = self.client.get_messages(chat_id, msg_id_flag)
                        except FloodWait as e:
                            logger.warning("Sleeping for {}s".format(e.x))
                            time.sleep(e.x + 30)
                            continue
                        except ChannelPrivate:
                            logger.warning(
                                "The channel/supergroup is not accessible [{}]".format(union_chat_message_id))
                            msg_id_flag = msg_id_flag - 1
                            continue
                        except ChannelInvalid:
                            logger.warning("The channel parameter is invalid [{}]".format(union_chat_message_id))
                            msg_id_flag = msg_id_flag - 1
                            continue

                        message = json.loads(str(message))
                        logger.debug("message: {}".format(str(message)[:70]))  # 输出一部分就可以
                        if "empty" in message and message["empty"]:
                            msg_id_flag = msg_id_flag - 1
                            continue
                        message['type'] = 'historical'
                        message['chat'] = {"id": chat_id, "type": message["chat"]["type"]}
                        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_MESSAGE_FILTER, value=json.dumps(message))
                        msg_id_flag = msg_id_flag - 1

                    # 采集完成
                    new_collect_to_msg_id = msg_id_flag
                    update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_HISTORY_MSG_INFO + ' SET collect_to_msg_id=%s,collect_time=%s where id=%s;'
                    self.connect.ping(reconnect=True)
                    self.mysql.execute(update_sql, (
                        new_collect_to_msg_id, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), id))
                    self.connect.commit()

                    i = i + 1
                    # 如果一次采集100条以上，则sleep 15秒
                    if collect_to_msg_id - now_msg_id >= NUM_OF_HISTORY_MSG_COLLECT_AT_ONE_TIME:
                        time.sleep(15)
                time.sleep(30)
            except BaseException:
                logger.error(traceback.format_exc())

    # es中是否存在某条消息
    def has_message(self, id):
        try:
            for e in self.es:
                e.get(index=configuration.ES_INDEX_MESSAGE, id=str(id))
        except:
            return False
        return True


class HistoryInfoGetter(KafkaConsumer):
    def __init__(self, topic, IpAddress, g_id, producer, client, es, session_timeout, request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, session_timeout=session_timeout,
                         request_timeout=request_timeout)
        self.producer = producer
        self.client = client
        self.es = es
        self.connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                                       user=configuration.MYSQL_USERNAME,
                                       passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
        self.mysql = self.connect.cursor()

    # 根据chat_id获取需要采集的历史消息信息并发送至历史消息采集模块
    def run(self):
        for msg in self.consumer:
            logger.debug('topic=' + self.topic + ',partition=' + str(msg.partition) + ', offset=' + str(msg.offset))
            try:
                chat_id = msg.value.decode('utf-8')
                logger.debug("historical info getter: {}".format(chat_id))
                if chat_id == "0": continue
                chat_id = int(chat_id)
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())
                continue

            try:
                time.sleep(20)
                top_msg_id = self.client.get_history(chat_id, limit=1)[0].message_id
                result = self.es[0].search(index=configuration.ES_INDEX_MESSAGE,
                                           body={"query": {"match": {"chat_id": chat_id}},
                                                 "sort": {"message_id": {"order": "desc"}}}, scroll='5m', size=1,
                                           request_timeout=60)
                # 设置500的目的是防止刚加入某个群组，该群组产生大量消息并已存储导致最低消息id获取有误，我们认为一个群组几秒钟内不会产生超过100的消息，如果超过500条消息，采集之前的历史消息意义也不大
                if result["hits"]["total"]["value"] < 500:
                    now_msg_id = 0
                else:
                    now_msg_id = result["hits"]["hits"][0]["_source"]["message_id"]
                history_info = {
                    "chat_id": chat_id,
                    "now_msg_id": now_msg_id,
                    "last_msg_id": top_msg_id
                }
                logger.info("history_info: {}".format(str(history_info)))
                insert_to_history_msg_info(history_info, self.connect, self.mysql)
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())


# 每隔20分钟生成一次历史消息采集任务（防止有的大群组或大频道消息无法实时监听）
class HistoryTaskGenerator:

    def __init__(self, client, es):
        self.client = client
        self.es = es
        self.connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                                       user=configuration.MYSQL_USERNAME,
                                       passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
        self.mysql = self.connect.cursor()

    def run(self):
        while True:
            time.sleep(60 * 20)
            # 获取所有历史消息信息
            self.generate_history_task()

    # 生成历史消息的采集任务
    def generate_history_task(self):
        logger.debug("historical message collect task Generator is running")
        # 获取所有历史消息信息
        try:
            history_infos = self.get_all_history_info(self.client, self.es[0])
            logger.debug("all historical message info: {}".format(history_infos))
            # 将历史消息信息发送至历史消息信息发送模块
            for history_info in history_infos[::-1]:
                insert_to_history_msg_info(history_info, self.connect, self.mysql)
        except BaseException as e:
            logger.error(str(e) + traceback.format_exc())

    # 获取当前采集账号所需采集的历史消息信息
    def get_all_history_info(self, client, es):
        # 获取所有id
        collected_group_ids = []
        collected_groups = self.get_collected_group_by_phone(configuration.PHONENUM)
        for collected_group in collected_groups:  # 先将当前采集账号采集的群组采集状态全部设置为"不在采集"
            collected_group_ids.append(collected_group[0])
        # 获取当前账号所加入chat的所有最新消息id以及当前采集到的消息id
        now_and_last_msg_id = []
        i = 0
        while i < len(collected_group_ids):  # 使用while循环遍历方便出错重新请求
            chat_id = collected_group_ids[i]
            logger.info("getting last msg: {}".format(chat_id))
            info = {}
            info["chat_id"] = chat_id

            try:
                info["last_msg_id"] = client.get_history(chat_id, limit=1)[0].message_id
                result = es.search(index=configuration.ES_INDEX_MESSAGE,
                                   body={"query": {"match": {"chat_id": info["chat_id"]}},
                                         "sort": {"message_id": {"order": "desc"}}},
                                   scroll='5m', size=1, request_timeout=60)
                if result["hits"]["total"]["value"] == 0:
                    info["now_msg_id"] = 0
                else:
                    info["now_msg_id"] = result["hits"]["hits"][0]["_source"]["message_id"]
                now_and_last_msg_id.append(info)
                i = i + 1
            except FloodWait as e:
                logger.warning("Sleeping for {}s".format(e.x))
                time.sleep(e.x)
            except ChannelPrivate:
                logger.error("The channel/supergroup [{}] is not accessible".format(chat_id))
                i = i + 1
            except ChannelInvalid:
                logger.error("The channel/supergroup [{}] is invalid".format(chat_id))
                i = i + 1
            except BaseException as e:
                logger.error(traceback.format_exc())
                i = i + 1
        logger.debug("sum to {} chats.".format(len(now_and_last_msg_id)))
        return now_and_last_msg_id

    # 通过手机号获取所有已采集过的群组信息
    def get_collected_group_by_phone(self, phone):
        sql = 'select * from ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' where collect_account=%s;'
        self.mysql.execute(sql, (phone))
        res = self.mysql.fetchall()
        return res


# 把需要采集的历史消息信息存储起来
def insert_to_history_msg_info(history_info, connect, mysql):
    query_sql = 'SELECT MAX(last_msg_id) FROM ' + configuration.MYSQL_TABLE_HISTORY_MSG_INFO + ' WHERE collect_account=%s AND chat_id=%s;'
    insert_sql = 'INSERT INTO ' + configuration.MYSQL_TABLE_HISTORY_MSG_INFO + ' (chat_id,now_msg_id,last_msg_id,collect_account,collect_to_msg_id,insert_time) VALUES (%s,%s,%s,%s,%s,%s)'
    connect.ping(reconnect=True)
    mysql.execute(query_sql, (configuration.PHONENUM, history_info["chat_id"]))
    res = mysql.fetchall()
    if not res[0][0]:
        now_msg_id = history_info["now_msg_id"]
    else:
        now_msg_id = history_info["now_msg_id"] if (history_info["now_msg_id"] > res[0][0]) else res[0][0]
    # 如果新消息只有1条及以内，则等待下次生成
    if history_info["last_msg_id"] - now_msg_id <= 0: return
    mysql.execute(insert_sql, (history_info["chat_id"], now_msg_id, history_info["last_msg_id"],
                               configuration.PHONENUM, history_info["last_msg_id"],
                               time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    connect.commit()
