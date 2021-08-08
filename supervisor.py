# -*- coding:utf-8 -*-
import sys
import time
import json
import threading
from pyrogram import Client, idle

from elasticsearch import Elasticsearch
import traceback

import configuration
import network
import kafkaProducer
from utils import util
from listener import chat_action_listener, new_message_listener
from filter import message_filter, link_filter
from collect import history, chat, chat_member
from init import init
from action import join_chat, leave_chat, manage_join_chat, manage_leave_chat
from storage import message_storage, chat_storage, user_storage, chat_member_storage, find_group_storage, proxy_storage, \
    onion_storage, need_download_file_storage, media_storage


async def main():
    # 历史消息采集模块
    history_msg_collector = history.HistoryCollector(producer=producer, client=client_new_message, es=es)
    thread = threading.Thread(target=history_msg_collector.run)
    thread.start()
    logger.info('history collector ' + 'created')

    # 历史消息信息获取模块
    history_info_getter = history.HistoryInfoGetter(topic=configuration.KAFKA_TOPIC_HISTORY_INFO_GETTER,
                                                    IpAddress=configuration.KAFKA_BROKER_IP,
                                                    g_id=configuration.KAFKA_HISTORY_INFO_GETTER_GROUP_ID,
                                                    producer=producer, client=client_new_message, es=es,
                                                    session_timeout=configuration.KAFKA_HISTORY_GETTER_SESSION_TIMEOUT_MS,
                                                    request_timeout=configuration.KAFKA_HISTORY_GETTER_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=history_info_getter.run)
    thread.start()
    logger.info('history info getter created')

    # 定时生成历史消息采集任务
    thread = threading.Thread(target=history_task_generator.run)
    thread.start()
    logger.info('history task generator created')

    # 启动群组信息采集的消费者线程，获取chat信息
    chat_collector = chat.Chat(topic=configuration.KAFKA_TOPIC_CHAT, IpAddress=configuration.KAFKA_BROKER_IP,
                               g_id=configuration.KAFKA_CHAT_GROUP_ID, producer=producer, client=client_new_message,
                               session_timeout=configuration.KAFKA_CHAT_COLLECTOR_SESSION_TIMEOUT_MS,
                               request_timeout=configuration.KAFKA_CHAT_COLLECTOR_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=chat_collector.run)
    thread.start()
    logger.info('chat_collector created')

    # 启动群组成员信息采集的消费者线程，群组成员获取，频道的成员列表只有管理员权限才能获取，采集账号不可能拥有管理员权限
    chat_member_collector = chat_member.ChatMember(topic=configuration.KAFKA_TOPIC_CHAT_MEMBER,
                                                   IpAddress=configuration.KAFKA_BROKER_IP,
                                                   g_id=configuration.KAFKA_CHAT_MEMBER_GROUP_ID, producer=producer,
                                                   client=client_new_message, es=es,
                                                   es_index=configuration.ES_INDEX_CHAT_MEMBER,
                                                   session_timeout=configuration.KAFKA_CHAT_MEMBER_COLLECTOR_SESSION_TIMEOUT_MS,
                                                   request_timeout=configuration.KAFKA_CHAT_MEMBER_COLLECTOR_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=chat_member_collector.run)
    thread.start()
    # 单独线程去处理数据库中需要获取所有群组人物的任务
    thread = threading.Thread(target=chat_member_collector.get_all_chat_member)
    thread.start()
    logger.info('chat_member_collector created')

    # message filter(timely and historical message)
    message_filter_consumer = message_filter.NewMessageFilterConsumer(topic=configuration.KAFKA_TOPIC_MESSAGE_FILTER,
                                                                      IpAddress=configuration.KAFKA_BROKER_IP,
                                                                      g_id=configuration.KAFKA_MASSAGE_FILTER_GROUP_ID,
                                                                      producer=producer, es=es,
                                                                      session_timeout=configuration.KAFKA_MESSAGE_FILTER_SESSION_TIMEOUT_MS,
                                                                      request_timeout=configuration.KAFKA_MESSAGE_FILTER_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=message_filter_consumer.run)
    thread.start()
    logger.info('message_filter created')

    # --link filter--
    link_filter_consumer = link_filter.LinkFilterConsumer(topic=configuration.KAFKA_TOPIC_LINK_FILTER,
                                                          IpAddress=configuration.KAFKA_BROKER_IP,
                                                          g_id=configuration.KAFKA_LINK_FILTER_GROUP_ID,
                                                          producer=producer,
                                                          session_timeout=configuration.KAFKA_LINK_FILTER_SESSION_TIMEOUT_MS,
                                                          request_timeout=configuration.KAFKA_LINK_FILTER_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=link_filter_consumer.run)
    thread.start()
    logger.info('link_filter created')

    # --joiner--
    joiner_chat_consumer = join_chat.JoinChatConsumer(topic=configuration.KAFKA_TOPIC_JOIN,
                                                      IpAddress=configuration.KAFKA_BROKER_IP,
                                                      g_id=configuration.KAFKA_JOIN_GROUP_ID, producer=producer,
                                                      client=client_new_message, es=es[0],
                                                      session_timeout=configuration.KAFKA_JOIN_CHAT_SESSION_TIMEOUT_MS,
                                                      request_timeout=configuration.KAFKA_JOIN_CHAT_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=joiner_chat_consumer.run)
    thread.start()
    logger.info('join chat consumer created')
    # --leave_chat--
    leave_chat_consumer = leave_chat.LeaveChatConsumer(topic=configuration.KAFKA_TOPIC_LEAVE,
                                                       IpAddress=configuration.KAFKA_BROKER_IP,
                                                       g_id=configuration.KAFKA_LEAVE_GROUP_ID, producer=producer,
                                                       client=client_new_message,
                                                       session_timeout=configuration.KAFKA_LEAVE_CHAT_SESSION_TIMEOUT_MS,
                                                       request_timeout=configuration.KAFKA_LEAVE_CHAT_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=leave_chat_consumer.run)
    thread.start()
    logger.info('leave chat consumer created')

    # 如果角色为"master"，则启动管理加入和离开的线程
    if configuration.ROLE == "master":
        join_chat_manager = manage_join_chat.ManageJoinChatConsumer(topic=configuration.KAFKA_TOPIC_MANAGE_JOIN,
                                                                    IpAddress=configuration.KAFKA_BROKER_IP,
                                                                    g_id=configuration.KAFKA_MANAGE_JOIN_GROUP_ID,
                                                                    producer=producer, client=client_new_message,
                                                                    es=es[0],
                                                                    session_timeout=configuration.KAFKA_MANAGE_JOIN_CHAT_SESSION_TIMEOUT_MS,
                                                                    request_timeout=configuration.KAFKA_MANAGE_JOIN_CHAT_REQUEST_TIMEOUT_MS)
        thread = threading.Thread(target=join_chat_manager.run)
        thread.start()
        logger.info('join chat manager consumer created')

        leave_chat_manager = manage_leave_chat.ManageLeaveChatConsumer(topic=configuration.KAFKA_TOPIC_MANAGE_LEAVE,
                                                                       IpAddress=configuration.KAFKA_BROKER_IP,
                                                                       g_id=configuration.KAFKA_MANAGE_LEAVE_GROUP_ID,
                                                                       producer=producer, client=client_new_message,
                                                                       session_timeout=configuration.KAFKA_MANAGE_LEAVE_CHAT_SESSION_TIMEOUT_MS,
                                                                       request_timeout=configuration.KAFKA_MANAGE_LEAVE_CHAT_REQUEST_TIMEOUT_MS)
        thread = threading.Thread(target=leave_chat_manager.run)
        thread.start()
        logger.info('leave chat manager consumer created')

    # 存储消息数据
    message_store_consumer = message_storage.MessageStoreConsumer(topic=configuration.KAFKA_TOPIC_MESSAGE_STORE,
                                                                  IpAddress=configuration.KAFKA_BROKER_IP,
                                                                  g_id=configuration.KAFKA_MESSAGE_STORE_GROUP_ID,
                                                                  producer=producer, es=es,
                                                                  es_index=configuration.ES_INDEX_MESSAGE,
                                                                  session_timeout=configuration.KAFKA_STORE_SESSION_TIMEOUT_MS,
                                                                  request_timeout=configuration.KAFKA_STORE_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=message_store_consumer.run)
    thread.start()
    logger.info('messageStorage created')

    # 存需要下载的文件信息
    need_download_file_store_consumer = need_download_file_storage.NeedDownloadFileStoreConsumer(
        topic=configuration.KAFKA_TOPIC_NEED_DOWNLOAD_FILE_STORE, IpAddress=configuration.KAFKA_BROKER_IP,
        g_id=configuration.KAFKA_NEED_DOWNLOAD_FILE_STORE_GROUP_ID, producer=producer, es=es,
        es_index=configuration.ES_INDEX_NEED_DOWNLOAD_FILE,
        session_timeout=configuration.KAFKA_STORE_SESSION_TIMEOUT_MS,
        request_timeout=configuration.KAFKA_STORE_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=need_download_file_store_consumer.run)
    thread.start()
    logger.info('need download file storage created')

    # 存储chat数据
    chat_store_consumer = chat_storage.ChatStoreConsumer(topic=configuration.KAFKA_TOPIC_CHAT_STORE,
                                                         IpAddress=configuration.KAFKA_BROKER_IP,
                                                         g_id=configuration.KAFKA_CHAT_STORE_GROUP_ID,
                                                         producer=producer, es=es, es_index=configuration.ES_INDEX_CHAT,
                                                         session_timeout=configuration.KAFKA_STORE_SESSION_TIMEOUT_MS,
                                                         request_timeout=configuration.KAFKA_STORE_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=chat_store_consumer.run)
    thread.start()
    logger.info('chatStorage created')

    # 存储user数据
    user_store_consumer = user_storage.UserStoreConsumer(topic=configuration.KAFKA_TOPIC_USER_STORE,
                                                         IpAddress=configuration.KAFKA_BROKER_IP,
                                                         g_id=configuration.KAFKA_USER_STORE_GROUP_ID,
                                                         producer=producer, es=es, es_index=configuration.ES_INDEX_USER,
                                                         session_timeout=configuration.KAFKA_STORE_SESSION_TIMEOUT_MS,
                                                         request_timeout=configuration.KAFKA_STORE_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=user_store_consumer.run)
    thread.start()
    logger.info('user storage created')

    # 存储chat_member数据
    chat_member_storage_consumer = chat_member_storage.ChatMemberStorageConsumer(
        topic=configuration.KAFKA_TOPIC_CHAT_MEMBER_STORE, IpAddress=configuration.KAFKA_BROKER_IP,
        g_id=configuration.KAFKA_CHAT_MEMBER_STORE_GROUP_ID, producer=producer, es=es,
        es_index=configuration.ES_INDEX_CHAT_MEMBER,
        session_timeout=configuration.KAFKA_STORE_SESSION_TIMEOUT_MS,
        request_timeout=configuration.KAFKA_STORE_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=chat_member_storage_consumer.run)
    thread.start()
    logger.info('chat member storage created')

    # 存储media数据
    # for i in range(configuration.media_storage_num):
    #     media_storage_consumer = media_storage.MediaStorageConsumer(topic=configuration.KAFKA_TOPIC_MEDIA_STORE,IpAddress=configuration.KAFKA_BROKER_IP,
    #                                                                 g_id=configuration.KAFKA_MEDIA_STORAGE_GROUP_ID,producer=producer,es=es,es_index=configuration.ES_INDEX_MEDIA,
    #                                                                 session_timeout=10000,request_timeout=20000)
    #     thread = threading.Thread(target=media_storage_consumer.run)
    #     thread.start()
    #     logger.info('media storage ' + str(i) + ' created')

    # 存储发现的群组
    find_group_store_consumer = find_group_storage.FindGroupStoreConsumer(
        topic=configuration.KAFKA_TOPIC_FIND_GROUP_STORE, IpAddress=configuration.KAFKA_BROKER_IP,
        g_id=configuration.KAFKA_FIND_GROUP_STORE_GROUP_ID, producer=producer,
        session_timeout=configuration.KAFKA_STORE_SESSION_TIMEOUT_MS,
        request_timeout=configuration.KAFKA_STORE_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=find_group_store_consumer.run)
    thread.start()
    logger.info('find_group Storage created')

    # 存储代理
    proxy_sql_store_consumer = proxy_storage.ProxyStoreConsumer(topic=configuration.KAFKA_TOPIC_PROXY_STORE,
                                                                IpAddress=configuration.KAFKA_BROKER_IP,
                                                                g_id=configuration.KAFKA_PROXY_STORE_GROUP_ID,
                                                                producer=producer,
                                                                session_timeout=configuration.KAFKA_STORE_SESSION_TIMEOUT_MS,
                                                                request_timeout=configuration.KAFKA_STORE_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=proxy_sql_store_consumer.run)
    thread.start()
    logger.info('proxySqlStorage created')

    # 存储暗网地址
    onion_sql_store_consumer = onion_storage.OnionStoreConsumer(topic=configuration.KAFKA_TOPIC_ONION_STORE,
                                                                IpAddress=configuration.KAFKA_BROKER_IP,
                                                                g_id=configuration.KAFKA_ONION_STORE_GROUP_ID,
                                                                producer=producer,
                                                                session_timeout=configuration.KAFKA_STORE_SESSION_TIMEOUT_MS,
                                                                request_timeout=configuration.KAFKA_STORE_REQUEST_TIMEOUT_MS)
    thread = threading.Thread(target=onion_sql_store_consumer.run)
    thread.start()
    logger.info('onionSqlStorage created')


if __name__ == "__main__":
    logger = util.init_logger()

    logger.debug('the system of collecting telegram group message start')
    try:
        # --connect to kafka and create es producer entity--
        producer = kafkaProducer.KafkaProducer(IpAddress=configuration.KAFKA_BROKER_IP,
                                               producerID=configuration.KAFKA_PRODUCER_ID,
                                               acks=configuration.KAFKA_PRODUCER_ACK,
                                               retry=configuration.KAFKA_PRODUCER_RETRY)
        logger.info('init kafka successfully')

        # --connect to es and create es entity--
        es = []
        for ip in configuration.ES_IP:
            es.append(Elasticsearch(ip, http_auth=(configuration.ES_USERNAME, configuration.ES_PASSWORD), timeout=60))
        logger.info('init es successfully')

        # --connect to telegram and create client entity(two client with different interface)--
        if (configuration.ip == None):
            client_new_message = Client(configuration.session_name_new_message, api_id=configuration.api_id,
                                        api_hash=configuration.api_hash)
            client_chat_action = Client(configuration.session_name_chat_action, api_id=configuration.api_id,
                                        api_hash=configuration.api_hash)
        else:
            client_new_message = Client(configuration.session_name_new_message, api_id=configuration.api_id,
                                        api_hash=configuration.api_hash,
                                        proxy=dict(hostname=configuration.ip, port=configuration.port))
            client_chat_action = Client(configuration.session_name_chat_action, api_id=configuration.api_id,
                                        api_hash=configuration.api_hash,
                                        proxy=dict(hostname=configuration.ip, port=configuration.port))

        logger.info("init telegram client successfully")

        network.start_telegram_client(client=client_new_message, retry_time=0)  # connect at first time
        network.start_telegram_client(client=client_chat_action, retry_time=0)  # connect at first time
        logger.info("two telegram client started")

        # 当前采集账号的id
        self_id = json.loads(str(client_new_message.get_me()))["id"]

        # 上报自身节点基本信息及采集信息
        logger.debug("reporting node info...")
        init.report_node_info()
        init.collect_chat_and_report(client_new_message, producer, es)
        logger.debug("report node info finished!")

        # 获取所有历史消息信息
        logger.debug("generating history message collect task...")
        history_task_generator = history.HistoryTaskGenerator(client=client_new_message, es=es)
        history_task_generator.generate_history_task()
        logger.debug("generated history message collect task.")

        # 向kafka topic发送一条数据，如果没有topic则创建topic
        init.send_test_data_to_topic(producer=producer)

        # start thread of each workers module
        client_new_message.run(main())

        # 开启监听
        new_message_listener = new_message_listener.NewMessageListener(client=client_new_message, producer=producer)
        new_message_listener.run()
        chat_action_listener = chat_action_listener.ChatActionListener(client=client_chat_action, producer=producer,
                                                                       es=es, self_id=self_id)
        chat_action_listener.run()
        logger.info("listener module started")

        idle()

    except BaseException as e:
        logger.error(traceback.format_exc())
        time.sleep(0.5)
        sys.exit(1)
