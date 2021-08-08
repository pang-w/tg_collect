
import json

import pymysql
import time

from pyrogram.errors import FloodWait, ChannelPrivate, ChannelInvalid

import configuration
import logging
from elasticsearch.helpers import bulk

from filter.filter import filter_chat

logger = logging.getLogger(configuration.logger_name)

connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                                   user=configuration.MYSQL_USERNAME,
                                   passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
mysql = connect.cursor()
mysql.execute(configuration.MYSQL_CREATE_COLLECT_ACCOUNT_TABLE)
mysql.execute(configuration.MYSQL_CREATE_COLLECTED_GROUP_TABLE)
mysql.execute(configuration.MYSQL_CREATE_NEED_UPDATE_CHAT_MEMBER_TABLE)
mysql.execute(configuration.MYSQL_CREATE_HISTORY_MSG_INFO_TABLE)
connect.commit()

# 上报自己节点基本信息
def report_node_info():
    phone_num = configuration.PHONENUM
    role = configuration.ROLE

    if port_occupied(phone_num, configuration.SERVER_IP, configuration.SERVER_PORT):
        raise Exception("%s:%s已被占用", (configuration.SERVER_IP, configuration.SERVER_PORT))

    if has_phone_num(phone_num):
        # 修改上报信息
        update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_COLLECT_ACCOUNT + ' SET prefix=%s,api_id=%s,api_hash=%s,docker_container_id=%s,server_ip=%s,server_port=%s,working=%s,report_time=%s where phone_num=%s;'
        mysql.execute(update_sql,(configuration.PHONECTR, configuration.api_id, configuration.api_hash, configuration.DOCKER_CONTAINER_ID, configuration.SERVER_IP, configuration.SERVER_PORT, 1,
                                  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), phone_num))
        connect.commit()
        return

    # 新账号
    insert_sql = 'INSERT INTO ' + configuration.MYSQL_TABLE_COLLECT_ACCOUNT + ' (phone_num,prefix,role,api_id,api_hash,docker_container_id,server_ip,server_port,working,report_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    if role=="master":
        if has_master_node():  # 只有一个master节点
            raise Exception("master节点已存在")
        else:
            mysql.execute(insert_sql, (phone_num, configuration.PHONECTR, role, configuration.api_id, configuration.api_hash, configuration.DOCKER_CONTAINER_ID, configuration.SERVER_IP, configuration.SERVER_PORT, 1,
                                       time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    elif role=="slave" or role=="slave_verify":
        if not has_master_node():
            raise Exception("请先启动master节点！")
        else:
            mysql.execute(insert_sql,(phone_num, configuration.PHONECTR, role, configuration.api_id, configuration.api_hash, configuration.DOCKER_CONTAINER_ID, configuration.SERVER_IP, configuration.SERVER_PORT, 1,
                                      time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    else:
        raise Exception("请正确配置配置文件中的'role'")
    connect.commit()


# 端口是否已被占用，保证一个采集账号对应唯一的ip和port，以供后面flask服务使用
def port_occupied(phone_num, ip, port):
    check_sql = 'select * from ' + configuration.MYSQL_TABLE_COLLECT_ACCOUNT + ' where phone_num!=%s and server_ip=%s and server_port=%s;'
    mysql.execute(check_sql, (phone_num, ip, port))
    res = mysql.fetchall()
    if res.__len__() == 0:
        return False
    return True


# 采集当前账号所有的chat信息并上报当前节点的群组采集情况
def collect_chat_and_report(client, producer, es):
    # 尽量避免使用iter_dialogs()和get_dialogs()函数
    if configuration.first_start:  # 第一次启动节点时需使用iter_dialogs()获取所有会话列表
        collect_chat_and_report_by_dialogs(client, producer, es)
    else:  # 如果不是第一次启动，则从mysql中读取所有群组，进而进行初始化
        collect_chat_and_report_by_mysql(client, producer, es)

# 第一次启动节点时需使用iter_dialogs获取所有会话列表
def collect_chat_and_report_by_dialogs(client, producer, es):
    phone_num = configuration.PHONENUM
    chats = []
    chat_ids = []
    logger.debug('start initialize all group data!')
    dialogs = client.iter_dialogs()

    i = 0
    while i < len(dialogs):  # 使用while循环遍历方便出错重新请求
        dialog = dialogs[i]

        collected_groups = get_all_collected_group()
        collected_group_ids = []
        for collected_group in collected_groups:
            collected_group_ids.append({"group_id": collected_group[0], "collect_account": collected_group[1]})

        try:
            if dialog.chat.type == "group" or dialog.chat.type == "supergroup" or dialog.chat.type == "channel":
                logger.debug('collecting chat data [' + str(dialog.chat.id) + ']......')
                chat = json.loads(str(client.get_chat(dialog.chat.id)))
                chat_title = "" if not ("title" in chat) else chat["title"]
                # 判断账号之间是否存在重复采集的情况，如果存在重复采集的情况，直接抛出异常，将程序停止
                for collected_group_id in collected_group_ids:
                    # 如果chat["id"]已采集但是采集账号与当前账号不一致，则返回错误
                    if chat["id"] == collected_group_id["group_id"] and collected_group_id["collect_account"] != configuration.PHONENUM:
                        logger.warning("chat [{} {}] has been collected with another account [{}]!".format(chat["id"], chat_title, collected_group_id["collect_account"]))
                        # 删除当前账号所上报的节点信息
                        delete_sql = "delete from " + configuration.MYSQL_TABLE_COLLECT_ACCOUNT + ' where phone_num=%s;'
                        mysql.execute(delete_sql, (configuration.PHONENUM))
                        connect.commit()
                        raise BaseException("chat [{} {}] has been collected with another account [{}]!".format(chat["id"], chat_title, collected_group_id["collect_account"]))
                save_chat = filter_chat(chat, producer)
                chat_ids.append(save_chat["chat_id"])
                chats.append(save_chat)
            i = i+1
        except FloodWait as e:
            logger.warning("Sleeping for {}s".format(e.x))
            time.sleep(e.x)
        except ChannelPrivate:
            logger.warning("The channel/supergroup [{}] is not accessible".format(dialog.chat.id))
            i = i + 1
        except ChannelInvalid:
            logger.error("The channel/supergroup [{}] is invalid".format(dialog.chat.id))
            i = i + 1


    # 将所有群组信息发送至es存储
    save_all_chats(chats, es)
    logger.debug('initialization of all chat data finished！Sum to {} group/supergroup/channel.'.format(len(chats)))

    # 将所有chat保存至待采集群组成员表
    for chat in chats:
        if chat["type"] != "channel" and chat["members_count"] > 0:
            insert_to_need_update_chat_member(int(chat["chat_id"]))

    # 将不在chat_ids中的已采集群组的状态设置为不在采集
    collected_groups = get_collected_group_by_phone(phone_num)
    collected_group_ids = []
    for collected_group in collected_groups:  # 先将当前采集账号采集的群组采集状态全部设置为"不在采集"
        collected_group_ids.append(collected_group[0])
        update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' SET status=0 where group_id=' + str(collected_group[0])
        mysql.execute(update_sql)
    connect.commit()
    for chat in chats:  # 再将当前采集账号的所有群组的采集状态都设置为"正在采集"
        type = chat["type"]
        title = chat["title"] if "title" in chat else None
        username = chat["username"] if "username" in chat else None
        members_count = chat["members_count"] if "members_count" in chat else 0
        invite_link = chat["invite_link"] if "invite_link" in chat else None
        description = chat["description"] if "description" in chat else None
        if chat["chat_id"] in collected_group_ids:
            update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' SET type=%s, title=%s, username=%s, members_count=%s, invite_link=%s, description=%s, collect_account=%s, status=%s where group_id=%s'
            mysql.execute(update_sql, (type, title, username, members_count, invite_link, description, phone_num, 1, chat["chat_id"]))
        else:
            insert_sql = 'INSERT INTO ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' (group_id,type,title,username,members_count,invite_link,description,collect_account,status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            mysql.execute(insert_sql, (chat["chat_id"], type, title, username, members_count, invite_link, description, phone_num, 1))
    connect.commit()

# 如果不是第一次启动，则从mysql中读取所有群组，进而进行初始化
def collect_chat_and_report_by_mysql(client, producer, es):
    logger.debug('start initialize all group data!')
    # 获取所有id
    phone_num = configuration.PHONENUM
    chats = []  # 存放所有获取到的新的群组信息
    chat_ids = []  # 从数据库中读取得到的群组id列表
    not_collecting_chats_ids = []  # 存放获取不到基本信息的群组id
    collected_groups = get_collected_group_by_phone(phone_num)
    for collected_group in collected_groups:  # 获取群组id列表
        chat_ids.append(collected_group[0])

    i = 0
    while i < len(chat_ids):  # 使用while循环遍历方便出错重新请求
        chat_id = chat_ids[i]

        try:
            logger.debug('collecting chat info [' + str(chat_id) + ']......')
            chat = json.loads(str(client.get_chat(chat_id)))
            save_chat = filter_chat(chat, producer)
            chats.append(save_chat)
            i = i+1
        except FloodWait as e:
            logger.warning("Sleeping for {}s".format(e.x))
            time.sleep(e.x)
        except ChannelPrivate:
            logger.warning("The channel/supergroup [{}] is not accessible".format(chat_id))
            not_collecting_chats_ids.append(chat_id)
            i = i + 1
        except ChannelInvalid:
            logger.error("The channel/supergroup [{}] is invalid".format(chat_id))
            i = i + 1

    # 将所有群组信息发送至es存储
    save_all_chats(chats, es)
    logger.debug('initialization of all chat data finished！Sum to {} group/supergroup/channel.'.format(len(chats)))

    # 将所有chat保存至待采集群组成员表
    for chat in chats:
        if chat["type"] != "channel" and chat["members_count"] > 0:
            insert_to_need_update_chat_member(int(chat["chat_id"]))

    for chat in chats:  # 更新所有正在采集的群组的基本信息
        type = chat["type"]
        title = chat["title"] if "title" in chat else None
        username = chat["username"] if "username" in chat else None
        members_count = chat["members_count"] if "members_count" in chat else 0
        invite_link = chat["invite_link"] if "invite_link" in chat else None
        description = chat["description"] if "description" in chat else None
        update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' SET type=%s, title=%s, username=%s, members_count=%s, invite_link=%s, description=%s, collect_account=%s, status=%s where group_id=%s'
        mysql.execute(update_sql, (type, title, username, members_count, invite_link, description, phone_num, 1, chat["chat_id"]))
    connect.commit()

    for chat_id in not_collecting_chats_ids:  # 将获取不到基本信息的群组的采集状态设为0
        update_sql = 'UPDATE ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' SET status=%s where group_id=%s'
        mysql.execute(update_sql, (0, chat_id))
    connect.commit()


# 保存所有获取的chat
def save_all_chats(chats, es):
    chat_actions = []
    for chat in chats:
        result = es[0].search(index=configuration.ES_INDEX_CHAT,
                                   body={"query": {"match": {"chat_id": chat["chat_id"]}}}, size=1)
        if len(result["hits"]["hits"]) != 0:
            res_chat = result["hits"]["hits"][0]["_source"]
            if "migrate_to_chat_id" in res_chat:
                chat["migrate_to_chat_id"] = res_chat["migrate_to_chat_id"]
            if "migrate_from_chat_id" in res_chat:
                chat["migrate_from_chat_id"] = res_chat["migrate_from_chat_id"]
        chat_actions.append({"_id": str(chat["chat_id"]), "_source": chat})
    for e in es:
        bulk(client=e, actions=chat_actions, index=configuration.ES_INDEX_CHAT)


# 节点信息里是否有某个手机号
def has_phone_num(phone_num):
    check_sql = 'select * from ' + configuration.MYSQL_TABLE_COLLECT_ACCOUNT + ' where phone_num=%s;'
    mysql.execute(check_sql, (phone_num))
    res = mysql.fetchall()
    if res.__len__() == 0:
        return False
    return True

# 是否已有master节点
def has_master_node():
    check_sql = 'select * from ' + configuration.MYSQL_TABLE_COLLECT_ACCOUNT + ' where role=%s;'
    mysql.execute(check_sql, ("master"))
    res = mysql.fetchall()
    if res.__len__() == 0:
        return False
    return True

# 通过手机号获取所有已采集过的群组信息
def get_collected_group_by_phone(phone):
    sql = 'select * from ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' where collect_account=%s;'
    mysql.execute(sql, (phone))
    res = mysql.fetchall()
    return res

# 获取所有已采集群组信息
def get_all_collected_group():
    sql = 'select group_id, collect_account from ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ';'
    mysql.execute(sql)
    res = mysql.fetchall()
    return res

# kafka发送数据至topic，创建topic，保证能消费到数据
def send_test_data_to_topic(producer):
    # 历史消息相关
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_HISTORY_INFO_GETTER, value="0")
    # collector and filter
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT, value="0")
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_MEMBER, value="0")
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_MESSAGE_FILTER, value="0")
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_LINK_FILTER, value="0")
    # 加入离开群组相关
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_JOIN, value="0")
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_LEAVE, value="0")
    if configuration.ROLE=="master":
        producer.sendMsg(topic=configuration.KAFKA_TOPIC_MANAGE_JOIN, value="0")
        producer.sendMsg(topic=configuration.KAFKA_TOPIC_MANAGE_LEAVE, value="0")
    # 存储相关
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_MESSAGE_STORE, value="0")
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_NEED_DOWNLOAD_FILE_STORE, value="0")
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_STORE, value="0")
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_USER_STORE, value="0")
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_CHAT_MEMBER_STORE, value="0")
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_FIND_GROUP_STORE, value="0")
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_PROXY_STORE, value="0")
    producer.sendMsg(topic=configuration.KAFKA_TOPIC_ONION_STORE, value="0")
    time.sleep(10)


# 把需要更新所有群组关系的群组存起来，放在单独任务中更新群组成员列表
def insert_to_need_update_chat_member(chat_id):
    if not need_insert_to_update_chat_member(chat_id): # 如果不需要插入
        return
    insert_sql = 'INSERT INTO ' + configuration.MYSQL_TABLE_NEED_UPDATE_CHAT_MEMBER + ' (chat_id,collect_account,updated,insert_time) VALUES (%s,%s,%s,%s)'
    mysql.execute(insert_sql, (chat_id, configuration.PHONENUM, 0, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    connect.commit()

# 判断是否需要插入一条需更新群组关系的记录，如果表中已有未处理的任务，返回False，即不需要插入新的，否则返回True，即需要插入新的
def need_insert_to_update_chat_member(chat_id):
    check_sql = 'select * from ' + configuration.MYSQL_TABLE_NEED_UPDATE_CHAT_MEMBER + ' where chat_id=%s and updated=0;'
    mysql.execute(check_sql, (chat_id))
    res = mysql.fetchall()
    if res.__len__() == 0:
        return True
    return False
