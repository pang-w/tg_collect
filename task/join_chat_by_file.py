# -*- coding:utf-8 -*-

import sys
import os

import pymysql

sys.path.append("..")
sys.path.append(os.path.abspath(os.path.dirname(__file__)) + "/..")
import os
import re

from kafkaProducer import KafkaProducer
import configuration
import time

from utils import util

# 发送的需要是私有群组的整个链接或超级群组和频道的username
def extract_to_join_data(link):
    link = link.strip()

    group_type = re.compile(r"http[s]?://(?:t\.me|telegram\.me)/")
    m = group_type.match(link)
    if m:  # 数据有效
        ui = link[m.end():]
        # 如果是私有群组链接，直接返回此链接
        hash_type = re.compile(r"joinchat/")
        m = hash_type.match(ui)
        if m:
            return link

        index = ui.find('/')
        if not (index == -1):
            ui = ui[:index]
        username_type = re.compile(r'^[a-zA-Z][\w\d]{3,30}[a-zA-Z\d]$')
        if not username_type.match(ui):
            logger.debug("link [{}] is invalid".format(link))
            return  # 无效的用户名
        return ui
    logger.debug("link [{}] is invalid".format(link))

# 发送的需要是私有群组的整个链接或超级群组和频道的username
def is_joined(username_or_link):
    check_sql = 'select * from ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' where join_by=%s;'
    connect.ping(reconnect=True)
    connect.commit()
    mysql.execute(check_sql, (username_or_link))
    res = mysql.fetchall()
    if res.__len__() == 0:
        return False
    return True


if __name__ == "__main__":

    if configuration.ROLE != "master":
        sys.exit(0)

    connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                              user=configuration.MYSQL_USERNAME,
                              passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
    mysql = connect.cursor()

    logger = util.init_other_logger('joinChatByFlie', 'joinChatByFlie')
    producer = KafkaProducer(configuration.KAFKA_BROKER_IP, configuration.KAFKA_PRODUCER_ID,
                             configuration.KAFKA_PRODUCER_ACK, configuration.KAFKA_PRODUCER_RETRY)

    file_path = os.path.abspath(os.path.dirname(__file__))
    file_name = os.path.join(file_path, 'link.txt')
    logger.debug(file_name)
    file = open(file_name, 'r')
    list = file.readlines()
    for link in list:
        link = link[:-1]
        username_or_link = extract_to_join_data(link)
        if username_or_link:
            producer.sendMsg(topic=configuration.KAFKA_TOPIC_MANAGE_JOIN, value=str(username_or_link))
            logger.debug("link [{}] is being processed, ui: [{}].".format(link, username_or_link))
            interval_time = 180
            second = 0
            while second < interval_time:
                time.sleep(5)
                second = second + 5
                if is_joined(username_or_link):
                    logger.debug("[{}] was joined successfully!".format(username_or_link))
                    break
            if not is_joined(username_or_link):
                logger.debug("[{}] was joined failed!".format(username_or_link))
            time.sleep(interval_time - second)
    logger.debug('all links in file have been processed!')
    file.close()
