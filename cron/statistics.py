# -*- coding:utf-8 -*-

import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__)) + "/..")
import traceback
import configuration
import datetime
import time
import pymysql
from elasticsearch import Elasticsearch

from utils import util

# es连接
ES_INDEX_MESSAGE = configuration.ES_INDEX_MESSAGE
ES_INDEX_MEDIA = configuration.ES_INDEX_MEDIA

TABLE_NAME_STATISTICS_HOUR = "statistics_hour"
TABLE_NAME_STATISTICS_DAY = "statistics_day"
CREATE_TABLE_STATISTICS_HOUR = "CREATE TABLE if not exists " + TABLE_NAME_STATISTICS_HOUR + "(`id` bigint NOT NULL AUTO_INCREMENT,`collect_account` varchar(255) NOT NULL,`chat_id` bigint NOT NULL,`chat_title` varchar(255) DEFAULT NULL,`msg_count` int NOT NULL,`msg_send_count` int NOT NULL,`msg_reply_count` int NOT NULL,`collected_msg_count` int NOT NULL,`collected_msg_send_count` int NOT NULL,`collected_msg_reply_count` int NOT NULL,`collected_media_count` int NOT NULL,`time` datetime NOT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
CREATE_TABLE_STATISTICS_DAY = "CREATE TABLE if not exists " + TABLE_NAME_STATISTICS_DAY + "(`id` bigint NOT NULL AUTO_INCREMENT,`collect_account` varchar(255) NOT NULL,`chat_id` bigint NOT NULL,`chat_title` varchar(255) DEFAULT NULL,`msg_count` int NOT NULL,`msg_send_count` int NOT NULL,`msg_reply_count` int NOT NULL,`collected_msg_count` int NOT NULL,`collected_msg_send_count` int NOT NULL,`collected_msg_reply_count` int NOT NULL,`collected_media_count` int NOT NULL,`date` date NOT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"


class Statistic:

    def __init__(self):
        self.connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                                       user=configuration.MYSQL_USERNAME,
                                       passwd=configuration.MYSQL_PASSWORD, db=configuration.MYSQL_DATABASE)
        self.mysql = self.connect.cursor()
        self.mysql.execute(CREATE_TABLE_STATISTICS_HOUR)  # 初始化表结构
        self.mysql.execute(CREATE_TABLE_STATISTICS_DAY)  # 初始化表结构
        self.connect.commit()
        self.es = Elasticsearch(configuration.ES_IP, http_auth=(configuration.ES_USERNAME, configuration.ES_PASSWORD),
                                timeout=60)
        self.logger = util.init_other_logger('statistic', 'statistic')
        self.sleep_time = 0
        self.first_run = True

    def run(self):
        while True:
            time.sleep(self.sleep_time)
            self.stastics_hour()  # 执行按小时统计

            # 如果是凌晨0点多或者第一次运行，则执行天数统计
            if datetime.datetime.now().hour == 0:
                self.stastics_day()
            elif self.first_run:
                self.stastics_day()

            now = datetime.datetime.now()
            next_hour = (now + datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")  # 后面一小时的整点
            # 加10秒确保时间在整点之后
            self.sleep_time = (datetime.datetime.strptime(next_hour, "%Y-%m-%d %H:%M:%S") - now).seconds + 10
            self.first_run = False

    def stastics_hour(self):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00")
        last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")

        self.logger.debug("statistics_hour: {} - {}".format(last_hour, now))

        # 删除24小时前的统计数据
        last_week_hour = (datetime.datetime.now() - datetime.timedelta(hours=24 * 1)).strftime("%Y-%m-%d %H:00:00")
        try:
            delete_sql = "delete from " + TABLE_NAME_STATISTICS_HOUR + " where time<%s"
            self.mysql.execute(delete_sql, (last_week_hour))

            all_collect_account = self.get_all_collect_account()

            for collect_account in all_collect_account:
                collect_account = collect_account[0]
                all_collected_group = self.get_all_collected_group_by_phone(collect_account)
                for collected_group in all_collected_group:
                    chat_id = collected_group[0]
                    chat_title = collected_group[1]

                    # 所有消息
                    result_msg_all = self.es.search(index=ES_INDEX_MESSAGE,
                                                    body={"query": {
                                                        "bool": {"must": [{"term": {"chat_id": {"value": chat_id}}},
                                                                          {"range": {
                                                                              "date": {"gt": last_hour, "lte": now}}}]
                                                                 }}, "track_total_hits": True}, size=0)
                    # 不存在回复消息id的数据，即主动发送的数据
                    result_msg_send = self.es.search(index=ES_INDEX_MESSAGE,
                                                     body={"query": {
                                                         "bool": {"must": [{"term": {"chat_id": {"value": chat_id}}},
                                                                           {"range": {
                                                                               "date": {"gt": last_hour, "lte": now}}}],
                                                                  "must_not": [
                                                                      {"exists": {"field": "reply_to_message_id"}}]
                                                                  }}, "track_total_hits": True}, size=0)
                    # 存在回复消息id的数据，即回贴消息
                    result_msg_reply = self.es.search(index=ES_INDEX_MESSAGE,
                                                      body={"query": {
                                                          "bool": {"must": [{"term": {"chat_id": {"value": chat_id}}},
                                                                            {"range": {
                                                                                "date": {"gt": last_hour, "lte": now}}},
                                                                            {"exists": {
                                                                                "field": "reply_to_message_id"}}]
                                                                   }}, "track_total_hits": True}, size=0)

                    # 采集到的所有消息
                    result_collected_msg_all = self.es.search(index=ES_INDEX_MESSAGE,
                                                              body={"query": {
                                                                  "bool": {"must": [
                                                                      {"term": {"chat_id": {"value": chat_id}}},
                                                                      {"range": {"insert_date": {"gt": last_hour,
                                                                                                 "lte": now}}}]
                                                                  }}, "track_total_hits": True}, size=0)
                    # 采集到的不存在回复消息id的数据，即主动发送的数据
                    result_collected_msg_send = self.es.search(index=ES_INDEX_MESSAGE,
                                                               body={"query": {
                                                                   "bool": {"must": [
                                                                       {"term": {"chat_id": {"value": chat_id}}},
                                                                       {"range": {"insert_date": {"gt": last_hour,
                                                                                                  "lte": now}}}],
                                                                       "must_not": [
                                                                           {"exists": {
                                                                               "field": "reply_to_message_id"}}]
                                                                   }}, "track_total_hits": True}, size=0)
                    # 采集到的存在回复消息id的数据，即回贴消息
                    result_collected_msg_reply = self.es.search(index=ES_INDEX_MESSAGE,
                                                                body={"query": {
                                                                    "bool": {"must": [
                                                                        {"term": {"chat_id": {"value": chat_id}}},
                                                                        {"range": {"insert_date": {"gt": last_hour,
                                                                                                   "lte": now}}},
                                                                        {"exists": {
                                                                            "field": "reply_to_message_id"}}]
                                                                    }}, "track_total_hits": True}, size=0)
                    # 采集到的媒体下载量(包括群组头像)
                    result_collected_media = self.es.search(index=ES_INDEX_MEDIA,
                                                            body={"query": {
                                                                "bool": {
                                                                    "must": [{"term": {"chat_id": {"value": chat_id}}},
                                                                             {"range": {"insert_date": {"gt": last_hour,
                                                                                                        "lte": now}}}]
                                                                }}, "track_total_hits": True}, size=0)

                    # 消息总量
                    msg_count = result_msg_all["hits"]["total"]["value"]
                    # 发帖数量
                    msg_send_count = result_msg_send["hits"]["total"]["value"]
                    # 回帖数量
                    msg_reply_count = result_msg_reply["hits"]["total"]["value"]
                    # 采集到的消息总量

                    collected_msg_count = result_collected_msg_all["hits"]["total"]["value"]
                    # 采集到的发帖数量
                    collected_msg_send_count = result_collected_msg_send["hits"]["total"]["value"]
                    # 采集到的回帖数量
                    collected_msg_reply_count = result_collected_msg_reply["hits"]["total"]["value"]
                    # 采集到的下载媒体量（含群组头像）
                    collected_media_count = result_collected_media["hits"]["total"]["value"]

                    insert_sql = 'INSERT INTO ' + TABLE_NAME_STATISTICS_HOUR + ' (collect_account,chat_id,chat_title,msg_count,msg_send_count,msg_reply_count,collected_msg_count,collected_msg_send_count,collected_msg_reply_count,collected_media_count,time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    self.mysql.execute(insert_sql, (
                        collect_account, chat_id, chat_title, msg_count, msg_send_count, msg_reply_count,
                        collected_msg_count,
                        collected_msg_send_count, collected_msg_reply_count, collected_media_count, now))
                self.connect.commit()
        except Exception as e:
            self.logger.error(traceback.format_exc())

    def stastics_day(self):
        now = datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")
        pre_day = (datetime.datetime.now() - datetime.timedelta(hours=24)).strftime("%Y-%m-%d 00:00:00")

        self.logger.debug("statistics_day: {} - {}".format(pre_day, now))

        try:
            all_collect_account = self.get_all_collect_account()

            for collect_account in all_collect_account:
                collect_account = collect_account[0]
                all_collected_group = self.get_all_collected_group_by_phone(collect_account)
                for collected_group in all_collected_group:
                    chat_id = collected_group[0]
                    chat_title = collected_group[1]

                    # 所有消息
                    result_msg_all = self.es.search(index=ES_INDEX_MESSAGE,
                                                    body={"query": {
                                                        "bool": {"must": [{"term": {"chat_id": {"value": chat_id}}},
                                                                          {"range": {
                                                                              "date": {"gt": pre_day, "lte": now}}}]
                                                                 }}, "track_total_hits": True}, size=0)
                    # 不存在回复消息id的数据，即主动发送的数据
                    result_msg_send = self.es.search(index=ES_INDEX_MESSAGE,
                                                     body={"query": {
                                                         "bool": {"must": [{"term": {"chat_id": {"value": chat_id}}},
                                                                           {"range": {
                                                                               "date": {"gt": pre_day, "lte": now}}}],
                                                                  "must_not": [
                                                                      {"exists": {"field": "reply_to_message_id"}}]
                                                                  }}, "track_total_hits": True}, size=0)
                    # 存在回复消息id的数据，即回贴消息
                    result_msg_reply = self.es.search(index=ES_INDEX_MESSAGE,
                                                      body={"query": {
                                                          "bool": {"must": [{"term": {"chat_id": {"value": chat_id}}},
                                                                            {"range": {
                                                                                "date": {"gt": pre_day, "lte": now}}},
                                                                            {"exists": {
                                                                                "field": "reply_to_message_id"}}]
                                                                   }}, "track_total_hits": True}, size=0)

                    # 采集到的所有消息
                    result_collected_msg_all = self.es.search(index=ES_INDEX_MESSAGE,
                                                              body={"query": {
                                                                  "bool": {"must": [
                                                                      {"term": {"chat_id": {"value": chat_id}}},
                                                                      {"range": {"insert_date": {"gt": pre_day,
                                                                                                 "lte": now}}}]
                                                                  }}, "track_total_hits": True}, size=0)
                    # 采集到的不存在回复消息id的数据，即主动发送的数据
                    result_collected_msg_send = self.es.search(index=ES_INDEX_MESSAGE,
                                                               body={"query": {
                                                                   "bool": {"must": [
                                                                       {"term": {"chat_id": {"value": chat_id}}},
                                                                       {"range": {"insert_date": {"gt": pre_day,
                                                                                                  "lte": now}}}],
                                                                       "must_not": [
                                                                           {"exists": {
                                                                               "field": "reply_to_message_id"}}]
                                                                   }}, "track_total_hits": True}, size=0)
                    # 采集到的存在回复消息id的数据，即回贴消息
                    result_collected_msg_reply = self.es.search(index=ES_INDEX_MESSAGE,
                                                                body={"query": {
                                                                    "bool": {"must": [
                                                                        {"term": {"chat_id": {"value": chat_id}}},
                                                                        {"range": {"insert_date": {"gt": pre_day,
                                                                                                   "lte": now}}},
                                                                        {"exists": {
                                                                            "field": "reply_to_message_id"}}]
                                                                    }}, "track_total_hits": True}, size=0)
                    # 采集到的媒体下载量(包括群组头像)
                    result_collected_media = self.es.search(index=ES_INDEX_MEDIA,
                                                            body={"query": {
                                                                "bool": {
                                                                    "must": [{"term": {"chat_id": {"value": chat_id}}},
                                                                             {"range": {
                                                                                 "insert_date": {"gt": pre_day,
                                                                                                 "lte": now}}}]
                                                                }}, "track_total_hits": True}, size=0)

                    # 消息总量
                    msg_count = result_msg_all["hits"]["total"]["value"]
                    # 发帖数量
                    msg_send_count = result_msg_send["hits"]["total"]["value"]
                    # 回帖数量
                    msg_reply_count = result_msg_reply["hits"]["total"]["value"]
                    # 采集到的消息总量

                    collected_msg_count = result_collected_msg_all["hits"]["total"]["value"]
                    # 采集到的发帖数量
                    collected_msg_send_count = result_collected_msg_send["hits"]["total"]["value"]
                    # 采集到的回帖数量
                    collected_msg_reply_count = result_collected_msg_reply["hits"]["total"]["value"]
                    # 采集到的下载媒体量（含群组头像）
                    collected_media_count = result_collected_media["hits"]["total"]["value"]

                    insert_sql = 'INSERT INTO ' + TABLE_NAME_STATISTICS_DAY + ' (collect_account,chat_id,chat_title,msg_count,msg_send_count,msg_reply_count,collected_msg_count,collected_msg_send_count,collected_msg_reply_count,collected_media_count,date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    self.mysql.execute(insert_sql, (
                        collect_account, chat_id, chat_title, msg_count, msg_send_count, msg_reply_count,
                        collected_msg_count,
                        collected_msg_send_count, collected_msg_reply_count, collected_media_count, pre_day))
            self.connect.commit()

        except Exception as e:
            self.logger.error(traceback.format_exc())

    def get_all_collect_account(self):
        sql = 'select phone_num from ' + configuration.MYSQL_TABLE_COLLECT_ACCOUNT + ' group by phone_num;'
        self.mysql.execute(sql)
        res = self.mysql.fetchall()
        return res

    def get_all_collected_group_by_phone(self, phone):
        sql = 'select group_id, title from ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' where collect_account=%s;'
        self.mysql.execute(sql, (phone))
        res = self.mysql.fetchall()
        return res


if __name__ == "__main__":
    # 只有master角色进行统计
    if configuration.ROLE != "master":
        sys.exit(0)

    Statistic().run()
