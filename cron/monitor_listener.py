# -*- coding:utf-8 -*-
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__)) + "/..")
import configuration
import time
import datetime
import pymysql
from utils import util
import traceback
from elasticsearch import Elasticsearch


# 根据采集账号获取所有群组
def get_all_collected_group_by_phone(phone):
    sql = 'select group_id, title from ' + configuration.MYSQL_TABLE_COLLECTED_GROUP + ' where collect_account=%s;'
    mysql.execute(sql, (phone))
    res = mysql.fetchall()
    return res

# 查看一段时间内的新消息数量
def getMsgNumBetweenTime(phone, time1, time2):
    count = 0
    all_collected_group = get_all_collected_group_by_phone(phone)
    for collected_group in all_collected_group:
        chat_id = collected_group[0]
        # 所有消息
        result_msg_all = es.search(index=ES_INDEX_MESSAGE,
                                   body={"query": {
                                       "bool": {"must": [{"term": {"chat_id": {"value": chat_id}}},
                                                         {"range": {"date": {"gt": time1, "lte": time2}}}]
                                                }}, "track_total_hits": True}, size=0)
        # 消息总量
        msg_count = result_msg_all["hits"]["total"]["value"]
        count = count + msg_count
    return count

def kill_process(process_name):
    os.popen("ps -ef |grep %s|grep -v grep|awk '{ print $2 }'|xargs kill -9" % process_name)
    time.sleep(1)

def restart_supervisor():
    path = "/root/tg_collect/supervisor.py"
    os.system("nohup python %s >> /dev/null &" % path)
    logger.debug('supervisor.py process restart done')

# 每小时执行一次，如果十二个小时内没有新消息，则重新启动
if __name__ == '__main__':
    logger = util.init_other_logger('cron_monitor_listener', 'cron_monitor_listener')
    logger.debug("run")
    try:
        # mysql连接
        connect = pymysql.connect(host=configuration.MYSQL_HOST, port=configuration.MYSQL_PORT,
                                  user=configuration.MYSQL_USERNAME, passwd=configuration.MYSQL_PASSWORD,
                                  db=configuration.MYSQL_DATABASE)
        mysql = connect.cursor()
        connect.commit()

        # es连接
        ES_INDEX_MESSAGE = configuration.ES_INDEX_MESSAGE
        es = Elasticsearch(configuration.ES_IP, http_auth=(configuration.ES_USERNAME, configuration.ES_PASSWORD), timeout=60)


        hours_distance = 12  # 时间差，多长时间内没消息才判定为监听有错，然后重启

        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        time1 = (datetime.datetime.now() - datetime.timedelta(hours=hours_distance)).strftime("%Y-%m-%d %H:%M:%S")

        msg_count = getMsgNumBetweenTime(configuration.PHONENUM, time1, now)
        logger.debug("total {} new messages from {} to {}.".format(msg_count, time1, now))

        if msg_count == 0:
            logger.debug('restart supervisor.py process')
            kill_process("supervisor")
            restart_supervisor()

    except Exception:
        logger.error(traceback.format_exc())