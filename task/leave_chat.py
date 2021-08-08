# -*- coding:utf-8 -*-

import sys
import os
sys.path.append("..")
sys.path.append(os.path.abspath(os.path.dirname(__file__)) + "/..")
import configuration
import kafkaProducer
from utils import util
import time

if __name__ == "__main__":

    if configuration.ROLE == "master":

        logger = util.init_other_logger('leaveChatTask', 'leaveChatTask')

        producer = kafkaProducer.KafkaProducer(IpAddress=configuration.KAFKA_BROKER_IP, producerID=configuration.KAFKA_PRODUCER_ID,
                                               acks=configuration.KAFKA_PRODUCER_ACK, retry=configuration.KAFKA_PRODUCER_RETRY)

        # 要退出采集系统的群组id，从mysql的collected_group中查找
        chat_id = -100123412341234

        logger.debug('chat_id [{}] is sending...'.format(chat_id))
        producer.sendMsg(topic=configuration.KAFKA_TOPIC_MANAGE_LEAVE, value=str(chat_id))
        time.sleep(20)
        logger.debug("send chat_id [{}] successfully.".format(chat_id))