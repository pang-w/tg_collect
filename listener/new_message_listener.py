# encoding: utf-8
# 监听新消息

import json
import logging
import configuration

from pyrogram.handlers import MessageHandler

# logger=logging.getLogger(configuration.logger_name)
from utils import util

logger = util.init_other_logger('listen', 'listen')

# 用来监听新消息
class NewMessageListener():

    def __init__(self, client, producer):
        self.client = client
        self.producer = producer

    def run(self):
        my_handler = MessageHandler(self.handler)
        self.client.add_handler(my_handler)

        logger.info('start listening new message')

    def handler(self, client, message):
        message = json.loads(str(message))
        message['type'] = 'timely'
        logger.debug('new message: {}'.format(message))
        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_MESSAGE_FILTER, value=str(json.dumps(message)))