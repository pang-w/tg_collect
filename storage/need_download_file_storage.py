# -*- coding:utf-8 -*-
import time
import traceback
from datetime import datetime

import configuration
from elasticsearch.exceptions import ConnectionTimeout, ConnectionError
from storage.storager import EsStorageConsumer
import json
import logging
logger=logging.getLogger(configuration.logger_name)


class NeedDownloadFileStoreConsumer(EsStorageConsumer):

    def __init__(self, topic, IpAddress, g_id, producer,es,es_index,session_timeout,request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, producer=producer,es=es,es_index=es_index,session_timeout=session_timeout,request_timeout=request_timeout)

    def run(self):
        for msg in self.consumer:
            if configuration.clean_topic:
                print("need download file storage：", json.loads(msg.value.decode('utf-8')))
            else:
                logger.debug('topic='+self.topic+',partition='+str(msg.partition)+', offset='+str(msg.offset))
                try:
                    msg = msg.value.decode('utf-8')
                    logger.debug("need download file storage: {}".format(msg))
                    if msg == "0": continue

                    file_info = json.loads(msg)
                    file_info["update_time"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    date = datetime.strptime(file_info["date"], '%Y-%m-%d %H:%M:%S')
                    _id = str(file_info['chat_id'])+'_'+str(file_info['message_id'])+'_' + date.strftime('%Y%m%d%H%M%S')
                except BaseException as e:
                    logger.error(str(msg.value) + ' reason:' + repr(e))
                    continue

                # store to need download file index
                try:
                    for e in self.es:
                        e.index(index=self.es_index,id=_id,body=file_info)
                    logger.debug('store to ES successfully,need_download_file_union_id='+_id)
                except BaseException:
                    logger.error(str(msg) + traceback.format_exc())
                    self.reput(file_info)
                    time.sleep(10)
