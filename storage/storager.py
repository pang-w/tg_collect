# -*- coding:utf-8 -*-
from kafkaConsumer import KafkaConsumer
import logging
import configuration
import json
logger=logging.getLogger(configuration.logger_name)

class StorageConsumer(KafkaConsumer):

    def __init__(self, topic, IpAddress, g_id, producer,session_timeout,request_timeout):
        super().__init__(topic=topic,IpAddress=IpAddress,g_id=g_id,session_timeout=session_timeout,request_timeout=request_timeout)
        self.producer = producer

    def reput(self,msg):
        try:
            msgj=json.dumps(msg,ensure_ascii=False)
            self.producer.sendMsg(topic=self.topic,value=msgj)
        except BaseException as e:
            logger.error(str(self.topic)+' reput is failed:'+str(msg)+' reason:'+repr(e))

class EsStorageConsumer(StorageConsumer):
    def __init__(self, topic, IpAddress, g_id, producer,es,es_index,session_timeout,request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, producer=producer,session_timeout=session_timeout,request_timeout=request_timeout)
        self.es = es
        self.es_index = es_index
        # self.es_type = es_type

    def tg_to_drawl(self,data,_id,_type):
        try:
            hitt={'id':_id,'type':_type,'data':data}
            hitj=json.dumps(hitt,ensure_ascii=False)
            self.producer.sendMsg(topic='tg_to_drawl',value=hitj)
        except BaseException as e:
            logger.error(str(self.topic) + ' reput is failed:' + str(hitt) + ' reason:' + repr(e))




