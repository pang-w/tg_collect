# -*- coding:utf-8 -*-
import configuration
from elasticsearch.exceptions import ConnectionTimeout
from storage.storager import EsStorageConsumer
import json
import util
import logging
logger = logging.getLogger(configuration.logger_name)

class ServiceMessageStoreConsumer(EsStorageConsumer):

    def __init__(self, topic, IpAddress, g_id,producer, es,es_index,es_type,session_timeout,request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, producer=producer,es=es,es_index=es_index,es_type=es_type,session_timeout=session_timeout,request_timeout=request_timeout)

    def run(self):
        for msg in self.consumer:
            logger.debug('partition='+str(msg.partition)+', offset='+str(msg.offset))
            try:
                sermsg=json.loads(msg.value.decode('utf-8'))

                #union id
                group_id=util.get_to_id(sermsg['to_id'])
                group_id=group_id['id']
                _id=str(group_id)+'_'+str(sermsg['id'])
            except BaseException as e:
                logger.error(str(msg.value) + ' reason:' + repr(e))
                continue

            #store to service_message type
            try:
                for e in self.es:
                    e.index(index=self.es_index, doc_type=self.es_type,id=_id,body=sermsg)

                logger.debug('store to ES successfully,service_message_union_id:' + _id)
            except (ConnectionTimeout,BaseException )as e :
                logger.warning('store data failly,retry,service_message_union_id:' + _id+'   ,reason:'+repr(e))
                self.reput(sermsg)
