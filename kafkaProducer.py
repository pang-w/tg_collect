# -*- coding:utf-8 -*-
import network
import logging
import configuration
logger=logging.getLogger(configuration.logger_name)

class KafkaProducer():
    def __init__(self,IpAddress,producerID,acks,retry):
        self.ip_address = IpAddress
        self.producer_id=producerID
        self.acks=acks
        self.retry=retry
        self.producer=network.create_kafka_producer(ip_address=IpAddress,client_id=self.producer_id,acks=self.acks,retry=self.retry)

    def _on_send_error(self,excp):
        logger.error('kafka send erro:'+str(excp))

    def sendMsg(self,topic,value):
        try:
            self.producer.send(topic=topic,value=value.encode('utf-8')).add_errback(self._on_send_error)
        except BaseException as e:
            logger.error('kafka sendMsg erro:'+repr(e)+',topic:'+topic)
