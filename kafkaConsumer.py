# -*- coding:utf-8 -*-
import network

class KafkaConsumer():

    def __init__(self,topic,IpAddress,g_id,session_timeout,request_timeout):
        self.topic = topic
        self.ip_address = IpAddress
        self.g_id = g_id
        self.session_timeout = session_timeout
        self.request_timeout = request_timeout
        self.consumer = network.create_kafka_consumer(topic=self.topic, ip_address=self.ip_address, g_id=self.g_id,
                                                      session_timeout=self.session_timeout,
                                                      request_timeout=self.request_timeout)
