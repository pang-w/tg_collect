from kafka import KafkaProducer,KafkaConsumer,errors
import logging
import time
import configuration
import os
logger=logging.getLogger(configuration.logger_name)

def create_kafka_consumer(topic,ip_address,g_id,session_timeout,request_timeout):
    print('创建一个消费者')
    try:
        consumer=KafkaConsumer(topic, bootstrap_servers=ip_address, group_id=g_id,
                  session_timeout_ms=session_timeout, request_timeout_ms=request_timeout)
        return consumer
    except (errors.NoBrokersAvailable,BaseException) as e:
        logger.critical(repr(e), 'critical')
        time.sleep(0.5)
        os._exit(1)

def create_kafka_producer(ip_address,client_id,acks,retry):
    try:
        producer = KafkaProducer(bootstrap_servers=ip_address,client_id=client_id,acks=acks,retries=retry)
        return producer
    except (errors.NoBrokersAvailable,BaseException) as e:
        logger.critical(repr(e))
        time.sleep(0.5)
        os._exit(1)


def start_telegram_client(client,retry_time):
    try:
        client.start()
        logger.debug('telegram client start successfully')
    except (RuntimeError,BaseException) as e:
        if (retry_time > configuration.max_retry_time):
            logger.critical('telegram_client start failly,retry times reach the limit')
            time.sleep(0.3)
            os._exit(1)
        #not reach the limit
        wait_time=configuration.time_out*((retry_time//2)+1)
        logger.warning('the request of telegram_client start timesout,ready to retry after waiting'+str(wait_time)+'s')
        #sleep wait
        time.sleep(wait_time)
        #retry
        start_telegram_client(client,retry_time+1)



