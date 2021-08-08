import sys
import time

sys.path.append("..")

import kafkaProducer
import configuration
from init import init

# 通过向kafka topic发送消息来创建topic
if __name__ == "__main__":

    producer = kafkaProducer.KafkaProducer(IpAddress=configuration.KAFKA_BROKER_IP,
                                           producerID=configuration.KAFKA_PRODUCER_ID,
                                           acks=configuration.KAFKA_PRODUCER_ACK,
                                           retry=configuration.KAFKA_PRODUCER_RETRY)

    init.send_test_data_to_topic(producer)
    print("等待30秒")
    time.sleep(30)