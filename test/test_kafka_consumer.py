
import sys
import time

sys.path.append("..")
import configuration
import network

topic = configuration.KAFKA_TOPIC_HISTORY_COLLECTOR
ip_address = configuration.KAFKA_BROKER_IP
g_id=configuration.KAFKA_HISTORY_COLLECTOR_GROUP_ID
session_timeout=configuration.KAFKA_HISTORY_COLLECTOR_SESSION_TIMEOUT_MS
request_timeout=configuration.KAFKA_HISTORY_COLLECTOR_REQUEST_TIMEOUT_MS

consumer = network.create_kafka_consumer(topic=topic,
                                         ip_address=ip_address,
                                         g_id=g_id,
                                         session_timeout=session_timeout,
                                         request_timeout=request_timeout)

for msg in consumer:
    print('topic=' + topic + ',partition=' + str(msg.partition) + ', offset=' + str(msg.offset))
    msg = msg.value.decode('utf-8')
    print(msg)
    time.sleep(2)

