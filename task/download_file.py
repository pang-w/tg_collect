# -*- coding:utf-8 -*-
# 下载从消息中过滤出来的需要下载的文件
import os

import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__)) + "/..")
import configuration
import time
from pyrogram import Client
from utils import util
import kafkaProducer
from elasticsearch import Elasticsearch
from fdfs_client.client import Fdfs_client
import traceback
from collect import download_file

if __name__ == "__main__":
    logger=util.init_other_logger('file_downloador', 'file_downloador')
    try:
        producer = kafkaProducer.KafkaProducer(IpAddress=configuration.KAFKA_BROKER_IP,
                                               producerID=configuration.KAFKA_PRODUCER_ID,
                                               acks=configuration.KAFKA_PRODUCER_ACK,
                                               retry=configuration.KAFKA_PRODUCER_RETRY)
        logger.info('init kafka successfully')

        # --connect to es and create es entity--
        es = []
        for ip in configuration.ES_IP:
            # es.append(Elasticsearch(ip))
            es.append(Elasticsearch(ip, http_auth=('elastic', '123456')))
        logger.info('init es successfully')

        if (configuration.ip == None):
            client_download = Client(configuration.session_name_download_file, api_id=configuration.api_id,
                              api_hash=configuration.api_hash)
        else:
            client_download = Client(configuration.session_name_download_file, api_id=configuration.api_id,
                              api_hash=configuration.api_hash,
                              proxy=dict(hostname=configuration.ip, port=configuration.port))
        logger.info("init telegram client successfully")

        fdfs_conf_path = os.path.dirname(os.path.dirname(__file__))
        fdfs_conf_path = os.path.join(fdfs_conf_path, 'fdfs_client.conf')
        dfs_client = Fdfs_client(fdfs_conf_path)

        with client_download:
            file_downloador = download_file.FileDownloador(producer=producer, client=client_download, es=es, es_index=configuration.ES_INDEX_MEDIA, dfs_client=dfs_client)
            file_downloador.run()
    except BaseException as e:
        logger.error(traceback.format_exc())
        time.sleep(0.5)
