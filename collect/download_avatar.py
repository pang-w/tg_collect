# encoding: utf-8

import json
import os
import time

import logging
import traceback

from pyrogram.errors import FloodWait, LocationInvalid, PeerIdInvalid, FileIdInvalid

import configuration
from kafkaConsumer import KafkaConsumer

logger=logging.getLogger("avatar_downloador")

avatar_chat_dir = configuration.DOWNLOAD_CHAT_AVATAR_ROOT_DIR
avatar_user_dir = configuration.DOWNLOAD_USER_AVATAR_ROOT_DIR
file_dir = configuration.DOWNLOAD_FILE_AVATAR_ROOT_DIR

class AvatarDownloadorConsumer(KafkaConsumer):
    def __init__(self,topic,IpAddress,g_id,producer,client,es, es_index,dfs_client,session_timeout,request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, session_timeout=session_timeout,
                         request_timeout=request_timeout)
        self.producer = producer
        self.client = client
        self.es = es
        self.es_index = es_index
        self.dfs_client = dfs_client

    def run(self):  # 文件id和ref
        for msg in self.consumer:
            logger.debug('topic=' + self.topic + ',partition=' + str(msg.partition) + ', offset=' + str(msg.offset))
            try:
                file_info = json.loads(msg.value.decode('utf-8'))
            except BaseException as e:
                logger.warning(str(msg.value) + ' reason:' + repr(e))
                continue

            logger.info("start process avatar info：{}".format(file_info))

            try:
                if self.has_file(file_info["file_id"]):
                    logger.info("The avatar has been downloaded!")
                    continue

                # chat 头像的处理
                if file_info["type"] == "chat_avatar":
                    rel_dir = str(file_info["chat_id"]) + time.strftime('_%Y%m%d%H%M%S', time.localtime(time.time())) + ".jpg"
                    try:
                        path = self.client.download_media(message=file_info["file_id"], file_name=avatar_chat_dir + rel_dir)
                    except FloodWait as e:
                        logger.warning("Sleeping for {}s".format(e.x))
                        time.sleep(e.x + 10)
                        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_DOWNLOAD_AVATAR, value=str(json.dumps(file_info)))
                        continue
                    except (LocationInvalid, PeerIdInvalid, FileIdInvalid):
                        logger.warning("download chat avatar failed [{}], The file address/id/access_hash is invalid!".format(file_info["chat_id"]))
                        continue
                    if not path:  # 存储不成功，重新发送
                        logger.warning("download chat avatar failed [{}], try again!".format(file_info["chat_id"]))
                        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_DOWNLOAD_AVATAR, value=str(json.dumps(file_info)))
                        continue
                    logger.info("download chat avatar successfully [{}]".format(file_info["chat_id"]))
                    # file_info["location"] = rel_dir  # 存储相对位置
                    # 直接存储
                    _id = str(file_info["chat_id"]) + "_" + file_info["file_id"]

                # user 头像的处理
                elif file_info["type"] == "user_avatar":
                    rel_dir = str(file_info["user_id"]) + time.strftime('_%Y%m%d%H%M%S', time.localtime(time.time())) + ".jpg"
                    try:
                        path = self.client.download_media(message=file_info["file_id"], file_name=avatar_user_dir + rel_dir)
                    except FloodWait as e:
                        logger.warning("Sleeping for {}s".format(e.x))
                        time.sleep(e.x + 10)
                        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_DOWNLOAD_AVATAR, value=str(json.dumps(file_info)))
                        continue
                    except (LocationInvalid, PeerIdInvalid, FileIdInvalid):
                        logger.warning("download user avatar failed [{}], The file address/id/access_hash is invalid!".format(file_info["user_id"]))
                        continue
                    if not path:  # 存储不成功，不重新发送
                        logger.warning("download user avatar failed [{}]!".format(file_info["user_id"]))
                        continue
                    logger.info("download user avatar successfully [{}]".format(file_info["user_id"]))
                    # file_info["location"] = rel_dir  # 存储相对位置
                    _id = str(file_info["user_id"]) + "_" + file_info["file_id"]
                else:
                    logger.warning("check the download avatar data format!")
                    continue

                remote_file_path = self.upload_file_to_server(path)  # 上传文件
                self.cleanfile(path)  # 删除本地文件
                file_info["location"] = remote_file_path
                file_info["date"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  # 当前时间
                file_info["insert_date"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

                for e in self.es:
                    e.index(index=self.es_index, id=_id, body=file_info)
                logger.info('store to ES successfully,media id:' + _id)

            except BaseException:
                logger.error(msg + traceback.format_exc())

    # 查询此文件是否已存在
    def has_file(self, file_id):
        try:
            for e in self.es:
                result = e.search(index=configuration.ES_INDEX_MEDIA, body={"query": {"match": {"file_id": file_id}}}, request_timeout=60)
                if result["hits"]["total"]["value"] == 0:
                    return False
        except:
            return False
        return True

    def upload_file_to_server(self,file_path):
        try:
            ret = self.dfs_client.upload_by_filename(file_path)
            if 'Status' in ret and 'Storage IP' in ret and ret['Status'] == 'Upload successed.':
                # data={'remote_file_path':ret['Storage IP'] + '/'+ret['Remote file_id'], 'local_file_path':ret['Local file name'], 'file_size': ret['Uploaded size']}
                return ret['Storage IP'] + '/' + ret['Remote file_id']
            logger.error('upload file is failed,retry,file_name:' + file_path)
            time.sleep(10)
        except BaseException as e:
            logger.error('upload file is failed,retry,file_name:%s,reason:%s' % (file_path, repr(e)))
            time.sleep(10)

    def cleanfile(self,path):
        try:
            if path:
                os.remove(path)
            logger.debug('cleanfile is ok')
        except BaseException as e:
            logger.error(repr(e))

