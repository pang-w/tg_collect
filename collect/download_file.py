# encoding: utf-8
import json
import os
import time
import datetime

import logging

from pyrogram.errors import FloodWait

import configuration

logger=logging.getLogger("file_downloador")

avatar_chat_dir = configuration.DOWNLOAD_CHAT_AVATAR_ROOT_DIR
avatar_user_dir = configuration.DOWNLOAD_USER_AVATAR_ROOT_DIR
file_dir = configuration.DOWNLOAD_FILE_AVATAR_ROOT_DIR

class FileDownloador():
    def __init__(self,producer,client,es, es_index, dfs_client):
        self.producer = producer
        self.client = client
        self.es = es
        self.es_index = es_index
        self.dfs_client = dfs_client

    def run(self):  # 文件id和ref
        while True:

            logger.debug("file_downloador is running")

            # 按顺序下载所有未下载的文件
            # result = self.es[0].search(index=configuration.ES_INDEX_NEED_DOWNLOAD_FILE,
            #                            body={"query": {"bool": {"must": [{"term": {"status.keyword": {"value": "Not downloaded"}}},
            #                                                              {"term": {"collect_account.keyword": {"value": configuration.PHONENUM}}}
            #                                                              ]}}}, size=10)
            # 下载所有即时消息的文件
            # result = self.es[0].search(index=configuration.ES_INDEX_NEED_DOWNLOAD_FILE,
            #                            body={"query": {"bool": {"must": [{"term": {"status.keyword": {"value": "Not downloaded"}}},
            #                                                              {"term": {"collect_account.keyword": {"value": configuration.PHONENUM}}},
            #                                                              {"term": {"source.keyword": {"value": "timely"}}}
            #                                                         ]}}}, size=10)
            # 按顺序下载所有历史消息的文件
            # result = self.es[0].search(index=configuration.ES_INDEX_NEED_DOWNLOAD_FILE,
            #                            body={"query": {"bool": {"must": [{"term": {"status.keyword": {"value": "Not downloaded"}}},
            #                                                              {"term": {"collect_account.keyword": {"value": configuration.PHONENUM}}},
            #                                                              {"term": {"source.keyword": {"value": "historical"}}}
            #                                                         ]}}}, size=10)
            # 下载所有即时文件中小于50M的文件
            max_file_size = 100  # 单位：MB
            result = self.es[0].search(index=configuration.ES_INDEX_NEED_DOWNLOAD_FILE,
                                       body={"query": {"bool": {"must": [{"term": {"status.keyword": {"value": "Not downloaded"}}},
                                                                         {"term": {"collect_account.keyword": {"value": configuration.PHONENUM}}},
                                                                         {"term": {"source.keyword": {"value": "timely"}}},
                                                                         {"range": {"file_size": {"lte": max_file_size*1024*1024}}}
                                                                    ]}},"sort": [{"date": {"order": "desc"}}]}, size=5)
            if result["hits"]["total"]["value"] == 0:
                # 如果没有实时消息，则下载历史消息
                result = self.es[0].search(index=configuration.ES_INDEX_NEED_DOWNLOAD_FILE,
                                           body={"query": {"bool": {"must": [
                                               {"term": {"status.keyword": {"value": "Not downloaded"}}},
                                               {"term": {"collect_account.keyword": {"value": configuration.PHONENUM}}},
                                               {"term": {"source.keyword": {"value": "historical"}}},
                                               {"range": {"file_size": {"lte": max_file_size * 1024 * 1024}}}
                                           ]}},"sort": [{"date": {"order": "desc"}}]}, size=5)
                if result["hits"]["total"]["value"] == 0:
                    time.sleep(10)
                    continue

            need_download_file_infos = result["hits"]["hits"]
            for need_download_file_info in need_download_file_infos:
                source = need_download_file_info["_source"]
                _id = need_download_file_info["_id"]  # 维持同一 _id

                logger.info("start process file info：{}".format(source))

                if self.has_file(source["file_id"]):
                    logger.info("the file has been downloaded!：{}".format(source))
                    result = self.es[0].search(index=configuration.ES_INDEX_MEDIA,
                                      body={"query": {"match": {"file_id.keyword": source["file_id"]}}})
                    # 将文件保存信息复制一份并保存
                    media = result["hits"]["hits"][0]["_source"]
                    media["chat_id"] = source["chat_id"]
                    media["message_id"] = source["message_id"]
                    if "user_id" in source:
                        media["user_id"] = source["user_id"]
                    media["date"] = source["date"]
                    media["insert_date"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

                    # 直接存储到es media
                    for e in self.es:
                        e.index(index=self.es_index, id=_id, body=media)
                    logger.info('store to ES media successfully, union id:' + _id)

                    # 直接存储到es need_download_file
                    source["status"] = "Downloaded"
                    for e in self.es:
                        e.index(index=configuration.ES_INDEX_NEED_DOWNLOAD_FILE, id=_id, body=source)
                    logger.info('update ES need_download_file successfully, union id:' + _id)
                else:
                    rel_dir = self.get_file_rel_dir(source)  # 根据消息中夹带文件的信息生成相对目录
                    try:
                        path = self.client.download_media(message=source["file_id"], file_ref=source["file_ref"], file_name=file_dir + rel_dir)
                    except FloodWait as e:
                        logger.warning("Sleeping for {}s".format(e.x))
                        time.sleep(e.x + 10)
                        continue
                    except BaseException as e:
                        logger.warning(e)
                        path = None

                    # 如果path为空,可能是文件id和ref过期，重新获取消息，比较新消息中的文件信息是否发生变化，若发生变化，则重新过滤消息进而进行下载，若消息没变，则直接设置为失效
                    if not path:
                        logger.warning("download file failed [{}]!".format(source))
                        if self.get_time_dif_with_now(source["update_time"]) < 2*60*60:  # 如果更新时间update_time与当前时间差小于2个小时（ref不可能过期的时间），直接设为无效
                            source["status"] = "Invalid"
                            for e in self.es:
                                e.index(index=configuration.ES_INDEX_NEED_DOWNLOAD_FILE, id=_id, body=source)
                            time.sleep(30)
                            continue
                        # 如果更新时间超过2个小时，则重新获取消息的文件信息
                        try:
                            message = self.client.get_messages(source["chat_id"], source["message_id"])
                            message = json.loads(str(message))
                        except FloodWait as e:
                            logger.warning("Sleeping for {}s".format(e.x))
                            time.sleep(e.x + 10)
                            continue
                        except BaseException as e:
                            logger.warning(e)
                            continue
                        # 消息获取成功，则将此待下载文件信息改为无效，若消息没获取成功，则在上一步就直接跳出此次循环，执行不到这
                        logger.debug("reacquired message!".format(message))
                        source["status"] = "Invalid"
                        for e in self.es:
                            e.index(index=configuration.ES_INDEX_NEED_DOWNLOAD_FILE, id=_id, body=source)
                        # 比较新的message和当前source里的file类型和file_id以及file_ref是否一样，一样则此次循环结束,不一样则重新过滤消息，
                        message["type"] = source["source"]
                        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_MESSAGE_FILTER, value=json.dumps(message))
                        time.sleep(30)
                        continue

                    # 执行到此表示已下载成功
                    logger.info("download file successfully, local file path is [{}]".format(path))

                    # 上传至文件服务器
                    remote_file_path = self.upload_file_to_server(path)  # 上传文件
                    if not remote_file_path:
                        continue
                    self.cleanfile(path)  # 删除本地文件

                    # 存储已下载文件的相关信息
                    save_media_info = {}
                    save_media_info["chat_id"] = source["chat_id"]
                    save_media_info["message_id"] = source["message_id"]
                    if "user_id" in source:
                        save_media_info["user_id"] = source["user_id"]
                    save_media_info["type"] = source["type"]
                    save_media_info["file_id"] = source["file_id"]
                    save_media_info["file_ref"] = source["file_ref"]
                    save_media_info["date"] = source["date"]
                    if "duration" in source:
                        save_media_info["duration"] = source["duration"]
                    if "file_name" in source:
                        save_media_info["file_name"] = source["file_name"]
                    if "mime_type" in source:
                        save_media_info["mime_type"] = source["mime_type"]
                    if "file_size" in source:
                        save_media_info["file_size"] = source["file_size"]

                    # save_media_info["location"] = rel_dir  # 存储相对位置
                    save_media_info["location"] = remote_file_path  # 存储文件服务器的文件位置

                    # 直接存储到es media
                    save_media_info["insert_date"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    for e in self.es:
                        e.index(index=self.es_index,id=_id,body=save_media_info)
                    logger.info('store to ES successfully, union id:' + _id)

                    # 直接修改es need_download_file
                    source["status"] = "Downloaded"  # 将此信息改为已下载
                    for e in self.es:
                        e.index(index=configuration.ES_INDEX_NEED_DOWNLOAD_FILE,id=_id,body=source)
                    logger.info('update ES need_download_file successfully, union id:' + _id)
                    time.sleep(30)
                time.sleep(30)

    # 查询文件是否存在（从media中查询）
    def has_file(self, file_id):
        try:
            for e in self.es:
                result = e.search(index=configuration.ES_INDEX_MEDIA,
                                  body={"query": {"match": {"file_id.keyword": file_id}}})
                if result["hits"]["total"]["value"] == 0:
                    return False
        except:
            return False
        return True

    def process_filename(self, file_name):
        if len(file_name) == 0 or file_name[-1] == ".":
            return ".zip"
        strs = file_name.split(".")
        if len(strs) > 1:
            file_name = "_".join(strs[:-1]) + "." + strs[-1]
        if file_name[0] == ".":
            return "_" + file_name
        return file_name

    def get_file_rel_dir(self, file_info):
        dir = str(file_info["chat_id"]) + "/"
        name = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
        if file_info["type"] == "photo":
            name = name + ".jpg"
        elif file_info["type"] == "document":
            if "file_name" in file_info:
                name = name + "_" + self.process_filename(file_info["file_name"])
            else:
                name = name + ".zip"
        elif file_info["type"] == "video":
            if "file_name" in file_info:
                name = name + "_" + self.process_filename(file_info["file_name"])
            else:
                name = name + ".mp4"
        elif file_info["type"] == "audio":
            if "file_name" in file_info:
                name = name + "_" + self.process_filename(file_info["file_name"])
            else:
                name = name + ".mp3"
        elif file_info["type"] == "voice":
            name = name + ".ogg"
        return dir + name

    def upload_file_to_server(self,file_path):
        try:
            ret = self.dfs_client.upload_by_filename(file_path)
            if 'Status' in ret and 'Storage IP' in ret and ret['Status'] == 'Upload successed.':
                # data={'remote_file_path':ret['Storage IP'] + '/'+ret['Remote file_id'], 'local_file_path':ret['Local file name'], 'file_size': ret['Uploaded size']}
                return ret['Storage IP'] + '/' + ret['Remote file_id']
            logger.error('upload file is failed,retry,file_name:' + file_path)
            time.sleep(10)
        except BaseException as e:
            logger.error('upload file is failed,file_name:%s,reason:%s' % (file_path, repr(e)))
            time.sleep(10)
            return None

    def cleanfile(self,path):
        try:
            if path:
                os.remove(path)
            logger.debug('cleanfile is ok')
        except BaseException as e:
            logger.error(repr(e))

    def get_time_dif_with_now(self, time):
        date_time = datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
        now = datetime.datetime.now()
        return int((now - date_time).total_seconds())
