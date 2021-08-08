# encoding: utf-8

import json
import re

import traceback
import logging
import configuration
from kafkaConsumer import KafkaConsumer
from utils import util

# logger=logging.getLogger(configuration.logger_name)
logger = util.init_other_logger('link_filter', 'link_filter')

class LinkFilterConsumer(KafkaConsumer):

    def __init__(self, topic, IpAddress, g_id, producer, session_timeout, request_timeout):
        super().__init__(topic=topic, IpAddress=IpAddress, g_id=g_id, session_timeout=session_timeout,
                         request_timeout=request_timeout)
        self.producer = producer

    def run(self):
        for msg in self.consumer:
            logger.debug('topic=' + self.topic + ',partition=' + str(msg.partition) + ', offset=' + str(msg.offset))
            try:
                msg = msg.value.decode('utf-8')
                if msg == "0": continue
                logger.debug("link filter: {}".format(msg))
                link_info = json.loads(msg)
            except BaseException:
                logger.error(str(msg.value) + traceback.format_exc())
                continue
            # rule
            proxy_type = re.compile(r"(?:http[s]?://t\.me/|tg://)proxy\?")
            group_type = re.compile(r"http[s]?://(?:t\.me|telegram\.me)/")
            onion_type = re.compile(r"(?:https|http)://(?:www\.|)([a-zA-Z0-9]{16}|[a-zA-Z0-9]{56})\.onion")
            try:
                if proxy_type.match(link_info["link"]):
                    self.filter_new_proxy_find(link=link_info["link"], date=link_info["date"])
                    continue
                if group_type.match(link_info["link"]):
                    self.filter_new_group_find(link_info)
                    continue
                m = onion_type.findall(link_info["link"])
                if m:
                    self.filter_new_onion_find(m[0])
                    continue
            except BaseException as e:
                logger.warning(repr(e))
                continue



    def filter_new_proxy_find(self, link, date):
        server_p=re.compile(r"server=(.+?)&")
        port_p=re.compile(r"port=(\d{1,5})")
        # secret_p=re.compile(r"secret=((?:dd)*\w{100})")
        secret_p = re.compile(r"secret=")

        result={}

        m1=server_p.search(link)
        m2=port_p.search(link)
        m3=secret_p.search(link)

        if m1:
            result['server'] = m1.group(1)
            index=result['server'].find('@')
            if not index==-1:
                result['server']=result['server'][:index]
            #incase the ip like:098.065.22.11
            if not(self.ipv6_check(result['server']) or self.ipv4_check(result['server']) or self.host_check(result['server'])):
                logger.warning('this proxy is unvaild:' + str(link))
                return

        if m2:
            result['port']=m2.group(1)
            #incase the port like:1
            try:
                result['port']=self.vaildPort(result['port'])
            except BaseException as e:
                logger.warning('this proxy is unvaild:' + str(link)+' reason:'+repr(e))
                return
        if m3:
            result['secret']=link[m3.end():]
        if not(len(result)==3):
            logger.warning('this proxy is not intact:'+str(link))
            return

        #store to sql
        result['date']=date
        result=json.dumps(result,ensure_ascii=False)
        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_PROXY_STORE, value=result)
        logger.debug('new proxy:'+str(link))

    def filter_new_group_find(self, link_info):
        link = link_info["link"]

        group_type = re.compile(r"http[s]?://(?:t\.me|telegram\.me)/")
        m = group_type.match(link)
        type = '@'
        ui = link[m.end():]

        # 判断是否是hash
        hash_type = re.compile(r"joinchat/")
        m = hash_type.match(ui)
        if m:  # is hash
            type = 'hash'
            ui = ui[m.end():]

        index = ui.find('/')
        if not (index == -1):
            ui = ui[:index]

        username_type = re.compile(r'^[a-zA-Z][\w\d]{3,30}[a-zA-Z\d]$')
        if not username_type.match(ui):
            return  # 无效的用户名

        data = {
            'link': link,
            'type': type,
            'ui': ui,
            'chat_id': link_info["chat_id"],
            'message_id': link_info["message_id"],
            'text': link_info["text"],
            'find_time': link_info["date"]
        }
        data = json.dumps(data, ensure_ascii=False)

        # 发送至链接保存模块保存
        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_FIND_GROUP_STORE, value=data)

    def filter_new_onion_find(self,link):
        self.producer.sendMsg(topic=configuration.KAFKA_TOPIC_ONION_STORE, value=link)
        logger.debug('new onion:'+str(link))

    def vaildPort(self,port):
        try:
            p=int(port)
            if(p>66535 or p<1):
                raise Exception('port is unvaild')
            return str(p)
        except BaseException:
            raise Exception('port is unvaild')

    def ipv6_check(self,addr):
        '''
        Returns True if the IPv6 address (and optional subnet) are valid, otherwise
        returns False.
        '''
        ip6_regex = (
            r'(^(?:[A-F0-9]{1,4}:){7}[A-F0-9]{1,4}$)|'
            r'(\A([0-9a-f]{1,4}:){1,1}(:[0-9a-f]{1,4}){1,6}\Z)|'
            r'(\A([0-9a-f]{1,4}:){1,2}(:[0-9a-f]{1,4}){1,5}\Z)|'
            r'(\A([0-9a-f]{1,4}:){1,3}(:[0-9a-f]{1,4}){1,4}\Z)|'
            r'(\A([0-9a-f]{1,4}:){1,4}(:[0-9a-f]{1,4}){1,3}\Z)|'
            r'(\A([0-9a-f]{1,4}:){1,5}(:[0-9a-f]{1,4}){1,2}\Z)|'
            r'(\A([0-9a-f]{1,4}:){1,6}(:[0-9a-f]{1,4}){1,1}\Z)|'
            r'(\A(([0-9a-f]{1,4}:){1,7}|:):\Z)|(\A:(:[0-9a-f]{1,4}){1,7}\Z)|'
            r'(\A((([0-9a-f]{1,4}:){6})(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3})\Z)|'
            r'(\A(([0-9a-f]{1,4}:){5}[0-9a-f]{1,4}:(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3})\Z)|'
            r'(\A([0-9a-f]{1,4}:){5}:[0-9a-f]{1,4}:(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}\Z)|'
            r'(\A([0-9a-f]{1,4}:){1,1}(:[0-9a-f]{1,4}){1,4}:(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}\Z)|'
            r'(\A([0-9a-f]{1,4}:){1,2}(:[0-9a-f]{1,4}){1,3}:(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}\Z)|'
            r'(\A([0-9a-f]{1,4}:){1,3}(:[0-9a-f]{1,4}){1,2}:(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}\Z)|'
            r'(\A([0-9a-f]{1,4}:){1,4}(:[0-9a-f]{1,4}){1,1}:(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}\Z)|'
            r'(\A(([0-9a-f]{1,4}:){1,5}|:):(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}\Z)|'
            r'(\A:(:[0-9a-f]{1,4}){1,5}:(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}\Z)')
        return bool(re.match(ip6_regex, addr, flags=re.IGNORECASE))

    def host_check(self,host):
        p = re.compile(r'(?:[A-Z0-9_](?:[A-Z0-9-_]{0,247}[A-Z0-9])?\.)+(?:[A-Z]{2,6}|[A-Z0-9-]{2,}(?<!-))\Z',re.IGNORECASE)
        if p.match(host):
            return True
        else:
            return False

    def ipv4_check(self,ipAddr):
        p = re.compile('^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
        if p.match(ipAddr):
            return True
        else:
            return False