# -*- coding:utf-8 -*-
"""
第一次部署整个分布式系统时，需要修改三个mysql、es、fdfs、kafka的配置信息，这类配置为所有节点公用

每个节点单独的配置如手机号等，需要读取外部配置文件（项目同级目录下的self_config/self_config.ini）

"""
import socks
import os
import configparser

# tg_collect项目的上级目录
root_dir = os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]



############################################################
# 读取配置文件，获取外部配置文件信息(当前采集账号独有的配置)     #
############################################################
self_config_dir = os.path.join(root_dir, 'self_config')
if not os.path.exists(self_config_dir):
    os.mkdir(self_config_dir)
self_config_file=os.path.join(self_config_dir, 'self_config.ini')
if not os.path.exists(self_config_file):
    raise BaseException('without %s' % self_config_file)
cf = configparser.RawConfigParser()
cf.read(self_config_file)
# 读取角色信息
ROLE = cf.get("node_info", "ROLE")  # master、slave、slave_verify可选
# 读取账号信息
PHONENUM = cf.get("telegram_account", "PHONENUM")    # 手机号
PHONECTR = cf.get("telegram_account", "PHONECTR")    # 手机号前缀
api_id = cf.get("telegram_account", "api_id")        # 采集账号api_id
api_hash = cf.get("telegram_account", "api_hash")    # 采集账号api_hash
tg_password = cf.get("telegram_account","password")  # 二次登录所需密码
# 读取docker和配置的flask ip、port信息
DOCKER_CONTAINER_ID = cf.get("node_info", "DOCKER_CONTAINER_ID")
SERVER_IP = cf.get("node_info", "SERVER_IP")
SERVER_PORT = cf.get("node_info", "SERVER_PORT")
# 代理信息
protocol_map={ "socks.SOCKS5": socks.SOCKS5 }
protocol = protocol_map[cf.get("node_info", "protocol")]
ip = cf.get("node_info", "ip")
port = int(cf.get("node_info", "socket"))
# 读取启动参数
# 标识是否第一次启动节点，需使用iter_dialog()函数一次获取所有会话列表（所加入的所有群组），
# 之后加入群组都是通过代码加入，如果发现群组个数有什么错误，可以将此参数设为true重启，则重新获取所有群组
first_start = cf.getboolean("startup_parameters", "first_start")




############################################################
# 公用的配置（各种环境：三个mysql、es、fdfs、kafka）  ##########
############################################################
# tg_collect数据库信息
MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3306
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = '12345678'
# tg_collect数据库表及建表语句
MYSQL_DATABASE = "tg_collect"
MYSQL_TABLE_FIND_GROUP = "find_group"
MYSQL_TABLE_COLLECT_ACCOUNT = "collect_account"
MYSQL_TABLE_COLLECTED_GROUP = "collected_group"
MYSQL_TABLE_NEED_UPDATE_CHAT_MEMBER = "need_update_chat_member"
MYSQL_TABLE_HISTORY_MSG_INFO = "history_msg_info"
MYSQL_CREATE_COLLECT_ACCOUNT_TABLE = "CREATE TABLE if not exists " + MYSQL_TABLE_COLLECT_ACCOUNT + "(`phone_num` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '手机号',`prefix` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '前缀',`role` varchar(15) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '节点账号的角色，master or slave',`api_id` int NOT NULL,`api_hash` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,`docker_container_id` varchar(50) DEFAULT NULL COMMENT 'docker容器id',`server_ip` varchar(20) DEFAULT NULL COMMENT '服务器ip',`server_port` varchar(20) DEFAULT NULL COMMENT '服务器端口',`working` int NOT NULL COMMENT '标识当前采集账号是否正在工作',`report_time` datetime NOT NULL COMMENT '上报节点信息的时间',PRIMARY KEY (`phone_num`) USING BTREE) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;"
MYSQL_CREATE_COLLECTED_GROUP_TABLE = "CREATE TABLE if not exists " + MYSQL_TABLE_COLLECTED_GROUP + "(`group_id` bigint NOT NULL COMMENT '群组id',`type` varchar(10) NOT NULL COMMENT '类型',`title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,`username` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,`members_count` int(11) NOT NULL,`invite_link` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,`description` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,`collect_account` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,`status` int NOT NULL,`join_by` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '记录通过系统添加的群组是使用什么username or link添加的，也方便加入成功后对结果进行查询',PRIMARY KEY (`group_id`) USING BTREE) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;"
MYSQL_CREATE_FIND_GROUP_TABLE = "CREATE TABLE if not exists " + MYSQL_TABLE_FIND_GROUP + "(`id` int(11) NOT NULL AUTO_INCREMENT,`link` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,`type` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,`ui` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,`chat_id` bigint(20) NOT NULL,`message_id` bigint(20) NOT NULL,`first_find_time` datetime(0) NULL,`last_find_time` datetime(0) NULL,`text` varchar(10000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,PRIMARY KEY (`id`) USING BTREE) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;"
MYSQL_CREATE_NEED_UPDATE_CHAT_MEMBER_TABLE = "CREATE TABLE if not exists " + MYSQL_TABLE_NEED_UPDATE_CHAT_MEMBER + "(`id` int NOT NULL AUTO_INCREMENT,`chat_id` bigint NOT NULL,`collect_account` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,`updated` int NOT NULL,`insert_time` datetime NULL,`update_time` datetime NULL DEFAULT NULL,PRIMARY KEY (`id`) USING BTREE) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;"
MYSQL_CREATE_HISTORY_MSG_INFO_TABLE = "CREATE TABLE if not exists " + MYSQL_TABLE_HISTORY_MSG_INFO + "(`id` int NOT NULL AUTO_INCREMENT,`chat_id` bigint NOT NULL,`now_msg_id` bigint NOT NULL,`last_msg_id` bigint NOT NULL,`collect_account` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,`collect_to_msg_id` bigint NOT NULL,`insert_time` datetime NOT NULL,`collect_time` datetime DEFAULT NULL,PRIMARY KEY (`id`) USING BTREE) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;"

# 存代理的数据库信息
MYSQL_PROXY_HOST = '127.0.0.1'
MYSQL_PROXY_PORT = 3306
MYSQL_PROXY_USER = 'root'
MYSQL_PROXY_PASSWD = '12345678'
# 存储代理数据库表及建表语句
MYSQL_PROXY_DB = 'mtproxy'
MYSQL_TABLE_PROXY = "proxy"
MYSQL_CREATE_PROXY_TABLE = "CREATE TABLE if not exists " + MYSQL_TABLE_PROXY + "(id int(11) NOT NULL AUTO_INCREMENT,server varchar(256) ,port varchar(6) ,secret varchar(100),date varchar(20), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1"

# 存储暗网地址的数据库信息
MYSQL_ONION_HOST = '127.0.0.1'
MYSQL_ONION_PORT = 3306
MYSQL_ONION_USER = 'root'
MYSQL_ONION_PASSWD = '12345678'
# 存储暗网数据库表名及建表语句
MYSQL_ONION_DB = 'awbackmonitor'
MYSQL_TABLE_ONION = "address_collection"
MYSQL_CREATE_ONION_TABLE = "CREATE TABLE if not exists " + MYSQL_TABLE_ONION + "(id int(11) NOT NULL AUTO_INCREMENT,url varchar(100) ,first_discovered_time datetime,first_discovered_method varchar(20),last_discovered_time datetime,last_discovered_method varchar(20),fail_count int(5), is_checked int(11), type varchar(10), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1"

# ES信息
ES_IP = ['127.0.0.1:9200']
# ES的用户名和密码如果有则需要修改这两个参数，如果没有不用管这两个参数
ES_USERNAME = 'username'
ES_PASSWORD = 'password'
# ES的5个index
ES_INDEX_USER = 'user'
ES_INDEX_MESSAGE = 'message'
ES_INDEX_NEED_DOWNLOAD_FILE = 'need_download_file'
ES_INDEX_CHAT = 'chat'
ES_INDEX_CHAT_MEMBER = 'chat_member'
ES_INDEX_MEDIA = 'media'

# FDFS信息（配置信息需要拷贝配置文件到tg_collect/task）
# 文件下载本地暂存目录（所有下载文件和日志保存至tg_out目录下）
out_dir =os.path.join(root_dir, 'tg_out')
if not (os.path.exists(out_dir)):
    os.mkdir(out_dir)
DOWNLOAD_CHAT_AVATAR_ROOT_DIR=os.path.join(out_dir, 'tg_download/avatar/chat/')
DOWNLOAD_USER_AVATAR_ROOT_DIR=os.path.join(out_dir, 'tg_download/avatar/user/')
DOWNLOAD_FILE_AVATAR_ROOT_DIR=os.path.join(out_dir, 'tg_download/file/')

# KAFKA信息（只需改动broker_ip）
KAFKA_BROKER_IP = ['127.0.0.1:9092']  # 根据kafka集群or单机配置相应ip
KAFKA_PRODUCER_ID = ''
KAFKA_PRODUCER_ACK = 'all'
KAFKA_PRODUCER_RETRY = 5
# master节点专属topic
KAFKA_TOPIC_MANAGE_JOIN = 'manage_join'  # 管理将要加入的群组发送至哪个节点进行采集
KAFKA_TOPIC_MANAGE_LEAVE = 'manage_leave'  # 管理将要停止采集的群组发送至相应节点进行退群
# --每个节点都有的topic--
KAFKA_TOPIC_JOIN = PHONENUM + '_join'
KAFKA_TOPIC_LEAVE = PHONENUM+'_leave'
KAFKA_TOPIC_MESSAGE_FILTER = PHONENUM+'_message_filter'
KAFKA_TOPIC_LINK_FILTER = PHONENUM+'_link_filter'
KAFKA_TOPIC_CHAT = PHONENUM + '_chat'
KAFKA_TOPIC_USER = PHONENUM + '_user'
KAFKA_TOPIC_CHAT_MEMBER = PHONENUM + '_chat_member'
# --history message topic--
KAFKA_TOPIC_HISTORY_INFO_GETTER=PHONENUM+'_history_info_getter'
# --download--
KAFKA_TOPIC_DOWNLOAD_AVATAR= PHONENUM + '_download_avatar'
# --Store kafka topic--
KAFKA_TOPIC_MESSAGE_STORE= PHONENUM + '_message_store'
KAFKA_TOPIC_CHAT_STORE= PHONENUM + '_chat_store'
KAFKA_TOPIC_USER_STORE= PHONENUM + '_user_store'
KAFKA_TOPIC_CHAT_MEMBER_STORE= PHONENUM + '_chat_member_store'
KAFKA_TOPIC_MEDIA_STORE= PHONENUM + '_media_store'
KAFKA_TOPIC_FIND_GROUP_STORE= PHONENUM + '_find_group_store'
KAFKA_TOPIC_PROXY_STORE= PHONENUM + '_proxy_store'
KAFKA_TOPIC_ONION_STORE= PHONENUM + '_onion_store'
KAFKA_TOPIC_NEED_DOWNLOAD_FILE_STORE = PHONENUM + '_need_download_file_store'

# kafka消费者组id（不用修改）
KAFKA_MANAGE_JOIN_GROUP_ID='1'
KAFKA_MANAGE_LEAVE_GROUP_ID='2'
KAFKA_JOIN_GROUP_ID='3'
KAFKA_LEAVE_GROUP_ID='4'
KAFKA_MASSAGE_FILTER_GROUP_ID="5"
KAFKA_LINK_FILTER_GROUP_ID="6"
KAFKA_CHAT_GROUP_ID= "7"
KAFKA_CHAT_MEMBER_GROUP_ID= "8"
KAFKA_DOWNLOAD_GROUP_ID='9'
# --history consumer kafka group id--
KAFKA_HISTORY_INFO_GETTER_GROUP_ID='11'
#--Store_consumer_kafka_group_id--
KAFKA_MESSAGE_STORE_GROUP_ID='14'
KAFKA_CHAT_STORE_GROUP_ID='15'
KAFKA_USER_STORE_GROUP_ID="16"
KAFKA_CHAT_MEMBER_STORE_GROUP_ID="17"
KAFKA_FIND_GROUP_STORE_GROUP_ID='18'
KAFKA_PROXY_STORE_GROUP_ID='19'
KAFKA_ONION_STORE_GROUP_ID='20'
KAFKA_NEED_DOWNLOAD_FILE_STORE_GROUP_ID='21'
KAFKA_MEDIA_STORAGE_GROUP_ID="22"

# kafka每个topic的session timeout
KAFKA_HISTORY_GETTER_SESSION_TIMEOUT_MS = 30000
KAFKA_HISTORY_SENDER_SESSION_TIMEOUT_MS = 30000
KAFKA_HISTORY_COLLECTOR_SESSION_TIMEOUT_MS = 30000
KAFKA_REPORT_CHAT_SESSION_TIMEOUT_MS = 30000
KAFKA_CHAT_COLLECTOR_SESSION_TIMEOUT_MS = 30000
KAFKA_CHAT_MEMBER_COLLECTOR_SESSION_TIMEOUT_MS = 30000
KAFKA_MESSAGE_FILTER_SESSION_TIMEOUT_MS = 30000
KAFKA_LINK_FILTER_SESSION_TIMEOUT_MS = 30000
KAFKA_JOIN_CHAT_SESSION_TIMEOUT_MS = 30000
KAFKA_LEAVE_CHAT_SESSION_TIMEOUT_MS = 30000
KAFKA_MANAGE_JOIN_CHAT_SESSION_TIMEOUT_MS = 30000
KAFKA_MANAGE_LEAVE_CHAT_SESSION_TIMEOUT_MS = 30000
KAFKA_STORE_SESSION_TIMEOUT_MS=30000

#--kafka_more_timeout_ms--
MORE_TIMEOUT=30000
#--kafka_consumer_request_timeout_ms
KAFKA_HISTORY_GETTER_REQUEST_TIMEOUT_MS = KAFKA_HISTORY_GETTER_SESSION_TIMEOUT_MS + MORE_TIMEOUT
KAFKA_HISTORY_SENDER_REQUEST_TIMEOUT_MS = KAFKA_HISTORY_SENDER_SESSION_TIMEOUT_MS + MORE_TIMEOUT
KAFKA_HISTORY_COLLECTOR_REQUEST_TIMEOUT_MS = KAFKA_HISTORY_COLLECTOR_SESSION_TIMEOUT_MS + MORE_TIMEOUT
KAFKA_REPORT_CHAT_REQUEST_TIMEOUT_MS = KAFKA_REPORT_CHAT_SESSION_TIMEOUT_MS + MORE_TIMEOUT
KAFKA_CHAT_COLLECTOR_REQUEST_TIMEOUT_MS = KAFKA_CHAT_COLLECTOR_SESSION_TIMEOUT_MS + MORE_TIMEOUT
KAFKA_CHAT_MEMBER_COLLECTOR_REQUEST_TIMEOUT_MS = KAFKA_CHAT_MEMBER_COLLECTOR_SESSION_TIMEOUT_MS + MORE_TIMEOUT
KAFKA_MESSAGE_FILTER_REQUEST_TIMEOUT_MS = KAFKA_MESSAGE_FILTER_SESSION_TIMEOUT_MS + MORE_TIMEOUT
KAFKA_LINK_FILTER_REQUEST_TIMEOUT_MS = KAFKA_LINK_FILTER_SESSION_TIMEOUT_MS + MORE_TIMEOUT
KAFKA_JOIN_CHAT_REQUEST_TIMEOUT_MS = KAFKA_JOIN_CHAT_SESSION_TIMEOUT_MS + MORE_TIMEOUT
KAFKA_LEAVE_CHAT_REQUEST_TIMEOUT_MS = KAFKA_LEAVE_CHAT_SESSION_TIMEOUT_MS + MORE_TIMEOUT
KAFKA_MANAGE_JOIN_CHAT_REQUEST_TIMEOUT_MS = KAFKA_MANAGE_JOIN_CHAT_SESSION_TIMEOUT_MS + MORE_TIMEOUT
KAFKA_MANAGE_LEAVE_CHAT_REQUEST_TIMEOUT_MS = KAFKA_MANAGE_LEAVE_CHAT_SESSION_TIMEOUT_MS + MORE_TIMEOUT
KAFKA_STORE_REQUEST_TIMEOUT_MS = KAFKA_STORE_SESSION_TIMEOUT_MS+MORE_TIMEOUT




# master节点可采集的最大群组数
max_chat_num_of_master_can_collect = 50
max_chat_num_of_slave_can_collect = 250




# session文件存放地址
session_path = self_config_dir
session_name = PHONENUM  # 以手机号作为session文件的前缀名
downloador_session_name = os.path.join(session_path, session_name + '_download')
session_name = os.path.join(session_path, session_name)
session_name_new_message = session_name + '_new_message'
session_name_chat_action = session_name + '_chat_action'
session_name_update_chat_member = session_name + '_update_chat_member'
session_name_download_avatar = session_name + '_download_avatar'
session_name_download_file = session_name + '_download_file'




# 每次启动session时等待的最大时间
time_out=60
# 启动失败重试次数
max_retry_time=5  # the times of retry




# 日志目录配置（也放在tg_out目录中）
log_file = os.path.join(out_dir, 'log')
if (not (os.path.exists(log_file))):
    os.mkdir(log_file)
if ROLE == "master":
    log_file_name='log_master_' + PHONENUM
elif ROLE == "slave":
    log_file_name = 'log_slave_' + PHONENUM
elif ROLE == "slave_verify":
    log_file_name = 'log_slave_verify_' + PHONENUM
else:
    raise Exception("config ROLE error")
log_file = os.path.join(log_file, log_file_name)
if not (os.path.exists(log_file)):
    os.mkdir(log_file)


#test
logger_folder=log_file
logger_name='telegram'
logger_name_erro='erro_log'
logger_name_all='all_log'
logger_name_join_hisory='join_hisory'
logger_name_file_join_hisory='join_hisory'
logger_name_slave_report='slave_report'
logger_name_file_slave_report='slave_report'