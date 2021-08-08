# -*- coding:utf-8 -*-
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__)) + "/..")
import time
from utils import util
import traceback

# 查看进程是否正在运行
def check_process_state(process_name):
    cmd = "ps -ef | grep %s | grep -v grep" % process_name
    res = os.system(cmd)
    return res == 0

def kill_process(process_name):
    os.popen("ps -ef |grep %s|grep -v grep|awk '{ print $2 }'|xargs kill -9" % process_name)
    time.sleep(1)

def restart_download_file():
    path = "/root/tg_collect/task/download_file.py"
    os.system("nohup python %s >> /dev/null &" % path)
    logger.debug('download_file process restart done')

def restart_download_avatar():
    path = "/root/tg_collect/task/download_avatar.py"
    os.system("nohup python %s >> /dev/null &" % path)
    logger.debug('download_avatar process restart done')

# 每小时执行一次，进行检测文件下载和头像下载进程是否正常，不正常则重启
if __name__ == '__main__':
    logger = util.init_other_logger('cron_monitor_download', 'cron_monitor_download')
    logger.debug("run")
    try:
        state_download_file = check_process_state("download_file")
        state_download_avatar = check_process_state("download_avatar")
        if not state_download_file:
            log_info = 'download_file process is stopped, restart'
            logger.debug(log_info)
            kill_process("download_file")
            restart_download_file()

        if not state_download_avatar:
            log_info = 'download_avatar process is stopped, restart'
            logger.debug(log_info)
            kill_process("download_avatar")
            restart_download_avatar()

    except Exception:
        logger.error(traceback.format_exc())