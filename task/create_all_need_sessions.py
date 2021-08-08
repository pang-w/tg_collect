# -*- coding:utf-8 -*-

import sys
sys.path.append("..")
import configuration
from pyrogram import Client

if __name__ == "__main__":
    if (configuration.ip == None):
        client_new_message = Client(configuration.session_name_new_message, api_id=configuration.api_id, api_hash=configuration.api_hash)
        client_chat_action = Client(configuration.session_name_chat_action, api_id=configuration.api_id, api_hash=configuration.api_hash)
        client_download_avatar = Client(configuration.session_name_download_avatar, api_id=configuration.api_id, api_hash=configuration.api_hash)
        client_download_file = Client(configuration.session_name_download_file, api_id=configuration.api_id, api_hash=configuration.api_hash)
    else:
        client_new_message = Client(configuration.session_name_new_message, api_id=configuration.api_id, api_hash=configuration.api_hash, proxy=dict(hostname=configuration.ip, port=configuration.port))
        client_chat_action = Client(configuration.session_name_chat_action, api_id=configuration.api_id, api_hash=configuration.api_hash, proxy=dict(hostname=configuration.ip, port=configuration.port))
        client_download_avatar = Client(configuration.session_name_download_avatar, api_id=configuration.api_id, api_hash=configuration.api_hash, proxy=dict(hostname=configuration.ip, port=configuration.port))
        client_download_file = Client(configuration.session_name_download_file, api_id=configuration.api_id, api_hash=configuration.api_hash, proxy=dict(hostname=configuration.ip, port=configuration.port))

    print("输入手机号'{}'，然后依次输入获取的code！".format(configuration.PHONECTR + " " + configuration.PHONENUM))

    with client_new_message:
        print("create client_new_message successfully!")
    with client_chat_action:
        print("create client_chat_action successfully!")
    with client_download_avatar:
        print("create client_download_avatar successfully!")
    with client_download_file:
        print("create client_download_file successfully!")
