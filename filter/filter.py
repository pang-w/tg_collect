
import json
import time
import configuration

def filter_chat(chat, producer):
    # print("过滤前的群组信息为：", chat)
    save_chat = {}
    save_chat["chat_id"] = chat["id"]
    save_chat["type"] = chat["type"]
    if "is_verified" in chat: # 如果有，则获取，没有就不处理
        save_chat["is_verified"] = chat["is_verified"]
    if "is_restricted" in chat:
        save_chat["is_restricted"] = chat["is_restricted"]
    if "is_scam" in chat:
        save_chat["is_scam"] = chat["is_scam"]
    if "is_support" in chat:
        save_chat["is_support"] = chat["is_support"]
    if "title" in chat:
        save_chat["title"] = chat["title"]
    if "username" in chat:
        save_chat["username"] = chat["username"]
    if "first_name" in chat:
        save_chat["first_name"] = chat["first_name"]
    if "last_name" in chat:
        save_chat["last_name"] = chat["last_name"]
    if "photo" in chat:
        photo_data = {}
        photo_data["type"] = "chat_avatar"
        photo_data["chat_id"] = chat["id"]
        photo_data["file_id"] = chat["photo"]["big_file_id"]
        save_chat["photo"] = chat["photo"]["big_file_id"]
        # producer.sendMsg(topic=configuration.KAFKA_TOPIC_DOWNLOAD, value=str(json.dumps(photo_data)))
        producer.sendMsg(topic=configuration.KAFKA_TOPIC_DOWNLOAD_AVATAR, value=str(json.dumps(photo_data)))
    if "bio" in chat:
        save_chat["bio"] = chat["bio"]
    if "description" in chat:
        save_chat["description"] = chat["description"]
    if "invite_link" in chat:
        save_chat["invite_link"] = chat["invite_link"]
    if "pinned_message_id" in chat:
        save_chat["pinned_message_id"] = chat["pinned_message_id"]["message_id"]
    if "members_count" in chat:
        save_chat["members_count"] = chat["members_count"]
    if "restrictions" in chat:
        save_chat["restriction_reason"] = ""
        reasons = []
        for restriction in chat["restrictions"]:
            reasons.append("platform:" + restriction["platform"] + "，reason:" + restriction["reason"] + ", text:" + restriction["text"])
        save_chat["restriction_reason"] = "；".join(reasons)
    if "linked_chat" in chat:
        save_chat["linked_chat_id"] = chat["linked_chat"]["id"]
    # 记录更新时间
    save_chat["update_time"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    return save_chat

def filter_user(user, producer, es):
    save_user = {}
    save_user["user_id"] = user["id"]
    if "is_deleted" in user: # 如果有，则获取，没有就不处理
        save_user["is_deleted"] = user["is_deleted"]
    if "is_bot" in user:
        save_user["is_bot"] = user["is_bot"]
    if "is_verified" in user:
        save_user["is_verified"] = user["is_verified"]
    if "is_restricted" in user:
        save_user["is_restricted"] = user["is_restricted"]
    if "is_scam" in user:
        save_user["is_scam"] = user["is_scam"]
    if "is_support" in user:
        save_user["is_support"] = user["is_support"]
    # 当用户被删除则first_name为空
    if "first_name" in user:
        save_user["first_name"] = user["first_name"]
    # 当用户被删除则last_name为空
    if "last_name" in user:
        save_user["last_name"] = user["last_name"]
    if "status" in user:
        save_user["status"] = user["status"]
    # 当用户被删除则用户名为空
    if "username" in user:
        save_user["username"] = user["username"]
    if "phone_number" in user:
        save_user["phone_number"] = user["phone_number"]
    if "photo" in user:
        save_user["photo"] = user["photo"]["big_file_id"]
        if not has_file(es, user["photo"]["big_file_id"]):
            photo_data = {}
            photo_data["type"] = "user_avatar"
            photo_data["user_id"] = user["id"]
            photo_data["file_id"] = user["photo"]["big_file_id"]
            producer.sendMsg(topic=configuration.KAFKA_TOPIC_DOWNLOAD_AVATAR, value=str(json.dumps(photo_data)))
    if "restrictions" in user:
        reasons = []
        for restriction in user["restrictions"]:
            reasons.append("platform:" + restriction["platform"] + "，reason:" + restriction["reason"] + ", text:" + restriction["text"])
        save_user["restriction_reason"] = "；".join(reasons)

    # 更新时间
    save_user["update_time"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    return save_user

def filter_chat_member(member, chat_id):
    del member["_"]
    member["chat_id"] = chat_id
    member["user_id"] = member["user"]["id"]
    member["left"] = False
    del member["user"]
    if "invited_by" in member:
        member["invited_by_id"] = member["invited_by"]["id"]
        del member["invited_by"]
    if "promoted_by" in member:
        member["promoted_by_id"] = member["promoted_by"]["id"]
        del member["promoted_by"]
    if "restricted_by" in member:
        member["restricted_by_id"] = member["restricted_by"]["id"]
        del member["restricted_by"]

    # 删除一些无用的权限字段
    if "can_be_edited" in member: del member["can_be_edited"]
    if "can_post_messages" in member: del member["can_post_messages"]
    if "can_edit_messages" in member: del member["can_edit_messages"]
    if "can_delete_messages" in member: del member["can_delete_messages"]
    if "can_restrict_members" in member: del member["can_restrict_members"]
    if "can_promote_members" in member: del member["can_promote_members"]
    if "can_change_info" in member: del member["can_change_info"]
    if "can_invite_users" in member: del member["can_invite_users"]
    if "can_pin_messages" in member: del member["can_pin_messages"]
    if "can_send_messages" in member: del member["can_send_messages"]
    if "can_send_media_messages" in member: del member["can_send_media_messages"]
    if "can_send_stickers" in member: del member["can_send_stickers"]
    if "can_send_animations" in member: del member["can_send_animations"]
    if "can_send_games" in member: del member["can_send_games"]
    if "can_use_inline_bots" in member: del member["can_use_inline_bots"]
    if "can_add_web_page_previews" in member: del member["can_add_web_page_previews"]
    if "can_send_polls" in member: del member["can_send_polls"]

    # 记录更新时间
    member["update_time"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    return member


# 判断一条消息是否需要下载
def need_to_download(message):
    if "media" in message and message["media"]:
        if "document" in message:
            dd = {'application/x-executable', "image/webp", "application/x-ms-dos-executable",
                  'application/x-apple-diskimage', 'application/x-newton-compatible-pkg', 'application/x-executable',
                  'application/x-tdesktop-theme', 'application/x-bad-tgsticker', 'application/x-tgsticker'}
            document = message["document"]
            if "mime_type" in document and document["mime_type"] in dd:
                return False
            return True
        if "photo" in message:
            return True
        if "audio" in message:
            return True
        if "video" in message:
            return True
        if "voice" in message:
            return True
        return False
    else:
        return False


# 查询此文件是否已存在
def has_file(es, file_id):
    try:
        for e in es:
            result = e.search(index=configuration.ES_INDEX_MEDIA, body={"query": {"match": {"file_id": file_id}}}, request_timeout=60)
            if result["hits"]["total"]["value"] == 0:
                return False
    except:
        return False
    return True