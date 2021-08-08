from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

es = Elasticsearch(["localhost:9200"])
body = {
    "query": {
        "match_all": {}
    }
}
message_body = {
    "query": {
        "range": {
          "insert_date": {
            "gte": "2021-01-01 00:00:00"
          }
        }
    }
}

def user_to_transfer():
    result = es.search(
        index="user",
        scroll="200m",
        timeout="1m",
        size=1000,
        body=body
    )
    i = 0
    actions = []
    for item in result.get("hits").get("hits"):
        i = i + 1
        _id = item["_id"]
        _source = item["_source"]
        _source["user_id"] = _source.pop("id")
        actions.append({
            "_id": _id,
            "_source": _source
        })
    bulk(client=es, actions=actions, index="user_transfer")
    print(i)
    scroll_id = result["_scroll_id"]
    while result["_scroll_id"] and (len(result["hits"]["hits"]) > 0):
        result = es.scroll(
            scroll_id=scroll_id,
            scroll='5m'
        )
        actions = []
        for item in result.get("hits").get("hits"):
            i = i + 1
            _id = item["_id"]
            _source = item["_source"]
            _source["user_id"] = _source.pop("id")
            actions.append({
                "_id": _id,
                "_source": _source
            })
        bulk(client=es, actions=actions, index="user_transfer")
        print(i)

def chat_to_transfer():
    result = es.search(
        index="chat",
        scroll="5m",
        timeout="1m",
        size=1000,
        body=body
    )
    i = 0
    actions = []
    for item in result.get("hits").get("hits"):
        i = i + 1
        _id = item["_id"]
        _source = item["_source"]
        _source["chat_id"] = _source.pop("id")
        actions.append({
            "_id": _id,
            "_source": _source
        })
    bulk(client=es, actions=actions, index="chat_transfer")
    print(i)
    scroll_id = result["_scroll_id"]
    while result["_scroll_id"] and (len(result["hits"]["hits"]) > 0):
        result = es.scroll(
            scroll_id=scroll_id,
            scroll='5m'
        )
        actions = []
        for item in result.get("hits").get("hits"):
            i = i + 1
            _id = item["_id"]
            _source = item["_source"]
            _source["chat_id"] = _source.pop("id")
            actions.append({
                "_id": _id,
                "_source": _source
            })
        bulk(client=es, actions=actions, index="chat_transfer")
        print(i)

# 索引名称和索引设置改变，具体数据不变
def transfer(old_index, new_index, body):
    result = es.search(
        index=old_index,
        scroll="300m",
        timeout="1m",
        size=1000,
        body=body
    )
    i = 0
    actions = []
    for item in result.get("hits").get("hits"):
        i = i + 1
        _id = item["_id"]
        _source = item["_source"]
        actions.append({
            "_id": _id,
            "_source": _source
        })
    bulk(client=es, actions=actions, index=new_index)
    print(i)
    scroll_id = result["_scroll_id"]
    while result["_scroll_id"] and (len(result["hits"]["hits"]) > 0):
        result = es.scroll(
            scroll_id=scroll_id,
            scroll='5m'
        )
        actions = []
        for item in result.get("hits").get("hits"):
            i = i + 1
            _id = item["_id"]
            _source = item["_source"]
            actions.append({
                "_id": _id,
                "_source": _source
            })
        bulk(client=es, actions=actions, index=new_index)
        print(i)


if __name__ == "__main__":

    # 转移至中间索引
    transfer("user", "user_transfer", body)
    # transfer("chat", "chat_transfer", body)
    # transfer("chat_member", "chat_member_transfer", body)
    # transfer("media", "media_transfer", body)
    # transfer("need_download_file", "need_download_file_transfer", body)
    # transfer("message", "message_transfer", message_body)

    # 中间索引转移至正式索引
    # transfer("user_transfer", "user", body)
    # transfer("chat_transfer", "chat", body)
    # transfer("chat_member_transfer", "chat_member", body)
    # transfer("media_transfer", "media", body)
    # transfer("need_download_file_transfer", "need_download_file", body)
    # transfer("message_transfer", "message", message_body)