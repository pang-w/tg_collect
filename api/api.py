# coding=utf-8
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__)) + "/..")
import configuration
from flask import Flask
from flask import request,jsonify

api = Flask(__name__)
api.config['JSON_AS_ASCII'] = False

@api.before_request
def intercept():
    error_msg = jsonify({"status": 0, "msg": "secret error"})
    try:
        json = request.get_json()
        secret = json["secret"]
    except Exception:
        return error_msg
    if secret != "iie!@#$%":
        return error_msg

@api.route("/",methods=["POST"])
def index():
    return '<h1>Home</h1>'

@api.route("/addgroup",methods=["POST"])
def addgroup():
    return '添加群组进采集系统'

@api.route("/leavegroup",methods=["POST"])
def leavegroup():
    return '退出采集系统'

@api.route("/addnode",methods=["POST"])
def addnode():
    return '增加节点'

if __name__ == "__main__":
    if configuration.ROLE == "master":
        api.run(port=configuration.SERVER_PORT)