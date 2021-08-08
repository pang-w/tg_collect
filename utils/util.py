# -*- coding:utf-8 -*-
import os
import logging
import logging.handlers
import configuration
from elasticsearch.exceptions import ConnectionTimeout,NotFoundError
from elasticsearch import Elasticsearch
import time
import base64
import json
from datetime import datetime
logger=logging.getLogger(configuration.logger_name)

def init_logger():
    logger = logging.getLogger(configuration.logger_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(module)s line-%(lineno)d %(funcName)s %(message)s')

    #common log file
    file_handler_all = logging.handlers.RotatingFileHandler(filename="%s/%s.log"%(configuration.logger_folder, configuration.logger_name_all), maxBytes=10 * 1024 * 1024, backupCount=10)
    file_handler_all.setFormatter(formatter)
    file_handler_all.setLevel(logging.DEBUG)
    logger.addHandler(file_handler_all)

    # erro log file
    file_handler_erro = logging.handlers.RotatingFileHandler(filename="%s/%s.log" % (configuration.logger_folder, configuration.logger_name_erro), maxBytes=10 * 1024 * 1024, backupCount=10)
    file_handler_erro.setFormatter(formatter)
    file_handler_erro.setLevel(logging.WARNING)
    logger.addHandler(file_handler_erro)

    #consle
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)

    logger.debug("logger init done")
    return logger

def init_other_logger(logger_name, logger_file_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(module)s line-%(lineno)d %(funcName)s %(message)s')

    #common log file
    file_handler_all = logging.handlers.RotatingFileHandler(filename="%s/%s.log"%(configuration.logger_folder, logger_file_name), maxBytes=10 * 1024 * 1024, backupCount=10)
    file_handler_all.setFormatter(formatter)
    file_handler_all.setLevel(logging.DEBUG)
    logger.addHandler(file_handler_all)

    # erro log file
    file_handler_erro = logging.handlers.RotatingFileHandler(filename="%s/%s.log" % (configuration.logger_folder, configuration.logger_name_erro), maxBytes=10 * 1024 * 1024, backupCount=10)
    file_handler_erro.setFormatter(formatter)
    file_handler_erro.setLevel(logging.WARNING)
    logger.addHandler(file_handler_erro)

    #consle
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)

    logger.debug("logger init done")
    return logger

def init_only_file_logger(logger_name, logger_file_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(module)s line-%(lineno)d %(funcName)s %(message)s')

    #common log file
    file_handler_all = logging.handlers.RotatingFileHandler(filename="%s/%s.log"%(configuration.logger_folder, logger_file_name), maxBytes=10 * 1024 * 1024, backupCount=10)
    file_handler_all.setFormatter(formatter)
    file_handler_all.setLevel(logging.DEBUG)
    logger.addHandler(file_handler_all)

    logger.debug("logger init done")
    return logger
