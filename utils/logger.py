import datetime
import logging
import colorlog
import pymongo
import multiprocessing as mp
from urllib.parse import quote_plus
# 进程共享变量
from utils import CONNECT_CONFIG

# 所有数据都从这个队列传入日志进程
LOGGER_QUEUE = mp.Queue()

class momgo_handler(logging.Handler):
    '''将日志写道mongo数据库的自定义handler'''
    def __init__(self, mongo_clint: pymongo.MongoClient, name: str) -> None:
        logging.Handler.__init__(self)
        database = mongo_clint["logger"]
        time_series_options = {
            "timeField": "timestamp",
            "metaField": "message"
        }
        if name not in database.list_collection_names():
            self.collection = database.create_collection(name, timeseries=time_series_options, expireAfterSeconds=604800)
        else:
            self.collection = database[name]

    def emit(self, record) -> None:
        try:
            msg = self.format(record)
            temp_msg = msg.split(":")
            level = temp_msg[0]
            msg = ":".join(temp_msg[1:])
            self.collection.insert_one({
                "timestamp": datetime.datetime.now(),
                "message": {
                    "等级": level,
                    "消息": msg
                }
            })
        except Exception:
            self.handleError(record)


def make_logger(mongo_clint: pymongo.MongoClient, logger_name: str)-> logging.Logger:
    '''生成日志的工厂方法'''
    temp_log = logging.getLogger(logger_name)
    temp_log.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(
        colorlog.ColoredFormatter(
            '%(log_color)s%(levelname)s: %(asctime)s %(message)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            },
            datefmt='## %Y-%m-%d %H:%M:%S'
        ))
    temp_log.addHandler(console)
    mongoio = momgo_handler(mongo_clint, logger_name)
    mongoio.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s:%(message)s')
    mongoio.setFormatter(formatter)
    temp_log.addHandler(mongoio)
    return temp_log


def logger_main():
    '''日志进程运行的主函数'''
    # 初始化数据连接
    mongo_config = CONNECT_CONFIG.value["数据处理服务存储"]
    if mongo_config["user"] != "" and mongo_config["password"] != "":
        url = "mongodb://" + mongo_config["user"] + ":" + quote_plus(mongo_config["password"]) + "@" + mongo_config["ip"] + ":" + str(mongo_config["port"])
    else:
        url = "mongodb://" + mongo_config["ip"] + ":" + str(mongo_config["port"])
    mongo_client = pymongo.MongoClient(url)
    # 初始化日志
    all_logger: dict[str, logging.Logger] = {}
    all_logger["root"] = make_logger(mongo_client, "root")
    for node_name in CONNECT_CONFIG.value:
        all_logger[node_name] = make_logger(mongo_client, node_name)
    # 开始读取队列信息打印
    while True:
        res = LOGGER_QUEUE.get(True)
        try:
            if res["type"] == "debug":
                all_logger[res["name"]].debug(res["msg"])
            elif res["type"] == "info":
                all_logger[res["name"]].info(res["msg"])
            elif res["type"] == "warning":
                all_logger[res["name"]].warning(res["msg"])
            elif res["type"] == "error":
                all_logger[res["name"]].error(res["msg"])
            elif res["type"] == "critical":
                all_logger[res["name"]].critical(res["msg"])
            else:
                all_logger["root"].warning("对应的日志信息类型不正确，无法打印")
                continue
        except:
            pass
        finally:
            all_logger["root"].warning("对应的日志信息类型不正确，无法打印")
            continue
            
        