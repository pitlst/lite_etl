import datetime
import logging
import colorlog
import os
import toml
import pymongo
from urllib.parse import quote_plus

# ------------------------------------------数据库连接配置------------------------------------------
CONNECT_CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "connect.toml"))
# ------------------------------------------mongo数据库连接------------------------------------------
MONGO_CONNECT_CONFIG = CONNECT_CONFIG["数据处理服务存储"]
if MONGO_CONNECT_CONFIG["user"] != "" and MONGO_CONNECT_CONFIG["password"] != "":
    url = "mongodb://" + MONGO_CONNECT_CONFIG["user"] + ":" + quote_plus(MONGO_CONNECT_CONFIG["password"]) + "@" + MONGO_CONNECT_CONFIG["ip"] + ":" + str(MONGO_CONNECT_CONFIG["port"])
else:
    url = "mongodb://" + MONGO_CONNECT_CONFIG["ip"] + ":" + str(MONGO_CONNECT_CONFIG["port"])
MONGO_CLIENT = pymongo.MongoClient(url)
# ------------------------------------------日志配置------------------------------------------

class momgo_handler(logging.Handler):
    def __init__(self) -> None:
        logging.Handler.__init__(self)
        database = MONGO_CLIENT["logger"]
        time_series_options = {
            "timeField": "timestamp",
            "metaField": "message"
        }
        if "sync" not in database.list_collection_names():
            self.collection = database.create_collection("sync", timeseries=time_series_options, expireAfterSeconds=604800)
        else:
            self.collection = database["sync"]

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

LOG = logging.getLogger("sync")
LOG.setLevel(logging.DEBUG)
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
LOG.addHandler(console)
mongoio = momgo_handler()
mongoio.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s:%(message)s')
mongoio.setFormatter(formatter)
LOG.addHandler(mongoio)
