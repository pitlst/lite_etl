import datetime
import logging
import colorlog
from pymongo.database import Database
from utils import EXECUTER

class momgo_handler(logging.Handler):
    '''将日志写道mongo数据库的自定义handler'''
    async def __init__(self, name: str) -> None:
        logging.Handler.__init__(self)
        database: Database = await EXECUTER.get_client(["数据处理服务存储"])["logger"]
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


async def make_logger(logger_name: str)-> logging.Logger:
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
    mongoio = momgo_handler(logger_name)
    mongoio.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s:%(message)s')
    mongoio.setFormatter(formatter)
    temp_log.addHandler(mongoio)
    return temp_log
            
        