import datetime
import logging
import colorlog
from sqlglot import exp
from utils import EXECUTER

class clickhouse_handler(logging.Handler):
    '''将日志写道mongo数据库的自定义handler'''
    def __init__(self, name: str) -> None:
        logging.Handler.__init__(self)
        self.name = name
        self.columns = ["log_time", "level", "msg"]
        self.client = EXECUTER.get_client("clickhouse日志")
        # 检查表是否存在

    def emit(self, record) -> None:
        try:
            msg = self.format(record)
            temp_msg = msg.split(":")
            level = temp_msg[0]
            msg = ":".join(temp_msg[1:])
            data = {"log_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "level": level, "msg": msg}
            values = [exp.Literal.number(data[col]) if isinstance(data[col], int) else exp.Literal.string(data[col]) for col in self.columns]
            insert = exp.Insert(
                this=exp.Table(this=self.name),
                expressions=[exp.Tuple(expressions=values)],
                columns=[exp.Identifier(this=col) for col in self.columns]
            )
            with self.client.connect() as connection:
                connection.execute(insert.sql())
        except Exception:
            self.handleError(record)


def make_logger(logger_name: str)-> logging.Logger:
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
    mongoio = clickhouse_handler(logger_name)
    mongoio.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s:%(message)s')
    mongoio.setFormatter(formatter)
    temp_log.addHandler(mongoio)
    return temp_log
            
        