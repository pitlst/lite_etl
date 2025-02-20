import logging
import colorlog
from utils.connect import CONNECTER

class duckdb_handler(logging.Handler):
    """将日志写到本地duckDB数据库的自定义handler"""
    columns = ["log_time", "level", "msg"]
    def __init__(self, name: str) -> None:
        logging.Handler.__init__(self)
        self.name = name
        self.cursor = CONNECTER.get_logger()
        # 指定创建时间为默认时间戳，id自动生成
        self.cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{self.name}" (
                {self.columns[0]} TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
                {self.columns[1]} VARCHAR, 
                {self.columns[2]} VARCHAR
            )
            '''
        )
        
    def __del__(self):
        self.cursor.close()

    def emit(self, record) -> None:
        try:
            temp_msg = self.format(record).split(":")
            level = temp_msg[0]
            msg = ":".join(temp_msg[1:])
            self.cursor.execute(f"INSERT INTO \"{self.name}\" ({self.columns[1]}, {self.columns[2]}) VALUES (?, ?)", [level, msg])
        except Exception:
            self.handleError(record)


def make_logger(logger_name: str)-> logging.Logger:
    """生成日志的工厂方法"""
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
    mongoio = duckdb_handler(logger_name)
    mongoio.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s:%(message)s')
    mongoio.setFormatter(formatter)
    temp_log.addHandler(mongoio)
    return temp_log
            
        