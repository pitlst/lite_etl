import os
import toml
import multiprocessing as mp
# 数据库连接配置
CONNECT_CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "connect.toml"))
CONNECT_CONFIG = mp.Value("connect_config", CONNECT_CONFIG)
# 配置文件对应的路径
SELECT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "select")
SELECT_PATH = mp.Value(SELECT_PATH)
TABLE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "file")
TABLE_PATH = mp.Value(TABLE_PATH)
# 全局单例的日志队列
from utils.logger import LOGGER_QUEUE
# 数据库连接获取
from utils.connect import create_client