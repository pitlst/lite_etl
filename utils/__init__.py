import os
import toml
import multiprocessing as mp
# 数据库连接配置
CONNECT_CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "connect.toml"))
# 配置文件对应的路径
SELECT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "select")
TABLE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "file")

from utils.logger import make_logger
from utils.connect import EXECUTER