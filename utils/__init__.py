import os
import toml
import multiprocessing as mp
# 数据库连接配置
CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source", "config.toml"))
connect_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source")
CONNECT_CONFIG = toml.load(os.path.join(connect_path, "debug_connect.toml")) if CONFIG["debug"] else toml.load(os.path.join(connect_path, "connect.toml"))
# 配置文件对应的路径
SELECT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source", "select")
TABLE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source", "file")

from utils.connect import executer
EXECUTER = executer()
from utils.logger import make_logger