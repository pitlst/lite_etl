'''一些常用的方法'''

'''引入的类和全局变量'''
import os
import toml
# 数据库连接配置
source_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source")
CONFIG = toml.load(os.path.join(source_path, "config.toml"))
CONNECT_CONFIG = toml.load(os.path.join(source_path, "connect.toml"))
# 配置文件对应的路径
LOCAL_DB_PATH = os.path.join(source_path, "database")
SELECT_PATH = os.path.join(source_path, "select")
TABLE_PATH = os.path.join(source_path, "file")
# 数据源
from utils.connect import executer, local_db
EXECUTER = executer()
# 日志的工厂方法
from utils.logger import make_logger