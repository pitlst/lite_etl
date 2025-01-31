import os
import toml
from dataclasses import dataclass

@dataclass(frozen=True)
class config:
    '''全局所有的配置'''
    # 资源文件对应的路径
    SOURCE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source")
    # 是否在测试环境
    DEBUG = True
    # 配置文件
    if DEBUG:
        CONNECT_CONFIG = toml.load(os.path.join(SOURCE_PATH, "debug_connect.toml"))
    else:
        CONNECT_CONFIG = toml.load(os.path.join(SOURCE_PATH, "connect.toml"))
    # 配置文件对应的路径
    LOCAL_DB_PATH = os.path.join(SOURCE_PATH, "database")
    SELECT_PATH = os.path.join(SOURCE_PATH, "select")
    TABLE_PATH = os.path.join(SOURCE_PATH, "file")

    
CONFIG = config()