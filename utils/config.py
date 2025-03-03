import os
import toml
from dataclasses import dataclass

@dataclass(frozen=True)
class config:
    """全局所有的配置"""
    # 资源文件对应的路径
    SOURCE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source")
    # 是否在测试环境
    DEBUG = False
    # 配置文件
    CONNECT_CONFIG = toml.load(os.path.join(SOURCE_PATH, "debug_connect.toml")) if DEBUG else toml.load(os.path.join(SOURCE_PATH, "connect.toml"))
    # 同步配置的间隔时长
    INTERVAL_DURATION: float = 5 * 60
    # 配置文件对应的路径
    LOCAL_DB_PATH = os.path.join(SOURCE_PATH, "database")
    SELECT_PATH = os.path.join(SOURCE_PATH, "select")
    TABLE_PATH = os.path.join(SOURCE_PATH, "file")
    # 对应看板前端开启的IP和端口
    BI_HOST = "127.0.0.1" if DEBUG else "0.0.0.0"
    BI_PORT = 10086
    BACK_HOST = "127.0.0.1" if DEBUG else "0.0.0.0"
    BACK_PORT = 10086

CONFIG = config()