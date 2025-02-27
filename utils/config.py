import os
import toml
from dataclasses import dataclass

@dataclass(frozen=True)
class config:
    """全局所有的配置"""
    # 是否在测试环境
    DEBUG = False
    # 同步配置的间隔时长
    INTERVAL_DURATION: float = 5 * 60
    # 资源文件对应的路径
    SOURCE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source")
    # 相关文件对应的路径
    SELECT_PATH = os.path.join(SOURCE_PATH, "sql")
    TABLE_PATH = os.path.join(SOURCE_PATH, "file")

CONFIG = config()