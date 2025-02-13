import os
from typing import Any
import duckdb
import pymongo
import sqlalchemy
import threading
from urllib.parse import quote_plus
from utils.config import CONFIG


class connecter:
    _instance = None
    _sql_connect: dict[str, sqlalchemy.Engine] = {}
    _nosql_connect: dict[str, pymongo.MongoClient] = {}
    _lock = threading.Lock()
    __slot__ = ["_instance", "_lock", "_sql_connect", "_nosql_connect"]

    def __init__(self) -> None:
        self.make_client(CONFIG.CONNECT_CONFIG)

    def __new__(cls, *args, **kwargs):
        '''基于锁的多线程安全单例'''
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(connecter, cls).__new__(cls)
        return cls._instance

    def make_client(self, connect_config: dict[str, Any]) -> None:
        '''
        创建数据连接的工厂类
        用于在对应进程中创建对应进程的数据库连接
        '''
        for ch in connect_config:
            temp = connect_config[ch]
            if temp["type"] == "oracle":
                connect_str = "oracle+cx_oracle://" + \
                    temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + \
                    str(temp["port"]) + "/?service_name=" + temp["database"]
                self._sql_connect[ch] = sqlalchemy.create_engine(
                    connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            elif temp["type"] == "sqlserver":
                connect_str = "mssql+pyodbc://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(
                    temp["port"]) + "/" + temp["database"] + "?driver=ODBC+Driver+17+for+SQL+Server"
                self._sql_connect[ch] = sqlalchemy.create_engine(
                    connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            elif temp["type"] == "mysql":
                connect_str = "mysql+mysqldb://" + temp["user"] + ":" + \
                    quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["database"]
                self._sql_connect[ch] = sqlalchemy.create_engine(
                    connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            elif temp["type"] == "pgsql":
                connect_str = "postgresql://" + temp["user"] + ":" + \
                    quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["database"]
                self._sql_connect[ch] = sqlalchemy.create_engine(
                    connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            elif temp["type"] == "mongo":
                if temp["user"] != "" and temp["password"] != "":
                    url: str = "mongodb://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"])
                else:
                    url: str = "mongodb://" + temp["ip"] + ":" + str(temp["port"])
                self._nosql_connect[ch] = pymongo.MongoClient(url)
            else:
                raise ValueError("不支持的数据库类型：" + temp["type"])

    def get_sql(self, key: str) -> sqlalchemy.engine.Engine:
        if key not in self._sql_connect.keys():
            raise ValueError("不存在对应的连接")
        with self._lock:
            return self._sql_connect[key]

    def get_nosql(self, key: str) -> pymongo.MongoClient:
        if key not in self._nosql_connect.keys():
            raise ValueError("不存在对应的连接")
        with self._lock:
            return self._nosql_connect[key]


CONNECTER = connecter()
LOCALDB = duckdb.connect(os.path.realpath(os.path.join(CONFIG.LOCAL_DB_PATH, "data.db")))
