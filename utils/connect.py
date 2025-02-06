import os
import duckdb
import pymongo
import sqlalchemy
import threading
from urllib.parse import quote_plus
from utils import CONFIG

class connecter:
    _instance = None
    _connect: dict[str, sqlalchemy.Engine | pymongo.MongoClient | None] = {}
    _lock = threading.Lock()
    __slot__ = ["_instance", "_lock", "connect"]
    
    def __init__(self) -> None:
        self.make_client(CONFIG.CONNECT_CONFIG)
     
    def __new__(cls, *args, **kwargs):
        '''基于锁的多线程安全单例'''
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(connecter, cls).__new__(cls)
        return cls._instance

    def make_client(self, connect_config: str) -> None:
        '''
        创建数据连接的工厂类
        用于在对应进程中创建对应进程的数据库连接
        '''
        for ch in connect_config:
            temp = connect_config[ch]
            if temp["type"] == "oracle":
                connect_str = "oracle+cx_oracle://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/?service_name=" + temp["database"]
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            elif temp["type"] == "sqlserver":
                connect_str = "mssql+pyodbc://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["database"] + "?driver=ODBC+Driver+17+for+SQL+Server"
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            elif temp["type"] == "mysql":
                connect_str = "mysql+mysqldb://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["database"]
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            elif temp["type"] == "pgsql":
                connect_str = "postgresql://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["database"]
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            elif temp["type"] == "mongo":
                if temp["user"] != "" and temp["password"] != "":
                    url = "mongodb://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"])
                else:
                    url = "mongodb://" + temp["ip"] + ":" + str(temp["port"])
                self._connect[ch] = pymongo.MongoClient(url)
            else:
                raise ValueError("不支持的数据库类型：" + temp["type"])
                
    def __getitem__(self, key):
        return self._connect[key]

    def __setitem__(self, key, value):
        if self._instance:
            with self._lock:
                self._connect[key] = value

    def __delitem__(self, key):
        if self._instance:
            with self._lock:
                del self._connect[key]

CONNECTER = connecter()
LOCALDB = duckdb.connect(os.path.join(CONFIG.LOCAL_DB_PATH, "data.db"))