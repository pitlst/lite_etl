import os
import time
import redis
import duckdb
import pymongo
import sqlalchemy
from urllib.parse import quote_plus
from utils import CONNECT_CONFIG, LOCAL_DB_PATH

class executer:
    def __init__(self) -> None:
        self.connect: dict[str, sqlalchemy.Engine | pymongo.MongoClient | redis.Redis | None] = {}
        for ch in CONNECT_CONFIG:
            self.connect[ch] = self.make_client(ch)
        self.reconnect_time = time.time()

    @staticmethod
    def make_client(name: str):
        '''
        创建数据连接的工厂类
        用于在对应进程中创建对应进程的数据库连接
        '''
        assert name in CONNECT_CONFIG.keys(), "连接要求的名称不在数据库连接的配置文件中"
        for ch in CONNECT_CONFIG:
            if ch == name:
                temp = CONNECT_CONFIG[ch]
                if temp["type"] == "oracle":
                    connect_str = "oracle+cx_oracle://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/?service_name=" + temp["database"]
                    return sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
                elif temp["type"] == "sqlserver":
                    connect_str = "mssql+pyodbc://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["database"] + "?driver=ODBC+Driver+17+for+SQL+Server"
                    return sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
                elif temp["type"] == "mysql":
                    connect_str = "mysql+mysqldb://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["database"]
                    return sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
                elif temp["type"] == "pgsql":
                    connect_str = "postgresql://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["database"]
                    return sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
                elif temp["type"] == "mongo":
                    if temp["user"] != "" and temp["password"] != "":
                        url = "mongodb://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"])
                    else:
                        url = "mongodb://" + temp["ip"] + ":" + str(temp["port"])
                    return pymongo.MongoClient(url)
                elif temp["type"] == "redis":
                    return redis.Redis(host=temp["ip"], port=temp["port"], db=temp["database"], decode_responses=True, charset='UTF-8', encoding='UTF-8')
                elif temp["type"] == "clickhouse":
                    connect_str = "clickhouse://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["database"]
                    return sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
                else:
                    raise ValueError("不支持的数据库类型：" + temp["type"])
                
    def get_client(self, name: str):
        return self.connect[name]
    
    
local_db = duckdb.connect(os.path.join(LOCAL_DB_PATH, "data.db"))