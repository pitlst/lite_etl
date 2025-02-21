import os
import duckdb
import pymongo
import sqlalchemy
from typing import Any
from urllib.parse import quote_plus
from utils.config import CONFIG

class connecter:
    def __init__(self) -> None:
        self._sql_connect: dict[str, sqlalchemy.Engine] = {}
        self._nosql_connect: dict[str, pymongo.MongoClient] = {}
        self.make_client(CONFIG.CONNECT_CONFIG)

    def make_client(self, connect_config: dict[str, Any]) -> None:
        """
        创建数据连接的工厂类
        用于在对应进程中创建对应进程的数据库连接
        """
        self._logger: duckdb.DuckDBPyConnection = duckdb.connect(os.path.realpath(os.path.join(CONFIG.LOCAL_DB_PATH, "logger.db")))
        self._local: duckdb.DuckDBPyConnection = duckdb.connect(os.path.realpath(os.path.join(CONFIG.LOCAL_DB_PATH, "data.db")))
        # 其他的连接，跟着配置文件走
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
        return self._sql_connect[key]

    def get_nosql(self, key: str) -> pymongo.MongoClient:
        if key not in self._nosql_connect.keys():
            raise ValueError("不存在对应的连接")
        return self._nosql_connect[key]
    
    def get_logger(self) -> duckdb.DuckDBPyConnection:
        return self._logger.cursor()
    
    def get_local(self) -> duckdb.DuckDBPyConnection:
        return self._local.cursor()
    
    def close_all(self):
        """关闭所有数据库连接"""
        for engine in self._sql_connect.values():
            engine.dispose()
        self._logger.close()
        self._local.close()

CONNECTER = connecter()