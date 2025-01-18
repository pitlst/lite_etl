import redis
import pymongo
import clickhouse_connect
import sqlalchemy as sl
from urllib.parse import quote_plus
# 进程共享变量
from utils import CONNECT_CONFIG


def create_client(name: str):
    '''
    创建数据连接的工厂类
    用于在对应进程中创建对应进程的数据库连接
    '''
    temp_config: dict = CONNECT_CONFIG.value
    assert name in temp_config.keys(), "连接要求的名称不在数据库连接的配置文件中"
    for ch in temp_config:
        if ch == name:
            temp = temp_config[ch]
            if temp["type"] == "oracle":
                connect_str = "oracle+cx_oracle://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/?service_name=" + temp["mode"]
                return sl.create_engine(connect_str, poolclass=sl.NullPool)
            elif temp["type"] == "sqlserver":
                connect_str = "mssql+pyodbc://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["mode"] + "?driver=ODBC+Driver+17+for+SQL+Server"
                return sl.create_engine(connect_str, poolclass=sl.NullPool)
            elif temp["type"] == "mysql":
                connect_str = "mysql+mysqldb://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["mode"]
                return sl.create_engine(connect_str, poolclass=sl.NullPool)
            elif temp["type"] == "pgsql":
                connect_str = "postgresql://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"]) + "/" + temp["mode"]
                return sl.create_engine(connect_str, poolclass=sl.NullPool)
            elif temp["type"] == "mongo":
                if temp["user"] != "" and temp["password"] != "":
                    url = "mongodb://" + temp["user"] + ":" + quote_plus(temp["password"]) + "@" + temp["ip"] + ":" + str(temp["port"])
                else:
                    url = "mongodb://" + temp["ip"] + ":" + str(temp["port"])
                return pymongo.MongoClient(url)
            elif temp["type"] == "redis":
                return redis.Redis(host=temp["ip"], port=temp["port"], db=0, decode_responses=True)
            elif temp["type"] == "clickhouse":
                return clickhouse_connect.get_client(host=temp["ip"], port=temp["port"], database=temp["database"], username=temp["user"], password=temp["password"])
            else:
                raise ValueError("不支持的数据库类型：" + temp["type"])
