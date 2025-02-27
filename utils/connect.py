import pymongo
import sqlalchemy
from urllib.parse import quote_plus


class connecter_sql:
    """所有的数据库连接"""

    def __init__(self) -> None:
        self.connect: dict[str, sqlalchemy.engine.Engine] = {
            "BI与数开用数据库": sqlalchemy.create_engine(
                url=f'''
                mysql+mysqldb://cheakf:{quote_plus("Swq8855830.")}@10.24.5.32:3306/dataframe_flow_v2
                ''',
                poolclass=sqlalchemy.QueuePool
            ),
            "EAS": sqlalchemy.create_engine(
                url=f'''
                oracle+cx_oracle://easselect:{quote_plus("easselect")}@172.18.1.121:1521/?service_name=eas
                ''',
                poolclass=sqlalchemy.QueuePool
            ),
            "MES": sqlalchemy.create_engine(
                url=f'''
                oracle+cx_oracle://unimax_cg:{quote_plus("unimax_cg")}@10.24.212.17:1521/?service_name=ORCL
                ''',
                poolclass=sqlalchemy.QueuePool
            ),
            "SHR": sqlalchemy.create_engine(
                url=f'''
                oracle+cx_oracle://shr_query:{quote_plus("shr_queryZj123!")}@10.24.204.67:1521/?service_name=ORCL
                ''',
                poolclass=sqlalchemy.QueuePool
            ),
            "生产辅助系统": sqlalchemy.create_engine(
                url=f'''
                mssql+pyodbc://metro:{quote_plus("Metro2023!")}@10.24.5.154:1433/城轨事业部
                ''',
                poolclass=sqlalchemy.QueuePool
            ),
            "金蝶云苍穹-正式库": sqlalchemy.create_engine(
                url=f'''
                mysql+mysqldb://cosmic:{quote_plus("Mysql@2022!")}@10.24.206.138:3306/crrc_secd
                ''',
                poolclass=sqlalchemy.QueuePool
            ),
            "金蝶云苍穹-测试库": sqlalchemy.create_engine(
                url=f'''
                mysql+mysqldb://kingdee:{quote_plus("kingdee2020")}@10.24.204.37:3306/crrc_phm
                ''',
                poolclass=sqlalchemy.QueuePool
            ),
            "机车数据库": sqlalchemy.create_engine(
                url=f'''
                mysql+mysqldb://crrc_temp:{quote_plus("ETxkrRpCFDZ4LmMr")}@10.29.31.159:3306/crrc_temp
                ''',
                poolclass=sqlalchemy.QueuePool
            ),
            "智能立库": sqlalchemy.create_engine(
                url=f'''
                mysql+mysqldb://root:{quote_plus("jftxAdmin")}@10.24.5.21:3307/smart_warehouse
                ''',
                poolclass=sqlalchemy.QueuePool
            ),
            "考勤系统": sqlalchemy.create_engine(
                url=f'''
                mssql+pyodbc://sa:{quote_plus("Z@hc#8705!$")}@10.24.7.48:1433/GM_MT_70
                ''',
                poolclass=sqlalchemy.QueuePool
            ),
            "数据运用平台-正式库": sqlalchemy.create_engine(
                url=f'''
                mysql+mysqldb://root:{quote_plus("WnXPLOS8ch")}@10.24.207.80:8082/zj_data
                ''',
                poolclass=sqlalchemy.QueuePool
            ),
            "数据运用平台-测试库": sqlalchemy.create_engine(
                url=f'''
                mysql+mysqldb://zjuser:{quote_plus("zjuser@123")}@10.24.7.145:3316/zj_data
                ''',
                poolclass=sqlalchemy.QueuePool
            ),
            "本地clickhouse": sqlalchemy.create_engine(
                url=f'''
                clickhouse+native://cheakf:{quote_plus("Swq8855830.")}@10.24.5.59:8123/default
                ''',
                poolclass=sqlalchemy.QueuePool
            )
        }

    def __getitem__(self, key: str) -> sqlalchemy.engine.Engine:
        return self.connect[key]

    def __setitem__(self, key: str, value: sqlalchemy.engine.Engine) -> None:
        self.connect[key] = value

    def __delitem__(self, key: str) -> None:
        del self.connect[key]

    def close(self) -> None:
        """关闭所有的连接"""
        for con in self.connect.values():
            con.dispose()


class connecter_nosql:
    """所有的数据库连接"""

    def __init__(self) -> None:
        self.connect: dict[str, pymongo.MongoClient] = {
            "外网访客系统": pymongo.MongoClient("mongodb://18.0.163.64:10086/"),
            "本地mongo存储": pymongo.MongoClient("mongodb://localhost:27017/")
        }

    def __getitem__(self, key: str) -> pymongo.MongoClient:
        return self.connect[key]

    def __setitem__(self, key: str, value: pymongo.MongoClient) -> None:
        self.connect[key] = value

    def __delitem__(self, key: str) -> None:
        del self.connect[key]

    def close(self) -> None:
        """关闭所有的连接"""
        for con in self.connect.values():
            con.close()

class connect:
    SQL = connecter_sql()
    NOSQL = connecter_nosql()
    
    
CONNECT = connect()