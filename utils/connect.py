import pymongo
import sqlalchemy
from typing import Any
from dataclasses import dataclass
from urllib.parse import quote_plus
from utils.config import CONFIG

@dataclass
class connecter:
    """所有的数据库连接"""
    # BI与数开用数据库
    local_bi_db = sqlalchemy.create_engine(
        url=f'''
        mysql+mysqldb://cheakf:{quote_plus("Swq8855830.")}@10.24.5.32:3306/dataframe_flow_v2
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    eas_db = sqlalchemy.create_engine(
        url=f'''
        oracle+cx_oracle://easselect:{quote_plus("easselect")}@172.18.1.121:1521/?service_name=eas
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    mes_db = sqlalchemy.create_engine(
        url=f'''
        oracle+cx_oracle://unimax_cg:{quote_plus("unimax_cg")}@10.24.212.17:1521/?service_name=ORCL
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    shr_db = sqlalchemy.create_engine(
        url=f'''
        oracle+cx_oracle://shr_query:{quote_plus("shr_queryZj123!")}@10.24.204.67:1521/?service_name=ORCL
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    # 生产辅助系统用的数据库
    huhe_db = sqlalchemy.create_engine(
        url=f'''
        mssql+pyodbc://metro:{quote_plus("Metro2023!")}@10.24.5.154:1433/城轨事业部
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    kingdee_db = sqlalchemy.create_engine(
        url=f'''
        mysql+mysqldb://cosmic:{quote_plus("Mysql@2022!")}@10.24.206.138:3306/crrc_secd
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    kingdee_test_db = sqlalchemy.create_engine(
        url=f'''
        mysql+mysqldb://kingdee:{quote_plus("kingdee2020")}@10.24.204.37:3306/crrc_phm
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    # 机车的数据库，实际部署在城轨这里
    locomotive_db = sqlalchemy.create_engine(
        url=f'''
        mysql+mysqldb://crrc_temp:{quote_plus("ETxkrRpCFDZ4LmMr")}@10.29.31.159:3306/crrc_temp
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    # 智能立库
    intelligent_vaulte_db = sqlalchemy.create_engine(
        url=f'''
        mysql+mysqldb://root:{quote_plus("jftxAdmin")}@10.24.5.21:3307/smart_warehouse
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    # 考勤系统
    kq_db = sqlalchemy.create_engine(
        url=f'''
        mssql+pyodbc://sa:{quote_plus("Z@hc#8705!$")}@10.24.7.48:1433/GM_MT_70
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    # 数据治理平台
    data_governance_db = sqlalchemy.create_engine(
        url=f'''
        mysql+mysqldb://root:{quote_plus("WnXPLOS8ch")}@10.24.207.80:8082/zj_data
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    data_governance_test_db = sqlalchemy.create_engine(
        url=f'''
        mysql+mysqldb://zjuser:{quote_plus("zjuser@123")}@10.24.7.145:3316/zj_data
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    # 本地的clickhouse
    local_cl_db = sqlalchemy.create_engine(
        url=f'''
        clickhouse+native://cheakf:{quote_plus("Swq8855830.")}@10.24.5.59:8123/default
        ''', 
        poolclass=sqlalchemy.QueuePool
    )
    # 外网访客系统数据库，该数据库仅能在10.24.5.54访问得到
    interested_parties_db = pymongo.MongoClient("mongodb://18.0.163.64:10086/")
    # 本地mongo存储，不开放互联网连接，日志存储在这里
    local_mongo_db = pymongo.MongoClient("mongodb://localhost:27017/")


CONNECTER = connecter()
            

 