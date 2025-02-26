import os
import json
import pandas as pd
import pymongo
import sqlalchemy
import sqlglot
from utils.config import CONFIG
from utils.connect import CONNECTER
from tasks.base import task, task_connect_with


def is_path_or_sql(input_string):
    """用于判断是路径还是sql"""
    # 定义路径的常见分隔符
    path_separators = ['/', '\\', '.']
    # 定义 SQL 的常见关键字
    sql_keywords = ['SELECT', 'UPDATE', 'INSERT', 'DELETE', 'FROM', 'WHERE', 'SET', 'AND', 'OR']
    # 检查是否包含路径分隔符
    path_detected = any(sep in input_string for sep in path_separators)
    # 检查是否包含 SQL 关键字
    sql_detected = any(keyword in input_string for keyword in sql_keywords)
    # 有大写select我就认为他是查询的语句
    if sql_detected:
        return False
    elif path_detected:
        return True
    else:
        return None


def check_sql(source_sql_or_path: str, source_connect_name: str) -> str:
    """从文件读取并检查用于查询的sql是否正确"""
    label = is_path_or_sql(source_sql_or_path)
    if label is None:
        raise ValueError("输入的sql路径或sql不正确")
    elif label:
        with open(os.path.join(CONFIG.SELECT_PATH, source_sql_or_path), mode="r", encoding="utf-8") as file:
            sql_str = file.read()
    else:
        sql_str = source_sql_or_path
    sqlglot.parse_one(sql_str, read=CONFIG.CONNECT_CONFIG[source_connect_name]["type"])
    return sql_str


class sync_nosql(task):
    """将nosql的文档型数据源抽取到本地存储"""
    def __init__(self,
                 name: str,
                 source_connect: pymongo.MongoClient,
                 source_database_name: str,
                 source_document_name: str,
                 target_database_name: str = "ods",
                 target_document_name: str | None = None,
                 chunksize: int = 10000) -> None:
        super().__init__(name)
        target_document_name = source_document_name if target_document_name is None else target_document_name
        self.chunksize = chunksize
        self.source_doc = source_connect[source_database_name][source_document_name]
        self.target_doc = CONNECTER.local_mongo_db[target_database_name][target_document_name]

    def task_main(self) -> None:
        self.log.info("读取数据")
        data_group = self.source_doc.find({}, batch_size=self.chunksize)
        self.log.info("清空原有数据")
        self.target_doc.drop()
        self.log.info("将数据写入本地缓存")
        self.target_doc.insert_many(data_group)


class sync_sql(task):
    """
    通过sql的全量更新同步

    注意：该类的全量更新通过pandas实现，其数据会以批量的方式写入内存再写出
    """

    def __init__(self,
                 name: str,
                 source_sql: str,
                 source_connect: sqlalchemy.engine.Engine,
                 target_table_name: str,
                 target_connect: sqlalchemy.engine.Engine,
                 target_connect_schema: str | None = None,
                 chunksize: int = 10000) -> None:
        super().__init__(name)
        self.target_table_name = target_table_name
        self.target_connect_schema = target_connect_schema
        self.chunksize = chunksize
        self.source_sql = source_sql
        
        

        self.source_client: sqlalchemy.engine.Engine = CONNECTER.get_sql(source_connect_name)
        self.target_client: sqlalchemy.engine.Engine = CONNECTER.get_sql(target_connect_name)

    def task_main(self) -> None:
        self.log.info("读取数据")
        data_group = None
        with task_connect_with(self.source_client, self.log) as connection:
            data_group = pd.read_sql_query(sqlalchemy.text(self.sql_str), connection, chunksize=self.chunksize)
        if data_group is None:
            raise ValueError("未查询到数据或者连接失败")

        with task_connect_with(self.target_client, self.log) as connection:
            self.log.info("检查目标数据库是存在目标表")
            if sqlalchemy.inspect(connection).has_table(self.target_table_name, schema=self.target_connect_schema):
                self.log.info("存在目标表，正在删除......")
                if not self.target_connect_schema is None:
                    connection.execute(sqlalchemy.text(f"DROP TABLE \"{self.target_connect_schema}\".\"{self.target_table_name}\""))
                else:
                    connection.execute(sqlalchemy.text(f"DROP TABLE \"{self.target_table_name}\""))
            else:
                self.log.info("不存在目标表")
            self.log.info("写入数据")
            # 实际上这里插入语句的生成是借助pandas的tosql函数
            for data in data_group:
                data.to_sql(name=self.target_table_name, con=connection, schema=self.target_connect_schema, index=False, if_exists='append')
