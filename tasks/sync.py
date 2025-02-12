import os
import json
import pandas as pd
import sqlalchemy
import sqlglot
from utils.config import CONFIG
from utils.connect import CONNECTER, LOCALDB
from tasks.base import task, task_connect_with


def is_path_or_sql(input_string):
    '''用于判断是路径还是sql'''
    # 定义路径的常见分隔符
    path_separators = ['/', '\\', '.']
    # 定义 SQL 的常见关键字
    sql_keywords = ['SELECT', 'UPDATE', 'INSERT', 'DELETE', 'FROM', 'WHERE', 'SET', 'AND', 'OR']
    # 检查是否包含路径分隔符
    path_detected = any(sep in input_string for sep in path_separators)
    # 检查是否包含 SQL 关键字
    sql_detected = any(keyword in input_string for keyword in sql_keywords)
    if path_detected and sql_detected:
        return None
    elif path_detected:
        return True
    elif sql_detected:
        return False
    else:
        return None


def check_sql(source_sql_or_path: str, source_connect_name: str) -> str:
    '''从文件读取并检查用于查询的sql是否正确'''
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


class extract_sql(task):
    '''通过sql的全量抽取到本地存储'''

    def __init__(self,
                 name: str,
                 source_sql_or_path: str,
                 target_table_name: str,
                 source_connect_name: str,
                 target_connect_schema: str = "ods",
                 chunksize: int = 10000) -> None:
        super().__init__(name)
        self.target_table_name = target_table_name
        self.target_connect_schema = target_connect_schema
        self.chunksize = chunksize

        self.sql_str = check_sql(source_sql_or_path, source_connect_name)
        self.source_client = CONNECTER.get_sql(source_connect_name)

    def task_main(self) -> None:
        self.log.info("读取数据")
        data_group = None
        with task_connect_with(self.source_client, self.log) as connection:
            data_group = pd.read_sql_query(sqlalchemy.text(self.sql_str), connection, chunksize=self.chunksize)
        if data_group is None:
            raise ValueError("未查询到数据或者连接失败")

        with LOCALDB.cursor() as m_cursor:
            temp_name = self.name + ".temp_data"
            iterator = iter(data_group)

            self.log.info("删除并重建目标表中......")
            temp_data = next(iterator, None)
            if temp_data is None:
                raise ValueError("查询数据为空")
            m_cursor.register(temp_name, temp_data)
            m_cursor.execute(
                f'''
                CREATE OR REPLACE TABLE {self.target_connect_schema}.{self.target_table_name} AS
                SELECT * FROM {temp_name} LIMIT 0
                '''
            )

            self.log.info("写入数据")
            while not temp_data is None:
                # 这里第一次的数据是上面通过迭代器获取的
                m_cursor.register(temp_name, temp_data)
                m_cursor.execute(
                    f'''
                    INSERT INTO {self.target_connect_schema}.{self.target_table_name} SELECT * FROM {temp_name}
                    '''
                )
                # 这里获取下一次填入的数据，如果为none会自动退出循环
                temp_data = next(iterator, None)


class load_table(task):
    '''将本地存储的数据加载到目标存储中'''

    def __init__(self,
                 name: str,
                 target_connect_name: str,
                 source_table_name: str,
                 target_table_name: str,
                 source_connect_schema: str = "dm",
                 target_connect_schema: str | None = None,
                 chunksize: int = 10000) -> None:
        super().__init__(name)
        self.source_table_name = source_table_name
        self.source_connect_schema = source_connect_schema
        self.target_table_name = target_table_name
        self.target_connect_schema = target_connect_schema
        self.chunksize = chunksize

        self.target_client: sqlalchemy.engine.Engine = CONNECTER.get_sql(target_connect_name)

    def task_main(self) -> None:
        self.log.info("读取数据")
        with LOCALDB.cursor() as m_cursor:
            data_group = m_cursor.execute(
                f'''
                SELECT * FROM {self.source_connect_schema}.{self.source_table_name}
                '''
            ).fetch_df()

        with task_connect_with(self.target_client, self.log) as connection:
            self.log.info("检查目标数据库是存在目标表")
            if sqlalchemy.inspect(connection).has_table(self.target_table_name, schema=self.target_connect_schema):
                self.log.info("存在目标表，正在删除......")
                if not self.target_connect_schema is None:
                    connection.execute(sqlalchemy.text(f"DROP TABLE {self.target_connect_schema}.{self.target_table_name}"))
                else:
                    connection.execute(sqlalchemy.text(f"DROP TABLE {self.target_table_name}"))
            else:
                self.log.info("不存在目标表")
            self.log.info("写入数据")
            # 实际上这里插入语句的生成是借助pandas的tosql函数
            data_group.to_sql(name=self.target_table_name, con=connection, schema=self.target_connect_schema, index=False, if_exists='append', chunksize=self.chunksize)


class extract_nosql(task):
    '''
    将nosql的文档型数据源抽取到本地存储，并附带一个默认的转换

    注意：因为数据格式的不同，目前不支持增量
    '''

    def __init__(self,
                 name: str,
                 source_connect_name: str,
                 source_database_name: str,
                 source_document_name: str,
                 target_table_name: str,
                 target_connect_schema: str = "ods",
                 chunksize: int = 10000) -> None:
        super().__init__(name)
        self.target_table_name = target_table_name
        self.target_connect_schema = target_connect_schema
        self.chunksize = chunksize

        self.source_coll = CONNECTER.get_nosql(source_connect_name)[source_database_name][source_document_name]

    def task_main(self) -> None:
        self.log.info("读取数据")
        data_group = self.source_coll.find({}, batch_size=self.chunksize)

        with LOCALDB.cursor() as m_cursor:
            m_cursor.execute(
                f"""
                CREATE OR REPLACE TABLE {self.target_connect_schema}.{self.target_table_name} (
                    id VARCHAR PRIMARY KEY,
                    document JSON
                )
                """
            )
            temp_list = []
            for documents in data_group:
                temp_list.append([str(documents.pop('_id')), json.dumps(documents)])
                if len(temp_list) >= self.chunksize:
                    m_cursor.executemany(
                        f"""
                            INSERT OR IGNORE INTO {self.target_connect_schema}.{self.target_table_name} (id, document)
                            VALUES (?, ?)
                        """, temp_list)
                    temp_list = []
            if len(temp_list) != 0:
                m_cursor.executemany(
                    f"""
                        INSERT OR IGNORE INTO {self.target_connect_schema}.{self.target_table_name} (id, document)
                        VALUES (?, ?)
                    """, temp_list)


class sync_sql(task):
    '''
    通过sql的全量更新同步

    注意：该类的全量更新通过pandas实现，其数据会以批量的方式写入内存再写出
    '''

    def __init__(self,
                 name: str,
                 source_sql_or_path: str,
                 target_table_name: str,
                 source_connect_name: str,
                 target_connect_name: str,
                 target_connect_schema: str | None = None,
                 chunksize: int = 10000) -> None:
        super().__init__(name)
        self.target_table_name = target_table_name
        self.target_connect_schema = target_connect_schema
        self.chunksize = chunksize
        self.sql_str = check_sql(source_sql_or_path, source_connect_name)

        if not CONFIG.CONNECT_CONFIG[target_connect_name]["write_enable"]:
            raise ValueError(target_connect_name + ": 该数据源不应当作为目标，因为其不支持写入")

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
                    connection.execute(sqlalchemy.text(f"DROP TABLE {self.target_connect_schema}.{self.target_table_name}"))
                else:
                    connection.execute(sqlalchemy.text(f"DROP TABLE {self.target_table_name}"))
            else:
                self.log.info("不存在目标表")
            self.log.info("写入数据")
            # 实际上这里插入语句的生成是借助pandas的tosql函数
            for data in data_group:
                data.to_sql(name=self.target_table_name, con=connection, schema=self.target_connect_schema, index=False, if_exists='append')
