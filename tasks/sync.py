import pandas as pd
import sqlalchemy
from utils.connect import CONNECT
from tasks.base import task, task_connect_with


class sync_nosql(task):
    """将nosql的文档型数据源抽取到本地存储"""
    def __init__(self,
                 name: str,
                 source_connect_name: str,
                 source_database_name: str,
                 source_document_name: str,
                 target_connect_name: str = "本地mongo存储",
                 target_database_name: str = "sync",
                 target_document_name: str | None = None,
                 chunksize: int = 10000) -> None:
        super().__init__(name)
        target_document_name = source_document_name if target_document_name is None else target_document_name
        self.chunksize = chunksize
        self.source_doc = CONNECT.NOSQL[source_connect_name][source_database_name][source_document_name]
        self.target_doc = CONNECT.NOSQL[target_connect_name][target_database_name][target_document_name]

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
                 source_connect_name: str,
                 target_table_name: str,
                 target_connect_name: str,
                 target_connect_schema: str | None = None,
                 chunksize: int = 10000) -> None:
        super().__init__(name)
        self.target_table_name = target_table_name
        self.target_connect_schema = target_connect_schema
        self.chunksize = chunksize
        self.source_sql = source_sql
        
        self.source_client: sqlalchemy.engine.Engine = CONNECT.SQL[source_connect_name]
        self.target_client: sqlalchemy.engine.Engine = CONNECT.SQL[target_connect_name]

    def task_main(self) -> None:
        self.log.info("读取数据")
        data_group = None
        with task_connect_with(self.source_client, self.log) as connection:
            data_group = pd.read_sql_query(sqlalchemy.text(self.source_sql), connection, chunksize=self.chunksize)
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
