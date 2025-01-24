import os
import pandas as pd
import sqlalchemy
import sqlglot
import sqlglot.optimizer
from utils import SELECT_PATH, EXECUTER, CONNECT_CONFIG, local_db
from tasks import task, task_connect_with  

def check_sql(source_sql_path: str, source_connect_name: str) -> str:
    '''检查用于查询的sql是否正确'''
    with open(os.path.join(SELECT_PATH, source_sql_path), mode="r", encoding="utf-8") as file:
        sql_str = file.read() 
    sqlglot.parse(sql_str, read=CONNECT_CONFIG[source_connect_name]["type"])
    sqlglot.optimizer.optimize(sql_str)
    return sql_str
        

class total_sql_sync(task):
    '''
    通过sql的全量更新同步
    
    注意：该类的全量更新通过pandas实现，其数据会以批量的方式写入内存再写出
    '''
    def __init__(self, 
            name: str,        
            source_sql_path: str, 
            target_table_name: str, 
            source_connect_name: str,
            target_connect_name: str,
            target_connect_schema: str | None = None,
            chunksize = 10000
        ):
        super().__init__(name)
        self.target_table_name = target_table_name
        self.target_connect_schema = target_connect_schema
        self.chunksize = chunksize if not self.chunksize is None else 10000 
        self.sql_str = check_sql(source_sql_path, source_connect_name)

        if not CONNECT_CONFIG[target_connect_name]["write_enable"]:
            raise ValueError(target_connect_name + ": 该数据源不应当作为目标，因为其不支持写入")
        
        self.source_client: sqlalchemy.engine.Engine = EXECUTER.get_client(source_connect_name)
        self.target_client: sqlalchemy.engine.Engine = EXECUTER.get_client(target_connect_name)
    
    def task_template(self):
        self.log.info("读取数据")
        with self.source_client.connect() as connection:
            with task_connect_with(connection, self.log):
                data_group = pd.read_sql_query(sqlalchemy.text(self.sql_str), connection, chunksize=self.chunksize)
            
        with self.target_client.connect() as connection:
            self.log.info("检查目标数据库是存在目标表")
            if self.target_table_name in sqlalchemy.inspect(connection).get_table_names():
                self.log.info("存在目标表，正在清空......")
                with task_connect_with(connection, self.log):
                    connection.execute(f"TRUNCATE TABLE {self.target_table_name}")
            else:
                self.log.info("不存在目标表")
            self.log.info("写入数据")
            with task_connect_with(connection, self.log):
                for data in data_group:
                    data.to_sql(name=self.target_table_name, con=connection, schema=self.target_connect_schema, index=False, if_exists='append')
        

class incremental_sql_sync(task):
    '''
    通过sql的增量更新同步
    
    注意：不支持同步到当前的duckDB中，对于增量同步数据不会分片而是全量写入，请注意大小
    '''
    def __init__(self,             
            name: str,        
            source_incremental_sql_path: str, 
            target_incremental_sql_path: str,
            source_sync_sql_path: str,
            target_table_name: str,
            source_connect_name: str,
            target_connect_name: str,
            target_connect_schema: str | None = None,
        ):
        super().__init__(name)
        self.target_table_name = target_table_name
        self.target_connect_schema = target_connect_schema
        
        self.source_incremental_sql_str = check_sql(source_incremental_sql_path, source_connect_name)
        self.target_incremental_sql_str = check_sql(target_incremental_sql_path, target_connect_name)
        self.source_sync_sql_str = check_sql(source_sync_sql_path, source_connect_name)
        
        if not CONNECT_CONFIG[target_connect_name]["write_enable"]:
            raise ValueError(target_connect_name + ": 该数据源不应当作为目标，因为其不支持写入")
            
        self.source_client: sqlalchemy.engine.Engine = EXECUTER.get_client(source_connect_name)
        self.target_client: sqlalchemy.engine.Engine = EXECUTER.get_client(target_connect_name)
    
    def task_template(self):
        # 获取增量对比的信息
        self.log.info("读取来源对比数据")
        with self.source_client.connect() as connection:
            with task_connect_with(connection, self.log):
                source_incremental_data = pd.read_sql_query(sqlalchemy.text(self.source_incremental_sql_str), connection)
        self.log.info("读取目标对比数据")
        with self.target_client.connect() as connection:
            with task_connect_with(connection, self.log):
                target_incremental_data = pd.read_sql_query(sqlalchemy.text(self.target_incremental_sql_str), connection)
        self.log.info("对比数据")
        m_cursor = local_db.cursor()
        m_cursor.register("temp.source_incremental_data", source_incremental_data)
        m_cursor.register("temp.target_incremental_data", target_incremental_data)
        # 查询差异行
        diff_query = """
            SELECT id, A, B FROM df1
            EXCEPT
            SELECT id, A, B FROM df2
        """
        diff_df = m_cursor.execute(diff_query).fetchdf()
        
        
        
        
class incremental_sql_extract(task):
    '''通过sql的增量抽取到本地存储'''
    ...
    
class table_load(task):
    '''将本地存储加载到其他数据源'''
    ...

        

        
            