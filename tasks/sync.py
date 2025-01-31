import os
import pandas as pd
import sqlalchemy
import sqlglot
from sqlglot import exp
from utils import CONFIG, CONNECTER, LOCALDB
from tasks import task, task_connect_with  

def check_sql(source_sql_path: str, source_connect_name: str) -> str:
    '''从文件读取并检查用于查询的sql是否正确'''
    with open(os.path.join(CONFIG.SELECT_PATH, source_sql_path), mode="r", encoding="utf-8") as file:
        sql_str = file.read() 
    sqlglot.parse_one(sql_str, read=CONFIG.CONNECT_CONFIG[source_connect_name]["type"])
    return sql_str
        

class sync_sql_total(task):
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
            chunksize: int = 10000
        ):
        super().__init__(name)
        self.target_table_name = target_table_name
        self.target_connect_schema = target_connect_schema
        self.chunksize = chunksize
        self.sql_str = check_sql(source_sql_path, source_connect_name)

        if not CONFIG.CONNECT_CONFIG[target_connect_name]["write_enable"]:
            raise ValueError(target_connect_name + ": 该数据源不应当作为目标，因为其不支持写入")
        
        self.source_client: sqlalchemy.engine.Engine = CONNECTER[source_connect_name]
        self.target_client: sqlalchemy.engine.Engine = CONNECTER[target_connect_name]
    
    def task_main(self):
        self.log.info("读取数据")
        with self.source_client.connect() as connection:
            with task_connect_with(connection, self.log):
                data_group = pd.read_sql_query(sqlalchemy.text(self.sql_str), connection, chunksize=self.chunksize)
            
        with self.target_client.connect() as connection:
            with task_connect_with(connection, self.log):
                self.log.info("检查目标数据库是存在目标表")
                if sqlalchemy.inspect(connection).has_table(self.target_table_name, schema=self.target_connect_schema):
                    self.log.info("存在目标表，正在清空......")
                    if not self.target_connect_schema is None:
                        connection.execute(sqlalchemy.text(f"TRUNCATE TABLE {self.target_connect_schema}.{self.target_table_name}"))
                    else:
                        connection.execute(sqlalchemy.text(f"TRUNCATE TABLE {self.target_table_name}"))
                    connection.commit()
                else:
                    self.log.info("不存在目标表")
                self.log.info("写入数据")
                for data in data_group:
                    data.to_sql(name=self.target_table_name, con=connection, schema=self.target_connect_schema, index=False, if_exists='append')
        

class sync_sql_incremental_sql(task):
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
        
        if not CONFIG.CONNECT_CONFIG[target_connect_name]["write_enable"]:
            raise ValueError(target_connect_name + ": 该数据源不应当作为目标，因为其不支持写入")
            
        self.source_client: sqlalchemy.engine.Engine = CONNECTER[source_connect_name]
        self.target_client: sqlalchemy.engine.Engine = CONNECTER[source_connect_name]
    
    def task_main(self):
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
        m_cursor = LOCALDB.cursor()
        m_cursor.register("temp.source_incremental_data", source_incremental_data)
        m_cursor.register("temp.target_incremental_data", target_incremental_data)
        # 查询差异行
        diff_query = \
        """
            SELECT id, A, B FROM df1
            EXCEPT
            SELECT id, A, B FROM df2
        """
        diff_ids = m_cursor.execute(diff_query).fetchdf()['id'].tolist()
        parsed = sqlglot.parse_one(self.source_sync_sql_str)
        # 添加 id 作为查询条件
        if diff_ids:
            id_condition = exp.In(
                this=exp.Column(this="id"),
                expressions=[exp.Literal(this=str(id), is_string=False) for id in diff_ids]
            )
            parsed.where = parsed.where.and_(id_condition) if parsed.where else id_condition
        self.source_sync_sql_str = parsed.sql()
        with self.source_client.connect() as connection:
            with task_connect_with(connection, self.log):
                source_incremental_data = pd.read_sql_query(sqlalchemy.text(self.source_incremental_sql_str), connection)
        
        
        
class extract_sql_incremental(task):
    '''通过sql的增量抽取到本地存储'''
    ...
    
class extract_sql_total(task):
    '''通过sql的全量抽取到本地存储'''
    ...
    
class load_table_total(task):
    '''将本地存储全量加载到其他数据源'''
    ...
    
class load_table_incremental(task):
    '''将本地存储增量加载到其他数据源'''
    ...
    

    
class extract_nosql(task):
    '''
    将nosql的文档型数据源抽取到本地存储，并附带转换
    
    注意：因为数据格式的不同，目前不支持增量
    '''
    ...
        

        
            