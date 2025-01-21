import os
import sqlalchemy
import sqlglot
import sqlglot.optimizer
from datetime import date
from sqlglot import exp
from utils import SELECT_PATH, EXECUTER
from tasks import task   

class total_sql_sync(task):
    '''通过sql的全量更新同步'''
    def __init__(self, 
            name: str,        
            source_sql_path: str, 
            target_table_name: str, 
            source_connect_name: str,
            target_connect_name: str
        ):
        super().__init__(name)
        self.target_table_name = target_table_name
        with open(os.path.join(SELECT_PATH, source_sql_path), mode="r", encoding="utf-8") as file:
            sql_str = file.read() 
        # 检查sql
        sqlglot.parse(sql_str)
        sqlglot.optimizer.optimize(sql_str)
        self.sql_str = sql_str
        self.source_client: sqlalchemy.engine.Engine = EXECUTER.get_client(source_connect_name)
        self.target_client: sqlalchemy.engine.Engine = EXECUTER.get_client(target_connect_name)
    
    def task_template(self):
        with self.source_client.connect() as connection:
            result = connection.execute(self.sql_str)
        insert_ast = exp.Insert(
            this=exp.Table(this=self.target_table_name),
            expressions=[
                exp.Tuple(expressions=[
                    exp.Literal.string(value.isoformat()) if isinstance(value, date) else
                    exp.Literal.number(value) if isinstance(value, (int, float)) else
                    exp.Literal.string(str(value))
                    for value in row
                ])
                for row in result
            ]
        )
        with self.target_client.connect() as connection:
            connection.execute(insert_ast.sql())
        
    
class incremental_sql_sync(task):
    '''通过sql的全量更新同步'''
    async def __init__(self,             
            name: str,        
            source_incremental_sql_path: str, 
            target_incremental_sql_path: str,
            source_sync_sql_path: str,
            target_table_name: str,
            source_connect_name: str,
            target_connect_name: str
        ):
        super().__init__(name)
        with open(os.path.join(SELECT_PATH, source_incremental_sql_path), mode="r", encoding="utf-8") as file:
            source_incremental_sql_str = file.read() 
        with open(os.path.join(SELECT_PATH, target_incremental_sql_path), mode="r", encoding="utf-8") as file:
            target_incremental_sql_str = file.read() 
        with open(os.path.join(SELECT_PATH, source_sync_sql_path), mode="r", encoding="utf-8") as file:
            source_sync_sql_str = file.read() 
        # 检查sql
        for temp in [source_incremental_sql_str, target_incremental_sql_str, source_sync_sql_str]:
            sqlglot.parse(temp)
            sqlglot.optimizer.optimize(temp)
            
        self.target_table_name = target_table_name
        self.source_incremental_sql_str = source_incremental_sql_str
        self.target_incremental_sql_str = target_incremental_sql_str
        self.source_sync_sql_str = source_sync_sql_str
        self.source_client: sqlalchemy.engine.Engine = EXECUTER.get_client(source_connect_name)
        self.target_client: sqlalchemy.engine.Engine = EXECUTER.get_client(target_connect_name)
    
    async def task_template(self):
        # 获取增量对比的信息
        with self.source_client.connect() as connection:
            source_incremental_result: sqlalchemy.engine.CursorResult = connection.execute(self.source_incremental_sql_str)
        with self.target_client.connect() as connection:
            target_incremental_result: sqlalchemy.engine.CursorResult = connection.execute(self.target_incremental_sql_str)
        # 增量对比
        source_incremental_dict = source_incremental_result.mappings()
        target_incremental_dict = target_incremental_result.mappings()
        
            