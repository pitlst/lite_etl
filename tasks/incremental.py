import sqlglot
from dataclasses import dataclass
from utils import CONFIG
from tasks.base import task
from tasks.sync import extract_sql

@dataclass
class incremental_task_options:
    '''增量同步的函数初始化参数类'''
    name: str
    sync_sql_path: str
    sync_source_connect_name: str
    local_table_name: str
    local_schema: str = "ods"
    temp_table_schema : str = "temp"
    chunksize: int = 10000
    incremental_comparison_list: list[int] = [0]

class incremental_task(task):
    '''增量同步，将增量同步相同的部分抽象出来不做多次编写'''
    def __init__(self, input_options: incremental_task_options) -> None:
        super().__init__(input_options.name)
        self.sql_make(input_options)
        self.source_incremental_task = extract_sql(
            name=input_options.name + "_source",
            source_sql_or_path=self.source_sql_str,
            target_table_name=input_options.local_table_name,
            source_connect_name=input_options.sync_source_connect_name,
            target_connect_schema="temp"
        )
        self.source_total_task = extract_sql(
            name=input_options.name + "_source",
            source_sql_or_path=input_options.sync_sql_path,
            target_table_name=input_options.local_table_name,
            source_connect_name=input_options.sync_source_connect_name
        )
        
        
    def sql_make(self, input_options: incremental_task_options) -> None:
        '''将任务执行过程中所需要的sql提前在初始化阶段生成好'''
        with open(input_options.sync_sql_path, "r", encoding="utf-8") as file:
            sql_str = file.read()
        sql_ast = sqlglot.parse_one(sql_str, read=CONFIG.CONNECT_CONFIG[input_options.sync_source_connect_name]["type"])
        
        
    def task_main(self) -> None:
        ...
        