import os
import aiofiles
import pandas as pd
from utils import SELECT_PATH, EXECUTER
from tasks import task
            

class total_sql_sync(task):
    '''通过sql的全量更新同步'''
    async def __init__(self, name):
        await super().__init__(name)
    
    async def task_template(self, 
        sql_path: str, 
        source_connect_name: str,
        target_connect_name: str,
        ):
        async with aiofiles.open(os.path.join(SELECT_PATH, sql_path), mode="r", encoding="utf-8") as file:
            sql_str = await file.read() 
        client = await EXECUTER.get_client(source_connect_name)
        ...    
        
    
class incremental_sql_sync(task):
    '''通过sql的全量更新同步'''
    async def __init__(self, name):
        super().__init__(name)
        
    async def read_sql(self, sql_path: str)-> str: 
        with open(os.path.join(SELECT_PATH.value, sql_path), mode="r", encoding="utf-8") as file:
            sql_str = file.read() 
        return sql_str
    
    async def task_template(self, sql_path: str):
        ...    