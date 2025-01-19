import os
from utils import SELECT_PATH
from tasks import task
            

class total_sql_sync(task):
    '''通过sql的全量更新同步'''
    def __init__(self, name, sql_path: str):
        super().__init__(name)
        self.sql_path = sql_path
        
    async def task_main(self, sql_path: str):
        ...
    
    
class incremental_sql_sync(task):
    '''通过sql的全量更新同步'''
    def __init__(self, name, sql_path):
        super().__init__(name)
    
    async def task_main(self, sql_path: str):
        ...    