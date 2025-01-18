import os


def total_sql_sync(sql_path: str):
    '''通过sql的全量更新同步'''
    ...
    
def incremental_sql_sync(concern_column: list[str], sql_path: str):
    '''通过sql的增量更新同步'''
    ...