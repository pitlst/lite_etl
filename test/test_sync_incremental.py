# 手动指定模块的导入路径
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import sqlalchemy
import pandas as pd
from utils import CONNECTER
from tasks.sync.total import sync_sql_incremental
    
def main():
    print("全量数据库同步测试")

    print("创建任务")
    temp_task = sync_sql_incremental(
        name="增量同步测试",
        source_incremental_name="test/test_sync_incremental",
        target_connect_schema="test_schema",
        target_table_name="total_sync_test",
        source_connect_name="mysql测试",
        target_connect_name="mysql测试"
    )
    print("来源的增量检查sql为")
    print(temp_task.source_incremental_sql_str)
    print("目标的增量检查sql为")
    print(temp_task.target_incremental_sql_str)
    

if __name__ == "__main__":
    main()