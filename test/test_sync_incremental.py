# 手动指定模块的导入路径
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import sqlalchemy
import pandas as pd
from utils.connect import CONNECTER
from tasks.incremental import incremental_task, incremental_task_options 

pd.set_option('display.max_rows', None)  # 显示所有行
pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.width', None)  # 设置显示宽度为 None
pd.set_option('display.max_colwidth', None)  # 设置最大列宽为 None
    
def main():
    print("全量数据库同步测试")
    print("创建任务")
    temp_task = incremental_task(incremental_task_options(
        name="增量同步测试",
        sync_sql_path="test/test_sync_incremental.sql",
        sync_source_connect_name="mysql测试",
        local_table_name="test_sync_incremental",
        incremental_comparison_list=[0, 4]
    ))
    # with CONNECTER.get_sql("mysql测试").connect() as connect:
    #     print("创建测试用Schema")
    #     connect.execute(sqlalchemy.text(
    #         '''
    #         CREATE SCHEMA IF NOT EXISTS test_schema
    #         '''
    #     ))
    #     print("删除测试表如果存在")
    #     connect.execute(sqlalchemy.text(
    #         '''
    #         DROP TABLE IF EXISTS test_schema.users
    #         '''
    #     ))
    #     print("创建测试表")
    #     connect.execute(sqlalchemy.text(
    #         '''
    #         CREATE TABLE test_schema.users (
    #             id INT AUTO_INCREMENT PRIMARY KEY,
    #             name VARCHAR(50) NOT NULL,
    #             age INT,
    #             email VARCHAR(50)
    #         )
    #         '''
    #     ))
    #     print("添加测试数据")
    #     connect.execute(sqlalchemy.text(
    #         '''
    #         INSERT INTO test_schema.users VALUES
    #         (1, 'Alice', 25, 'alice@example.com'),
    #         (2, 'Bob', 30, 'bob@example.com'),
    #         (3, 'Charlie', 35, 'charlie@example.com')
    #         '''
    #     ))
    #     connect.commit()
    # print("运行任务")
    # temp_task.run()
    
    

if __name__ == "__main__":
    main()