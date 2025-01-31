# 手动指定模块的导入路径
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

def main():
    print("全量数据库同步测试")
    import sqlalchemy
    import pandas as pd
    from utils import CONNECTER
    from tasks.sync import sync_sql_total
    print("创建任务")
    temp_task = sync_sql_total(
        name="全量同步测试",
        source_sql_path="test/employees_select.sql",
        target_connect_schema="test_schema",
        target_table_name="total_sync_test",
        source_connect_name="mysql测试",
        target_connect_name="mysql测试"
    )
    with CONNECTER["mysql测试"].connect() as connect:
        print("创建测试用Schema")
        connect.execute(sqlalchemy.text(
            '''
            CREATE SCHEMA IF NOT EXISTS test_schema
            '''
        ))
        print("删除测试表如果存在")
        connect.execute(sqlalchemy.text(
            '''
            DROP TABLE IF EXISTS test_schema.users
            '''
        ))
        print("创建测试表")
        connect.execute(sqlalchemy.text(
            '''
            CREATE TABLE test_schema.users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INT,
                email VARCHAR(50)
            )
            '''
        ))
        print("添加测试数据")
        connect.execute(sqlalchemy.text(
            '''
            INSERT INTO test_schema.users VALUES
            (1, 'Alice', 25, 'alice@example.com'),
            (2, 'Bob', 30, 'bob@example.com'),
            (3, 'Charlie', 35, 'charlie@example.com')
            '''
        ))
        connect.commit()
    print("运行任务")
    temp_task.run()
    print("检查数据")
    with CONNECTER["mysql测试"].connect() as connect:
        sql_str = sqlalchemy.text(
            '''
            select
                id,
                "name",
                age,
                email
            from
                test_schema.total_sync_test
            '''
        )
        print(pd.read_sql_query(sql_str, connect))

if __name__ == "__main__":
    main()