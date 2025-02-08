# 手动指定模块的导入路径
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import sqlalchemy
import pandas as pd
from utils import CONNECTER, LOCALDB
from tasks.sync import sync_sql, extract_sql, extract_nosql, load_table

pd.set_option('display.max_rows', None)  # 显示所有行
pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.width', None)  # 设置显示宽度为 None
pd.set_option('display.max_colwidth', None)  # 设置最大列宽为 None

def main_sync_sql():
    print("全量数据库同步测试")
    print("创建任务")
    temp_task = sync_sql(
        name="全量同步测试",
        source_sql_or_path="test/employees_select.sql",
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
                name,
                age,
                email
            from
                test_schema.total_sync_test
            '''
        )
        test_data = pd.read_sql_query(sql_str, connect)
        print(test_data)
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']
    })
    assert df.equals(test_data), "测试数据不一致"
        
        
def main_extract_sql():
    print("全量数据库抽取测试")
    print("创建任务")
    temp_task = extract_sql(
        name="全量抽取测试",
        source_sql_or_path="test/employees_select.sql",
        target_table_name="total_sync_test",
        source_connect_name="mysql测试",
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
    print("打印本地表数据")
    test_data = LOCALDB.sql(f"SELECT * FROM ods.total_sync_test").df()
    print(test_data)
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']
    })
    assert df.equals(test_data), "测试数据不一致"
    
def main_extract_nosql():
    print("全量数据库抽取测试")
    print("创建任务")
    temp_task = extract_nosql(
        name="全量抽取测试",
        source_connect_name="mongo测试",
        source_database_name="test_schema",
        source_document_name="users_temp",
        target_table_name="total_sync_test"
    )
    
    print("获取 MongoDB 客户端")
    collection = CONNECTER["mongo测试"]["test_schema"]["users_temp"]
    print("删除已存在的集合")
    collection.drop()
    print("插入数据")
    test_data = [
            {
                "id": 1,
                "name": "Alice",
                "age": 25,
                "email": "alice@example.com",
                "additional_field": {"hobby": "reading"}
            },
            {
                "id": 2,
                "name": "Bob",
                "age": 30,
                "email": "bob@example.com",
                "additional_field": {"hobby": "gaming"}
            },
            {
                "id": 3,
                "name": "Charlie",
                "age": 35,
                "email": "charlie@example.com",
                "additional_field": {"hobby": "swimming"}
            }
        ]
    collection.insert_many(test_data)
    print("运行任务")
    temp_task.run()
    print("打印本地表数据")
    test_data = LOCALDB.sql(f"SELECT document FROM {temp_task.target_connect_schema}.{temp_task.target_table_name}").fetchdf()
    print(test_data)
    df = pd.DataFrame({
        'document': [
            '{"id": 1, "name": "Alice", "age": 25, "email": "alice@example.com", "additional_field": {"hobby": "reading"}}',
            '{"id": 2, "name": "Bob", "age": 30, "email": "bob@example.com", "additional_field": {"hobby": "gaming"}}',
            '{"id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com", "additional_field": {"hobby": "swimming"}}'
        ]
    })
    print(df)
    assert df.equals(test_data), "测试数据不一致"
    
    
def main_load_table():
    print("全量数据库写入测试")
    print("创建任务")
    temp_task = load_table(
        name="全量写入测试",
        target_connect_name="mysql测试",
        source_table_name="total_sync_test",
        target_table_name="total_sync_test"
    )
    print("准备测试数据")
    with LOCALDB.cursor() as m_cursor:
        print("创建表")
        m_cursor.execute(
            '''
            CREATE OR REPLACE TABLE dm.total_sync_test (
                id INT,
                name VARCHAR(50),
                age INT,
                email VARCHAR(50)
            )
            '''
        )
        print("添加测试数据")
        m_cursor.execute(
            '''
            INSERT INTO dm.total_sync_test (id, name, age, email)
            VALUES
                (1, 'John Doe', 25, 'john.doe@example.com'),
                (2, 'Jane Smith', 30, 'jane.smith@example.com'),
                (3, 'Bob Johnson', 35, 'bob.johnson@example.com'),
                (4, 'Alice Brown', 28, 'alice.brown@example.com'),
                (5, 'Mike Davis', 40, 'mike.davis@example.com');
            '''
        )
    print("运行任务")
    temp_task.run()
    print("打印目标表数据")
    with CONNECTER["mysql测试"].connect() as connect:
        sql_str = sqlalchemy.text(
            '''
            select
                id,
                name,
                age,
                email
            from
                total_sync_test
            '''
        )
        test_data = pd.read_sql_query(sql_str, connect)
        print(test_data)
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Mike Davis'],
        'age': [25, 30, 35, 28, 40],
        'email': ['john.doe@example.com', 'jane.smith@example.com', 'bob.johnson@example.com', 'alice.brown@example.com', 'mike.davis@example.com']
    })
    assert df.equals(test_data), "测试数据不一致"
    
if __name__ == "__main__":
    main_load_table()