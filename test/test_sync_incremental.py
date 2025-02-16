# 手动指定模块的导入路径

import pandas as pd
import sqlalchemy
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from utils.config import CONFIG
from utils.connect import CONNECTER, LOCALDB
from tasks.incremental import incremental_task, incremental_task_options

pd.set_option('display.max_rows', None)  # 显示所有行
pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.width', None)  # 设置显示宽度为 None
pd.set_option('display.max_colwidth', None)  # 设置最大列宽为 None


def main():
    print("全量数据库同步测试")
    print("创建任务")
    with CONNECTER.get_sql("mysql测试").connect() as connect:
        print("创建测试用Schema")
        connect.execute(sqlalchemy.text(
            '''
            CREATE SCHEMA IF NOT EXISTS test_schema
            '''
        ))
        print("删除测试表如果存在")
        connect.execute(sqlalchemy.text(
            '''
            DROP TABLE IF EXISTS test_schema.employee_performance
            '''
        ))
        print("创建测试表")
        connect.execute(sqlalchemy.text(
            '''
            CREATE TABLE IF NOT EXISTS test_schema.employee_performance (
                id INT AUTO_INCREMENT PRIMARY KEY COMMENT '员工编号',
                name VARCHAR(20) NOT NULL COMMENT '员工姓名',
                age INT NOT NULL COMMENT '年龄',
                department VARCHAR(50) NOT NULL COMMENT '部门',
                modification_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间'
            ) COMMENT='员工绩效表';
            '''
        ))
        print("添加mysql测试的测试数据")
        connect.execute(sqlalchemy.text(
            '''
            INSERT INTO test_schema.employee_performance (name, age, department) VALUES
            ('员工 1', 25, '开发部'),
            ('员工 2', 28, '设计部'),
            ('员工 3', 30, '测试部'),
            ('员工 4', 22, '市场部'),
            ('员工 5', 27, '人力资源部');
            '''
        ))
        connect.commit()
    
    
    print("第一次运行任务-退化为全量执行")
    incremental_task(incremental_task_options(name="增量同步测试",
                                            sync_sql_path="test/test_sync_incremental.sql",
                                            sync_source_connect_name="mysql测试",
                                            local_table_name="test_sync_incremental",
                                            incremental_comparison_list=[0, 4],
                                            is_delete=True
                                            )
    ).run()
    
    with CONNECTER.get_sql("mysql测试").connect() as connect:
        print("添加mysql测试的测试数据")
        connect.execute(sqlalchemy.text(
            '''
            INSERT INTO test_schema.employee_performance (name, age, department) VALUES
            ('员工 6', 35, '运营部'),
            ('员工 7', 33, '财务部'),
            ('员工 8', 26, '开发部'),
            ('员工 9', 29, '设计部'),
            ('员工 10', 31, '测试部');
            '''
        ))
        connect.commit()
    
    print("第二次运行任务-测试增量执行")
    incremental_task(incremental_task_options(name="增量同步测试2",
                                            sync_sql_path="test/test_sync_incremental.sql",
                                            sync_source_connect_name="mysql测试",
                                            local_table_name="test_sync_incremental",
                                            incremental_comparison_list=[0, 4],
                                            is_delete=True
                                            )
    ).run()
    
    with LOCALDB.cursor() as m_cursor:
        print("检查数据是否正确")
        actual_data = m_cursor.execute(
            '''
            SELECT * FROM ods.test_sync_incremental
            '''
        ).fetch_df()

    
    with open(os.path.join(CONFIG.SELECT_PATH, "test/test_sync_incremental.sql"), "r", encoding="utf-8") as file:
        sync_sql = file.read()
    with CONNECTER.get_sql("mysql测试").connect() as connect:
        data_group = pd.read_sql_query(sqlalchemy.text(sync_sql), connect)
        
    assert actual_data.equals(data_group) , "数据不匹配"


if __name__ == "__main__":
    main()
