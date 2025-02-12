# 手动指定模块的导入路径

import pandas as pd
import sqlalchemy
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from utils.connect import CONNECTER, LOCALDB
from tasks.incremental import incremental_task, incremental_task_options

pd.set_option('display.max_rows', None)  # 显示所有行
pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.width', None)  # 设置显示宽度为 None
pd.set_option('display.max_colwidth', None)  # 设置最大列宽为 None


def main():
    print("全量数据库同步测试")
    print("创建任务")
    temp_task = incremental_task(incremental_task_options(name="增量同步测试",
                                                          sync_sql_path="test/test_sync_incremental.sql",
                                                          sync_source_connect_name="mysql测试",
                                                          local_table_name="test_sync_incremental",
                                                          incremental_comparison_list=[0, 4]
                                                          )
                                 )
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
            ('员工 5', 27, '人力资源部'),
            ('员工 6', 35, '运营部'),
            ('员工 7', 33, '财务部'),
            ('员工 8', 26, '开发部'),
            ('员工 9', 29, '设计部'),
            ('员工 10', 31, '测试部');
            '''
        ))
        connect.commit()
    
    with LOCALDB.cursor() as m_LOCALDB:
        print("添加本地的测试数据")
        m_LOCALDB.execute(
            '''
            CREATE SCHEMA IF NOT EXISTS ods
            '''
        )
        m_LOCALDB.execute(
            '''
            CREATE OR REPLACE TABLE ods.test_sync_incremental (
                "员工编号" BIGINT PRIMARY KEY NOT NULL,
                "员工姓名" VARCHAR(50) NOT NULL,
                age INT NOT NULL,
                "部门" VARCHAR(100) NOT NULL,
                "最后修改时间" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        m_LOCALDB.execute(
            '''
            INSERT INTO ods.test_sync_incremental ("员工编号", "员工姓名", age, "部门") VALUES
            (1, '员工 1', 25, '开发部'),
            (2, '员工 2', 28, '设计部'),
            (3, '员工 3', 30, '测试部'),
            (4, '员工 4', 22, '市场部'),
            (5, '员工 5', 27, '人力资源部')
            '''
        )
        m_LOCALDB.execute(
            '''
            CREATE OR REPLACE TABLE ods.test_sync_incremental_incremental (
                "员工编号" INT PRIMARY KEY NOT NULL,
                "最后修改时间" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        m_LOCALDB.execute(
            '''
            INSERT INTO ods.test_sync_incremental_incremental ("员工编号", "最后修改时间") 
            SELECT "员工编号", "最后修改时间" FROM ods.test_sync_incremental;
            '''
        )
    
    print("运行任务")
    temp_task.run()


if __name__ == "__main__":
    main()
