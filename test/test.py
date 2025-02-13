# import test_connect
# import test_task
# import test_sync_total

# if __name__ == "__main__":
    # print("开始全部测试")
    # test_connect.main()
    # print("-------------------成功-------------------")
    # test_task.main()
    # print("-------------------成功-------------------")
    # test_sync_total.main_sync_sql()
    # print("-------------------成功-------------------")
    # test_sync_total.main_extract_sql()
    # print("-------------------成功-------------------")
    # test_sync_total.main_extract_nosql()
    # print("-------------------成功-------------------")
    # test_sync_total.main_load_table()
    # print("-------------------成功-------------------")
    # print("-------------------全部测试完成-------------------")
    
import sqlglot
from sqlglot import exp

id_list = ['1', '2']

# 创建一个 IN 表达式
in_expression = exp.In(
    this=exp.column('id', table='f'),  # 列表达式
    expressions=[exp.Literal.string(id_) for id_ in id_list]
)

# 创建一个 WHERE 子句
where_clause = exp.Where(this=in_expression)

# 创建一个 SELECT 查询
select_query = exp.Select(
    expressions=[exp.column('id')],  # 查询的列
    from_=exp.Table(name='f'),  # 表名
    where=where_clause  # WHERE 子句
)

# 生成 SQL
sql = select_query.sql()
print(sql)