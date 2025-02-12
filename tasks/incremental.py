import os
import pandas as pd
import sqlglot
from sqlglot.optimizer import optimize
from sqlglot import exp, condition
from dataclasses import dataclass, field
from utils.config import CONFIG
from utils.connect import LOCALDB, CONNECTER
from tasks.base import task, task_connect_with


@dataclass
class incremental_task_options:
    '''增量同步的函数初始化参数类'''
    name: str
    sync_sql_path: str
    sync_source_connect_name: str
    local_table_name: str
    # 用于指定查询模板中哪些列是用于比较数据是否变更的，默认指定第一列为id
    incremental_comparison_list: list[int] = field(default_factory=lambda: [0])
    # 用于指定相关分录的查询模板
    other_entry_sql_path: list[str] = field(default_factory=list)
    local_schema: str = "ods"
    temp_table_schema: str = "temp"
    chunksize: int = 10000


class incremental_task(task):
    '''增量同步，将增量同步相同的部分抽象出来不做多次编写'''

    def __init__(self, input_options: incremental_task_options) -> None:
        super().__init__(input_options.name)
        self.input_options = input_options
        self.ast_make()
        self.source_client = CONNECTER.get_sql(self.input_options.sync_source_connect_name)

    def ast_make(self) -> None:
        '''将任务执行过程中所需要的sql语法树提前在初始化阶段生成好'''
        # 保存查询模板的语法树便于运行时替换
        with open(os.path.join(CONFIG.SELECT_PATH, self.input_options.sync_sql_path), "r", encoding="utf-8") as file:
            sync_sql_ast = sqlglot.parse_one(file.read(), read=CONFIG.CONNECT_CONFIG[self.input_options.sync_source_connect_name]["type"])
        self.sync_sql_str = optimize(sync_sql_ast).sql(dialect=CONFIG.CONNECT_CONFIG[self.input_options.sync_source_connect_name]["type"])
        # 一些简单的检查
        temp_select = sync_sql_ast.find(exp.Select)
        if temp_select is None:
            raise ValueError("传入的sql不是查询语句")
        temp_from = sync_sql_ast.find(exp.From)
        if temp_from is None:
            raise ValueError("传入的sql不是查询语句")
        # 对于超过select列数的incremental_comparison_list会直接跳过
        self.incremental_select_colnums: list[exp.Column | exp.Alias] = []
        for index, select in enumerate(temp_select.expressions):
            if index in self.input_options.incremental_comparison_list:
                self.incremental_select_colnums.append(select)

        # 生成增量的语法树并替换原语法树中的select
        incremental_from: exp.From = sync_sql_ast.find(exp.From).copy() if not sync_sql_ast.find(exp.From) is None else None  # type: ignore 这部分就是上文的temp_from的复制，已经检查过了
        incremental_where = sync_sql_ast.find(exp.Where)
        if not incremental_where is None:
            incremental_where = incremental_where.copy()
        incremental_select_new = exp.Select(expressions=[col.copy() for col in self.incremental_select_colnums])
        incremental_ast = incremental_select_new.from_(incremental_from).where(incremental_where)
        self.incremental_sql_str = optimize(incremental_ast).sql(dialect=CONFIG.CONNECT_CONFIG[self.input_options.sync_source_connect_name]["type"])

        # 保存查询模板的语法树便于运行时替换
        self.other_entry_ast = []
        for other_entry in self.input_options.other_entry_sql_path:
            with open(os.path.join(CONFIG.SELECT_PATH, other_entry), "r", encoding="utf-8") as file:
                self.other_entry_ast.append(sqlglot.parse_one(file.read(), read=CONFIG.CONNECT_CONFIG[self.input_options.sync_source_connect_name]["type"]))

    def where_addition(self, input_ast: sqlglot.Expression, id_name: str, id_list: list[str]) -> str:
        '''在查询语句的where条件中添加id的查询条件'''
        in_condition = exp.In(
            this=exp.column(col=id_name),
            expression=exp.Tuple(expressions=sorted(id_list))
        )
        where_expr = input_ast.find(exp.Where)
        if where_expr and where_expr.expressions:
            new_where = exp.And(this=where_expr.this, expression=in_condition)
        else:
            new_where = in_condition
        input_ast.set("where", new_where)
        return optimize(input_ast).sql(dialect=CONFIG.CONNECT_CONFIG[self.input_options.sync_source_connect_name]["type"])

    def task_main(self) -> None:
        self.log.info("运行增量检查")
        data_group = None
        with task_connect_with(self.source_client, self.log) as connection:
            data_group = pd.read_sql_query(sqlalchemy.text(self.sql_str), connection, chunksize=self.chunksize)

        self.log.info("比较相关数据")
        with LOCALDB.cursor() as m_cursor:
            source_data_struct = m_cursor.execute(
                f'''
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{temp_target_name}' AND table_schema = 'm_temp';
                '''
            ).fetchdf()
            local_data_struct = m_cursor.execute(
                f'''
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{temp_target_name}' AND table_schema = 'ods';
                '''
            ).fetchdf()
            print(source_data_struct)
            print(local_data_struct)
            # 如果来源的表结构发生了变更
            if not source_data_struct.equals(local_data_struct):
                # 直接变为全量同步
                ...
            else:
                id_name = source_data_struct['column_name'][0]
                # 查询来源中新增的 id（即表 B 不存在的 id）默认第一个是id
                df_new = m_cursor.execute(
                f"""
                    SELECT a.{id_name}
                    FROM m_temp.{temp_target_name} AS a
                    LEFT JOIN ods.{temp_target_name} AS b ON a.{id_name} = b.{id_name}
                    WHERE b.{id_name} IS NULL;
                """).fetchdf()
                # 查询没有新增但是变更的了的来源id
                other_names = source_data_struct['column_name'][1:]
                query_sql = sqlglot.parse_one(
                    sql=f"""
                        SELECT a.{id_name}
                        FROM m_temp.{temp_target_name} AS a
                        LEFT JOIN ods.{temp_target_name} AS b ON a.{id_name} = b.{id_name}
                    """,
                    read="duckDB"
                )
                temp_where = condition(f'''a.{other_names[0]} <> b.{other_names[0]}''')
                for other_name in other_names[1:]:
                    temp_where.or_(f'''a.{other_name} <> b.{other_name}''')
                query_sql.set("where", exp.Where(this=temp_where))
                print(query_sql)

        self.log.info("动态生成sql")

        self.log.info("运行同步增量的任务")
