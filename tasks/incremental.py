import sqlglot
from sqlglot.optimizer import optimize
from sqlglot import exp
from dataclasses import dataclass
from utils.config import CONFIG
from utils.connect import LOCALDB
from tasks.base import task
from tasks.sync import extract_sql


@dataclass
class incremental_task_options:
    '''增量同步的函数初始化参数类'''
    name: str
    sync_sql_path: str
    sync_source_connect_name: str
    local_table_name: str
    local_schema: str = "ods"
    temp_table_schema: str = "temp"
    chunksize: int = 10000
    # 用于指定查询模板中哪些列是用于比较数据是否变更的
    incremental_comparison_list: list[int] = [0]
    # 用于指定相关分录的查询模板
    other_entry_sql_path: list[str] = []


class incremental_task(task):
    '''增量同步，将增量同步相同的部分抽象出来不做多次编写'''

    def __init__(self, input_options: incremental_task_options) -> None:
        super().__init__(input_options.name)
        self.input_options = input_options
        self.ast_make()

    def ast_make(self) -> None:
        '''将任务执行过程中所需要的sql语法树提前在初始化阶段生成好'''
        # 保存查询模板的语法树便于运行时替换
        with open(self.input_options.sync_sql_path, "r", encoding="utf-8") as file:
            self.sync_sql_ast = sqlglot.parse_one(file.read(), read=CONFIG.CONNECT_CONFIG[self.input_options.sync_source_connect_name]["type"])

        # 对于超过select列数的incremental_comparison_list会直接跳过
        incremental_select = []
        self.incremental_select_colnums_name = []
        for index, select in enumerate(self.sync_sql_ast.find_all(exp.Select)):
            if index in self.input_options.incremental_comparison_list:
                incremental_select.append(select)
                self.incremental_select_colnums_name.append(select.alias or select.name)

        # 生成增量的语法树并替换原语法树中的select
        self.incremental_ast = self.sync_sql_ast.copy()
        self.incremental_ast.find(exp.Select).set("expressions", incremental_select)
        self.incremental_sql_str = optimize(self.incremental_ast).sql(dialect=CONFIG.CONNECT_CONFIG[self.input_options.sync_source_connect_name]["type"])

        # 保存查询模板的语法树便于运行时替换
        self.other_entry_ast = []
        for other_entry in self.input_options.other_entry_sql_path:
            with open(other_entry, "r", encoding="utf-8") as file:
                self.other_entry_ast.append(sqlglot.parse_one(file.read(), read=CONFIG.CONNECT_CONFIG[self.input_options.sync_source_connect_name]["type"]))

    def where_addition(self, input_ast: sqlglot.Expression, id_name: str, id_list: list[str]) -> str:
        '''在查询语句的where条件中添加id的查询条件'''
        in_condition = exp.In(exp.column(col=id_name), tuple(sorted(id_list)))
        where_expr: exp.Where = input_ast.args.get("where")
        if where_expr and where_expr.expressions:
            new_where = exp.And(this=where_expr.this, expression=in_condition)
        else:
            new_where = in_condition
        input_ast.set("where", new_where)
        return input_ast.sql(dialect=CONFIG.CONNECT_CONFIG[self.input_options.sync_source_connect_name]["type"])

    def task_main(self) -> None:
        self.log.info("运行增量检查的子任务")
        temp_target_name = self.input_options.local_table_name + "_incremental"
        source_incremental_task = extract_sql(
            name=self.name + "_incremental",
            source_sql_or_path=self.incremental_sql_str,
            target_table_name=temp_target_name,
            source_connect_name=self.input_options.sync_source_connect_name,
            target_connect_schema="temp"
        )
        source_incremental_task.task_main()
        
        self.log.info("比较相关数据")
        with LOCALDB.cursor() as m_cursor:
            print(m_cursor.execute(f"SELECT * FROM temp.{temp_target_name}").fetchdf())
            # 查询表 A 中新增的 id（即表 B 不存在的 id）
            # df_new = m_cursor.execute(
            # f"""
            #     SELECT a.id
            #     FROM temp.{temp_target_name} AS a
            #     LEFT JOIN ods.{temp_target_name} AS b ON a.id = b.id
            #     WHERE b.id IS NULL;
            # """).fetchdf()
            
            # df_diff = m_cursor.execute(
            # f"""
            #     SELECT a.id
            #     FROM temp.{temp_target_name}_incremental AS a
            #     LEFT JOIN ods.{temp_target_name}_incremental AS b ON a.id = b.id
            #     WHERE a.id IS NULL OR b.id IS NULL OR a.name != b.name OR a.value != b.value;
            # """).fetchdf()
                        
                        
        self.log.info("动态生成sql")

        self.log.info("运行同步增量的任务")
        