import os
import sqlalchemy
import pandas as pd
import sqlglot
from sqlglot.optimizer import optimize
from sqlglot import exp, condition
from dataclasses import dataclass, field
from utils.config import CONFIG
from utils.connect import LOCALDB, CONNECTER
from tasks.base import task, task_connect_with
from tasks.sync import extract_sql


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
    temp_table_schema: str = "m_temp"
    chunksize: int = 10000
    # 对于查询中没有的数据是否删除
    is_delete: bool = False


class incremental_task(task):
    '''增量同步，将增量同步相同的部分抽象出来不做多次编写'''

    def __init__(self, input_options: incremental_task_options) -> None:
        super().__init__(input_options.name)
        self.options = input_options
        self.ast_make()
        self.schema_make()
        self.source_client = CONNECTER.get_sql(self.options.sync_source_connect_name)

    def ast_make(self) -> None:
        '''将任务执行过程中所需要的sql语法树提前在初始化阶段生成好'''
        # 保存查询模板的语法树便于运行时替换
        with open(os.path.join(CONFIG.SELECT_PATH, self.options.sync_sql_path), "r", encoding="utf-8") as file:
            self.sync_sql_ast = sqlglot.parse_one(file.read(), read=CONFIG.CONNECT_CONFIG[self.options.sync_source_connect_name]["type"])
        self.sync_sql_str = optimize(self.sync_sql_ast).sql(dialect=CONFIG.CONNECT_CONFIG[self.options.sync_source_connect_name]["type"])
        # 一些简单的检查
        temp_select = self.sync_sql_ast.find(exp.Select)
        if temp_select is None:
            raise ValueError("传入的sql不是查询语句")
        temp_from = self.sync_sql_ast.find(exp.From)
        if temp_from is None:
            raise ValueError("传入的sql不是查询语句")
        # 对于超过select列数的incremental_comparison_list会直接跳过
        self.incremental_select_colnums: list[exp.Column | exp.Alias] = []
        for index, select in enumerate(temp_select.expressions):
            if index in self.options.incremental_comparison_list:
                self.incremental_select_colnums.append(select)

        # 生成增量的语法树并替换原语法树中的select
        incremental_from: exp.From = self.sync_sql_ast.find(exp.From).copy() if not self.sync_sql_ast.find(exp.From) is None else None  # type: ignore 这部分就是上文的temp_from的复制，已经检查过了
        incremental_where = self.sync_sql_ast.find(exp.Where)
        if not incremental_where is None:
            incremental_where = incremental_where.copy()
        incremental_select_new = exp.Select(expressions=[col.copy() for col in self.incremental_select_colnums])
        incremental_ast = incremental_select_new.from_(incremental_from).where(incremental_where)
        self.incremental_sql_str = optimize(incremental_ast).sql(dialect=CONFIG.CONNECT_CONFIG[self.options.sync_source_connect_name]["type"])

        # 保存查询模板的语法树便于运行时替换
        self.other_entry_ast = []
        for other_entry in self.options.other_entry_sql_path:
            with open(os.path.join(CONFIG.SELECT_PATH, other_entry), "r", encoding="utf-8") as file:
                self.other_entry_ast.append(sqlglot.parse_one(file.read(), read=CONFIG.CONNECT_CONFIG[self.options.sync_source_connect_name]["type"]))
                
    def schema_make(self) -> None:
        '''确保对应的schema存在'''
        with LOCALDB.cursor() as m_cursor:
            m_cursor.execute(
                f'''
                CREATE SCHEMA IF NOT EXISTS {self.options.local_schema}
                '''
            )
            m_cursor.execute(
                f'''
                CREATE SCHEMA IF NOT EXISTS {self.options.temp_table_schema}
                '''
            )
    
    @staticmethod
    def where_add(input_ast: exp.Expression, id_name: exp.Column | exp.Alias, id_list: list[str], dialect: str | None = None)-> str:
        '''对语法树添加筛选条件并生成'''
        if len(id_list) != 0:
            in_condition = exp.In(
                this=id_name.this if isinstance(id_name, exp.Alias) else id_name, 
                expressions=[exp.Literal.string(id_) for id_ in id_list]
            )
        else:
            in_condition = exp.false()
        temp_sync_sql_ast = input_ast.copy()
        where_expr = temp_sync_sql_ast.find(exp.Where)
        if where_expr and where_expr.expressions:
            new_where = exp.And(this=where_expr.this, expression=in_condition)
        else:
            new_where = in_condition
        temp_sync_sql_ast.set("where", exp.Where(this=new_where))
        return optimize(temp_sync_sql_ast).sql(dialect=dialect)

    def task_main(self) -> None:
        self.log.info("运行增量检查")
        data_group = None
        with task_connect_with(self.source_client, self.log) as connection:
            data_group = pd.read_sql_query(sqlalchemy.text(self.incremental_sql_str), connection)
        if data_group is None:
            raise ValueError("未查询到数据或者连接失败")
        
        self.log.info("比较相关数据")
        with LOCALDB.cursor() as m_cursor:
            m_cursor.execute(
                f'''
                CREATE OR REPLACE TABLE {self.options.temp_table_schema}.{self.options.local_table_name}_id AS SELECT * FROM data_group
                '''
            )
            source_data_struct = m_cursor.execute(
                f'''
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{self.options.local_table_name}_id' AND table_schema = '{self.options.temp_table_schema}';
                '''
            ).fetchdf()
            local_data_struct = m_cursor.execute(
                f'''
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{self.options.local_table_name}_id' AND table_schema = '{self.options.local_schema}';
                '''
            ).fetchdf()
            if len(source_data_struct) == 0 or len(local_data_struct) == 0 or not source_data_struct.equals(local_data_struct):
                self.log.info("结构发生变化，转化为全量同步")
                self.log.info("替换对应id索引")
                m_cursor.execute(
                    f'''
                    CREATE OR REPLACE TABLE {self.options.local_schema}.{self.options.local_table_name}_id AS
                    SELECT * from {self.options.temp_table_schema}.{self.options.local_table_name}_id
                    '''
                )
                self.log.info("同步全量数据")
                extract_sql(
                    name=self.options.name + "_total",
                    source_sql_or_path=self.sync_sql_str,
                    target_table_name=self.options.local_table_name,
                    source_connect_name=self.options.sync_source_connect_name,
                    target_connect_schema=self.options.local_schema,
                    chunksize=self.options.chunksize
                ).task_main()
            else:
                self.log.info("动态生成sql")
                # 默认第一列为id
                id_name = str(source_data_struct['column_name'][0])
                
                # 来源中新增的 id 即表 B 不存在的 id
                df_new = m_cursor.execute(
                    f"""
                    SELECT a.{id_name}
                    FROM {self.options.temp_table_schema}.{self.options.local_table_name}_id AS a
                    LEFT JOIN {self.options.local_schema}.{self.options.local_table_name}_id AS b ON a.{id_name} = b.{id_name}
                    WHERE b.{id_name} IS NULL;
                    """
                ).fetchdf()
                sync_add_sql_str = self.where_add(
                    self.sync_sql_ast, 
                    self.incremental_select_colnums[0], 
                    df_new[id_name].astype(str).tolist(), 
                    dialect=CONFIG.CONNECT_CONFIG[self.options.sync_source_connect_name]["type"]
                )
                
                # 查询没有新增但是变更的了的来源id
                query_sql = sqlglot.parse_one(
                    sql=f"""
                    SELECT a.{id_name}
                    FROM {self.options.temp_table_schema}.{self.options.local_table_name}_id AS a
                    LEFT JOIN {self.options.local_schema}.{self.options.local_table_name}_id AS b ON a.{id_name} = b.{id_name}
                    """,
                    read="duckdb"
                )
                temp_where = None
                for other_name in source_data_struct['column_name'][1:]:
                    temp_where = exp.or_(temp_where, condition(f'''a.{other_name} <> b.{other_name}'''))
                query_sql.set("where", exp.Where(this=temp_where))
                df_diff = m_cursor.execute(optimize(query_sql).sql(dialect="duckdb")).fetchdf()
                sync_diff_sql_str = self.where_add(
                    self.sync_sql_ast, 
                    self.incremental_select_colnums[0], 
                    df_diff[id_name].astype(str).tolist(), 
                    dialect=CONFIG.CONNECT_CONFIG[self.options.sync_source_connect_name]["type"]
                )

                self.log.info("运行增量数据同步")
                data_sync_add, data_sync_diff = None, None
                with task_connect_with(self.source_client, self.log) as connection:
                    data_sync_add = pd.read_sql_query(sqlalchemy.text(sync_add_sql_str), connection)
                    data_sync_diff = pd.read_sql_query(sqlalchemy.text(sync_diff_sql_str), connection)
                if data_sync_add is None or data_sync_diff is None:
                    raise ValueError("未查询到数据或者连接失败")
                
                self.log.info("运行本地数据更改")
                # 先添加变更的数据
                m_cursor.execute(
                    f'''
                    INSERT INTO {self.options.local_schema}.{self.options.local_table_name}
                    SELECT * FROM data_sync_add
                    '''
                )
                # 然后更新变更的数据
                column_name_list = m_cursor.execute(
                    f'''
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{self.options.local_table_name}' AND table_schema = '{self.options.local_schema}';
                    '''
                ).fetchdf()["column_name"].astype(str).tolist()
                m_cursor.execute(
                    f'''
                    UPDATE {self.options.local_schema}.{self.options.local_table_name} 
                    SET ''' + ",".join([f"{column_name} = data_sync_diff.{column_name}" for column_name in column_name_list[1:]]) + 
                    f'''
                    FROM data_sync_diff
                    WHERE {self.options.local_schema}.{self.options.local_table_name}.{id_name} = data_sync_diff.{id_name}
                    '''
                )
                
                self.log.info("删除多余的数据")
                if self.options.is_delete:
                    # 查询消失了的id
                    df_disapp = m_cursor.execute(
                        f"""
                        SELECT a.{id_name}
                        FROM {self.options.local_schema}.{self.options.local_table_name}_id AS a
                        LEFT JOIN {self.options.temp_table_schema}.{self.options.local_table_name}_id AS b ON a.{id_name} = b.{id_name}
                        WHERE b.{id_name} IS NULL;
                        """
                    ).fetchdf()
                    delete_sql = sqlglot.parse_one(
                        sql=f'''DELETE FROM {self.options.local_schema}.{self.options.local_table_name}''',
                        read="duckdb"
                    )
                    delete_sql.set("where", exp.Where(this=exp.In(
                            this=exp.Column(expressions=id_name),
                            expression=exp.Tuple(expressions=sorted(df_disapp[id_name].astype(str).tolist()))
                    )))
                    m_cursor.execute(optimize(delete_sql).sql(dialect="duckdb"))
                    
                