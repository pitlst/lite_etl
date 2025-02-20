import os
import duckdb
import sqlalchemy
import pandas as pd
import sqlglot
from sqlglot.optimizer import optimize
from sqlglot import exp, condition
from dataclasses import dataclass, field
from utils.config import CONFIG
from utils.connect import CONNECTER
from tasks.base import task, task_connect_with
from tasks.sync import extract_sql


@dataclass
class incremental_task_options:
    """增量同步的函数初始化参数类"""
    name: str
    sync_sql_path: str
    sync_source_connect_name: str
    local_table_name: str
    # 用于指定查询模板中哪些列是用于比较数据是否变更的，默认指定第一列为id
    incremental_comparison_list: list[int] = field(default_factory=lambda: [0])
    # 用于指定相关分录的查询模板
    other_entry_sql_path: dict[str, str] = field(default_factory=dict)
    local_schema: str = "ods"
    temp_table_schema: str = "m_temp"
    # 如果单次新增或修改数超过chunksize，则会退化为全量同步
    chunksize: int = 10000
    # 对于查询中没有的数据是否删除
    is_delete: bool = False


class incremental_task(task):
    """增量同步，将增量同步相同的部分抽象出来不做多次编写"""

    def __init__(self, input_options: incremental_task_options) -> None:
        super().__init__(input_options.name)
        self.options = input_options
        # 读取并生成sql的语法树
        self.ast_make()
        with CONNECTER.get_local() as m_cursor:
            # 确保本地schema存在
            self.schema_make(m_cursor)
            # 获取当前本地id的缓存
            self.update_local(m_cursor)
        self.source_client = CONNECTER.get_sql(self.options.sync_source_connect_name)

    def ast_make(self) -> None:
        """将任务执行过程中所需要的sql语法树提前在初始化阶段生成好"""
        # 保存主表查询模板的语法树便于运行时替换
        db_type = CONFIG.CONNECT_CONFIG[self.options.sync_source_connect_name]["type"]
        with open(os.path.join(CONFIG.SELECT_PATH, self.options.sync_sql_path), "r", encoding="utf-8") as file:
            self.sync_sql_ast = sqlglot.parse_one(file.read(), read=db_type)
        self.sync_sql_str = optimize(self.sync_sql_ast, dialect=db_type).sql(dialect=db_type)
        # 保存其他分录查询模板的语法树便于运行时替换
        self.other_entry_ast = {}
        for other_entry_key, other_entry_value in self.options.other_entry_sql_path.items():
            with open(os.path.join(CONFIG.SELECT_PATH, other_entry_value), "r", encoding="utf-8") as file:
                self.other_entry_ast[other_entry_key] = sqlglot.parse_one(file.read(), read=db_type)

        # 对于超过select列数的incremental_comparison_list会直接跳过
        self.incremental_select_colnums: list[exp.Column | exp.Alias] = []
        for index, select in enumerate(self.sync_sql_ast.find(exp.Select).expressions):  # type: ignore 已经检查过了
            if index in self.options.incremental_comparison_list:
                self.incremental_select_colnums.append(select)
        if len(self.incremental_select_colnums) == 0:
            raise ValueError("获取用于对比的增量id发生错误")

        # 生成增量的语法树并替换原语法树中的select
        incremental_from: exp.From = self.sync_sql_ast.find(exp.From).copy() if not self.sync_sql_ast.find(exp.From) is None else None  # type: ignore 已经检查过了
        incremental_where = self.sync_sql_ast.find(exp.Where)
        if not incremental_where is None:
            incremental_where = incremental_where.copy()
        incremental_ast = exp.Select(expressions=[col.copy() for col in self.incremental_select_colnums])
        incremental_ast.set("from", incremental_from)
        incremental_ast.set("where", incremental_where)
        self.incremental_sql_str = optimize(incremental_ast, dialect=db_type).sql(dialect=db_type)

    def update_local(self, m_cursor: duckdb.DuckDBPyConnection) -> None:
        """获取当前本地id的缓存"""
        self.local_id_struct = self.get_table_struct(m_cursor, self.options.local_schema, self.options.local_table_name + "_id")
        self.id_name = str(self.local_id_struct['column_name'][0]) if len(self.local_id_struct) != 0 else None

    def schema_make(self, m_cursor: duckdb.DuckDBPyConnection) -> None:
        """确保对应的schema存在"""
        m_cursor.execute(
            f'''
            CREATE SCHEMA IF NOT EXISTS "{self.options.local_schema}"
            '''
        )
        m_cursor.execute(
            f'''
            CREATE SCHEMA IF NOT EXISTS "{self.options.temp_table_schema}"
            '''
        )

    @staticmethod
    def where_add(input_ast: exp.Expression, id_name: exp.Column | exp.Alias, id_list: list[str], dialect: str | None = None) -> str:
        """对语法树添加筛选条件并生成"""
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

    @staticmethod
    def get_table_struct(m_cursor: duckdb.DuckDBPyConnection, schema: str, table: str) -> pd.DataFrame:
        """获取本地表的表结构"""
        return m_cursor.execute(
            f'''
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table}' AND table_schema = '{schema}'
            '''
        ).fetchdf()

    def replace_id(self, m_cursor: duckdb.DuckDBPyConnection) -> None:
        """从缓存替换索引"""
        m_cursor.execute(
            f'''
            CREATE OR REPLACE TABLE "{self.options.local_schema}"."{self.options.local_table_name}_id" AS
            SELECT * from "{self.options.temp_table_schema}"."{self.options.local_table_name}_id"
            '''
        )

    def get_new(self, m_cursor: duckdb.DuckDBPyConnection) -> bool:
        """获取新出现的数据"""
        df_new = m_cursor.execute(
            f'''
            SELECT a."{self.id_name}"
            FROM "{self.options.temp_table_schema}"."{self.options.local_table_name}_id" AS a
            LEFT JOIN "{self.options.local_schema}"."{self.options.local_table_name}_id" AS b ON a."{self.id_name}" = b."{self.id_name}"
            WHERE b."{self.id_name}" IS NULL
            '''
        ).fetchdf()
        sync_add_sql_str = self.where_add(
            self.sync_sql_ast,
            self.incremental_select_colnums[0],
            df_new[self.id_name].astype(str).tolist(),
            dialect=CONFIG.CONNECT_CONFIG[self.options.sync_source_connect_name]["type"]
        )
        data_sync_add = None
        with task_connect_with(self.source_client, self.log) as connection:
            data_sync_add = pd.read_sql_query(sqlalchemy.text(sync_add_sql_str), connection)
        # 检查是否获取到数据
        if data_sync_add is None:
            return False
        # 检查获取到的数据大小是否超过最大
        if len(data_sync_add) > self.options.chunksize:
            return False

        # 检查这里获取的数据类型是否和本地的缓存相同，以保证直接插入
        m_cursor.execute(
            f'''
            CREATE OR REPLACE TABLE "{self.options.temp_table_schema}"."{self.options.local_table_name}_add" AS
            SELECT * from data_sync_add
            '''
        ).fetchdf()
        local_struct = self.get_table_struct(m_cursor, self.options.local_schema, self.options.local_table_name)
        source_struct = self.get_table_struct(m_cursor, self.options.temp_table_schema, self.options.local_table_name + "_add")
        if local_struct.equals(source_struct):
            return False
        return True

    def get_diff(self, m_cursor: duckdb.DuckDBPyConnection) -> bool:
        """获取出现变更的数据"""
        query_sql = sqlglot.parse_one(
            sql=f'''
            SELECT a."{self.id_name}"
            FROM "{self.options.temp_table_schema}"."{self.options.local_table_name}_id" AS a
            LEFT JOIN "{self.options.local_schema}."{self.options.local_table_name}_id" AS b ON a."{self.id_name}" = b."{self.id_name}"
            WHERE NOT b."{self.id_name}" IS NULL
            ''',
            read="duckdb"
        )
        temp_where = query_sql.find(exp.Where) 
        for other_name in self.local_id_struct['column_name'][1:]:
            temp_where = exp.Or(this=temp_where.this, expression=condition(f'''a.{other_name} <> b.{other_name}''')) # type: ignore
        query_sql.set("where", exp.Where(this=temp_where))
        df_diff = m_cursor.execute(optimize(query_sql).sql(dialect="duckdb")).fetchdf()
        sync_diff_sql_str = self.where_add(
            self.sync_sql_ast,
            self.incremental_select_colnums[0],
            df_diff[self.id_name].astype(str).tolist(),
            dialect=CONFIG.CONNECT_CONFIG[self.options.sync_source_connect_name]["type"]
        )
        data_sync_diff = None
        with task_connect_with(self.source_client, self.log) as connection:
            data_sync_diff = pd.read_sql_query(sqlalchemy.text(sync_diff_sql_str), connection)
        # 检查是否获取到数据
        if data_sync_diff is None:
            return False
        # 检查获取到的数据大小是否超过最大
        if len(data_sync_diff) > self.options.chunksize:
            return False

        # 检查这里获取的数据类型是否和本地的缓存相同，以保证直接插入
        m_cursor.execute(
            f'''
            CREATE OR REPLACE TABLE "{self.options.temp_table_schema}"."{self.options.local_table_name}_diff" AS
            SELECT * from data_sync_diff
            '''
        ).fetchdf()
        local_struct = self.get_table_struct(m_cursor, self.options.local_schema, self.options.local_table_name)
        source_struct = self.get_table_struct(m_cursor, self.options.temp_table_schema, self.options.local_table_name + "_diff")
        if local_struct.equals(source_struct):
            return False
        return True

    def total_sync(self) -> None:
        """全量同步"""
        self.then(extract_sql(
            name=self.options.name + "_total",
            source_sql_or_path=self.sync_sql_str,
            target_table_name=self.options.local_table_name,
            source_connect_name=self.options.sync_source_connect_name,
            target_connect_schema=self.options.local_schema,
            chunksize=self.options.chunksize
        ))
        for temp_ast_key, temp_ast_value in self.other_entry_ast.items():
            self.then(extract_sql(
                name=self.options.name + "_" + temp_ast_key,
                source_sql_or_path=optimize(temp_ast_value).sql(dialect=CONFIG.CONNECT_CONFIG[self.options.sync_source_connect_name]["type"]),
                target_table_name=self.options.local_table_name + "_" + temp_ast_key,
                source_connect_name=self.options.sync_source_connect_name,
                target_connect_schema=self.options.local_schema,
                chunksize=self.options.chunksize
            ))
    
    def trans_total_sync(self, m_cursor: duckdb.DuckDBPyConnection) -> None:
        """将任务转换成全量同步"""
        self.total_sync()
        self.log.info("替换对应id索引")
        self.replace_id(m_cursor)
        self.update_local(m_cursor)

    def delete_sync(self, m_cursor: duckdb.DuckDBPyConnection) -> None:
        """删除多余的数据"""
        df_disapp = m_cursor.execute(
            f'''
            SELECT a."{self.id_name}"
            FROM "{self.options.local_schema}"."{self.options.local_table_name}_id" AS a
            LEFT JOIN "{self.options.temp_table_schema}"."{self.options.local_table_name}_id" AS b ON a.{self.id_name} = b.{self.id_name}
            WHERE b."{self.id_name}" IS NULL
            '''
        ).fetchdf()
        if len(df_disapp) != 0:
            delete_sql = sqlglot.parse_one(
                sql=f'''DELETE FROM "{self.options.local_schema}"."{self.options.local_table_name}"''',
                read="duckdb"
            )
            delete_sql = self.where_add(
                delete_sql,
                exp.Column(this=self.id_name),
                df_disapp[self.id_name].astype(str).tolist(),
                dialect="duckdb"
            )
            m_cursor.execute(delete_sql)
        else:
            self.log.debug("无删除数据")

    def task_main(self) -> None:
        self.log.info("运行增量检查")
        data_group = None
        with task_connect_with(self.source_client, self.log) as connection:
            data_group = pd.read_sql_query(sqlalchemy.text(self.incremental_sql_str), connection)
        if data_group is None:
            raise ValueError("未查询到数据或者连接失败")

        self.log.info("比较相关数据")
        with CONNECTER.get_local() as m_cursor:
            m_cursor.execute(
                f'''
                CREATE OR REPLACE TABLE "{self.options.temp_table_schema}"."{self.options.local_table_name}_id" AS SELECT * FROM data_group
                '''
            )
            # 对于本地根本就没有相关表的直接退化为全量同步
            if self.id_name is None:
                self.log.info("本地无缓存第一次同步，转化为全量同步")
                self.trans_total_sync(m_cursor)
                return
            source_id_struct = self.get_table_struct(m_cursor, self.options.temp_table_schema, self.options.local_table_name + "_id")
            # 用于索引是否增量的结构发生变化直接变换为全量同步
            if len(source_id_struct) == 0 or len(self.local_id_struct) == 0 or not self.local_id_struct.equals(source_id_struct):
                self.log.info("结构发生变化，转化为全量同步")
                self.trans_total_sync(m_cursor)
                return

            self.log.info("运行增量数据同步")
            # 这两个没用使用到的变量是通过duckDB在内存中直接引用的，所以需要创建并且不能改变名称
            # 如果出现结构不同或者sql无法获取到数据则退化为全量同步
            # 异常仍会正常抛出
            if not self.get_new(m_cursor):
                self.log.info("结构发生变化或变更数过大，转化为全量同步")
                self.trans_total_sync(m_cursor)
                return
            if not self.get_diff(m_cursor):
                self.log.info("结构发生变化或变更数过大，转化为全量同步")
                self.trans_total_sync(m_cursor)
                return

            self.log.info("运行本地数据更改")
            # 先添加变更的数据
            m_cursor.execute(
                f'''
                INSERT INTO "{self.options.local_schema}"."{self.options.local_table_name}"
                SELECT * FROM "{self.options.temp_table_schema}"."{self.options.local_table_name}_add"
                '''
            )
            # 然后更新变更的数据
            column_name_list = self.get_table_struct(m_cursor, self.options.local_schema, self.options.local_table_name)["column_name"].astype(str).tolist()[1:]
            temp_sql = ",".join([f"\"{column_name}\" = \"{self.options.temp_table_schema}\".\"{self.options.local_table_name}_diff\".\"{column_name}\"" for column_name in column_name_list])
            m_cursor.execute(
                f'''
                UPDATE "{self.options.local_schema}"."{self.options.local_table_name}"
                SET ''' + temp_sql + f'''
                FROM "{self.options.temp_table_schema}"."{self.options.local_table_name}_diff"
                WHERE "{self.options.local_schema}"."{self.options.local_table_name}"."{self.id_name}" = "{self.options.temp_table_schema}"."{self.options.local_table_name}_diff"."{self.id_name}"
                '''
            )

            if not self.options.is_delete:
                self.log.info("替换对应id索引")
                # 如果不删除旧有的多余数据，就重新从更新后的数据中提取id表
                local_select_new = exp.Select(
                    expressions=[exp.Column(this=col.alias) if isinstance(col, exp.Alias) else col for col in self.incremental_select_colnums]
                ).from_(f"{self.options.local_schema}.{self.options.local_table_name}")
                m_cursor.execute(
                    f'''
                    CREATE OR REPLACE TABLE "{self.options.local_schema}"."{self.options.local_table_name}_id" AS
                    ''' + optimize(local_select_new).sql(dialect="duckdb")
                )
            else:
                self.log.info("删除多余的数据")
                self.delete_sync(m_cursor)
                # 最后直接使用缓存替换所有的id
                self.log.info("替换对应id索引")
                self.replace_id(m_cursor)