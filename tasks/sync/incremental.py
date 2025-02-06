import os
import json
import pandas as pd
import sqlalchemy
import sqlglot
from sqlglot import exp
from utils import CONFIG, CONNECTER, LOCALDB
from tasks.base import task, task_connect_with

class sql_make:
    '''sql生成模块'''
    def __init__(self, name: str):
        ...
        
    def make_source_select(self, sql_path: str, connect_name: str) -> str:
        '''生成用于查询来源系统变更信息的sql'''
        ...

    @staticmethod
    def get_ast(sql_path: str, connect_name: str) -> sqlglot.Expression:
        '''获取对应sql语句的语法树'''
        with open(os.path.join(CONFIG.SELECT_PATH, sql_path), mode="r", encoding="utf-8") as file:
            sql_str = file.read()
        return sqlglot.parse_one(sql_str, read=CONFIG.CONNECT_CONFIG[connect_name]["type"])

    @staticmethod
    def get_name_correspondence(sql_ast: sqlglot.Expression, config: dict) -> list:
        '''从sql和配置文件中获取列名、别名和缓存名字的对应关系'''
        column_correspond = []
        index = 0
        for expr in sql_ast.find(exp.Select).expressions:
            if isinstance(expr, exp.Alias):
                column_name = expr.this
                alias = expr.alias
                if str(column_name) in config["attention"]["colnmns"]:
                    column_correspond.append([column_name, alias, "colnmns_"+str(index)])
                    index += 1
                elif str(column_name) == config["attention"]["id"]:
                    column_correspond.append([column_name, alias, "id"])
            elif isinstance(expr, exp.Column):
                if str(expr) in config["attention"]["colnmns"]:
                    column_correspond.append([expr, expr, "colnmns_"+str(index)])
                    index += 1
                elif str(column_name) == config["attention"]["id"]:
                    column_correspond.append([expr, expr, "id"])
        if len(column_correspond):
            raise ValueError("未找到对应的语句")
        return column_correspond

class incremental_task(task):
    '''所有增量任务的基类，额外添加了sql生成的模块'''
    def __init__(self, name):
        super().__init__(name)
        self.make = sql_make(self.name)


class extract_sql(incremental_task):
    '''通过sql的增量抽取到本地存储'''
    ...
    

class load_table(incremental_task):
    '''将本地存储增量加载到其他数据源'''
    ...
    
    
class sync_sql_incremental(incremental_task):
    '''
    通过sql的增量更新同步

    注意：不支持同步到当前的duckDB中，对于增量同步数据不会分片而是全量写入，请注意大小
    '''

    def __init__(self,
                 name: str,
                 source_incremental_name: str,
                 target_table_name: str,
                 source_connect_name: str,
                 target_connect_name: str,
                 target_connect_schema: str | None = None,
                 ):
        super().__init__(name)
        self.target_table_name = target_table_name
        self.target_connect_schema = target_connect_schema
        self.source_connect_name = source_connect_name
        self.target_connect_name = target_connect_name

        if not CONFIG.CONNECT_CONFIG[target_connect_name]["write_enable"]:
            raise ValueError(target_connect_name + ": 该数据源不应当作为目标，因为其不支持写入")
        self.log.info("正在进行sql的预构建")
        self.make_sql(source_incremental_name, source_connect_name)
        self.log.info("sql构建完成")
        self.source_client: sqlalchemy.engine.Engine = CONNECTER[source_connect_name]
        self.target_client: sqlalchemy.engine.Engine = CONNECTER[source_connect_name]

    def make_sql(self, source_incremental_name, source_connect_name):
        '''根据配置文件生成运行过程使用的sql'''
        with open(os.path.join(CONFIG.SELECT_PATH, source_incremental_name + ".sql"), mode="r", encoding="utf-8") as file:
            sql_ast = sqlglot.parse_one(file.read(), read=CONFIG.CONNECT_CONFIG[source_connect_name]["type"])
        with open(os.path.join(CONFIG.SELECT_PATH, source_incremental_name + ".json"), mode="r", encoding="utf-8") as file:
            config = json.load(file)
        # 提取列名、别名和缓存名字的对应关系
        self.column_correspond = self.get_name_correspondence(sql_ast, config)

        # 生成来源的查询语句
        sql_ast.find(exp.Select).set("expressions", [ch[0] for ch in self.column_correspond])
        self.source_incremental_sql_str = sql_ast.sql(pretty=False)
        # 生成目标的查询语句
        target_exp = (
            exp
            .select(exp.Select(expressions=[exp.to_column(column[1]) for column in self.column_correspond]))
            .from_(exp.Table(this=exp.to_identifier(self.target_connect_schema + '.' + self.target_table_name if not self.target_connect_schema is None else self.target_table_name)))
        )
        self.target_incremental_sql_str = sqlglot.Generator(dialect=CONFIG.CONNECT_CONFIG[self.target_connect_name]["type"]).generate(target_exp)
    
    @staticmethod
    def get_name_correspondence(sql_ast: sqlglot.Expression, config: dict)-> None:
        '''获取列名、别名和缓存名字的对应关系'''
        column_correspond = []
        index = 0
        for expr in sql_ast.find(exp.Select).expressions:
            if isinstance(expr, exp.Alias):
                column_name = expr.this
                alias = expr.alias
                if str(column_name) in config["attention"]["colnmns"]:
                    column_correspond.append([column_name, alias, "colnmns_"+str(index)])
                    index += 1
                elif str(column_name) == config["attention"]["id"]:
                    column_correspond.append([column_name, alias, "id"])
            elif isinstance(expr, exp.Column):
                if str(expr) in config["attention"]["colnmns"]:
                    column_correspond.append([expr, expr, "colnmns_"+str(index)])
                    index += 1
                elif str(column_name) == config["attention"]["id"]:
                    column_correspond.append([expr, expr, "id"])
        if len(column_correspond):
            raise ValueError("未找到对应的语句")
        return column_correspond
    
    def make_source_select(self ):
        '''生成来源的查询语句'''
        pass
    
    def make_target_select(self ):
        '''生成来源的查询语句'''
        pass

    def task_main(self):
        # 获取增量对比的信息
        self.log.info("读取来源对比数据")
        with self.source_client.connect() as connection:
            with task_connect_with(connection, self.log):
                source_incremental_data = pd.read_sql_query(sqlalchemy.text(self.source_incremental_sql_str), connection)
        self.log.info("读取目标对比数据")
        with self.target_client.connect() as connection:
            with task_connect_with(connection, self.log):
                target_incremental_data = pd.read_sql_query(sqlalchemy.text(self.target_incremental_sql_str), connection)
        self.log.info("对比数据")
        # 给目标数据抽出来的换个名字，保证来源和目标的表列名称相同
        source_incremental_data.rename(columns={column[0]:column[2] for column in self.column_correspond}, inplace=True)
        target_incremental_data.rename(columns={column[1]:column[2] for column in self.column_correspond}, inplace=True)
        m_cursor = LOCALDB.cursor()
        m_cursor.register("source_incremental_data", source_incremental_data)
        m_cursor.register("target_incremental_data", target_incremental_data)
        # 查询新增行
        _subquery = (
            exp.select("id")
            .from_("target_incremental_data")
        )
        _exp = (
            exp
            .select("id")
            .from_("source_incremental_data")
            .where(exp.not_(exp.in_(exp.column("id"), _subquery)))
        )
        _query = sqlglot.Generator(dialect="duckDB").generate(_exp)
        new_ids = m_cursor.execute(_query).fetchdf()['id'].tolist()
        # 查询差异行
        diff_query = """
        SELECT target_incremental_data.id 
        FROM target_incremental_data 
        JOIN source_incremental_data 
        ON target_incremental_data.id = source_incremental_data.id
        WHERE 
        """
        
        parsed = sqlglot.parse_one(self.source_sync_sql_str)
        # 添加 id 作为查询条件
        if diff_ids:
            id_condition = exp.In(
                this=exp.Column(this="id"),
                expressions=[exp.Literal(this=str(id), is_string=False) for id in diff_ids]
            )
            parsed.where = parsed.where.and_(id_condition) if parsed.where else id_condition
        self.source_sync_sql_str = parsed.sql()
        with self.source_client.connect() as connection:
            with task_connect_with(connection, self.log):
                source_incremental_data = pd.read_sql_query(sqlalchemy.text(self.source_incremental_sql_str), connection)

