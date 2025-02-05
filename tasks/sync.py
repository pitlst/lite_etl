import os
import json
import pandas as pd
import sqlalchemy
import sqlglot
from sqlglot import exp
from utils import CONFIG, CONNECTER, LOCALDB
from tasks import task, task_connect_with


class extract_sql_total(task):
    '''通过sql的全量抽取到本地存储'''
    def __init__(self, 
                 name: str,
                 source_sql_path: str,
                 target_table_name: str,
                 source_connect_name: str,
                 chunksize: int = 10000
        ):
        super().__init__(name)
        self.target_table_name = target_table_name
        self.chunksize = chunksize


class load_table_total(task):
    '''将本地存储全量加载到其他数据源'''
    ...


class extract_sql_incremental(task):
    '''通过sql的增量抽取到本地存储'''
    ...
    

class load_table_incremental(task):
    '''将本地存储增量加载到其他数据源'''
    ...


class sync_sql_total(task):
    '''
    通过sql的全量更新同步

    注意：该类的全量更新通过pandas实现，其数据会以批量的方式写入内存再写出
    '''

    def __init__(self,
                 name: str,
                 source_sql_path: str,
                 target_table_name: str,
                 source_connect_name: str,
                 target_connect_name: str,
                 target_connect_schema: str | None = None,
                 chunksize: int = 10000
                 ):
        super().__init__(name)
        self.target_table_name = target_table_name
        self.target_connect_schema = target_connect_schema
        self.chunksize = chunksize
        self.sql_str = self.check_sql(source_sql_path, source_connect_name)

        if not CONFIG.CONNECT_CONFIG[target_connect_name]["write_enable"]:
            raise ValueError(target_connect_name + ": 该数据源不应当作为目标，因为其不支持写入")

        self.source_client: sqlalchemy.engine.Engine = CONNECTER[source_connect_name]
        self.target_client: sqlalchemy.engine.Engine = CONNECTER[target_connect_name]

    @staticmethod
    def check_sql(source_sql_path: str, source_connect_name: str) -> str:
        '''从文件读取并检查用于查询的sql是否正确'''
        with open(os.path.join(CONFIG.SELECT_PATH, source_sql_path), mode="r", encoding="utf-8") as file:
            sql_str = file.read()
        sqlglot.parse_one(sql_str, read=CONFIG.CONNECT_CONFIG[source_connect_name]["type"])
        return sql_str

    def task_main(self):
        self.log.info("读取数据")
        with self.source_client.connect() as connection:
            with task_connect_with(connection, self.log):
                data_group = pd.read_sql_query(sqlalchemy.text(self.sql_str), connection, chunksize=self.chunksize)

        with self.target_client.connect() as connection:
            with task_connect_with(connection, self.log):
                self.log.info("检查目标数据库是存在目标表")
                if sqlalchemy.inspect(connection).has_table(self.target_table_name, schema=self.target_connect_schema):
                    self.log.info("存在目标表，正在清空......")
                    if not self.target_connect_schema is None:
                        connection.execute(sqlalchemy.text(f"TRUNCATE TABLE {self.target_connect_schema}.{self.target_table_name}"))
                    else:
                        connection.execute(sqlalchemy.text(f"TRUNCATE TABLE {self.target_table_name}"))
                    connection.commit()
                else:
                    self.log.info("不存在目标表")
                self.log.info("写入数据")
                # 实际上这里插入语句的生成是借助pandas的tosql函数
                for data in data_group:
                    data.to_sql(name=self.target_table_name, con=connection, schema=self.target_connect_schema, index=False, if_exists='append')


class sync_sql_incremental(task):
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




class extract_nosql(task):
    '''
    将nosql的文档型数据源抽取到本地存储，并附带一个默认的转换

    注意：因为数据格式的不同，目前不支持增量
    '''
    ...
