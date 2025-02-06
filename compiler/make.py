import os
import sqlglot
from sqlglot import exp
from utils import CONFIG


class sql_make:
    '''给增量抽取或者增量写入生成sql'''
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
