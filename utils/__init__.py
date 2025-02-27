import os
import sqlglot
from utils.config import CONFIG

def get_sql(path: str) -> str:
    path = os.path.join(CONFIG.SELECT_PATH, path)
    with open(path, mode="r", encoding="utf-8") as file:
        sql_str = file.read()
    sqlglot.parse_one(sql_str)
    return sql_str