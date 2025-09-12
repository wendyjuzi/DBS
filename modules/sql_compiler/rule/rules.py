# sql_rules.py
KEYWORDS = {
    "SELECT", "FROM", "WHERE", "CREATE", "TABLE", "INSERT", "INTO", "VALUES", "DELETE",
    "UPDATE", "SET", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "ON", 
    "ORDER", "BY", "GROUP", "ASC", "DESC", "HAVING", "PRIMARY", "KEY", "FOREIGN",
    "REFERENCES", "CONSTRAINT", "NOT", "NULL", "UNIQUE", "DROP",
    # 事务控制
    "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "WORK",
    # 索引相关
    "INDEX", "UNIQUE", "BTREE", "HASH", "USING",
    # 逻辑操作符
    "AND", "OR", "NOT",
    # 范围查询
    "BETWEEN", "IN", "LIKE",
    # 聚合函数
    "COUNT", "SUM", "AVG", "MAX", "MIN",
    # 其他
    "DISTINCT", "ALL", "AS"
}

class Column:
    def __init__(self, name, type_):
        self.name = name
        self.type = type_

class Table:
    def __init__(self, name):
        self.name = name
        self.columns = []

# 初始化 Catalog
catalog = {}

# 符号表
symbol_table = {
    "tables": {}  # key: table_name, value: Table对象
}

# 常量表示
class Constant:
    def __init__(self, value, type_):
        self.value = value
        self.type = type_
