"""
自定义异常类
"""


class DatabaseError(Exception):
    """通用数据库错误。"""


class SQLSyntaxError(DatabaseError):
    """SQL语法错误。"""


class ExecutionError(DatabaseError):
    """执行阶段错误。"""


class StorageError(DatabaseError):
    """存储层错误。"""


class CatalogError(DatabaseError):
    """系统目录错误。"""
