# semantic.py

class SemanticError(Exception):
    def __init__(self, error_type, position, message):
        self.error_type = error_type
        self.position = position
        self.message = message

    def __str__(self):
        return f"[{self.error_type}, {self.position}, {self.message}]"


class Catalog:
    """
    模式目录（记录表结构）
    {table_name: {column_name: column_type}}
    """
    def __init__(self):
        self.tables = {}

    def create_table(self, table_name, columns):
        if table_name in self.tables:
            raise SemanticError("TableError", table_name, "表已存在")
        self.tables[table_name] = columns

    def has_table(self, table_name):
        return table_name in self.tables

    def has_column(self, table_name, column_name):
        return self.has_table(table_name) and column_name in self.tables[table_name]

    def get_column_type(self, table_name, column_name):
        return self.tables[table_name].get(column_name, None)


class SemanticAnalyzer:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def analyze(self, ast):
        # 如果传入的是列表，遍历处理
        if isinstance(ast, list):
            for node in ast:
                self._analyze_node(node)
        else:
            # 如果传入的是单个节点，直接处理
            self._analyze_node(ast)
    
    def _analyze_node(self, ast):
        node_type = ast.node_type.upper()
        if node_type == "CREATE_TABLE":
            self._check_create(ast)
        elif node_type == "INSERT":
            self._check_insert(ast)
        elif node_type == "SELECT":
            self._check_select(ast)
        elif node_type == "UPDATE":
            self._check_update(ast)
        elif node_type == "DELETE":
            self._check_delete(ast)

    def _check_create(self, ast):
        table_name = ast.value
        columns = {}
        for child in ast.children:
            col_name = child.value.split(":")[0]
            col_type = child.value.split(":")[1]
            columns[col_name] = col_type
        self.catalog.create_table(table_name, columns)
        print(f"[OK] CREATE TABLE {table_name} 语义检查通过")

    def _check_insert(self, ast):
        table_name = ast.value
        if not self.catalog.has_table(table_name):
            raise SemanticError("TableError", table_name, "表不存在")

        columns = [c.value for c in ast.children if c.node_type == "COLUMN"]
        values = [v.value for v in ast.children if v.node_type == "VALUE"]

        if len(columns) != len(values):
            raise SemanticError("ColumnCountError", table_name, "列数和值数量不一致")

        for col, val in zip(columns, values):
            if not self.catalog.has_column(table_name, col):
                raise SemanticError("ColumnError", col, "列不存在")
            expected_type = self.catalog.get_column_type(table_name, col)
            if expected_type == "INT":
                if not str(val).isdigit():
                    raise SemanticError("TypeError", col, f"期望 INT, 但得到 {val}")
            elif expected_type == "VARCHAR":
                if not isinstance(val, str):
                    raise SemanticError("TypeError", col, f"期望 VARCHAR, 但得到 {val}")

        print(f"[OK] INSERT INTO {table_name} 语义检查通过")

    def _check_select(self, ast):
        # 获取主表和所有涉及的表
        tables = []
        main_table = None
        
        # 查找 FROM 子句
        from_node = next((child for child in ast.children if child.node_type == "FROM"), None)
        if from_node:
            main_table = from_node.value
            if not self.catalog.has_table(main_table):
                raise SemanticError("TableError", main_table, "表不存在")
            tables.append(main_table)
            
            # 检查 JOIN 表
            for join_child in from_node.children:
                if join_child.node_type == "JOIN":
                    join_table = next((c.value for c in join_child.children if c.node_type == "TABLE"), None)
                    if join_table:
                        if not self.catalog.has_table(join_table):
                            raise SemanticError("TableError", join_table, "JOIN 中的表不存在")
                        tables.append(join_table)
                        
                        # 检查 ON 条件
                        on_node = next((c for c in join_child.children if c.node_type == "ON"), None)
                        if on_node:
                            self._check_join_condition(tables, on_node)

        # 检查 SELECT 列
        for child in ast.children:
            if child.node_type == "COLUMN":
                if not self._column_exists_in_tables(tables, child.value):
                    raise SemanticError("ColumnError", child.value, "列不存在于任何表中")

        # 检查 WHERE 子句
        where_node = next((child for child in ast.children if child.node_type == "WHERE"), None)
        if where_node:
            self._check_where_multi_table(tables, where_node)

        # 检查 GROUP BY 子句
        group_by_node = next((child for child in ast.children if child.node_type == "GROUP_BY"), None)
        if group_by_node:
            for group_col in group_by_node.children:
                if group_col.node_type == "COLUMN":
                    if not self._column_exists_in_tables(tables, group_col.value):
                        raise SemanticError("ColumnError", group_col.value, "GROUP BY 中的列不存在")

        # 检查 ORDER BY 子句
        order_by_node = next((child for child in ast.children if child.node_type == "ORDER_BY"), None)
        if order_by_node:
            for sort_col in order_by_node.children:
                if sort_col.node_type == "SORT":
                    col_name = sort_col.value.split(":")[0]
                    if not self._column_exists_in_tables(tables, col_name):
                        raise SemanticError("ColumnError", col_name, "ORDER BY 中的列不存在")

        print(f"[OK] SELECT 语义检查通过")

    def _check_delete(self, ast):
        table_name = ast.value
        if not self.catalog.has_table(table_name):
            raise SemanticError("TableError", table_name, "表不存在")

        for child in ast.children:
            if child.node_type == "WHERE":
                self._check_where(table_name, child)

        print(f"[OK] DELETE FROM {table_name} 语义检查通过")

    def _check_where(self, table_name, where_node):
        left = next((c for c in where_node.children if c.node_type == "LEFT"), None)
        right = next((c for c in where_node.children if c.node_type == "RIGHT"), None)
        op = next((c for c in where_node.children if c.node_type == "OP"), None)

        if left and not self.catalog.has_column(table_name, left.value):
            raise SemanticError("ColumnError", left.value, "WHERE 子句中的列不存在")

        if left:
            expected_type = self.catalog.get_column_type(table_name, left.value)
            if expected_type == "INT" and not str(right.value).isdigit():
                raise SemanticError("TypeError", left.value, f"期望 INT, 但 WHERE 得到 {right.value}")
            if expected_type == "VARCHAR" and isinstance(right.value, int):
                raise SemanticError("TypeError", left.value, f"期望 VARCHAR, 但 WHERE 得到 {right.value}")

    def _check_update(self, ast):
        table_name = ast.value
        if not self.catalog.has_table(table_name):
            raise SemanticError("TableError", table_name, "表不存在")

        # 检查 SET 子句中的列
        for child in ast.children:
            if child.node_type == "ASSIGNMENT":
                col_name, value = child.value.split("=")
                if not self.catalog.has_column(table_name, col_name):
                    raise SemanticError("ColumnError", col_name, "列不存在")
                
                # 检查类型匹配
                expected_type = self.catalog.get_column_type(table_name, col_name)
                if expected_type == "INT":
                    if not str(value).isdigit():
                        raise SemanticError("TypeError", col_name, f"期望 INT, 但得到 {value}")
                elif expected_type == "VARCHAR":
                    if not isinstance(value, str):
                        raise SemanticError("TypeError", col_name, f"期望 VARCHAR, 但得到 {value}")

        # 检查 WHERE 子句（如果存在）
        for child in ast.children:
            if child.node_type == "WHERE":
                self._check_where(table_name, child)

        print(f"[OK] UPDATE {table_name} 语义检查通过")

    def _column_exists_in_tables(self, tables, column_name):
        """检查列是否存在于任何表中，支持 table.column 格式"""
        # 如果是限定列名 (table.column)
        if '.' in column_name:
            table_name, col_name = column_name.split('.', 1)
            # 检查指定的表是否在表列表中，且列是否存在
            if table_name in tables and self.catalog.has_column(table_name, col_name):
                return True
            return False
        else:
            # 普通列名，检查是否存在于任何表中
            for table in tables:
                if self.catalog.has_column(table, column_name):
                    return True
            return False

    def _check_join_condition(self, tables, on_node):
        """检查 JOIN 的 ON 条件"""
        left = next((c.value for c in on_node.children if c.node_type == "LEFT"), None)
        right = next((c.value for c in on_node.children if c.node_type == "RIGHT"), None)
        
        if left and not self._column_exists_in_tables(tables, left):
            raise SemanticError("ColumnError", left, "JOIN ON 条件中的左侧列不存在")
            
        if right and not self._column_exists_in_tables(tables, right):
            raise SemanticError("ColumnError", right, "JOIN ON 条件中的右侧列不存在")

    def _check_where_multi_table(self, tables, where_node):
        """检查多表环境下的 WHERE 条件"""
        left = next((c for c in where_node.children if c.node_type == "LEFT"), None)
        right = next((c for c in where_node.children if c.node_type == "RIGHT"), None)
        
        if left and not self._column_exists_in_tables(tables, left.value):
            raise SemanticError("ColumnError", left.value, "WHERE 子句中的列不存在")
        
        # right 通常是常量，不需要检查表中是否存在
        # 但如果 right 也是列名，则需要检查
        if right and right.value and not str(right.value).replace(".", "").replace("'", "").isdigit():
            # 如果 right 不是数字也不是字符串常量，可能是列名
            if '.' in str(right.value) or not str(right.value).startswith("'"):
                if not self._column_exists_in_tables(tables, right.value):
                    raise SemanticError("ColumnError", right.value, "WHERE 子句中的右侧列不存在")
