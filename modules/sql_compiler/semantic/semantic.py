# semantic.py

# 尝试导入智能诊断模块
try:
    from modules.sql_compiler.diagnostics.error_diagnostic import SmartErrorDiagnostic, ErrorFormatter
    DIAGNOSTICS_AVAILABLE = True
except ImportError:
    DIAGNOSTICS_AVAILABLE = False

class SemanticError(Exception):
    def __init__(self, error_type, position, message, available_tables=None, available_columns=None):
        self.error_type = error_type
        self.position = position
        self.message = message
        
        if DIAGNOSTICS_AVAILABLE:
            # 使用智能诊断
            diagnostic_engine = SmartErrorDiagnostic()
            
            self.diagnostic = diagnostic_engine.diagnose_semantic_error(
                error_type, position, message, available_tables, available_columns
            )
            enhanced_message = ErrorFormatter.format_diagnostic(self.diagnostic)
            super().__init__(enhanced_message)
        else:
            # 回退到原始错误格式
            super().__init__(f"[{error_type}, {position}, {message}]")

    def __str__(self):
        if hasattr(self, 'diagnostic') and DIAGNOSTICS_AVAILABLE:
            return ErrorFormatter.format_diagnostic(self.diagnostic)
        else:
            return f"[{self.error_type}, {self.position}, {self.message}]"


class Catalog:
    """
    增强的模式目录（记录表结构、主键、外键约束）
    """
    def __init__(self):
        self.tables = {}  # {table_name: {column_name: column_type}}
        self.primary_keys = {}  # {table_name: [primary_key_columns]}
        self.foreign_keys = {}  # {table_name: [{column: str, references_table: str, references_column: str}]}
        self.constraints = {}  # {table_name: {column_name: [constraints]}} (NOT NULL, UNIQUE等)

    def create_table(self, table_name, columns, primary_keys=None, foreign_keys=None, constraints=None):
        if table_name in self.tables:
            raise SemanticError("TableError", table_name, "表已存在")
        
        self.tables[table_name] = columns
        self.primary_keys[table_name] = primary_keys or []
        self.foreign_keys[table_name] = foreign_keys or []
        self.constraints[table_name] = constraints or {}

    def drop_table(self, table_name):
        """删除表及其所有约束"""
        if table_name not in self.tables:
            raise SemanticError("TableError", table_name, "要删除的表不存在")
        
        # 检查是否有其他表引用此表作为外键
        for other_table, fks in self.foreign_keys.items():
            if other_table != table_name:
                for fk in fks:
                    if fk.get('references_table') == table_name:
                        raise SemanticError(
                            "ForeignKeyError", table_name, 
                            f"无法删除表，被表 {other_table} 的外键约束引用"
                        )
        
        # 删除表和所有相关约束
        del self.tables[table_name]
        del self.primary_keys[table_name]
        del self.foreign_keys[table_name]
        del self.constraints[table_name]

    def has_table(self, table_name):
        return table_name in self.tables

    def has_column(self, table_name, column_name):
        return self.has_table(table_name) and column_name in self.tables[table_name]

    def get_column_type(self, table_name, column_name):
        return self.tables[table_name].get(column_name, None)

    def get_primary_keys(self, table_name):
        return self.primary_keys.get(table_name, [])

    def get_foreign_keys(self, table_name):
        return self.foreign_keys.get(table_name, [])

    def has_primary_key(self, table_name, column_name):
        return column_name in self.primary_keys.get(table_name, [])

    def add_foreign_key(self, table_name, column_name, ref_table, ref_column):
        """添加外键约束"""
        if table_name not in self.foreign_keys:
            self.foreign_keys[table_name] = []
        
        self.foreign_keys[table_name].append({
            'column': column_name,
            'references_table': ref_table,
            'references_column': ref_column
        })

    def validate_foreign_key_references(self):
        """验证所有外键引用的完整性"""
        errors = []
        for table_name, fks in self.foreign_keys.items():
            for fk in fks:
                ref_table = fk['references_table']
                ref_column = fk['references_column']
                
                # 检查引用的表是否存在
                if not self.has_table(ref_table):
                    errors.append(f"表 {table_name} 的外键引用了不存在的表 {ref_table}")
                
                # 检查引用的列是否存在
                elif not self.has_column(ref_table, ref_column):
                    errors.append(f"表 {table_name} 的外键引用了表 {ref_table} 中不存在的列 {ref_column}")
                
                # 检查引用的列是否是主键（推荐但不强制）
                elif not self.has_primary_key(ref_table, ref_column):
                    # 这是一个警告而不是错误
                    pass
        
        return errors


class SemanticAnalyzer:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
    
    def _get_available_tables(self):
        """获取可用表列表"""
        return list(self.catalog.tables.keys())
    
    def _get_available_columns(self):
        """获取可用列映射"""
        return {table: list(columns.keys()) for table, columns in self.catalog.tables.items()}

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
        elif node_type == "DROP_TABLE":
            self._check_drop(ast)

    def _check_create(self, ast):
        table_name = ast.value
        columns = {}
        primary_keys = []
        foreign_keys = []
        constraints = {}
        
        for child in ast.children:
            if child.node_type == "COLUMN":
                col_name = child.value.split(":")[0]
                col_type = child.value.split(":")[1]
                columns[col_name] = col_type
                
            elif child.node_type == "PRIMARY_KEY":
                primary_keys = child.value.split(",")
                
            elif child.node_type == "FOREIGN_KEY":
                # 格式: column:ref_table.ref_column
                parts = child.value.split(":")
                fk_column = parts[0]
                ref_parts = parts[1].split(".")
                ref_table = ref_parts[0]
                ref_column = ref_parts[1]
                
                foreign_keys.append({
                    'column': fk_column,
                    'references_table': ref_table,
                    'references_column': ref_column
                })
                
            elif child.node_type == "CONSTRAINT":
                # 格式: column:constraint_type
                col_name, constraint_type = child.value.split(":")
                if col_name not in constraints:
                    constraints[col_name] = []
                constraints[col_name].append(constraint_type)
        
        # 验证主键列存在
        for pk_col in primary_keys:
            if pk_col not in columns:
                raise SemanticError(
                    "PrimaryKeyError", pk_col, f"主键列 '{pk_col}' 不存在于表定义中",
                    available_tables=self._get_available_tables(),
                    available_columns=self._get_available_columns()
                )
        
        # 验证外键列存在
        for fk in foreign_keys:
            if fk['column'] not in columns:
                raise SemanticError(
                    "ForeignKeyError", fk['column'], f"外键列 '{fk['column']}' 不存在于表定义中",
                    available_tables=self._get_available_tables(),
                    available_columns=self._get_available_columns()
                )
            
            # 检查引用的表是否存在
            ref_table = fk['references_table']
            if not self.catalog.has_table(ref_table):
                raise SemanticError(
                    "ForeignKeyError", ref_table, 
                    f"外键引用的表 '{ref_table}' 不存在",
                    available_tables=self._get_available_tables(),
                    available_columns=self._get_available_columns()
                )
            
            # 检查引用的列是否存在
            if not self.catalog.has_column(ref_table, fk['references_column']):
                raise SemanticError(
                    "ForeignKeyError", fk['references_column'], 
                    f"外键引用的列 '{fk['references_column']}' 在表 '{ref_table}' 中不存在",
                    available_tables=self._get_available_tables(),
                    available_columns=self._get_available_columns()
                )
        
        try:
            self.catalog.create_table(table_name, columns, primary_keys, foreign_keys, constraints)
            print(f"[OK] CREATE TABLE {table_name} 语义检查通过")
            
            # 输出约束信息
            if primary_keys:
                print(f"    主键: {', '.join(primary_keys)}")
            if foreign_keys:
                for fk in foreign_keys:
                    print(f"    外键: {fk['column']} -> {fk['references_table']}.{fk['references_column']}")
            if constraints:
                for col, cons in constraints.items():
                    print(f"    约束: {col} - {', '.join(cons)}")
                    
        except SemanticError as e:
            # 重新抛出带有上下文的错误
            raise SemanticError(
                e.error_type, e.position, e.message,
                available_tables=self._get_available_tables(),
                available_columns=self._get_available_columns()
            )

    def _check_insert(self, ast):
        table_name = ast.value
        if not self.catalog.has_table(table_name):
            raise SemanticError(
                "TableError", table_name, "表不存在",
                available_tables=self._get_available_tables(),
                available_columns=self._get_available_columns()
            )

        columns = [c.value for c in ast.children if c.node_type == "COLUMN"]
        values = [v.value for v in ast.children if v.node_type == "VALUE"]

        if len(columns) != len(values):
            raise SemanticError(
                "ColumnCountError", table_name, "列数和值数量不一致",
                available_tables=self._get_available_tables(),
                available_columns=self._get_available_columns()
            )

        for col, val in zip(columns, values):
            if not self.catalog.has_column(table_name, col):
                raise SemanticError(
                    "ColumnError", col, "列不存在",
                    available_tables=self._get_available_tables(),
                    available_columns=self._get_available_columns()
                )
            expected_type = self.catalog.get_column_type(table_name, col)
            if expected_type == "INT":
                if not str(val).isdigit():
                    raise SemanticError(
                        "TypeError", col, f"期望 INT, 但得到 {val}",
                        available_tables=self._get_available_tables(),
                        available_columns=self._get_available_columns()
                    )
            elif expected_type == "VARCHAR":
                if not isinstance(val, str):
                    raise SemanticError(
                        "TypeError", col, f"期望 VARCHAR, 但得到 {val}",
                        available_tables=self._get_available_tables(),
                        available_columns=self._get_available_columns()
                    )

        # 检查主键约束
        primary_keys = self.catalog.get_primary_keys(table_name)
        for pk_col in primary_keys:
            if pk_col not in columns:
                raise SemanticError(
                    "PrimaryKeyError", pk_col, f"INSERT 语句缺少主键列 '{pk_col}' 的值",
                    available_tables=self._get_available_tables(),
                    available_columns=self._get_available_columns()
                )
            # 检查主键值不为空
            pk_index = columns.index(pk_col)
            if not values[pk_index] or str(values[pk_index]).strip() == "":
                raise SemanticError(
                    "PrimaryKeyError", pk_col, f"主键列 '{pk_col}' 的值不能为空",
                    available_tables=self._get_available_tables(),
                    available_columns=self._get_available_columns()
                )

        # 检查外键约束
        foreign_keys = self.catalog.get_foreign_keys(table_name)
        for fk in foreign_keys:
            fk_col = fk['column']
            if fk_col in columns:
                fk_index = columns.index(fk_col)
                fk_value = values[fk_index]
                
                # 检查外键引用的表和列是否存在
                ref_table = fk['references_table']
                ref_column = fk['references_column']
                
                if not self.catalog.has_table(ref_table):
                    raise SemanticError(
                        "ForeignKeyError", fk_col, f"外键引用的表 '{ref_table}' 不存在",
                        available_tables=self._get_available_tables(),
                        available_columns=self._get_available_columns()
                    )
                    
                if not self.catalog.has_column(ref_table, ref_column):
                    raise SemanticError(
                        "ForeignKeyError", fk_col, f"外键引用的列 '{ref_column}' 在表 '{ref_table}' 中不存在",
                        available_tables=self._get_available_tables(),
                        available_columns=self._get_available_columns()
                    )

        print(f"[OK] INSERT INTO {table_name} 语义检查通过")
        if primary_keys:
            print(f"    主键约束检查通过")
        if foreign_keys:
            print(f"    外键约束检查通过")

    def _check_select(self, ast):
        # 获取主表和所有涉及的表
        tables = []
        table_aliases = {}  # 别名映射：alias -> table_name
        main_table = None
        
        # 查找 FROM 子句
        from_node = next((child for child in ast.children if child.node_type == "FROM"), None)
        if from_node:
            main_table = from_node.value
            if not self.catalog.has_table(main_table):
                raise SemanticError("TableError", main_table, "表不存在")
            tables.append(main_table)
            
            # 检查主表别名
            alias_node = next((child for child in from_node.children if child.node_type == "ALIAS"), None)
            if alias_node:
                table_aliases[alias_node.value] = main_table
            
            # 检查 JOIN 表
            for join_child in from_node.children:
                if join_child.node_type == "JOIN":
                    join_table = next((c.value for c in join_child.children if c.node_type == "TABLE"), None)
                    if join_table:
                        if not self.catalog.has_table(join_table):
                            raise SemanticError("TableError", join_table, "JOIN 中的表不存在")
                        tables.append(join_table)
                        
                        # 检查 JOIN 表别名
                        join_alias_node = next((c for c in join_child.children if c.node_type == "ALIAS"), None)
                        if join_alias_node:
                            table_aliases[join_alias_node.value] = join_table
                        
                        # 检查 ON 条件
                        on_node = next((c for c in join_child.children if c.node_type == "ON"), None)
                        if on_node:
                            self._check_join_condition(tables, on_node, table_aliases)

        # 检查 SELECT 列
        for child in ast.children:
            if child.node_type == "COLUMN":
                # 跳过 * 通配符的检查，它表示选择所有列
                if child.value == "*":
                    continue
                if not self._column_exists_in_tables_with_aliases(tables, child.value, table_aliases):
                    raise SemanticError(
                        "ColumnError", child.value, "列不存在于任何表中",
                        available_tables=self._get_available_tables(),
                        available_columns=self._get_available_columns()
                    )

        # 检查 WHERE 子句
        where_node = next((child for child in ast.children if child.node_type == "WHERE"), None)
        if where_node:
            self._check_where_multi_table(tables, where_node, table_aliases)

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

        # 检查右侧值是否为列名（如果不是常量的话）
        if right and right.value:
            right_value = str(right.value)
            # 如果是数字，不需要检查
            if right_value.replace(".", "").isdigit():
                pass  # 数字常量，跳过检查
            # 如果是字符串常量（词法分析器已经去掉了引号），不需要检查
            # 这里我们假设非数字的CONST都是字符串常量
            else:
                # 检查是否真的是列名
                if self.catalog.has_column(table_name, right.value):
                    # 如果确实是列名，则允许
                    pass
                else:
                    # 如果不是列名，则认为是字符串常量，跳过检查
                    pass

        if left and right:
            expected_type = self.catalog.get_column_type(table_name, left.value)
            right_value = str(right.value)
            
            # 处理字符串常量（去除引号）
            if right_value.startswith("'") and right_value.endswith("'"):
                right_value = right_value[1:-1]
            
            if expected_type == "INT" and not right_value.replace(".", "").isdigit():
                raise SemanticError("TypeError", left.value, f"期望 INT, 但 WHERE 得到 {right.value}")
            if expected_type in ["STRING", "VARCHAR"] and right_value.replace(".", "").isdigit():
                raise SemanticError("TypeError", left.value, f"期望 STRING, 但 WHERE 得到 {right.value}")

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

    def _column_exists_in_tables_with_aliases(self, tables, column_name, table_aliases):
        """检查列是否存在于任何表中，支持表别名和 table.column 格式"""
        # 如果是限定列名 (table.column 或 alias.column)
        if '.' in column_name:
            table_or_alias, col_name = column_name.split('.', 1)
            
            # 首先检查是否是别名
            if table_or_alias in table_aliases:
                actual_table = table_aliases[table_or_alias]
                if self.catalog.has_column(actual_table, col_name):
                    return True
            
            # 然后检查是否是实际表名
            if table_or_alias in tables and self.catalog.has_column(table_or_alias, col_name):
                return True
            
            return False
        else:
            # 普通列名，检查是否存在于任何表中
            for table in tables:
                if self.catalog.has_column(table, column_name):
                    return True
            return False

    def _check_join_condition(self, tables, on_node, table_aliases=None):
        """检查 JOIN 的 ON 条件"""
        left = next((c.value for c in on_node.children if c.node_type == "LEFT"), None)
        right = next((c.value for c in on_node.children if c.node_type == "RIGHT"), None)
        
        if table_aliases is None:
            table_aliases = {}
        
        if left and not self._column_exists_in_tables_with_aliases(tables, left, table_aliases):
            raise SemanticError(
                "ColumnError", left, "JOIN ON 条件中的左侧列不存在",
                available_tables=self._get_available_tables(),
                available_columns=self._get_available_columns()
            )
            
        if right and not self._column_exists_in_tables_with_aliases(tables, right, table_aliases):
            raise SemanticError(
                "ColumnError", right, "JOIN ON 条件中的右侧列不存在",
                available_tables=self._get_available_tables(),
                available_columns=self._get_available_columns()
            )

    def _check_where_multi_table(self, tables, where_node, table_aliases=None):
        """检查多表环境下的 WHERE 条件"""
        if table_aliases is None:
            table_aliases = {}
            
        left = next((c for c in where_node.children if c.node_type == "LEFT"), None)
        right = next((c for c in where_node.children if c.node_type == "RIGHT"), None)
        
        if left and not self._column_exists_in_tables_with_aliases(tables, left.value, table_aliases):
            raise SemanticError("ColumnError", left.value, "WHERE 子句中的列不存在")
        
        # right 通常是常量，不需要检查表中是否存在
        # 但如果 right 也是列名，则需要检查
        if right and right.value:
            right_value = str(right.value)
            # 如果是数字，不需要检查
            if right_value.replace(".", "").isdigit():
                pass  # 数字常量，跳过检查
            # 如果是字符串常量（词法分析器已经去掉了引号），不需要检查
            # 这里我们假设非数字的CONST都是字符串常量
            else:
                # 检查是否真的是列名
                if self._column_exists_in_tables_with_aliases(tables, right.value, table_aliases):
                    # 如果确实是列名，则允许
                    pass
                else:
                    # 如果不是列名，则认为是字符串常量，跳过检查
                    pass

    def _check_drop(self, ast):
        """检查 DROP TABLE 语句"""
        table_name = ast.value
        
        # 检查表是否存在
        if not self.catalog.has_table(table_name):
            raise SemanticError(
                "TableError", table_name, "要删除的表不存在",
                available_tables=self._get_available_tables(),
                available_columns=self._get_available_columns()
            )
        
        # 检查是否有其他表引用此表作为外键
        for other_table, fks in self.catalog.foreign_keys.items():
            if other_table != table_name:
                for fk in fks:
                    if fk.get('references_table') == table_name:
                        raise SemanticError(
                            "ForeignKeyError", table_name, 
                            f"无法删除表，被表 {other_table} 的外键约束引用",
                            available_tables=self._get_available_tables(),
                            available_columns=self._get_available_columns()
                        )
        
        # 执行删除操作
        try:
            self.catalog.drop_table(table_name)
            print(f"[OK] DROP TABLE {table_name} 语义检查通过")
            print(f"    表 {table_name} 已从目录中删除")
        except SemanticError as e:
            # 重新抛出带有上下文的错误
            raise SemanticError(
                e.error_type, e.position, e.message,
                available_tables=self._get_available_tables(),
                available_columns=self._get_available_columns()
            )
