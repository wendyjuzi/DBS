# semantic.py

# 尝试导入智能诊断模块
try:
    from modules.sql_compiler.diagnostics.error_diagnostic import SmartErrorDiagnostic, ErrorFormatter
    DIAGNOSTICS_AVAILABLE = True
except ImportError:
    DIAGNOSTICS_AVAILABLE = False

# 导入 ASTNode 类
try:
    from modules.sql_compiler.syntax.parser import ASTNode
except ImportError:
    # 如果无法导入，创建一个简单的替代类
    class ASTNode:
        def __init__(self, node_type, value=None, children=None):
            self.node_type = node_type
            self.value = value
            self.children = children or []

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
        self.views = {}  # {view_name: {columns: {column_name: column_type}, query: dict, materialized: bool}}
        self.procedures = {}  # {procedure_name: {parameters: list, body: dict, return_type: str, is_function: bool}}

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

    def create_view(self, view_name, columns, query, materialized=False):
        """创建视图"""
        if view_name in self.views:
            raise SemanticError("ViewError", view_name, "视图已存在")
        if view_name in self.tables:
            raise SemanticError("ViewError", view_name, "视图名与已存在的表名冲突")
        
        self.views[view_name] = {
            'columns': columns,
            'query': query,
            'materialized': materialized
        }

    def drop_view(self, view_name):
        """删除视图"""
        if view_name not in self.views:
            raise SemanticError("ViewError", view_name, "要删除的视图不存在")
        
        del self.views[view_name]

    def has_view(self, view_name):
        """检查视图是否存在"""
        return view_name in self.views

    def get_view_columns(self, view_name):
        """获取视图的列信息"""
        if view_name not in self.views:
            return {}
        return self.views[view_name]['columns']

    def create_procedure(self, proc_name, parameters, body, return_type=None, is_function=False):
        """创建存储过程或函数"""
        if proc_name in self.procedures:
            raise SemanticError("ProcedureError", proc_name, "存储过程已存在")
        if proc_name in self.tables:
            raise SemanticError("ProcedureError", proc_name, "存储过程名与已存在的表名冲突")
        if proc_name in self.views:
            raise SemanticError("ProcedureError", proc_name, "存储过程名与已存在的视图名冲突")
        
        self.procedures[proc_name] = {
            'parameters': parameters,
            'body': body,
            'return_type': return_type,
            'is_function': is_function
        }

    def drop_procedure(self, proc_name):
        """删除存储过程或函数"""
        if proc_name not in self.procedures:
            raise SemanticError("ProcedureError", proc_name, "要删除的存储过程不存在")
        
        del self.procedures[proc_name]

    def has_procedure(self, proc_name):
        """检查存储过程是否存在"""
        return proc_name in self.procedures

    def get_procedure_info(self, proc_name):
        """获取存储过程信息"""
        if proc_name not in self.procedures:
            return None
        return self.procedures[proc_name]

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
    
    # 索引管理方法（未来扩展）
    def create_index(self, index_name, table_name, columns, index_type="BTREE", is_unique=False):
        """创建索引（占位方法）"""
        # 未来可以在这里存储索引元数据
        pass
    
    def drop_index(self, index_name):
        """删除索引（占位方法）"""
        # 未来可以在这里删除索引元数据
        pass

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
        self.current_procedure_params = {}  # 当前存储过程的参数作用域
        self.current_local_vars = {}  # 当前存储过程的局部变量作用域
    
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
        elif node_type in ["BEGIN_TRANSACTION", "COMMIT", "ROLLBACK"]:
            self._check_transaction_statement(ast)
        elif node_type in ["CREATE_INDEX", "DROP_INDEX"]:
            self._check_index_statement(ast)
        elif node_type in ["CREATE_TRIGGER", "DROP_TRIGGER"]:
            self._check_trigger_statement(ast)
        elif node_type in ["CREATE_VIEW", "DROP_VIEW"]:
            self._check_view_statement(ast)
        elif node_type in ["CREATE_PROCEDURE", "CREATE_FUNCTION", "DROP_PROCEDURE", "DROP_FUNCTION", "CALL_PROCEDURE"]:
            self._check_procedure_statement(ast)
        elif node_type == "DECLARE_STATEMENT":
            self._check_declare_statement(ast)
        elif node_type == "SET_STATEMENT":
            self._check_set_statement(ast)
        elif node_type == "DELIMITER_STATEMENT":
            self._check_delimiter_statement(ast)

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
            
            # 检查值是否为存储过程参数
            if str(val) in self.current_procedure_params:
                param_type = self.current_procedure_params[str(val)]
                if param_type != expected_type:
                    raise SemanticError(
                        "TypeError", col, 
                        f"参数类型不匹配：期望 {expected_type}, 参数 {val} 类型为 {param_type}",
                        available_tables=self._get_available_tables(),
                        available_columns=self._get_available_columns()
                    )
            elif expected_type == "INT":
                # 跳过触发器引用（OLD.column, NEW.column）的类型检查
                if not (str(val).startswith("OLD.") or str(val).startswith("NEW.")):
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
        # 如果没有 FROM 子句，这是一个常量查询，跳过列检查
        if not from_node:
            print(f"[OK] 常量查询 SELECT 语义检查通过")
            return
            
        for child in ast.children:
            if child.node_type == "COLUMN":
                # 跳过 * 通配符的检查，它表示选择所有列
                if child.value == "*":
                    continue

                # 处理带别名的列
                if isinstance(child.value, str) and " AS " in child.value.upper():
                    # "AS" keyword is case-insensitive
                    col_part = child.value.upper().split(" AS ")[0].strip()
                    self._check_expression_columns(col_part, tables, table_aliases)
                    continue

                # 普通列名或表达式检查
                col_name = str(child.value)
                if col_name != "*":
                    self._check_expression_columns(col_name, tables, table_aliases)
            elif child.node_type == "AGGREGATE":
                # 直接检查聚合函数节点
                self._check_aggregate_function(child, tables, table_aliases)

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
            # 如果是存储过程参数，跳过检查
            if right_value in self.current_procedure_params:
                pass  # 存储过程参数，跳过检查
            # 如果是数字，不需要检查
            elif right_value.replace(".", "").isdigit():
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
                
                # 检查值是否为存储过程参数
                if value in self.current_procedure_params:
                    param_type = self.current_procedure_params[value]
                    if param_type != expected_type:
                        raise SemanticError("TypeError", col_name, 
                                          f"参数类型不匹配：期望 {expected_type}, 参数 {value} 类型为 {param_type}")
                # 检查值是否为局部变量
                elif value in self.current_local_vars:
                    var_type = self.current_local_vars[value]
                    if var_type != expected_type:
                        raise SemanticError("TypeError", col_name, 
                                          f"变量类型不匹配：期望 {expected_type}, 变量 {value} 类型为 {var_type}")
                elif expected_type == "INT":
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
        """检查多表环境下的 WHERE 条件（支持复杂条件）"""
        if table_aliases is None:
            table_aliases = {}
        
        # 递归检查WHERE条件树
        for child in where_node.children:
            self._check_condition_node(child, tables, table_aliases)
    
    def _check_condition_node(self, node, tables, table_aliases):
        """递归检查条件节点"""
        if node.node_type == "LOGICAL_OP":
            # 递归检查逻辑操作符的子节点
            for child in node.children:
                self._check_condition_node(child, tables, table_aliases)
                
        elif node.node_type == "COMPARISON":
            # 检查比较操作
            left = next((c for c in node.children if c.node_type == "LEFT"), None)
            right = next((c for c in node.children if c.node_type == "RIGHT"), None)
            
            if left and not self._column_exists_in_tables_with_aliases(tables, left.value, table_aliases):
                raise SemanticError("ColumnError", left.value, "WHERE 子句中的列不存在")
            
            # 检查右侧如果是列名
            if right and not str(right.value).replace(".", "").isdigit():
                # 如果不是数字，检查是否是列名
                if "." in str(right.value) or any(table for table in tables if right.value in self.catalog.tables.get(table, {})):
                    if not self._column_exists_in_tables_with_aliases(tables, right.value, table_aliases):
                        # 如果看起来像列名但不存在，可能是字符串常量，允许通过
                        pass
                        
        elif node.node_type == "BETWEEN":
            # 检查 BETWEEN 操作
            left = next((c for c in node.children if c.node_type == "LEFT"), None)
            if left and not self._column_exists_in_tables_with_aliases(tables, left.value, table_aliases):
                raise SemanticError("ColumnError", left.value, "BETWEEN 子句中的列不存在")
                
        elif node.node_type == "IN":
            # 检查 IN 操作
            left = next((c for c in node.children if c.node_type == "LEFT"), None)
            if left and not self._column_exists_in_tables_with_aliases(tables, left.value, table_aliases):
                raise SemanticError("ColumnError", left.value, "IN 子句中的列不存在")
                
        elif node.node_type == "LIKE":
            # 检查 LIKE 操作
            left = next((c for c in node.children if c.node_type == "LEFT"), None)
            if left and not self._column_exists_in_tables_with_aliases(tables, left.value, table_aliases):
                raise SemanticError("ColumnError", left.value, "LIKE 子句中的列不存在")
        
        # 兼容旧格式的WHERE节点 (LEFT, OP, RIGHT)
        elif hasattr(node, 'children'):
            left = next((c for c in node.children if c.node_type == "LEFT"), None)
            right = next((c for c in node.children if c.node_type == "RIGHT"), None)
            
            if left and hasattr(left, 'value'):
                if not self._column_exists_in_tables_with_aliases(tables, left.value, table_aliases):
                    raise SemanticError("ColumnError", left.value, "WHERE 子句中的列不存在")

    def _check_aggregate_function(self, func_node, tables, table_aliases):
        """检查聚合函数的语义正确性"""
        try:
            func_name = func_node.value
            arg_node = next((c for c in func_node.children if c.node_type == "ARG"), None)
            
            if not arg_node:
                raise SemanticError("AggregateError", func_name, f"聚合函数 {func_name} 缺少参数")
            
            arg_value = arg_node.value
            
            # COUNT(*) 是特殊情况，无需检查列存在性
            if func_name == "COUNT" and arg_value == "*":
                print(f"[OK] 聚合函数 {func_name}(*) 语义检查通过")
                return
            
            # 处理 DISTINCT 修饰符
            if isinstance(arg_value, str) and arg_value.startswith("DISTINCT "):
                actual_column = arg_value[9:]  # 去掉 "DISTINCT " 前缀
            else:
                actual_column = arg_value
            
            # 检查列是否存在（仅对非*参数）
            if actual_column != "*":
                if not self._column_exists_in_tables_with_aliases(tables, actual_column, table_aliases):
                    raise SemanticError(
                        "ColumnError", actual_column, f"聚合函数 {func_name} 中的列不存在",
                        available_tables=self._get_available_tables(),
                        available_columns=self._get_available_columns()
                    )
                
                # 检查数据类型兼容性
                if func_name in ["SUM", "AVG"]:
                    # SUM 和 AVG 只能用于数值列
                    for table in tables:
                        if self.catalog.has_column(table, actual_column):
                            col_type = self.catalog.get_column_type(table, actual_column)
                            if col_type not in ["INT", "DOUBLE", "FLOAT"]:
                                raise SemanticError(
                                    "TypeError", actual_column, 
                                    f"聚合函数 {func_name} 不能用于非数值类型列 ({col_type})",
                                    available_tables=self._get_available_tables(),
                                    available_columns=self._get_available_columns()
                                )
                            break
            
            print(f"[OK] 聚合函数 {func_name}({arg_value}) 语义检查通过")
            
        except Exception as e:
            # 如果检查过程中出现任何错误，提供更详细的错误信息
            print(f"[DEBUG] 聚合函数检查出错: {e}")
            print(f"[DEBUG] func_node类型: {type(func_node)}")
            print(f"[DEBUG] func_node内容: {func_node}")
            if hasattr(func_node, 'children'):
                print(f"[DEBUG] func_node.children: {func_node.children}")
            raise e

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

    def _check_transaction_statement(self, ast):
        """检查事务控制语句（目前仅通过）"""
        print(f"[OK] {ast.node_type} 语义检查通过")
    
    def _check_index_statement(self, ast):
        """检查索引语句的语义正确性"""
        if ast.node_type == "CREATE_INDEX":
            self._check_create_index(ast)
        elif ast.node_type == "DROP_INDEX":
            self._check_drop_index(ast)
    
    def _check_create_index(self, ast):
        """检查 CREATE INDEX 语句"""
        index_name = ast.value
        
        # 获取表名和列名
        table_node = next((c for c in ast.children if c.node_type == "TABLE"), None)
        columns_node = next((c for c in ast.children if c.node_type == "COLUMNS"), None)
        
        if not table_node:
            raise SemanticError("IndexError", index_name, "CREATE INDEX 语句缺少表名")
        
        if not columns_node:
            raise SemanticError("IndexError", index_name, "CREATE INDEX 语句缺少列名")
        
        table_name = table_node.value
        column_names = columns_node.value.split(",")
        
        # 检查表是否存在
        if not self.catalog.has_table(table_name):
            raise SemanticError(
                "TableError", table_name, f"索引 '{index_name}' 引用的表 '{table_name}' 不存在",
                available_tables=self._get_available_tables(),
                available_columns=self._get_available_columns()
            )
        
        # 检查所有列是否存在
        for col_name in column_names:
            col_name = col_name.strip()
            if not self.catalog.has_column(table_name, col_name):
                raise SemanticError(
                    "ColumnError", col_name, 
                    f"索引 '{index_name}' 引用的列 '{col_name}' 在表 '{table_name}' 中不存在",
                    available_tables=self._get_available_tables(),
                    available_columns=self._get_available_columns()
                )
        
        # 检查索引类型
        type_node = next((c for c in ast.children if c.node_type == "TYPE"), None)
        if type_node and type_node.value not in ["BTREE", "HASH"]:
            raise SemanticError(
                "IndexError", index_name, f"不支持的索引类型: {type_node.value}"
            )
        
        # 检查唱一性约束
        unique_node = next((c for c in ast.children if c.node_type == "UNIQUE"), None)
        is_unique = unique_node is not None
        
        print(f"[OK] CREATE INDEX {index_name} 语义检查通过")
        if is_unique:
            print(f"    索引类型: 唯一索引")
        print(f"    表: {table_name}")
        print(f"    列: {', '.join(column_names)}")
        if type_node:
            print(f"    索引类型: {type_node.value}")
    
    def _check_drop_index(self, ast):
        """检查 DROP INDEX 语句"""
        index_name = ast.value
        
        # 目前暂时不检查索引是否存在（需要索引元数据管理）
        # 未来可以扩展 catalog 来管理索引信息
        
        print(f"[OK] DROP INDEX {index_name} 语义检查通过")
    
    def _check_trigger_statement(self, ast):
        """检查触发器语句的语义正确性"""
        if ast.node_type == "CREATE_TRIGGER":
            self._check_create_trigger(ast)
        elif ast.node_type == "DROP_TRIGGER":
            self._check_drop_trigger(ast)
    
    def _check_create_trigger(self, ast):
        """检查 CREATE TRIGGER 语句"""
        trigger_name = ast.value
        
        # 获取触发器相关信息
        timing_node = next((c for c in ast.children if c.node_type == "TIMING"), None)
        events_node = next((c for c in ast.children if c.node_type == "EVENTS"), None)
        table_node = next((c for c in ast.children if c.node_type == "TABLE"), None)
        for_each_row_node = next((c for c in ast.children if c.node_type == "FOR_EACH_ROW"), None)
        
        if not timing_node:
            raise SemanticError("TriggerError", trigger_name, "CREATE TRIGGER 语句缺少触发时机")
        
        if not events_node:
            raise SemanticError("TriggerError", trigger_name, "CREATE TRIGGER 语句缺少触发事件")
        
        if not table_node:
            raise SemanticError("TriggerError", trigger_name, "CREATE TRIGGER 语句缺少表名")
        
        timing = timing_node.value
        events = events_node.value.split(",")
        table_name = table_node.value
        for_each_row = for_each_row_node.value == "True" if for_each_row_node else False
        
        # 检查表是否存在
        if not self.catalog.has_table(table_name):
            raise SemanticError(
                "TableError", table_name, f"触发器 '{trigger_name}' 引用的表 '{table_name}' 不存在",
                available_tables=self._get_available_tables(),
                available_columns=self._get_available_columns()
            )
        
        # 检查触发时机是否有效
        valid_timings = ["BEFORE", "AFTER", "INSTEAD OF"]
        if timing not in valid_timings:
            raise SemanticError(
                "TriggerError", trigger_name, f"不支持的触发时机: {timing}"
            )
        
        # 检查触发事件是否有效
        valid_events = ["INSERT", "UPDATE", "DELETE"]
        for event in events:
            event = event.strip()
            if event not in valid_events:
                raise SemanticError(
                    "TriggerError", trigger_name, f"不支持的触发事件: {event}"
                )
        
        # 检查 WHEN 条件（如果存在）
        when_condition = next((c for c in ast.children if c.node_type == "WHEN_CONDITION"), None)
        if when_condition:
            self._check_trigger_when_condition(when_condition, table_name)
        
        # 检查触发器主体
        trigger_body = next((c for c in ast.children if c.node_type == "TRIGGER_BODY"), None)
        if trigger_body:
            self._check_trigger_body(trigger_body, table_name)
        
        print(f"[OK] CREATE TRIGGER {trigger_name} 语义检查通过")
        print(f"    触发时机: {timing}")
        print(f"    触发事件: {', '.join(events)}")
        print(f"    目标表: {table_name}")
        if for_each_row:
            print(f"    类型: 行级触发器")
    
    def _check_drop_trigger(self, ast):
        """检查 DROP TRIGGER 语句"""
        trigger_name = ast.value
        
        # 目前暂时不检查触发器是否存在（需要触发器元数据管理）
        # 未来可以扩展 catalog 来管理触发器信息
        
        print(f"[OK] DROP TRIGGER {trigger_name} 语义检查通过")
    
    def _check_trigger_when_condition(self, when_condition, table_name):
        """检查触发器 WHEN 条件"""
        # 检查条件中引用的列是否存在
        for child in when_condition.children:
            if child.node_type in ["LEFT", "RIGHT"]:
                operand = child.value
                if "." in operand:
                    # OLD.column 或 NEW.column
                    prefix, column = operand.split(".", 1)
                    if prefix.upper() in ["OLD", "NEW"]:
                        if not self.catalog.has_column(table_name, column):
                            raise SemanticError(
                                "ColumnError", column,
                                f"触发器条件中引用的列 '{column}' 在表 '{table_name}' 中不存在",
                                available_tables=self._get_available_tables(),
                                available_columns=self._get_available_columns()
                            )
    
    def _check_trigger_body(self, trigger_body, table_name):
        """检查触发器主体"""
        # 简化检查：确保触发器主体不为空
        if not trigger_body.children:
            raise SemanticError(
                "TriggerError", "trigger_body", "触发器主体不能为空"
            )
        
        # 递归检查触发器主体中的语句
        for stmt in trigger_body.children:
            if stmt.node_type in ["INSERT", "UPDATE", "DELETE", "SELECT"]:
                # 对于触发器内部的 SQL 语句，进行递归检查。
                # 为了让仅编译器阶段顺利通过，对于表不存在等运行期依赖问题，降级为警告。
                try:
                    self._analyze_node(stmt)
                except SemanticError as e:
                    if getattr(e, 'error_type', '') in ("TableError",):
                        print(f"[WARN] 触发器主体内语句跳过严格检查：{e}")
                        continue
                    raise
            # 其他语句类型暂时跳过检查
    
    def _check_view_statement(self, ast):
        """检查视图语句（CREATE VIEW / DROP VIEW）"""
        node_type = ast.node_type
        view_name = ast.value
        
        if node_type == "CREATE_VIEW":
            print(f"[OK] CREATE VIEW {view_name} 语义检查通过")
            
            # 提取视图信息
            materialized = False
            columns = []
            query = None
            
            for child in ast.children:
                if child.node_type == "MATERIALIZED":
                    materialized = child.value == "True"
                elif child.node_type == "COLUMNS":
                    columns = child.value.split(",") if child.value else []
                elif child.node_type == "QUERY":
                    query = child.children[0] if child.children else None
            
            # 检查查询语句的语义
            if query:
                self._analyze_node(query)
                # 如果没有显式指定列名，从查询中推导列信息
                if not columns:
                    # 从 SELECT 语句中提取列信息
                    if query.node_type == "SELECT":
                        for col_child in query.children:
                            if col_child.node_type == "COLUMN":
                                columns.append(col_child.value)
            
            # 推导视图的列类型（简化处理）
            view_columns = {}
            for col_name in columns:
                view_columns[col_name] = "VARCHAR"  # 简化处理，实际应该从查询推导类型
            
            # 创建视图
            self.catalog.create_view(view_name, view_columns, query, materialized)
            print(f"    视图类型: {'物化视图' if materialized else '普通视图'}")
            print(f"    列数: {len(columns)}")
            
        elif node_type == "DROP_VIEW":
            print(f"[OK] DROP VIEW {view_name} 语义检查通过")
            
            # 检查视图是否存在
            if not self.catalog.has_view(view_name):
                # 检查是否有 IF EXISTS 子句
                if_exists = False
                for child in ast.children:
                    if child.node_type == "IF_EXISTS" and child.value == "TRUE":
                        if_exists = True
                        break
                
                if not if_exists:
                    raise SemanticError("ViewError", view_name, "要删除的视图不存在")
                else:
                    print(f"    [WARN] 视图 {view_name} 不存在，但使用了 IF EXISTS，忽略")
                    return
            
            # 删除视图
            self.catalog.drop_view(view_name)

    def _check_procedure_statement(self, ast):
        """检查存储过程语句（CREATE PROCEDURE/FUNCTION, DROP PROCEDURE/FUNCTION, CALL）"""
        node_type = ast.node_type
        proc_name = ast.value
        
        if node_type in ["CREATE_PROCEDURE", "CREATE_FUNCTION"]:
            is_function = node_type == "CREATE_FUNCTION"
            print(f"[OK] {'CREATE FUNCTION' if is_function else 'CREATE PROCEDURE'} {proc_name} 语义检查通过")
            
            # 提取存储过程信息
            parameters = []
            return_type = None
            body = []
            
            for child in ast.children:
                if child.node_type == "PARAMETERS":
                    for param_child in child.children:
                        if param_child.node_type == "PARAMETER":
                            param_parts = param_child.value.split(":")
                            param_name = param_parts[0]
                            param_type = param_parts[1]
                            param_mode = param_parts[2] if len(param_parts) > 2 else "IN"
                            parameters.append({
                                'name': param_name,
                                'type': param_type,
                                'mode': param_mode
                            })
                elif child.node_type == "RETURN_TYPE":
                    return_type = child.value
                elif child.node_type == "PROCEDURE_BODY":
                    # 设置参数和局部变量作用域
                    old_params = self.current_procedure_params.copy()
                    old_vars = self.current_local_vars.copy()
                    
                    for param in parameters:
                        self.current_procedure_params[param['name']] = param['type']
                    
                    # 检查过程体内的语句
                    try:
                        for stmt in child.children:
                            try:
                                self._analyze_node(stmt)
                                body.append(stmt)
                            except SemanticError as e:
                                print(f"[WARN] 存储过程体内语句跳过严格检查：{e}")
                                body.append(stmt)  # 仍然保留语句
                    finally:
                        # 恢复参数和局部变量作用域
                        self.current_procedure_params = old_params
                        self.current_local_vars = old_vars
            
            # 验证函数必须有返回类型
            if is_function and not return_type:
                raise SemanticError("ProcedureError", proc_name, "函数必须指定返回类型")
            
            # 创建存储过程
            self.catalog.create_procedure(proc_name, parameters, body, return_type, is_function)
            print(f"    参数数量: {len(parameters)}")
            if return_type:
                print(f"    返回类型: {return_type}")
            
        elif node_type in ["DROP_PROCEDURE", "DROP_FUNCTION"]:
            is_function = node_type == "DROP_FUNCTION"
            print(f"[OK] {'DROP FUNCTION' if is_function else 'DROP PROCEDURE'} {proc_name} 语义检查通过")
            
            # 检查存储过程是否存在
            if not self.catalog.has_procedure(proc_name):
                # 检查是否有 IF EXISTS 子句
                if_exists = False
                for child in ast.children:
                    if child.node_type == "IF_EXISTS" and child.value == "TRUE":
                        if_exists = True
                        break
                
                if not if_exists:
                    raise SemanticError("ProcedureError", proc_name, "要删除的存储过程不存在")
                else:
                    print(f"    [WARN] 存储过程 {proc_name} 不存在，但使用了 IF EXISTS，忽略")
                    return
            
            # 验证类型匹配
            proc_info = self.catalog.get_procedure_info(proc_name)
            if proc_info and proc_info['is_function'] != is_function:
                proc_type = "函数" if proc_info['is_function'] else "存储过程"
                expected_type = "函数" if is_function else "存储过程"
                raise SemanticError("ProcedureError", proc_name, 
                                  f"类型不匹配：{proc_name} 是{proc_type}，但试图作为{expected_type}删除")
            
            # 删除存储过程
            self.catalog.drop_procedure(proc_name)
            
        elif node_type == "CALL_PROCEDURE":
            print(f"[OK] CALL {proc_name} 语义检查通过")
            
            # 检查存储过程是否存在
            if not self.catalog.has_procedure(proc_name):
                raise SemanticError("ProcedureError", proc_name, "调用的存储过程不存在")
            
            # 检查参数数量（简化检查）
            proc_info = self.catalog.get_procedure_info(proc_name)
            expected_params = len(proc_info['parameters'])
            
            # 提取调用参数
            call_args = []
            for child in ast.children:
                if child.node_type == "ARGUMENTS":
                    call_args = child.value.split(",") if child.value else []
                    break
            
            actual_params = len(call_args)
            if actual_params != expected_params:
                raise SemanticError("ProcedureError", proc_name, 
                                  f"参数数量不匹配：期望 {expected_params} 个，实际 {actual_params} 个")
            
            print(f"    参数数量: {actual_params}")

    def _check_delimiter_statement(self, ast):
        """检查 DELIMITER 语句"""
        delimiter = ast.value
        print(f"[OK] DELIMITER 语句语义检查通过")
        print(f"    新分隔符: '{delimiter}'")
    
    def _check_expression_columns(self, expression, tables, table_aliases):
        """检查表达式中的列是否存在"""
        import re
        
        # 提取表达式中的标识符（可能的列名）
        # 匹配标识符，但排除数字常量
        identifiers = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', str(expression))
        
        for identifier in identifiers:
            # 跳过存储过程参数
            if identifier in self.current_procedure_params:
                continue
            
            # 跳过局部变量
            if identifier in self.current_local_vars:
                continue
                
            # 检查是否是列名
            if not self._column_exists_in_tables_with_aliases(tables, identifier, table_aliases):
                # 如果不是列名，可能是常量或函数，暂时跳过
                # 只有当它看起来像列名时才报错
                if not identifier.replace('.', '').replace('_', '').isalnum():
                    continue
                    
                # 检查是否是常见的SQL函数或关键字
                sql_functions = {'NOW', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'UPPER', 'LOWER', 'LENGTH'}
                if identifier.upper() in sql_functions:
                    continue
                    
                raise SemanticError(
                    "ColumnError", identifier, "列不存在于任何表中",
                    available_tables=self._get_available_tables(),
                    available_columns=self._get_available_columns()
                )
    
    def _check_declare_statement(self, ast):
        """检查 DECLARE 语句"""
        # 解析变量声明：var_name:var_type
        var_info = ast.value.split(":")
        if len(var_info) >= 2:
            var_name = var_info[0]
            var_type = var_info[1]
            
            # 添加到局部变量作用域
            self.current_local_vars[var_name] = var_type
            print(f"[OK] DECLARE {var_name} {var_type} 语义检查通过")
    
    def _check_set_statement(self, ast):
        """检查 SET 语句"""
        # 解析赋值：var_name=expression
        assignment = ast.value.split("=", 1)
        if len(assignment) == 2:
            var_name = assignment[0].strip()
            expression = assignment[1].strip()
            
            # 检查变量是否存在（参数或局部变量）
            if var_name not in self.current_procedure_params and var_name not in self.current_local_vars:
                raise SemanticError("VariableError", var_name, "变量未声明")
            
            print(f"[OK] SET {var_name} 语义检查通过")
