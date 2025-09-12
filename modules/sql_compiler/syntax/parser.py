from modules.sql_compiler.lexical.lexer import Lexer, Token, ERROR_TYPES
from modules.sql_compiler.rule.rules import KEYWORDS
from modules.sql_compiler.semantic.semantic import SemanticAnalyzer, Catalog, SemanticError

# 尝试导入智能诊断模块
try:
    from modules.sql_compiler.diagnostics.error_diagnostic import SmartErrorDiagnostic, ErrorFormatter
    DIAGNOSTICS_AVAILABLE = True
except ImportError:
    DIAGNOSTICS_AVAILABLE = False


class ParseError(Exception):
    """自定义语法分析错误类"""
    def __init__(self, message, token=None, context=""):
        self.message = message
        self.token = token
        self.context = context
        
        if DIAGNOSTICS_AVAILABLE:
            # 使用智能诊断
            diagnostic_engine = SmartErrorDiagnostic()
            expected = self._extract_expected_from_message(message)
            got = token.lexeme if token else ""
            line = token.line if token else 0
            column = token.column if token else 0
            
            self.diagnostic = diagnostic_engine.diagnose_syntax_error(
                message, expected, got, line, column, context
            )
            enhanced_message = ErrorFormatter.format_diagnostic(self.diagnostic)
            super().__init__(enhanced_message)
        else:
            # 回退到原始错误格式
            if token:
                super().__init__(f"Syntax Error: {message} at line {token.line}, column {token.column}")
            else:
                super().__init__(f"Syntax Error: {message}")
    
    def _extract_expected_from_message(self, message: str) -> str:
        """从错误消息中提取期望的内容"""
        if "expected" in message.lower():
            parts = message.split("expected")
            if len(parts) > 1:
                return parts[1].split("but")[0].strip()
        return ""

    def __str__(self):
        if hasattr(self, 'diagnostic') and DIAGNOSTICS_AVAILABLE:
            return ErrorFormatter.format_diagnostic(self.diagnostic)
        elif self.token:
            return f"Syntax Error: {self.message} at line {self.token.line}, column {self.token.column}"
        else:
            return f"Syntax Error: {self.message}"

class ASTNode:
    """抽象语法树节点"""
    def __init__(self, node_type, value=None, children=None):
        self.node_type = node_type  # 例如 'CREATE_TABLE', 'INSERT'
        self.value = value          # 节点值，如表名、列名
        self.children = children if children else []  # 子节点列表

    def __repr__(self, level=0):
        indent = "  " * level
        s = f"{indent}{self.node_type}: {self.value}\n"
        for child in self.children:
            s += child.__repr__(level + 1)
        return s

    def to_dict(self):
        """将 AST 节点转换为字典格式，适配执行计划生成器"""
        result = {"type": self.node_type}
        
        if self.node_type == "CREATE_TABLE":
            result["table"] = self.value
            columns = []
            for child in self.children:
                if child.node_type == "COLUMN":
                    col_name, col_type = child.value.split(":")
                    columns.append({"name": col_name, "type": col_type})
            result["columns"] = columns
            
        elif self.node_type == "INSERT":
            result["table"] = self.value
            result["columns"] = [child.value for child in self.children if child.node_type == "COLUMN"]
            result["values"] = [child.value for child in self.children if child.node_type == "VALUE"]
            
        elif self.node_type == "SELECT":
            result["columns"] = [child.value for child in self.children if child.node_type == "COLUMN"]
            
            # 处理 FROM 子句
            from_node = next((child for child in self.children if child.node_type == "FROM"), None)
            joins = []
            if from_node:
                result["table"] = from_node.value
                # 处理 JOIN
                for join_child in from_node.children:
                    if join_child.node_type == "JOIN":
                        join_table = next((c.value for c in join_child.children if c.node_type == "TABLE"), None)
                        on_node = next((c for c in join_child.children if c.node_type == "ON"), None)
                        on_condition = None
                        if on_node:
                            left = next((c.value for c in on_node.children if c.node_type == "LEFT"), None)
                            op = next((c.value for c in on_node.children if c.node_type == "OP"), None)
                            right = next((c.value for c in on_node.children if c.node_type == "RIGHT"), None)
                            on_condition = {"left": left, "op": op, "right": right}
                        joins.append({
                            "type": join_child.value,
                            "table": join_table,
                            "on": on_condition
                        })
            result["joins"] = joins
            
            # 处理 WHERE 条件
            where_node = next((child for child in self.children if child.node_type == "WHERE"), None)
            if where_node:
                left = next((c.value for c in where_node.children if c.node_type == "LEFT"), None)
                op = next((c.value for c in where_node.children if c.node_type == "OP"), None)
                right = next((c.value for c in where_node.children if c.node_type == "RIGHT"), None)
                result["where"] = {"left": left, "op": op, "right": right}
            else:
                result["where"] = None
            
            # 处理 GROUP BY
            group_by_node = next((child for child in self.children if child.node_type == "GROUP_BY"), None)
            if group_by_node:
                result["group_by"] = [c.value for c in group_by_node.children if c.node_type == "COLUMN"]
            else:
                result["group_by"] = None
            
            # 处理 ORDER BY
            order_by_node = next((child for child in self.children if child.node_type == "ORDER_BY"), None)
            if order_by_node:
                order_by = []
                for sort_child in order_by_node.children:
                    if sort_child.node_type == "SORT":
                        col, direction = sort_child.value.split(":")
                        order_by.append({"column": col, "direction": direction})
                result["order_by"] = order_by
            else:
                result["order_by"] = None
                
        elif self.node_type == "UPDATE":
            result["table"] = self.value
            # 处理 SET 子句
            assignments = {}
            for child in self.children:
                if child.node_type == "ASSIGNMENT":
                    col, val = child.value.split("=")
                    assignments[col] = val
            result["assignments"] = assignments
            
            # 处理 WHERE 条件
            where_node = next((child for child in self.children if child.node_type == "WHERE"), None)
            if where_node:
                left = next((c.value for c in where_node.children if c.node_type == "LEFT"), None)
                op = next((c.value for c in where_node.children if c.node_type == "OP"), None)
                right = next((c.value for c in where_node.children if c.node_type == "RIGHT"), None)
                result["where"] = {"left": left, "op": op, "right": right}
            else:
                result["where"] = None
                
        elif self.node_type == "DELETE":
            result["table"] = self.value
            # 处理 WHERE 条件
            where_node = next((child for child in self.children if child.node_type == "WHERE"), None)
            if where_node:
                left = next((c.value for c in where_node.children if c.node_type == "LEFT"), None)
                op = next((c.value for c in where_node.children if c.node_type == "OP"), None)
                right = next((c.value for c in where_node.children if c.node_type == "RIGHT"), None)
                result["where"] = {"left": left, "op": op, "right": right}
            else:
                result["where"] = None
                
        elif self.node_type == "DROP_TABLE":
            result["table"] = self.value
        else:
            # 默认格式，用于其他类型的节点
            result["value"] = self.value
            result["children"] = [child.to_dict() for child in self.children]
            
        return result

class Parser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0
        self.current_token = self.tokens[self.pos] if self.tokens else None
        self.in_trigger_context = False  # 标记是否在触发器上下文中
        self.current_delimiter = ";"  # 当前语句分隔符，默认为分号

    def advance(self):
        self.pos += 1
        if self.pos < len(self.tokens):
            self.current_token = self.tokens[self.pos]
        else:
            self.current_token = None

    def expect(self, token_type, lexeme=None, context=""):
        if not self.current_token:
            raise ParseError(f"Unexpected end of input, expected {token_type}", None, context)
        if self.current_token.type != token_type:
            context_info = f"{context}:expected_{token_type}_got_{self.current_token.type}"
            raise ParseError(f"Expected token type {token_type} but got {self.current_token.type}", self.current_token, context_info)
        if lexeme and self.current_token.lexeme.upper() != lexeme.upper():
            context_info = f"{context}:expected_{lexeme}_got_{self.current_token.lexeme}"
            raise ParseError(f"Expected '{lexeme}' but got '{self.current_token.lexeme}'", self.current_token, context_info)
        token = self.current_token
        self.advance()
        return token
    
    def expect_delimiter(self, context=""):
        """期望当前分隔符（支持多字符分隔符）"""
        if not self.current_token:
            raise ParseError(f"Unexpected end of input, expected delimiter '{self.current_delimiter}'", None, context)
        
        # 如果分隔符为空，则不期望任何分隔符
        if not self.current_delimiter:
            return None
        
        # 对于多字符分隔符，需要逐个检查字符
        if len(self.current_delimiter) == 1:
            # 单字符分隔符
            if self.current_token.type == "DELIMITER" and self.current_token.lexeme == self.current_delimiter:
                token = self.current_token
                self.advance()
                return token
        else:
            # 多字符分隔符
            delimiter_chars = list(self.current_delimiter)
            for i, char in enumerate(delimiter_chars):
                if not self.current_token or self.current_token.type != "DELIMITER" or self.current_token.lexeme != char:
                    context_info = f"{context}:expected_delimiter_{self.current_delimiter}_got_{self.current_token.lexeme if self.current_token else 'EOF'}"
                    raise ParseError(f"Expected delimiter '{self.current_delimiter}' but got '{self.current_token.lexeme if self.current_token else 'EOF'}'", self.current_token, context_info)
                self.advance()
            return None  # 多字符分隔符不需要返回token
        
        context_info = f"{context}:expected_delimiter_{self.current_delimiter}_got_{self.current_token.lexeme if self.current_token else 'EOF'}"
        raise ParseError(f"Expected delimiter '{self.current_delimiter}' but got '{self.current_token.lexeme if self.current_token else 'EOF'}'", self.current_token, context_info)

    def parse(self):
        """入口，返回 AST 列表（支持多条 SQL 语句）"""
        ast_list = []
        while self.current_token:
            # 检查是否是 DELIMITER 语句
            if self.current_token.lexeme.upper() == "DELIMITER":
                delimiter_node = self.delimiter_statement()
                ast_list.append(delimiter_node)
            else:
                node = self.statement()
                ast_list.append(node)
        return ast_list

    def delimiter_statement(self):
        """解析 DELIMITER 语句"""
        self.expect("KEYWORD", "DELIMITER")
        
        # 获取新的分隔符（可能由多个字符组成）
        if not self.current_token:
            raise ParseError("Expected delimiter after DELIMITER keyword", None, "delimiter_expected")
        
        new_delimiter = ""
        # 收集所有分隔符字符，直到遇到空格或行结束
        while self.current_token and self.current_token.type == "DELIMITER":
            new_delimiter += self.current_token.lexeme
            self.advance()
        
        if not new_delimiter:
            raise ParseError("Expected delimiter after DELIMITER keyword", None, "delimiter_expected")
        
        # 更新当前分隔符
        self.current_delimiter = new_delimiter
        
        # DELIMITER 语句本身以分号结束，不是以新的分隔符结束
        # 检查是否有分号结束符
        if self.current_token and self.current_token.type == "DELIMITER" and self.current_token.lexeme == ";":
            self.advance()
        
        # 返回 DELIMITER 节点
        return ASTNode("DELIMITER_STATEMENT", new_delimiter)

    def statement(self):
        if self.current_token.lexeme.upper() == "CREATE":
            # 检查是 CREATE TABLE 还是 CREATE INDEX
            next_token_idx = self.pos + 1
            if next_token_idx < len(self.tokens):
                next_token = self.tokens[next_token_idx]
                if next_token.lexeme.upper() == "INDEX":
                    return self.create_index()
                elif next_token.lexeme.upper() in ["UNIQUE"]:
                    # 检查 CREATE UNIQUE INDEX
                    unique_next_idx = self.pos + 2
                    if unique_next_idx < len(self.tokens):
                        unique_next_token = self.tokens[unique_next_idx]
                        if unique_next_token.lexeme.upper() == "INDEX":
                            return self.create_index()
                    return self.create_table()
                elif next_token.lexeme.upper() == "TRIGGER":
                    return self.create_trigger()
                else:
                    return self.create_table()
            else:
                return self.create_table()
        elif self.current_token.lexeme.upper() == "INSERT":
            return self.insert()
        elif self.current_token.lexeme.upper() == "SELECT":
            return self.select()
        elif self.current_token.lexeme.upper() == "UPDATE":
            return self.update()
        elif self.current_token.lexeme.upper() == "DELETE":
            return self.delete()
        elif self.current_token.lexeme.upper() == "DROP":
            # 检查是 DROP TABLE 还是 DROP INDEX
            next_token_idx = self.pos + 1
            if next_token_idx < len(self.tokens):
                next_token = self.tokens[next_token_idx]
                if next_token.lexeme.upper() == "INDEX":
                    return self.drop_index()
                elif next_token.lexeme.upper() == "TRIGGER":
                    return self.drop_trigger()
                else:
                    return self.drop_table()
            else:
                return self.drop_table()
        elif self.current_token.lexeme.upper() == "BEGIN":
            return self.begin_transaction()
        elif self.current_token.lexeme.upper() == "COMMIT":
            return self.commit()
        elif self.current_token.lexeme.upper() == "ROLLBACK":
            return self.rollback()
        else:
            raise ParseError(f"Unsupported statement beginning with '{self.current_token.lexeme}'", self.current_token)

    def create_table(self):
        self.expect("KEYWORD", "CREATE")
        self.expect("KEYWORD", "TABLE")
        table_name = self.expect("IDENTIFIER").lexeme
        self.expect("DELIMITER", "(")
        
        columns = []
        primary_keys = []
        foreign_keys = []
        constraints = {}
        
        while True:
            # 检查是否是约束定义
            if (self.current_token and self.current_token.type == "KEYWORD" and 
                self.current_token.lexeme.upper() in ["PRIMARY", "FOREIGN", "CONSTRAINT"]):
                
                if self.current_token.lexeme.upper() == "PRIMARY":
                    # 解析 PRIMARY KEY (col1, col2, ...)
                    self.advance()  # PRIMARY
                    self.expect("KEYWORD", "KEY")
                    self.expect("DELIMITER", "(")
                    while True:
                        pk_col = self.expect("IDENTIFIER").lexeme
                        primary_keys.append(pk_col)
                        if self.current_token.lexeme == ",":
                            self.advance()
                        else:
                            break
                    self.expect("DELIMITER", ")")
                    
                elif self.current_token.lexeme.upper() == "FOREIGN":
                    # 解析 FOREIGN KEY (col) REFERENCES table(col)
                    self.advance()  # FOREIGN
                    self.expect("KEYWORD", "KEY")
                    self.expect("DELIMITER", "(")
                    fk_col = self.expect("IDENTIFIER").lexeme
                    self.expect("DELIMITER", ")")
                    self.expect("KEYWORD", "REFERENCES")
                    ref_table = self.expect("IDENTIFIER").lexeme
                    self.expect("DELIMITER", "(")
                    ref_col = self.expect("IDENTIFIER").lexeme
                    self.expect("DELIMITER", ")")
                    
                    foreign_keys.append({
                        'column': fk_col,
                        'references_table': ref_table,
                        'references_column': ref_col
                    })
            else:
                # 解析普通列定义
                col_name = self.expect("IDENTIFIER").lexeme
                col_type = self.expect("IDENTIFIER").lexeme
                
                # 检查列级约束
                col_constraints = []
                while (self.current_token and self.current_token.type == "KEYWORD" and 
                       self.current_token.lexeme.upper() in ["PRIMARY", "NOT", "UNIQUE"]):
                    
                    if self.current_token.lexeme.upper() == "PRIMARY":
                        self.advance()
                        self.expect("KEYWORD", "KEY")
                        primary_keys.append(col_name)
                        col_constraints.append("PRIMARY_KEY")
                        
                    elif self.current_token.lexeme.upper() == "NOT":
                        self.advance()
                        self.expect("KEYWORD", "NULL")
                        col_constraints.append("NOT_NULL")
                        
                    elif self.current_token.lexeme.upper() == "UNIQUE":
                        self.advance()
                        col_constraints.append("UNIQUE")
                
                columns.append((col_name, col_type))
                if col_constraints:
                    constraints[col_name] = col_constraints
            
            if self.current_token.lexeme == ",":
                self.advance()
            else:
                break
                
        self.expect("DELIMITER", ")")
        # 检查当前分隔符，如果是分号则直接处理，否则使用 expect_delimiter
        if self.current_token and self.current_token.type == "DELIMITER" and self.current_token.lexeme == ";":
            self.advance()
        else:
            self.expect_delimiter()
        
        # 构建 AST 节点
        children = [ASTNode("COLUMN", col_name + ":" + col_type) for col_name, col_type in columns]
        
        if primary_keys:
            children.append(ASTNode("PRIMARY_KEY", ",".join(primary_keys)))
            
        for fk in foreign_keys:
            fk_node = ASTNode("FOREIGN_KEY", f"{fk['column']}:{fk['references_table']}.{fk['references_column']}")
            children.append(fk_node)
            
        for col_name, col_constraints in constraints.items():
            for constraint in col_constraints:
                children.append(ASTNode("CONSTRAINT", f"{col_name}:{constraint}"))
        
        return ASTNode("CREATE_TABLE", table_name, children)

    def insert(self):
        self.expect("KEYWORD", "INSERT")
        self.expect("KEYWORD", "INTO")
        table_name = self.expect("IDENTIFIER").lexeme
        self.expect("DELIMITER", "(")
        columns = []
        while True:
            col_name = self.expect("IDENTIFIER").lexeme
            columns.append(col_name)
            if self.current_token.lexeme == ",":
                self.advance()
            else:
                break
        self.expect("DELIMITER", ")")
        self.expect("KEYWORD", "VALUES")
        self.expect("DELIMITER", "(")
        values = []
        while True:
            val_token = self.current_token
            # 在触发器上下文中，允许 OLD/NEW 引用
            if self.in_trigger_context and val_token.lexeme.upper() in ["OLD", "NEW"]:
                # 解析 OLD.column 或 NEW.column
                prefix = val_token.lexeme.upper()
                self.advance()
                self.expect("DELIMITER", ".")
                column = self.expect("IDENTIFIER").lexeme
                values.append(f"{prefix}.{column}")
            elif val_token.type not in ["CONST", "IDENTIFIER"]:
                raise ParseError("Expected constant or identifier", val_token)
            else:
                values.append(val_token.lexeme)
                self.advance()
            
            if self.current_token.lexeme == ",":
                self.advance()
            else:
                break
        self.expect("DELIMITER", ")")
        # 在触发器上下文中，总是期望分号结束
        if self.in_trigger_context:
            if self.current_token and self.current_token.type == "DELIMITER" and self.current_token.lexeme == ";":
                self.advance()
            else:
                raise ParseError(f"Expected ';' in trigger context, got '{self.current_token.lexeme if self.current_token else 'EOF'}'", self.current_token, "trigger_insert_end")
        else:
            # 检查当前分隔符，如果是分号则直接处理，否则使用 expect_delimiter
            if self.current_token and self.current_token.type == "DELIMITER" and self.current_token.lexeme == ";":
                self.advance()
            else:
                self.expect_delimiter()
        return ASTNode("INSERT", table_name, [ASTNode("COLUMN", col) for col in columns] + [ASTNode("VALUE", v) for v in values])

    def select(self):
        self.expect("KEYWORD", "SELECT")
        
        # 解析列名（支持 * 通配符和聚合函数）
        columns = []
        while True:
            if self.current_token and self.current_token.type == "OPERATOR" and self.current_token.lexeme == "*":
                columns.append("*")
                self.advance()
            elif self.current_token and self.current_token.lexeme.upper() in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
                # 解析聚合函数
                func_node = self.parse_aggregate_function()
                columns.append(func_node)
            else:
                col_expr = self.parse_qualified_identifier()
                # 检查是否有别名 (AS alias)
                if self.current_token and self.current_token.lexeme.upper() == "AS":
                    self.advance()  # 跳过 AS
                    alias = self.expect("IDENTIFIER").lexeme
                    columns.append(f"{col_expr} AS {alias}")
                else:
                    columns.append(col_expr)
            
            if self.current_token and self.current_token.lexeme == ",":
                self.advance()
            else:
                break
                
        self.expect("KEYWORD", "FROM")
        
        # 解析 FROM 子句和可能的 JOIN
        from_clause = self.parse_from_clause()
        
        # 可选 WHERE 子句
        where_node = None
        if self.current_token and self.current_token.lexeme.upper() == "WHERE":
            where_node = self.parse_where()
        
        # 可选 GROUP BY 子句
        group_by_node = None
        if self.current_token and self.current_token.lexeme.upper() == "GROUP":
            group_by_node = self.parse_group_by()
        
        # 可选 ORDER BY 子句
        order_by_node = None
        if self.current_token and self.current_token.lexeme.upper() == "ORDER":
            order_by_node = self.parse_order_by()
            
        self.expect_delimiter()
        
        # 构建 AST
        children = []
        for c in columns:
            # hasattr is safer than isinstance due to potential circular imports
            if hasattr(c, 'node_type') and c.node_type == 'AGGREGATE':
                children.append(c)
            else:
                children.append(ASTNode("COLUMN", c))

        children.append(from_clause)
        if where_node:
            children.append(where_node)
        if group_by_node:
            children.append(group_by_node)
        if order_by_node:
            children.append(order_by_node)
            
        return ASTNode("SELECT", None, children)

    def parse_from_clause(self):
        """解析 FROM 子句，支持 JOIN 和表别名"""
        table_name = self.expect("IDENTIFIER").lexeme
        
        # 检查是否有表别名
        alias = None
        if (self.current_token and 
            self.current_token.type == "IDENTIFIER" and 
            self.current_token.lexeme.upper() not in ["JOIN", "INNER", "LEFT", "RIGHT", "WHERE", "GROUP", "ORDER"]):
            alias = self.current_token.lexeme
            self.advance()
        
        from_node = ASTNode("FROM", table_name)
        if alias:
            from_node.children.append(ASTNode("ALIAS", alias))
        
        # 检查是否有 JOIN
        while self.current_token and self.current_token.lexeme.upper() in ["JOIN", "INNER", "LEFT", "RIGHT"]:
            join_node = self.parse_join()
            from_node.children.append(join_node)
            
        return from_node

    def parse_join(self):
        """解析 JOIN 子句"""
        join_type = "INNER"  # 默认
        
        if self.current_token.lexeme.upper() in ["INNER", "LEFT", "RIGHT"]:
            join_type = self.current_token.lexeme.upper()
            self.advance()
            
        self.expect("KEYWORD", "JOIN")
        table_name = self.expect("IDENTIFIER").lexeme
        
        # 检查是否有表别名
        alias = None
        if (self.current_token and 
            self.current_token.type == "IDENTIFIER" and 
            self.current_token.lexeme.upper() != "ON"):
            alias = self.current_token.lexeme
            self.advance()
        
        self.expect("KEYWORD", "ON", context="join_on_condition")
        
        # 解析 ON 条件
        left = self.parse_qualified_identifier()
        op = self.expect("OPERATOR", context="join_on_operator").lexeme
        right = self.parse_qualified_identifier()
        
        on_condition = ASTNode("ON", None, [
            ASTNode("LEFT", left),
            ASTNode("OP", op), 
            ASTNode("RIGHT", right)
        ])
        
        join_children = [ASTNode("TABLE", table_name)]
        if alias:
            join_children.append(ASTNode("ALIAS", alias))
        join_children.append(on_condition)
        
        return ASTNode("JOIN", join_type, join_children)

    def parse_group_by(self):
        """解析 GROUP BY 子句"""
        self.expect("KEYWORD", "GROUP")
        self.expect("KEYWORD", "BY")
        
        columns = []
        while True:
            columns.append(self.parse_qualified_identifier())
            if self.current_token.lexeme == ",":
                self.advance()
            else:
                break
                
        return ASTNode("GROUP_BY", None, [ASTNode("COLUMN", c) for c in columns])

    def parse_order_by(self):
        """解析 ORDER BY 子句"""
        self.expect("KEYWORD", "ORDER")
        self.expect("KEYWORD", "BY")
        
        columns = []
        while True:
            col_name = self.parse_qualified_identifier()
            direction = "ASC"  # 默认升序
            
            if self.current_token and self.current_token.lexeme.upper() in ["ASC", "DESC"]:
                direction = self.current_token.lexeme.upper()
                self.advance()
                
            columns.append((col_name, direction))
            
            if self.current_token.lexeme == ",":
                self.advance()
            else:
                break
                
        return ASTNode("ORDER_BY", None, [
            ASTNode("SORT", f"{col}:{direction}") for col, direction in columns
        ])

    def delete(self):
        self.expect("KEYWORD", "DELETE")
        self.expect("KEYWORD", "FROM")
        table_name = self.expect("IDENTIFIER").lexeme
        where_node = None
        if self.current_token and self.current_token.lexeme.upper() == "WHERE":
            where_node = self.parse_where()
        self.expect_delimiter()
        children = []
        if where_node:
            children.append(where_node)
        return ASTNode("DELETE", table_name, children)

    def update(self):
        self.expect("KEYWORD", "UPDATE")
        table_name = self.expect("IDENTIFIER").lexeme
        self.expect("KEYWORD", "SET")
        
        # 解析 SET 子句
        assignments = []
        while True:
            # 在 SET 子句中，列名可能是关键字（如 count, sum 等）
            if self.current_token.type == "IDENTIFIER":
                col_name = self.current_token.lexeme
                self.advance()
            elif self.current_token.type == "KEYWORD":
                col_name = self.current_token.lexeme
                self.advance()
            else:
                raise ParseError("Expected column name", self.current_token)
            
            self.expect("OPERATOR", "=")
            # 解析赋值表达式（可能包含运算符和多个操作数）
            value = self.parse_assignment_expression()
            assignments.append((col_name, value))
            
            if self.current_token.lexeme == ",":
                self.advance()
            else:
                break
        
        # 可选 WHERE 子句
        where_node = None
        if self.current_token and self.current_token.lexeme.upper() == "WHERE":
            where_node = self.parse_where()
            
        self.expect_delimiter()
        
        children = [ASTNode("ASSIGNMENT", f"{col}={val}") for col, val in assignments]
        if where_node:
            children.append(where_node)
            
        return ASTNode("UPDATE", table_name, children)
    
    def parse_assignment_expression(self):
        """解析赋值表达式，如 count + 1, count * 2 等"""
        # 解析第一个操作数
        left = self.parse_assignment_operand()
        
        # 检查是否有运算符
        if self.current_token and self.current_token.type == "OPERATOR" and self.current_token.lexeme in ["+", "-", "*", "/"]:
            operator = self.current_token.lexeme
            self.advance()
            right = self.parse_assignment_operand()
            return f"{left} {operator} {right}"
        else:
            return left
    
    def parse_assignment_operand(self):
        """解析赋值表达式中的操作数"""
        if self.current_token.type == "CONST":
            val = self.current_token.lexeme
            self.advance()
            return val
        elif self.current_token.type == "IDENTIFIER":
            val = self.current_token.lexeme
            self.advance()
            return val
        elif self.current_token.type == "KEYWORD":
            # 在赋值表达式中，关键字可能表示列名
            val = self.current_token.lexeme
            self.advance()
            return val
        else:
            raise ParseError(f"Expected operand in assignment expression, got {self.current_token.type}", self.current_token)

    def parse_qualified_identifier(self):
        """解析可能带有表名限定的标识符 (table.column 或 column)"""
        identifier = self.expect("IDENTIFIER", context="qualified_identifier").lexeme
        
        # 检查是否有表名限定符 (.)
        if self.current_token and self.current_token.lexeme == ".":
            self.advance()  # 跳过 '.'
            column = self.expect("IDENTIFIER", context="qualified_identifier_column").lexeme
            return f"{identifier}.{column}"
        else:
            return identifier

    def parse_where(self):
        """解析 WHERE 子句，支持复杂条件表达式"""
        self.advance()  # 跳过 WHERE
        condition = self.parse_or_expression()
        return ASTNode("WHERE", None, [condition])
    
    def parse_or_expression(self):
        """解析 OR 表达式 (最低优先级)"""
        left = self.parse_and_expression()
        
        while self.current_token and self.current_token.lexeme.upper() == "OR":
            self.advance()  # 跳过 OR
            right = self.parse_and_expression()
            left = ASTNode("LOGICAL_OP", "OR", [left, right])
        
        return left
    
    def parse_and_expression(self):
        """解析 AND 表达式"""
        left = self.parse_not_expression()
        
        while self.current_token and self.current_token.lexeme.upper() == "AND":
            self.advance()  # 跳过 AND
            right = self.parse_not_expression()
            left = ASTNode("LOGICAL_OP", "AND", [left, right])
        
        return left
    
    def parse_not_expression(self):
        """解析 NOT 表达式"""
        if self.current_token and self.current_token.lexeme.upper() == "NOT":
            self.advance()  # 跳过 NOT
            expr = self.parse_comparison_expression()
            return ASTNode("LOGICAL_OP", "NOT", [expr])
        else:
            return self.parse_comparison_expression()
    
    def parse_comparison_expression(self):
        """解析比较表达式"""
        if self.current_token and self.current_token.lexeme == "(":
            # 处理括号表达式
            self.advance()  # 跳过 (
            expr = self.parse_or_expression()
            self.expect("DELIMITER", ")")
            return expr
        
        # 解析左操作数
        left = self.parse_qualified_identifier()
        
        # 检查操作符类型
        if not self.current_token:
            raise ParseError("Expected operator after identifier")
            
        if self.current_token.lexeme.upper() == "BETWEEN":
            return self.parse_between_expression(left)
        elif self.current_token.lexeme.upper() == "IN":
            return self.parse_in_expression(left)
        elif self.current_token.lexeme.upper() == "LIKE":
            return self.parse_like_expression(left)
        elif self.current_token.type == "OPERATOR":
            return self.parse_simple_comparison(left)
        else:
            raise ParseError(f"Unexpected token in WHERE clause: {self.current_token.lexeme}")
    
    def parse_simple_comparison(self, left):
        """解析简单比较操作 (=, >, <, >=, <=, !=, <>)"""
        op = self.expect("OPERATOR").lexeme
        right = self.parse_value_or_identifier()
        return ASTNode("COMPARISON", op, [ASTNode("LEFT", left), ASTNode("RIGHT", right)])
    
    def parse_between_expression(self, left):
        """解析 BETWEEN 表达式"""
        self.advance()  # 跳过 BETWEEN
        start_val = self.parse_value_or_identifier()
        self.expect("KEYWORD", "AND")
        end_val = self.parse_value_or_identifier()
        return ASTNode("BETWEEN", None, [
            ASTNode("LEFT", left), 
            ASTNode("START", start_val), 
            ASTNode("END", end_val)
        ])
    
    def parse_in_expression(self, left):
        """解析 IN 表达式"""
        self.advance()  # 跳过 IN
        self.expect("DELIMITER", "(")
        values = []
        while True:
            val = self.parse_value_or_identifier()
            values.append(ASTNode("VALUE", val))
            if self.current_token and self.current_token.lexeme == ",":
                self.advance()
            else:
                break
        self.expect("DELIMITER", ")")
        return ASTNode("IN", None, [ASTNode("LEFT", left)] + values)
    
    def parse_like_expression(self, left):
        """解析 LIKE 表达式"""
        self.advance()  # 跳过 LIKE
        pattern = self.parse_value_or_identifier()
        return ASTNode("LIKE", None, [ASTNode("LEFT", left), ASTNode("PATTERN", pattern)])
    
    def parse_value_or_identifier(self):
        """解析值或标识符"""
        if self.current_token.type == "CONST":
            val = self.current_token.lexeme
            self.advance()
            return val
        elif self.current_token.type == "IDENTIFIER":
            return self.parse_qualified_identifier()
        elif (self.current_token.type == "KEYWORD" and 
              self.current_token.lexeme.upper() in ["NEW", "OLD"]):
            # 在触发器中，NEW 和 OLD 是特殊的关键字，需要特殊处理
            prefix = self.current_token.lexeme.upper()
            self.advance()
            if self.current_token and self.current_token.lexeme == ".":
                self.advance()  # 跳过 '.'
                column = self.expect("IDENTIFIER").lexeme
                return f"{prefix}.{column}"
            else:
                return prefix
        else:
            raise ParseError(f"Expected value or identifier, got {self.current_token.type}")
    
    def parse_aggregate_function(self):
        """解析聚合函数 (COUNT, SUM, AVG, MAX, MIN)"""
        func_name = self.current_token.lexeme.upper()
        self.advance()  # 跳过函数名
        
        self.expect("DELIMITER", "(")
        
        # 处理参数
        if func_name == "COUNT":
            if self.current_token and self.current_token.lexeme == "*":
                arg = "*"
                self.advance()
            elif self.current_token and self.current_token.lexeme.upper() == "DISTINCT":
                self.advance()  # 跳过 DISTINCT
                arg = f"DISTINCT {self.parse_qualified_identifier()}"
            else:
                arg = self.parse_qualified_identifier()
        else:
            # SUM, AVG, MAX, MIN 只接受列名
            if self.current_token and self.current_token.lexeme.upper() == "DISTINCT":
                self.advance()  # 跳过 DISTINCT
                arg = f"DISTINCT {self.parse_qualified_identifier()}"
            else:
                arg = self.parse_qualified_identifier()
        
        self.expect("DELIMITER", ")")
        
        return ASTNode("AGGREGATE", func_name, [ASTNode("ARG", arg)])

    def drop_table(self):
        """解析 DROP TABLE 语句"""
        self.expect("KEYWORD", "DROP")
        self.expect("KEYWORD", "TABLE")
        table_name = self.expect("IDENTIFIER").lexeme
        self.expect_delimiter()
        return ASTNode("DROP_TABLE", table_name)

    def begin_transaction(self):
        """解析 BEGIN TRANSACTION 语句"""
        self.expect("KEYWORD", "BEGIN")
        # TRANSACTION 或 WORK 是可选的
        if self.current_token and self.current_token.lexeme.upper() in ["TRANSACTION", "WORK"]:
            self.advance()
        self.expect_delimiter()
        return ASTNode("BEGIN_TRANSACTION")

    def commit(self):
        """解析 COMMIT 语句"""
        self.expect("KEYWORD", "COMMIT")
        # WORK 是可选的
        if self.current_token and self.current_token.lexeme.upper() == "WORK":
            self.advance()
        self.expect_delimiter()
        return ASTNode("COMMIT")

    def rollback(self):
        """解析 ROLLBACK 语句"""
        self.expect("KEYWORD", "ROLLBACK")
        # WORK 是可选的
        if self.current_token and self.current_token.lexeme.upper() == "WORK":
            self.advance()
        self.expect_delimiter()
        return ASTNode("ROLLBACK")

    def create_index(self):
        """解析 CREATE INDEX 语句"""
        self.expect("KEYWORD", "CREATE")
        
        # 检查是否是 UNIQUE INDEX
        is_unique = False
        if self.current_token and self.current_token.lexeme.upper() == "UNIQUE":
            is_unique = True
            self.advance()
        
        self.expect("KEYWORD", "INDEX")
        index_name = self.expect("IDENTIFIER").lexeme
        
        self.expect("KEYWORD", "ON")
        table_name = self.expect("IDENTIFIER").lexeme
        
        # 解析列列表
        self.expect("DELIMITER", "(")
        columns = []
        while True:
            col_name = self.expect("IDENTIFIER").lexeme
            columns.append(col_name)
            if self.current_token and self.current_token.lexeme == ",":
                self.advance()
            else:
                break
        self.expect("DELIMITER", ")")
        
        # 可选的 USING 子句（指定索引类型）
        index_type = "BTREE"  # 默认为B+树
        if self.current_token and self.current_token.lexeme.upper() == "USING":
            self.advance()
            # 期望索引类型
            if self.current_token and self.current_token.lexeme.upper() in ["BTREE", "HASH"]:
                index_type = self.current_token.lexeme.upper()
                self.advance()
            else:
                # 不支持的索引类型，抛出带有上下文的错误
                context = f"create_index_using_type:{self.current_token.lexeme if self.current_token else 'EOF'}"
                raise ParseError(f"不支持的索引类型 '{self.current_token.lexeme}', 只支持 BTREE 和 HASH", 
                               self.current_token, context)
        
        # 可选的 WHERE 子句（部分索引）
        where_condition = None
        if self.current_token and self.current_token.lexeme.upper() == "WHERE":
            where_condition = self.parse_where()
        
        self.expect_delimiter()
        
        # 构建 AST 节点
        index_node = ASTNode("CREATE_INDEX", index_name)
        index_node.children.append(ASTNode("TABLE", table_name))
        index_node.children.append(ASTNode("COLUMNS", ",".join(columns)))
        index_node.children.append(ASTNode("TYPE", index_type))
        if is_unique:
            index_node.children.append(ASTNode("UNIQUE", "TRUE"))
        if where_condition:
            index_node.children.append(where_condition)
        
        return index_node
    
    def drop_index(self):
        """解析 DROP INDEX 语句"""
        self.expect("KEYWORD", "DROP")
        self.expect("KEYWORD", "INDEX")
        index_name = self.expect("IDENTIFIER").lexeme
        
        # 可选的 ON table_name
        table_name = None
        if self.current_token and self.current_token.lexeme.upper() == "ON":
            self.advance()
            table_name = self.expect("IDENTIFIER").lexeme
        
        self.expect_delimiter()
        
        drop_node = ASTNode("DROP_INDEX", index_name)
        if table_name:
            drop_node.children.append(ASTNode("TABLE", table_name))
        
        return drop_node
    
    def create_trigger(self):
        """解析 CREATE TRIGGER 语句"""
        self.expect("KEYWORD", "CREATE")
        self.expect("KEYWORD", "TRIGGER")
        trigger_name = self.expect("IDENTIFIER").lexeme
        
        # 触发时机: BEFORE | AFTER | INSTEAD OF
        timing = None
        if self.current_token and self.current_token.lexeme.upper() in ["BEFORE", "AFTER"]:
            timing = self.current_token.lexeme.upper()
            self.advance()
        elif self.current_token and self.current_token.lexeme.upper() == "INSTEAD":
            timing = "INSTEAD"
            self.advance()
            self.expect("KEYWORD", "OF")
            timing = "INSTEAD OF"
        else:
            raise ParseError("Expected BEFORE, AFTER, or INSTEAD OF", self.current_token, "trigger_timing")
        
        # 触发事件: INSERT | UPDATE | DELETE
        events = []
        while True:
            if self.current_token and self.current_token.lexeme.upper() in ["INSERT", "UPDATE", "DELETE"]:
                events.append(self.current_token.lexeme.upper())
                self.advance()
                
                # 检查是否有 OR 连接多个事件
                if self.current_token and self.current_token.lexeme.upper() == "OR":
                    self.advance()
                    continue
                else:
                    break
            else:
                break
        
        if not events:
            raise ParseError("Expected trigger event (INSERT, UPDATE, DELETE)", self.current_token, "trigger_event")
        
        # ON table_name
        self.expect("KEYWORD", "ON")
        table_name = self.expect("IDENTIFIER").lexeme
        
        # 可选的 FOR EACH ROW
        for_each_row = False
        if self.current_token and self.current_token.lexeme.upper() == "FOR":
            self.advance()
            self.expect("KEYWORD", "EACH")
            self.expect("KEYWORD", "ROW")
            for_each_row = True
        
        # 可选的 WHEN 条件
        when_condition = None
        if self.current_token and self.current_token.lexeme.upper() == "WHEN":
            self.advance()
            # 解析条件表达式
            when_condition = self.parse_trigger_condition()
        
        # 触发器主体: BEGIN ... END 或单个语句
        trigger_body = self.parse_trigger_body()
        
        # 防御性同步：如果此时仍然停留在 END（极端情况下主体未消费），则手动消费 END 和其后分号
        consumed_end = False
        if self.current_token and self.current_token.type == "KEYWORD" and self.current_token.lexeme.upper() == "END":
            self.advance()
            if self.current_token and self.current_token.type == "DELIMITER" and self.current_token.lexeme == ";":
                self.advance()
            consumed_end = True
        
        # 正常情况下，使用当前语句分隔符结束 CREATE TRIGGER 语句
        if not consumed_end:
            self.expect_delimiter()
        
        # 构建 AST 节点
        trigger_node = ASTNode("CREATE_TRIGGER", trigger_name)
        trigger_node.children.append(ASTNode("TIMING", timing))
        trigger_node.children.append(ASTNode("EVENTS", ",".join(events)))
        trigger_node.children.append(ASTNode("TABLE", table_name))
        trigger_node.children.append(ASTNode("FOR_EACH_ROW", str(for_each_row)))
        
        if when_condition:
            trigger_node.children.append(ASTNode("WHEN_CONDITION", None, [when_condition]))
        
        trigger_node.children.append(trigger_body)
        
        return trigger_node
    
    def drop_trigger(self):
        """解析 DROP TRIGGER 语句"""
        self.expect("KEYWORD", "DROP")
        self.expect("KEYWORD", "TRIGGER")
        trigger_name = self.expect("IDENTIFIER").lexeme
        
        # 可选的 ON table_name
        table_name = None
        if self.current_token and self.current_token.lexeme.upper() == "ON":
            self.advance()
            table_name = self.expect("IDENTIFIER").lexeme
        
        self.expect_delimiter()
        
        drop_node = ASTNode("DROP_TRIGGER", trigger_name)
        if table_name:
            drop_node.children.append(ASTNode("TABLE", table_name))
        
        return drop_node
    
    def parse_trigger_condition(self):
        """解析触发器 WHEN 条件"""
        # 支持复杂的逻辑表达式，如: WHEN NEW.salary > OLD.salary AND NEW.salary > 50000
        return self.parse_trigger_or_expression()
    
    def parse_trigger_or_expression(self):
        """解析触发器 OR 表达式"""
        left = self.parse_trigger_and_expression()
        
        while self.current_token and self.current_token.lexeme.upper() == "OR":
            self.advance()  # 跳过 OR
            right = self.parse_trigger_and_expression()
            left = ASTNode("LOGICAL_OP", "OR", [left, right])
        
        return left
    
    def parse_trigger_and_expression(self):
        """解析触发器 AND 表达式"""
        left = self.parse_trigger_comparison()
        
        while self.current_token and self.current_token.lexeme.upper() == "AND":
            self.advance()  # 跳过 AND
            right = self.parse_trigger_comparison()
            left = ASTNode("LOGICAL_OP", "AND", [left, right])
        
        return left
    
    def parse_trigger_comparison(self):
        """解析触发器比较表达式"""
        left = self.parse_trigger_operand()
        
        if self.current_token and self.current_token.type == "OPERATOR":
            operator = self.current_token.lexeme
            self.advance()
            right = self.parse_trigger_operand()
            return ASTNode("COMPARISON", operator, [ASTNode("LEFT", left), ASTNode("RIGHT", right)])
        else:
            # 单个操作数作为条件
            return left
    
    def parse_trigger_operand(self):
        """解析触发器操作数（支持 OLD.column, NEW.column）"""
        if self.current_token and self.current_token.lexeme.upper() in ["OLD", "NEW"]:
            prefix = self.current_token.lexeme.upper()
            self.advance()
            self.expect("DELIMITER", ".")
            column = self.expect("IDENTIFIER").lexeme
            return f"{prefix}.{column}"
        elif self.current_token and self.current_token.type == "IDENTIFIER":
            val = self.current_token.lexeme
            self.advance()
            return val
        elif self.current_token and self.current_token.type == "CONST":
            val = self.current_token.lexeme
            self.advance()
            return val
        else:
            raise ParseError("Expected operand in trigger condition", self.current_token, "trigger_operand")
    
    def parse_trigger_body(self):
        """解析触发器主体"""
        if self.current_token and self.current_token.lexeme.upper() == "BEGIN":
            # BEGIN ... END 块
            self.advance()
            statements = []
            
            while self.current_token and self.current_token.lexeme.upper() != "END":
                # 解析触发器内部的语句
                stmt = self.parse_trigger_statement()
                statements.append(stmt)
            
            self.expect("KEYWORD", "END")
            
            body_node = ASTNode("TRIGGER_BODY")
            for stmt in statements:
                body_node.children.append(stmt)
            return body_node
        else:
            # 单个语句 - 不需要分号结束，因为触发器定义本身会以分号结束
            old_delimiter = self.current_delimiter
            self.current_delimiter = ""  # 临时清空分隔符，避免期望分号
            stmt = self.parse_trigger_statement()
            self.current_delimiter = old_delimiter  # 恢复分隔符
            body_node = ASTNode("TRIGGER_BODY")
            body_node.children.append(stmt)
            return body_node
    
    def parse_trigger_statement(self):
        """解析触发器内部的语句"""
        # 设置触发器上下文标志
        old_context = self.in_trigger_context
        old_delimiter = self.current_delimiter
        self.in_trigger_context = True
        self.current_delimiter = ";"  # 触发器内部语句使用分号结束
        
        try:
            # 简化版本：支持基本的 INSERT, UPDATE, DELETE 语句
            if self.current_token and self.current_token.lexeme.upper() == "INSERT":
                return self.insert()
            elif self.current_token and self.current_token.lexeme.upper() == "UPDATE":
                return self.update()
            elif self.current_token and self.current_token.lexeme.upper() == "DELETE":
                return self.delete()
            else:
                # 其他语句类型（如变量赋值等）暂时作为通用语句处理
                stmt_text = ""
                while self.current_token and (self.current_token.type != "DELIMITER" or self.current_token.lexeme != ";"):
                    stmt_text += self.current_token.lexeme + " "
                    self.advance()
                
                if self.current_token and self.current_token.type == "DELIMITER" and self.current_token.lexeme == ";":
                    self.advance()
                
                return ASTNode("TRIGGER_STATEMENT", stmt_text.strip())
        finally:
            # 恢复原来的上下文标志和分隔符
            self.in_trigger_context = old_context
            self.current_delimiter = old_delimiter

# 测试
if __name__ == "__main__":
    sql_text = """
    CREATE TABLE student(id INT, name VARCHAR, age INT);
    INSERT INTO student(id,name,age) VALUES (1,'Alice',20);
    SELECT id,name FROM student WHERE age > 18;
    DELETE FROM student WHERE id = 1;
    """
    lexer = Lexer(sql_text)
    tokens, errors = lexer.tokenize()
    if errors:
        print("--- Lexical Errors ---")
        for e in errors:
            print(e)

    parser = Parser(tokens)
    try:
        ast_list = parser.parse()
        for ast in ast_list:
            print(ast)

        # 语义分析
        catalog = Catalog()
        analyzer = SemanticAnalyzer(catalog)
        analyzer.analyze(ast_list)

    except (ParseError, SemanticError) as e:
        print(e)

