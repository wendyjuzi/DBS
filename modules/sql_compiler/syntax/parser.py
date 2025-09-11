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

    def parse(self):
        """入口，返回 AST 列表（支持多条 SQL 语句）"""
        ast_list = []
        while self.current_token:
            node = self.statement()
            ast_list.append(node)
        return ast_list

    def statement(self):
        if self.current_token.lexeme.upper() == "CREATE":
            return self.create_table()
        elif self.current_token.lexeme.upper() == "INSERT":
            return self.insert()
        elif self.current_token.lexeme.upper() == "SELECT":
            return self.select()
        elif self.current_token.lexeme.upper() == "DELETE":
            return self.delete()
        elif self.current_token.lexeme.upper() == "UPDATE":
            return self.update()
        else:
            # 提供上下文信息给智能诊断
            context = f"statement_start:{self.current_token.lexeme}"
            raise ParseError(f"Unknown statement '{self.current_token.lexeme}'", self.current_token, context)

    def create_table(self):
        self.expect("KEYWORD", "CREATE")
        self.expect("KEYWORD", "TABLE")
        table_name = self.expect("IDENTIFIER").lexeme
        self.expect("DELIMITER", "(")
        columns = []
        while True:
            col_name = self.expect("IDENTIFIER").lexeme
            col_type = self.expect("IDENTIFIER").lexeme
            columns.append((col_name, col_type))
            if self.current_token.lexeme == ",":
                self.advance()
            else:
                break
        self.expect("DELIMITER", ")")
        self.expect("DELIMITER", ";")
        return ASTNode("CREATE_TABLE", table_name, [ASTNode("COLUMN", col_name + ":" + col_type) for col_name, col_type in columns])

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
            if val_token.type not in ["CONST", "IDENTIFIER"]:
                raise ParseError("Expected constant or identifier", val_token)
            values.append(val_token.lexeme)
            self.advance()
            if self.current_token.lexeme == ",":
                self.advance()
            else:
                break
        self.expect("DELIMITER", ")")
        self.expect("DELIMITER", ";")
        return ASTNode("INSERT", table_name, [ASTNode("COLUMN", col) for col in columns] + [ASTNode("VALUE", v) for v in values])

    def select(self):
        self.expect("KEYWORD", "SELECT")
        
        # 解析列名（支持 * 通配符）
        columns = []
        while True:
            if self.current_token and self.current_token.type == "OPERATOR" and self.current_token.lexeme == "*":
                columns.append("*")
                self.advance()
            else:
                columns.append(self.parse_qualified_identifier())
            
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
            
        self.expect("DELIMITER", ";")
        
        # 构建 AST
        children = [ASTNode("COLUMN", c) for c in columns]
        children.append(from_clause)
        if where_node:
            children.append(where_node)
        if group_by_node:
            children.append(group_by_node)
        if order_by_node:
            children.append(order_by_node)
            
        return ASTNode("SELECT", None, children)

    def parse_from_clause(self):
        """解析 FROM 子句，支持 JOIN"""
        table_name = self.expect("IDENTIFIER").lexeme
        from_node = ASTNode("FROM", table_name)
        
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
        
        return ASTNode("JOIN", join_type, [
            ASTNode("TABLE", table_name),
            on_condition
        ])

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
        self.expect("DELIMITER", ";")
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
            col_name = self.expect("IDENTIFIER").lexeme
            self.expect("OPERATOR", "=")
            val_token = self.current_token
            if val_token.type not in ["CONST", "IDENTIFIER"]:
                raise ParseError("Expected constant or identifier", val_token)
            value = val_token.lexeme
            self.advance()
            assignments.append((col_name, value))
            
            if self.current_token.lexeme == ",":
                self.advance()
            else:
                break
        
        # 可选 WHERE 子句
        where_node = None
        if self.current_token and self.current_token.lexeme.upper() == "WHERE":
            where_node = self.parse_where()
            
        self.expect("DELIMITER", ";")
        
        children = [ASTNode("ASSIGNMENT", f"{col}={val}") for col, val in assignments]
        if where_node:
            children.append(where_node)
            
        return ASTNode("UPDATE", table_name, children)

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
        """解析 WHERE 子句，返回 WHERE 节点"""
        self.advance()  # 跳过 WHERE
        left = self.parse_qualified_identifier()
        op = self.expect("OPERATOR").lexeme
        right = self.expect("CONST").lexeme
        return ASTNode("WHERE", None, [ASTNode("LEFT", left), ASTNode("OP", op), ASTNode("RIGHT", right)])

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

