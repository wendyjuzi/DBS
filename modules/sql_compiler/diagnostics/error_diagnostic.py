"""
智能错误诊断和纠错提示系统

功能：
1. 智能错误分析和分类
2. 上下文感知的纠错建议
3. 相似度匹配和拼写检查
4. 语法结构建议
5. 语义完整性检查
"""

import re
import difflib
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class ErrorSeverity(Enum):
    """错误严重程度"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """错误类别"""
    LEXICAL = "lexical"  # 词法错误
    SYNTAX = "syntax"  # 语法错误
    SEMANTIC = "semantic"  # 语义错误
    TYPE = "type"  # 类型错误
    REFERENCE = "reference"  # 引用错误
    CONSTRAINT = "constraint"  # 约束错误


@dataclass
class ErrorSuggestion:
    """错误建议"""
    suggestion: str  # 建议内容
    confidence: float  # 置信度 (0-1)
    fix_type: str  # 修复类型
    example: Optional[str] = None  # 示例代码


@dataclass
class DiagnosticResult:
    """诊断结果"""
    error_type: str
    message: str
    line: int
    column: int
    severity: ErrorSeverity
    category: ErrorCategory
    suggestions: List[ErrorSuggestion]
    context: Dict[str, Any]


class SmartErrorDiagnostic:
    """智能错误诊断器"""

    def __init__(self):
        # 常见拼写错误映射
        self.common_misspellings = {
            'SELCT': 'SELECT',
            'SLECT': 'SELECT',
            'SELLECT': 'SELECT',
            'CREAT': 'CREATE',
            'CRAETE': 'CREATE',
            'INSER': 'INSERT',
            'INSRET': 'INSERT',
            'UPDAT': 'UPDATE',
            'UPDAET': 'UPDATE',
            'DELET': 'DELETE',
            'DLEET': 'DELETE',
            'DORP': 'DROP',
            'DRPO': 'DROP',
            'DRAP': 'DROP',
            'WHER': 'WHERE',
            'WHRE': 'WHERE',
            'FORM': 'FROM',
            'FOM': 'FROM',
            'JION': 'JOIN',
            'JOING': 'JOIN',
            'GRUP': 'GROUP',
            'GRPUP': 'GROUP',
            'ODER': 'ORDER',
            'ORDR': 'ORDER',
            'INENR': 'INNER',
            'INNNER': 'INNER',
            'LEFFT': 'LEFT',
            'RGHT': 'RIGHT',
            'VARCHARE': 'VARCHAR',
            'VARCHA': 'VARCHAR',
            'INTEGR': 'INTEGER',
            'INEGER': 'INTEGER',
            # 事务相关拼写错误
            'TRAN': 'TRANSACTION',
            'TRANS': 'TRANSACTION',
            'TRANSACT': 'TRANSACTION',
            'BEGINN': 'BEGIN',
            'COMMITT': 'COMMIT',
            'ROLLBAK': 'ROLLBACK',
            'ROLLBAC': 'ROLLBACK'
        }

        # SQL关键字
        self.sql_keywords = {
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
            'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'JOIN',
            'INNER', 'LEFT', 'RIGHT', 'ON', 'GROUP', 'BY', 'ORDER',
            'ASC', 'DESC', 'HAVING', 'UNION', 'ALL', 'DISTINCT',
            'AS', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN',
            'IS', 'NULL', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN',
            # 事务相关关键字
            'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION', 'WORK'
        }

        # 数据类型
        self.data_types = {
            'INT', 'INTEGER', 'VARCHAR', 'CHAR', 'TEXT', 'DATE',
            'DATETIME', 'TIMESTAMP', 'FLOAT', 'DOUBLE', 'DECIMAL',
            'BOOLEAN', 'BOOL', 'BLOB', 'JSON'
        }

    def diagnose_lexical_error(self, error_msg: str, lexeme: str, line: int, column: int,
                               source_line: str = "") -> DiagnosticResult:
        """诊断词法错误"""
        suggestions = []

        # 检查是否是拼写错误
        if lexeme.upper() in self.common_misspellings:
            correct_word = self.common_misspellings[lexeme.upper()]
            suggestions.append(ErrorSuggestion(
                suggestion=f"将 '{lexeme}' 更正为 '{correct_word}'",
                confidence=0.9,
                fix_type="spelling_correction",
                example=f"正确写法: {correct_word}"
            ))

        # 模糊匹配建议
        elif lexeme.upper() not in self.sql_keywords:
            close_matches = difflib.get_close_matches(
                lexeme.upper(), self.sql_keywords, n=3, cutoff=0.6
            )
            for match in close_matches:
                suggestions.append(ErrorSuggestion(
                    suggestion=f"您是否想输入 '{match}'?",
                    confidence=0.7,
                    fix_type="fuzzy_match",
                    example=f"建议: {match}"
                ))

        # 检查未闭合字符串
        if "Unterminated String" in error_msg:
            suggestions.append(ErrorSuggestion(
                suggestion="字符串未正确闭合，请检查是否缺少引号",
                confidence=0.8,
                fix_type="string_termination",
                example="正确格式: 'your_string' 或 \"your_string\""
            ))

        # 检查无效数字
        if "Invalid Number" in error_msg:
            suggestions.append(ErrorSuggestion(
                suggestion="数字格式不正确，小数点只能有一个",
                confidence=0.9,
                fix_type="number_format",
                example="正确格式: 123 或 123.45"
            ))

        # 检查未知符号
        if "Unknown Symbol" in error_msg:
            if lexeme == '.':
                suggestions.append(ErrorSuggestion(
                    suggestion="点号 '.' 用于限定列名，如 table.column",
                    confidence=0.8,
                    fix_type="dot_usage",
                    example="使用: students.name, users.id"
                ))
            elif lexeme == '*':
                suggestions.append(ErrorSuggestion(
                    suggestion="星号 '*' 用于选择所有列",
                    confidence=0.9,
                    fix_type="wildcard_usage",
                    example="使用: SELECT * FROM table_name"
                ))
            elif lexeme in "+-/%":
                suggestions.append(ErrorSuggestion(
                    suggestion=f"'{lexeme}' 是算术运算符",
                    confidence=0.9,
                    fix_type="arithmetic_operator",
                    example=f"使用: column1 {lexeme} column2"
                ))
            else:
                suggestions.append(ErrorSuggestion(
                    suggestion=f"'{lexeme}' 不是有效的SQL符号",
                    confidence=0.9,
                    fix_type="invalid_symbol",
                    example="检查是否输入了错误的字符"
                ))

        return DiagnosticResult(
            error_type="LexicalError",
            message=self._enhance_error_message(error_msg, suggestions),
            line=line,
            column=column,
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.LEXICAL,
            suggestions=suggestions,
            context={"lexeme": lexeme, "source_line": source_line}
        )

    def diagnose_syntax_error(self, error_msg: str, expected: str, got: str, 
                             line: int, column: int, context: str = "") -> DiagnosticResult:
        """诊断语法错误"""
        suggestions = []
        
        # 处理索引类型错误的特殊情况
        if "不支持的索引类型" in error_msg:
            # 从错误消息中提取索引类型
            import re
            match = re.search(r"不支持的索引类型 '(\w+)'", error_msg)
            if match:
                index_type = match.groups()[0]
                suggestions.append(ErrorSuggestion(
                    suggestion=f"'{index_type}' 不是支持的索引类型",
                    confidence=0.95,
                    fix_type="unsupported_index_type",
                    example="支持的索引类型: BTREE, HASH"
                ))
                
                # 尝试模糊匹配
                index_types = ['BTREE', 'HASH']
                close_matches = difflib.get_close_matches(
                    index_type.upper(), index_types, n=2, cutoff=0.5
                )
                if close_matches:
                    for match_type in close_matches:
                        suggestions.append(ErrorSuggestion(
                            suggestion=f"您是否想使用索引类型 '{match_type}'?",
                            confidence=0.8,
                            fix_type="index_type_suggestion",
                            example=f"使用: USING {match_type}"
                        ))
                
                suggestions.append(ErrorSuggestion(
                    suggestion="B+树索引适合范围查询，哈希索引适合等值查询",
                    confidence=0.8,
                    fix_type="index_type_guidance",
                    example="范围查询选择BTREE，精确匹配选择HASH"
                ))
        
        # 分析未知语句（可能是拼写错误）
        elif "Unknown statement" in error_msg and context.startswith("statement_start:"):
            statement_word = context.split(":")[1]
            if statement_word.upper() in self.common_misspellings:
                correct_word = self.common_misspellings[statement_word.upper()]
                suggestions.append(ErrorSuggestion(
                    suggestion=f"将 '{statement_word}' 更正为 '{correct_word}'",
                    confidence=0.95,
                    fix_type="statement_spelling_correction",
                    example=f"正确写法: {correct_word}"
                ))
            else:
                # 模糊匹配SQL关键字
                close_matches = difflib.get_close_matches(
                    statement_word.upper(), ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP'], n=3, cutoff=0.6
                )
                for match in close_matches:
                    suggestions.append(ErrorSuggestion(
                        suggestion=f"您是否想输入 '{match}'?",
                        confidence=0.8,
                        fix_type="statement_fuzzy_match",
                        example=f"建议: {match}"
                    ))
        
        # 分析缺少的分号
        if "expected DELIMITER" in error_msg or "expected ;" in error_msg.lower():
            suggestions.append(ErrorSuggestion(
                suggestion="SQL语句结尾缺少分号 (;)",
                confidence=0.95,
                fix_type="missing_semicolon",
                example="在语句末尾添加 ;"
            ))

        # 分析意外的输入结束
        if "Unexpected end of input" in error_msg:
            suggestions.append(ErrorSuggestion(
                suggestion="语句不完整，可能缺少关键字或符号",
                confidence=0.9,
                fix_type="incomplete_statement",
                example="检查语句是否完整，如缺少分号、括号等"
            ))

        # 分析括号不匹配
        if "(" in expected or ")" in expected:
            suggestions.append(ErrorSuggestion(
                suggestion="括号不匹配，请检查左括号和右括号是否配对",
                confidence=0.8,
                fix_type="bracket_mismatch",
                example="正确格式: CREATE TABLE name (col1 INT, col2 VARCHAR)"
            ))

        # 分析关键字错误
        if "Expected token type KEYWORD" in error_msg:
            # 特殊处理 JOIN ON 条件缺失
            if "join_on_condition" in context:
                suggestions.append(ErrorSuggestion(
                    suggestion="JOIN语句缺少ON条件",
                    confidence=0.95,
                    fix_type="missing_join_on",
                    example="正确格式: JOIN table_name ON table1.column = table2.column"
                ))
            else:
                missing_keywords = self._suggest_missing_keywords(context, expected)
                for keyword in missing_keywords:
                    suggestions.append(ErrorSuggestion(
                        suggestion=f"可能缺少关键字 '{keyword}'",
                        confidence=0.7,
                        fix_type="missing_keyword",
                        example=f"尝试添加: {keyword}"
                    ))

        # 特殊处理事务语句错误
        if "Expected token type DELIMITER but got IDENTIFIER" in error_msg:
            if got.upper() in ['TRAN', 'TRANS', 'TRANSACT']:
                suggestions.append(ErrorSuggestion(
                    suggestion=f"'{got}' 不是有效的关键字，您可能想输入 'TRANSACTION'",
                    confidence=0.95,
                    fix_type="transaction_keyword_correction",
                    example="正确格式: BEGIN TRANSACTION; 或 BEGIN WORK; 或简单的 BEGIN;"
                ))
            elif "BEGIN" in context.upper():
                suggestions.append(ErrorSuggestion(
                    suggestion="BEGIN 语句语法错误",
                    confidence=0.9,
                    fix_type="begin_statement_syntax",
                    example="正确格式: BEGIN; 或 BEGIN TRANSACTION; 或 BEGIN WORK;"
                ))
            elif "USING" in context.upper() or "index" in context.lower() or "create_index_using_type" in context:
                # 索引类型错误
                if got.upper() in ['UNKNOWN', 'INVALID', 'UNSUPPORTED']:
                    suggestions.append(ErrorSuggestion(
                        suggestion=f"'{got}' 不是支持的索引类型",
                        confidence=0.95,
                        fix_type="unsupported_index_type",
                        example="支持的索引类型: BTREE, HASH"
                    ))
                    suggestions.append(ErrorSuggestion(
                        suggestion="B+树索引适合范围查询，哈希索引适合等值查询",
                        confidence=0.8,
                        fix_type="index_type_guidance",
                        example="使用: USING BTREE 或 USING HASH"
                    ))
                else:
                    # 其他可能的索引类型拼写错误
                    index_types = ['BTREE', 'HASH']
                    close_matches = difflib.get_close_matches(
                        got.upper(), index_types, n=2, cutoff=0.5
                    )
                    if close_matches:
                        for match in close_matches:
                            suggestions.append(ErrorSuggestion(
                                suggestion=f"您是否想使用索引类型 '{match}'?",
                                confidence=0.8,
                                fix_type="index_type_suggestion",
                                example=f"使用: USING {match}"
                            ))
                    else:
                        suggestions.append(ErrorSuggestion(
                            suggestion="只支持 BTREE 和 HASH 索引类型",
                            confidence=0.9,
                            fix_type="supported_index_types_info",
                            example="使用: USING BTREE 或 USING HASH"
                        ))
        
        # 分析标识符问题
        if "identifier" in expected.lower() or "IDENTIFIER" in expected:
            if got == "*" and "SELECT" in context.upper():
                suggestions.append(ErrorSuggestion(
                    suggestion="SELECT语句中的 '*' 通配符应该被正确识别",
                    confidence=0.9,
                    fix_type="wildcard_in_select",
                    example="使用: SELECT * FROM table_name"
                ))
            else:
                suggestions.extend(self._suggest_identifier_fixes(context, got))
        
        # 分析操作符问题（可能是缺少点号的限定标识符）
        if "Expected token type OPERATOR" in error_msg and got == "IDENTIFIER":
            if "join_on_operator" in context or "ON" in context.upper() or "join" in context.lower():
                suggestions.append(ErrorSuggestion(
                    suggestion="JOIN ON条件中的限定标识符缺少点号",
                    confidence=0.95,
                    fix_type="missing_dot_in_join_condition",
                    example="正确格式: ON students.class_id = classes.id"
                ))
            else:
                suggestions.append(ErrorSuggestion(
                    suggestion="可能是限定标识符缺少点号，应该使用 table.column 格式",
                    confidence=0.9,
                    fix_type="missing_dot_in_qualified_identifier",
                    example="正确格式: table_name.column_name"
                ))

        # 分析JOIN语法问题
        if ("join" in context.lower() or "JOIN" in context or 
            (got == "DELIMITER" and "Expected token type KEYWORD" in error_msg)):
            # 如果在JOIN后遇到分号，很可能是缺少ON条件
            if got == "DELIMITER":
                suggestions.append(ErrorSuggestion(
                    suggestion="JOIN语句缺少ON条件，不能直接以分号结束",
                    confidence=0.95,
                    fix_type="join_missing_on_semicolon",
                    example="正确格式: FROM users JOIN orders ON users.id = orders.user_id"
                ))
            else:
                suggestions.append(ErrorSuggestion(
                    suggestion="JOIN语法错误，请检查ON条件",
                    confidence=0.8,
                    fix_type="join_syntax",
                    example="正确格式: table1 JOIN table2 ON table1.id = table2.id"
                ))

        # 分析限定标识符问题（table.column）
        if "DELIMITER" in got and "." in context:
            suggestions.append(ErrorSuggestion(
                suggestion="使用限定标识符时，请确保格式正确",
                confidence=0.8,
                fix_type="qualified_identifier",
                example="正确格式: table_name.column_name"
            ))

        return DiagnosticResult(
            error_type="SyntaxError",
            message=self._enhance_error_message(error_msg, suggestions),
            line=line,
            column=column,
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.SYNTAX,
            suggestions=suggestions,
            context={"expected": expected, "got": got, "context": context}
        )

    def diagnose_semantic_error(self, error_type: str, position: str, message: str,
                                available_tables: List[str] = None,
                                available_columns: Dict[str, List[str]] = None) -> DiagnosticResult:
        """诊断语义错误"""
        suggestions = []

        if error_type == "TableError":
            # 表不存在错误
            if available_tables:
                close_matches = difflib.get_close_matches(
                    position, available_tables, n=3, cutoff=0.6
                )
                for match in close_matches:
                    suggestions.append(ErrorSuggestion(
                        suggestion=f"您是否想引用表 '{match}'?",
                        confidence=0.8,
                        fix_type="table_suggestion",
                        example=f"使用: {match}"
                    ))

            suggestions.append(ErrorSuggestion(
                suggestion="请确保表已创建，或检查表名拼写",
                confidence=0.9,
                fix_type="table_creation_check",
                example="使用: CREATE TABLE your_table (...)"
            ))

        elif error_type == "ColumnError":
            # 列不存在错误
            if position == "*":
                suggestions.append(ErrorSuggestion(
                    suggestion="'*' 通配符应该被正确识别，这可能是语义分析器的问题",
                    confidence=0.9,
                    fix_type="wildcard_semantic_error",
                    example="'*' 表示选择所有列"
                ))
            elif "索引" in message and "引用的列" in message:
                # 索引相关的列错误
                if available_columns:
                    # 从错误消息中提取表名
                    import re
                    match = re.search(r"在表 '(\w+)' 中不存在", message)
                    if match:
                        table_name = match.groups()[0]
                        if table_name in available_columns:
                            table_columns = available_columns[table_name]
                            close_matches = difflib.get_close_matches(
                                position, table_columns, n=3, cutoff=0.6
                            )
                            for match_col in close_matches:
                                suggestions.append(ErrorSuggestion(
                                    suggestion=f"您是否想引用列 '{match_col}'?",
                                    confidence=0.8,
                                    fix_type="index_column_suggestion",
                                    example=f"CREATE INDEX idx_name ON {table_name} ({match_col})"
                                ))
                
                suggestions.append(ErrorSuggestion(
                    suggestion="检查索引列名是否正确拼写和存在",
                    confidence=0.9,
                    fix_type="index_column_check",
                    example="确保CREATE INDEX中的列名在表中已定义"
                ))
                
                # 提供查看表结构的建议
                suggestions.append(ErrorSuggestion(
                    suggestion="可以先查看表结构确认列名",
                    confidence=0.7,
                    fix_type="table_structure_check",
                    example="使用 DESCRIBE table_name 查看表结构"
                ))
                
                # 提供表的实际列信息
                if available_columns:
                    match = re.search(r"在表 '(\w+)' 中不存在", message)
                    if match:
                        table_name = match.groups()[0]
                        if table_name in available_columns:
                            actual_columns = available_columns[table_name]
                            suggestions.append(ErrorSuggestion(
                                suggestion=f"表 '{table_name}' 的可用列: {', '.join(actual_columns)}",
                                confidence=0.9,
                                fix_type="available_columns_info",
                                example=f"选择其中一个列: {actual_columns[0] if actual_columns else 'N/A'}"
                            ))
            elif available_columns:
                all_columns = []
                for table, cols in available_columns.items():
                    all_columns.extend([f"{table}.{col}" for col in cols])
                    all_columns.extend(cols)
                
                # 移除重复项
                all_columns = list(set(all_columns))
                
                close_matches = difflib.get_close_matches(
                    position, all_columns, n=3, cutoff=0.6
                )
                for match in close_matches:
                    suggestions.append(ErrorSuggestion(
                        suggestion=f"您是否想引用列 '{match}'?",
                        confidence=0.8,
                        fix_type="column_suggestion",
                        example=f"使用: {match}"
                    ))
            
            # 检查是否需要表名限定
            if '.' not in position and available_columns and len(available_columns) > 1:
                suggestions.append(ErrorSuggestion(
                    suggestion="在多表查询中，请使用表名限定列名",
                    confidence=0.7,
                    fix_type="table_qualification",
                    example="使用: table_name.column_name"
                ))

        elif error_type == "TypeError":
            # 类型错误
            suggestions.append(ErrorSuggestion(
                suggestion="数据类型不匹配，请检查值的类型",
                confidence=0.9,
                fix_type="type_mismatch",
                example="INT类型使用数字，VARCHAR类型使用字符串"
            ))

        elif error_type == "ColumnCountError":
            # 列数量错误
            suggestions.append(ErrorSuggestion(
                suggestion="INSERT语句中列数和值数量不匹配",
                confidence=0.9,
                fix_type="column_count_mismatch",
                example="确保VALUES中的值数量与列数量相同"
            ))
        
        elif error_type == "PrimaryKeyError":
            # 主键错误
            if "缺少主键列" in message:
                suggestions.append(ErrorSuggestion(
                    suggestion="INSERT语句必须包含所有主键列的值",
                    confidence=0.95,
                    fix_type="missing_primary_key",
                    example="INSERT INTO table(id, name) VALUES(1, 'John') -- id是主键"
                ))
            elif "不能为空" in message:
                suggestions.append(ErrorSuggestion(
                    suggestion="主键列的值不能为空或NULL",
                    confidence=0.95,
                    fix_type="null_primary_key",
                    example="确保主键列有有效的非空值"
                ))
            elif "不存在于表定义中" in message:
                suggestions.append(ErrorSuggestion(
                    suggestion="主键引用的列必须在表中定义",
                    confidence=0.9,
                    fix_type="invalid_primary_key_column",
                    example="检查PRIMARY KEY中的列名是否正确"
                ))
        
        elif error_type == "ForeignKeyError":
            # 外键错误
            if "不存在于表定义中" in message:
                suggestions.append(ErrorSuggestion(
                    suggestion="外键列必须在当前表中定义",
                    confidence=0.9,
                    fix_type="invalid_foreign_key_column",
                    example="确保FOREIGN KEY中的列名在表中存在"
                ))
            elif "引用的表" in message and "不存在" in message:
                suggestions.append(ErrorSuggestion(
                    suggestion="外键引用的表必须已经存在",
                    confidence=0.9,
                    fix_type="missing_referenced_table",
                    example="先创建被引用的表，再创建包含外键的表"
                ))
            elif "引用的列" in message and "不存在" in message:
                suggestions.append(ErrorSuggestion(
                    suggestion="外键引用的列必须在目标表中存在",
                    confidence=0.9,
                    fix_type="missing_referenced_column",
                    example="检查REFERENCES中的列名是否正确"
                ))
            elif "无法删除表" in message:
                suggestions.append(ErrorSuggestion(
                    suggestion="不能删除被其他表外键引用的表",
                    confidence=0.95,
                    fix_type="foreign_key_dependency",
                    example="先删除引用此表的外键约束，再删除表"
                ))

        elif error_type == "IndexError":
            # 索引错误
            if "引用的列" in message and "不存在" in message:
                # 分析列名，提供相似列名建议
                if available_columns:
                    # 从错误消息中提取表名和列名
                    # 格式: "索引 'idx_name' 引用的列 'col_name' 在表 'table_name' 中不存在"
                    import re
                    match = re.search(r"引用的列 '(\w+)' 在表 '(\w+)' 中不存在", message)
                    if match:
                        col_name, table_name = match.groups()
                        if table_name in available_columns:
                            table_columns = available_columns[table_name]
                            close_matches = difflib.get_close_matches(
                                col_name, table_columns, n=3, cutoff=0.6
                            )
                            for match_col in close_matches:
                                suggestions.append(ErrorSuggestion(
                                    suggestion=f"您是否想引用列 '{match_col}'?",
                                    confidence=0.8,
                                    fix_type="index_column_suggestion",
                                    example=f"CREATE INDEX idx_name ON {table_name} ({match_col})"
                                ))
                
                suggestions.append(ErrorSuggestion(
                    suggestion="检查索引列名是否正确拼写和存在",
                    confidence=0.9,
                    fix_type="index_column_check",
                    example="确保CREATE INDEX中的列名在表中已定义"
                ))
                
                # 提供查看表结构的建议
                suggestions.append(ErrorSuggestion(
                    suggestion="可以先查看表结构确认列名",
                    confidence=0.7,
                    fix_type="table_structure_check",
                    example="使用 DESCRIBE table_name 查看表结构"
                ))
            
            elif "表" in message and "不存在" in message:
                # 索引引用的表不存在
                if available_tables:
                    # 从错误消息中提取表名
                    import re
                    match = re.search(r"引用的表 '(\w+)' 不存在", message)
                    if match:
                        table_name = match.groups()[0]
                        close_matches = difflib.get_close_matches(
                            table_name, available_tables, n=3, cutoff=0.6
                        )
                        for match_table in close_matches:
                            suggestions.append(ErrorSuggestion(
                                suggestion=f"您是否想引用表 '{match_table}'?",
                                confidence=0.8,
                                fix_type="index_table_suggestion",
                                example=f"CREATE INDEX idx_name ON {match_table} (column_name)"
                            ))
                
                suggestions.append(ErrorSuggestion(
                    suggestion="请确保表已创建，或检查表名拼写",
                    confidence=0.9,
                    fix_type="index_table_check",
                    example="先使用 CREATE TABLE 创建表，再创建索引"
                ))
            
            elif "不支持的索引类型" in message:
                suggestions.append(ErrorSuggestion(
                    suggestion="只支持 BTREE 和 HASH 索引类型",
                    confidence=0.95,
                    fix_type="supported_index_types",
                    example="使用: USING BTREE 或 USING HASH"
                ))
                
                suggestions.append(ErrorSuggestion(
                    suggestion="B+树索引适合范围查询，哈希索引适合等值查询",
                    confidence=0.8,
                    fix_type="index_type_guidance",
                    example="范围查询选择BTREE，精确匹配选择HASH"
                ))
            
            else:
                # 通用索引错误建议
                suggestions.append(ErrorSuggestion(
                    suggestion="检查索引语法是否正确",
                    confidence=0.7,
                    fix_type="index_syntax_check",
                    example="CREATE INDEX index_name ON table_name (column1, column2)"
                ))
                
                suggestions.append(ErrorSuggestion(
                    suggestion="索引名在数据库中应该是唯一的",
                    confidence=0.6,
                    fix_type="index_name_uniqueness",
                    example="使用描述性的索引名，如 idx_table_column"
                ))

        return DiagnosticResult(
            error_type="SemanticError",
            message=self._enhance_error_message(message, suggestions),
            line=0,  # 语义错误通常没有具体行号
            column=0,
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.SEMANTIC,
            suggestions=suggestions,
            context={
                "error_type": error_type,
                "position": position,
                "available_tables": available_tables,
                "available_columns": available_columns
            }
        )

    def _suggest_missing_keywords(self, context: str, expected: str) -> List[str]:
        """根据上下文建议缺少的关键字"""
        suggestions = []
        context_upper = context.upper()

        if "SELECT" in context_upper and "FROM" not in context_upper:
            suggestions.append("FROM")
        elif "INSERT" in context_upper and "INTO" not in context_upper:
            suggestions.append("INTO")
        elif "INSERT INTO" in context_upper and "VALUES" not in context_upper:
            suggestions.append("VALUES")
        elif "UPDATE" in context_upper and "SET" not in context_upper:
            suggestions.append("SET")
        elif "DELETE" in context_upper and "FROM" not in context_upper:
            suggestions.append("FROM")
        elif "JOIN" in context_upper and "ON" not in context_upper:
            suggestions.append("ON")

        return suggestions

    def _suggest_identifier_fixes(self, context: str, got: str) -> List[ErrorSuggestion]:
        """建议标识符修复"""
        suggestions = []

        # 检查是否包含非法字符
        if re.search(r'[^a-zA-Z0-9_.]', got):
            suggestions.append(ErrorSuggestion(
                suggestion="标识符只能包含字母、数字、下划线和点号",
                confidence=0.9,
                fix_type="identifier_format",
                example="正确格式: table_name, column1, user_id, table.column"
            ))

        # 检查是否以数字开头
        if got and got[0].isdigit():
            suggestions.append(ErrorSuggestion(
                suggestion="标识符不能以数字开头",
                confidence=0.9,
                fix_type="identifier_start",
                example=f"使用: _{got} 或 t{got}"
            ))

        return suggestions

    def _enhance_error_message(self, original_message: str, suggestions: List[ErrorSuggestion]) -> str:
        """增强错误消息"""
        if not suggestions:
            return original_message

        enhanced = f"{original_message}\n\n💡 智能建议："
        for i, suggestion in enumerate(suggestions, 1):
            confidence_icon = "🎯" if suggestion.confidence > 0.8 else "💭"
            enhanced += f"\n{confidence_icon} {i}. {suggestion.suggestion}"
            if suggestion.example:
                enhanced += f"\n   示例: {suggestion.example}"

        return enhanced


class ErrorFormatter:
    """错误格式化器"""

    @staticmethod
    def format_diagnostic(diagnostic: DiagnosticResult) -> str:
        """格式化诊断结果"""
        severity_icons = {
            ErrorSeverity.INFO: "ℹ️",
            ErrorSeverity.WARNING: "⚠️",
            ErrorSeverity.ERROR: "❌",
            ErrorSeverity.CRITICAL: "🚨"
        }

        category_icons = {
            ErrorCategory.LEXICAL: "🔤",
            ErrorCategory.SYNTAX: "📝",
            ErrorCategory.SEMANTIC: "🧠",
            ErrorCategory.TYPE: "🏷️",
            ErrorCategory.REFERENCE: "🔗",
            ErrorCategory.CONSTRAINT: "🔒"
        }

        icon = severity_icons.get(diagnostic.severity, "❓")
        cat_icon = category_icons.get(diagnostic.category, "📋")

        result = f"{icon} {cat_icon} {diagnostic.error_type}"

        if diagnostic.line > 0:
            result += f" (第{diagnostic.line}行, 第{diagnostic.column}列)"

        result += f"\n{diagnostic.message}"

        return result

    @staticmethod
    def format_suggestion_summary(diagnostics: List[DiagnosticResult]) -> str:
        """格式化建议摘要"""
        if not diagnostics:
            return "✅ 没有发现错误"

        summary = f"📊 发现 {len(diagnostics)} 个问题:\n"

        by_category = {}
        for diag in diagnostics:
            category = diag.category.value
            if category not in by_category:
                by_category[category] = 0
            by_category[category] += 1

        for category, count in by_category.items():
            summary += f"  • {category}: {count}个\n"

        return summary
