# lexer.py
import re
from modules.sql_compiler.rule.rules import KEYWORDS
from modules.sql_compiler.lexical.my_token import Token

# 尝试导入智能诊断模块
try:
    from modules.sql_compiler.diagnostics.error_diagnostic import SmartErrorDiagnostic, ErrorFormatter
    DIAGNOSTICS_AVAILABLE = True
except ImportError:
    DIAGNOSTICS_AVAILABLE = False

ERROR_TYPES = {
    "UNTERMINATED_STRING": "Unterminated String",
    "UNKNOWN_SYMBOL": "Unknown Symbol", 
    "INVALID_NUMBER": "Invalid Number",
    "INVALID_IDENTIFIER": "Invalid Identifier",
    "MISSPELLED_STATEMENT_HEAD": "Misspelled Statement Head",
}


class Lexer:
    def __init__(self, sql_text):
        self.text = sql_text
        self.pos = 0
        self.line = 1
        self.column = 1
        self.tokens = []
        self.errors = []  # 保存错误四元式
        self.is_statement_start = True  # 跟踪是否在语句开头
        if DIAGNOSTICS_AVAILABLE:
            try:
                self.diagnostics = SmartErrorDiagnostic()
            except Exception as e:
                print(f"智能诊断系统初始化失败: {e}")
                self.diagnostics = None
        else:
            self.diagnostics = None

    def peek(self):
        if self.pos < len(self.text):
            return self.text[self.pos]
        return None

    def advance(self):
        char = self.peek()
        self.pos += 1
        if char == '\n':
            self.line += 1
            self.column = 1
        else:
            self.column += 1
        return char

    def add_token(self, type_, lexeme, line, column):
        self.tokens.append(Token(type_, lexeme, line, column))

    def add_error(self, error_type, lexeme, line, column):
        # 使用智能诊断系统生成丰富的错误消息
        if DIAGNOSTICS_AVAILABLE:
            try:
                # 使用智能诊断
                source_line = self._get_source_line(line)
                diagnostic = self.diagnostics.diagnose_lexical_error(
                    error_type, lexeme, line, column, source_line
                )
                # 将格式化的诊断结果作为错误消息存储
                formatted_error = ErrorFormatter.format_diagnostic(diagnostic)
                self.errors.append(formatted_error)
                print(formatted_error)
                return
            except Exception as e:
                print(f"诊断系统错误: {e}")
        
        # 回退到原始错误格式
        if error_type == ERROR_TYPES["MISSPELLED_STATEMENT_HEAD"]:
            error = f"拼写错误的关键字 {lexeme} (第{line}行, 第{column}列)"
        else:
            error = [error_type, lexeme, line, column]
        
        self.errors.append(error)
        
        # 控制台输出
        if error_type == ERROR_TYPES["MISSPELLED_STATEMENT_HEAD"]:
            print(f"❌ Lexical Error: {lexeme} at line {line}, column {column}")
        else:
            print(f"❌ Lexical Error: {error_type} '{lexeme}' at line {line}, column {column}")

    def _get_source_line(self, line_num):
        """获取源代码行"""
        lines = self.text.split('\n')
        if 1 <= line_num <= len(lines):
            return lines[line_num - 1]
        return ""
    
    def _levenshtein_distance(self, a: str, b: str) -> int:
        """计算编辑距离"""
        la, lb = len(a), len(b)
        if la == 0:
            return lb
        if lb == 0:
            return la
        dp = [[0] * (lb + 1) for _ in range(la + 1)]
        for i in range(la + 1):
            dp[i][0] = i
        for j in range(lb + 1):
            dp[0][j] = j
        for i in range(1, la + 1):
            ca = a[i - 1]
            for j in range(1, lb + 1):
                cb = b[j - 1]
                cost = 0 if ca == cb else 1
                dp[i][j] = min(
                    dp[i - 1][j] + 1,
                    dp[i][j - 1] + 1,
                    dp[i - 1][j - 1] + cost
                )
        return dp[la][lb]
    
    def _suggest_statement_head(self, lexeme: str) -> str:
        """为语句开头的标识符建议关键字"""
        statement_heads = ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "SHOW", "BEGIN", "COMMIT", "ROLLBACK", "CALL"]
        
        upper_lexeme = lexeme.upper()
        best_match = None
        best_distance = float('inf')
        
        for head in statement_heads:
            distance = self._levenshtein_distance(upper_lexeme, head)
            if distance <= 2 and distance < best_distance:
                best_match = head
                best_distance = distance
        
        return best_match

    def skip_whitespace(self):
        while self.peek() is not None and self.peek().isspace():
            self.advance()

    def lex_identifier_or_keyword(self):
        start_col = self.column
        start_pos = self.pos
        while self.peek() is not None and (self.peek().isalnum() or self.peek() == '_'):
            self.advance()
        lexeme = self.text[start_pos:self.pos]

        # 错误：数字开头的标识符
        if lexeme[0].isdigit():
            self.add_error(ERROR_TYPES["INVALID_IDENTIFIER"], lexeme, self.line, start_col)
            return

        if lexeme.upper() in KEYWORDS:
            type_ = "KEYWORD"
        else:
            # 如果在语句开头且是标识符，检查是否是拼写错误的语句头
            if self.is_statement_start:
                suggestion = self._suggest_statement_head(lexeme)
                if suggestion:
                    self.add_error(ERROR_TYPES["MISSPELLED_STATEMENT_HEAD"], 
                                 lexeme, self.line, start_col)
                    return
            type_ = "IDENTIFIER"
        
        self.add_token(type_, lexeme, self.line, start_col)
        
        # 更新语句开头标志
        if type_ == "KEYWORD" and lexeme.upper() in ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "SHOW", "BEGIN", "COMMIT", "ROLLBACK", "CALL"]:
            self.is_statement_start = False

    def lex_number(self):
        start_col = self.column
        start_pos = self.pos
        dot_count = 0
        while self.peek() is not None and (self.peek().isdigit() or self.peek() == '.'):
            if self.peek() == '.':
                dot_count += 1
            self.advance()
        lexeme = self.text[start_pos:self.pos]

        if dot_count > 1:
            self.add_error(ERROR_TYPES["INVALID_NUMBER"], lexeme, self.line, start_col)
            return

        self.add_token("CONST", lexeme, self.line, start_col)

    def lex_string(self):
        start_col = self.column
        self.advance()  # skip opening '
        start_pos = self.pos
        while self.peek() is not None and self.peek() != "'":
            self.advance()

        if self.peek() != "'":
            self.add_error(ERROR_TYPES["UNTERMINATED_STRING"], self.text[start_pos:self.pos], self.line, start_col)
            return

        lexeme = self.text[start_pos:self.pos]
        self.advance()  # skip closing '
        self.add_token("CONST", lexeme, self.line, start_col)

    def lex_operator_or_delimiter(self):
        start_col = self.column
        char = self.advance()
        if char in "=<>":
            if self.peek() == '=':
                char += self.advance()
            elif char == '<' and self.peek() == '>':
                char += self.advance()  # 支持 <> (不等于)
            elif char == '!' and self.peek() == '=':
                char += self.advance()  # 支持 != (不等于)
            self.add_token("OPERATOR", char, self.line, start_col)
        elif char in "+-*/%":
            self.add_token("OPERATOR", char, self.line, start_col)
        elif char == '!' and self.peek() == '=':
            char += self.advance()  # 处理 != 操作符
            self.add_token("OPERATOR", char, self.line, start_col)
        elif char in "(),;.*":
            self.add_token("DELIMITER", char, self.line, start_col)
            # 分号标志语句结束，下一个token可能是新语句的开头
            if char == ";":
                self.is_statement_start = True
        elif char in "$@#%&":
            # 支持常见的自定义分隔符字符
            self.add_token("DELIMITER", char, self.line, start_col)
        else:
            self.add_error(ERROR_TYPES["UNKNOWN_SYMBOL"], char, self.line, start_col)

    def tokenize(self):
        while self.peek() is not None:
            self.skip_whitespace()
            if self.peek() is None:
                break
            char = self.peek()
            if char.isalpha() or char == '_':
                self.lex_identifier_or_keyword()
            elif char.isdigit():
                self.lex_number()
            elif char == "'":
                self.lex_string()
            else:
                self.lex_operator_or_delimiter()
        return self.tokens, self.errors


if __name__ == "__main__":
    sql_text = """
    DELETE FROM student WHERE id = 12.34.56;
    """
    lexer = Lexer(sql_text)

    # 注意解包，tokenize 返回 (tokens, errors)
    tokens, errors = lexer.tokenize()

    print("\n--- Tokens ---")
    for t in tokens:
        # 每个 token 单独一行输出
        print(f"[{t.type}, {t.lexeme}, {t.line}, {t.column}]")

    print("\n--- Errors ---")
    for e in errors:
        print(e)

