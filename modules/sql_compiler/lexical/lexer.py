# lexer.py
import re
from modules.sql_compiler.rule.rules import KEYWORDS
from modules.sql_compiler.lexical.my_token import Token

ERROR_TYPES = {
    "UNTERMINATED_STRING": "Unterminated String",
    "UNKNOWN_SYMBOL": "Unknown Symbol",
    "INVALID_NUMBER": "Invalid Number",
    "INVALID_IDENTIFIER": "Invalid Identifier",
}


class Lexer:
    def __init__(self, sql_text):
        self.text = sql_text
        self.pos = 0
        self.line = 1
        self.column = 1
        self.tokens = []
        self.errors = []  # 保存错误四元式

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
        error = [error_type, lexeme, line, column]
        self.errors.append(error)
        print(f"❌ Lexical Error: {error_type} '{lexeme}' at line {line}, column {column}")

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

        type_ = "KEYWORD" if lexeme.upper() in KEYWORDS else "IDENTIFIER"
        self.add_token(type_, lexeme, self.line, start_col)

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
            self.add_token("OPERATOR", char, self.line, start_col)
        elif char in "(),;.*":
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

