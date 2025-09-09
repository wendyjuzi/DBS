# my_token.py
class Token:
    def __init__(self, type_, lexeme, line, column):
        self.type = type_      # 种别码：KEYWORD, IDENTIFIER, CONST, OPERATOR, DELIMITER
        self.lexeme = lexeme   # 词素值
        self.line = line       # 行号
        self.column = column   # 列号

    def __repr__(self):
        return f"[{self.type}, {self.lexeme}, {self.line}, {self.column}]"
