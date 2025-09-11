"""
增强的错误类，集成智能诊断功能
"""

from .error_diagnostic import SmartErrorDiagnostic, ErrorFormatter
from typing import List, Dict, Optional


class EnhancedLexicalError:
    """增强的词法错误"""

    def __init__(self, error_type: str, lexeme: str, line: int, column: int, source_line: str = ""):
        self.diagnostic_engine = SmartErrorDiagnostic()
        self.diagnostic = self.diagnostic_engine.diagnose_lexical_error(
            error_type, lexeme, line, column, source_line
        )

    def __str__(self):
        return ErrorFormatter.format_diagnostic(self.diagnostic)


class EnhancedParseError(Exception):
    """增强的语法错误"""

    def __init__(self, message: str, token=None, context: str = ""):
        self.diagnostic_engine = SmartErrorDiagnostic()

        expected = self._extract_expected_from_message(message)
        got = token.lexeme if token else ""
        line = token.line if token else 0
        column = token.column if token else 0

        self.diagnostic = self.diagnostic_engine.diagnose_syntax_error(
            message, expected, got, line, column, context
        )

        enhanced_message = ErrorFormatter.format_diagnostic(self.diagnostic)
        super().__init__(enhanced_message)

    def _extract_expected_from_message(self, message: str) -> str:
        """从错误消息中提取期望的内容"""
        if "expected" in message.lower():
            parts = message.split("expected")
            if len(parts) > 1:
                return parts[1].split("but")[0].strip()
        return ""


class EnhancedSemanticError(Exception):
    """增强的语义错误"""

    def __init__(self, error_type: str, position: str, message: str,
                 available_tables: Optional[List[str]] = None,
                 available_columns: Optional[Dict[str, List[str]]] = None):
        self.diagnostic_engine = SmartErrorDiagnostic()

        self.diagnostic = self.diagnostic_engine.diagnose_semantic_error(
            error_type, position, message, available_tables, available_columns
        )

        enhanced_message = ErrorFormatter.format_diagnostic(self.diagnostic)
        super().__init__(enhanced_message)