"""
日志配置
"""

import logging
import sys


def get_logger(name: str = "db") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(stream=sys.stdout)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return logger


class NullLogger:
    """轻量空记录器，避免在早期导入阶段循环依赖。"""

    def __init__(self, name: str = "db") -> None:
        self._name = name

    def info(self, msg: str, *args, **kwargs) -> None:
        pass

    def warning(self, msg: str, *args, **kwargs) -> None:
        pass

    def error(self, msg: str, *args, **kwargs) -> None:
        pass

    def debug(self, msg: str, *args, **kwargs) -> None:
        pass