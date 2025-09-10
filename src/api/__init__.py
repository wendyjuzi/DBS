"""
API层模块
提供统一的数据库访问接口

注意：为避免在纯本地调用时强依赖 Flask/werkzeug，默认不导入 REST 端。
需要 REST 时，请 from src.api.rest_api import create_rest_app
"""

from .db_api import DatabaseAPI

__all__ = ['DatabaseAPI']