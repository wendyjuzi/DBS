"""
数据库系统主程序入口
"""

import sys
import os
import click

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from frontend.cli import DBShell
from frontend.web.app import create_app


@click.group()
def cli():
    """数据库系统命令行工具"""
    pass


@cli.command()
def shell():
    """启动命令行界面"""
    shell = DBShell()
    shell.cmdloop()


@cli.command()
@click.option('--host', default='localhost', help='服务器主机地址')
@click.option('--port', default=8080, help='服务器端口')
@click.option('--debug', is_flag=True, help='启用调试模式')
def web(host, port, debug):
    """启动Web界面"""
    app = create_app()
    app.run(host=host, port=port, debug=debug)


@cli.command()
def test():
    """运行测试"""
    import subprocess
    subprocess.run(['pytest', 'tests/'])


if __name__ == '__main__':
    cli()
