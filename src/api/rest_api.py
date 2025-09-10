"""
RESTful API接口
提供HTTP接口供外部调用
"""

from flask import Flask, jsonify, request, g
from .db_api import get_database


def create_rest_app(data_dir: str = 'data'):
    """创建REST API应用，支持自定义数据目录"""
    app = Flask(__name__)

    # 在应用上下文中设置数据库
    @app.before_request
    def before_request():
        from .db_api import get_database
        # 可以通过配置或请求参数设置数据目录
        g.db = get_database(data_dir)

    @app.route('/api/tables', methods=['GET'])
    def list_tables():
        """获取所有表"""
        tables = g.db.list_tables()
        return jsonify({'tables': tables})

    @app.route('/api/tables/<table_name>', methods=['GET'])
    def get_table(table_name):
        """获取表信息"""
        table_info = g.db.get_table_info(table_name)
        if not table_info:
            return jsonify({'error': '表不存在'}), 404
        return jsonify(table_info)

    @app.route('/api/tables', methods=['POST'])
    def create_table():
        """创建表"""
        data = request.get_json()
        if not data or 'table_name' not in data or 'columns' not in data:
            return jsonify({'error': '缺少必要参数'}), 400

        success = g.db.create_table(data['table_name'], data['columns'])
        if success:
            return jsonify({'message': '表创建成功'}), 201
        else:
            return jsonify({'error': '表创建失败'}), 400

    @app.route('/api/tables/<table_name>/data', methods=['POST'])
    def insert_data(table_name):
        """插入数据"""
        data = request.get_json()
        if not data:
            return jsonify({'error': '缺少数据'}), 400

        success = g.db.insert_row(table_name, data)
        if success:
            return jsonify({'message': '数据插入成功'}), 201
        else:
            return jsonify({'error': '数据插入失败'}), 400

    # 修改查询接口返回更多信息
    @app.route('/api/tables/<table_name>/data', methods=['GET'])
    def query_data(table_name):
        """查询数据"""
        limit = request.args.get('limit', 100, type=int)
        data = g.db.scan_rows(table_name, limit)  # 使用新的scan_rows接口
        return jsonify({'data': data, 'count': len(data)})

    @app.route('/api/stats', methods=['GET'])
    def get_stats():
        """获取统计信息"""
        stats = g.db.get_storage_stats()
        return jsonify(stats)

    @app.route('/api/health', methods=['GET'])
    def health_check():
        """健康检查"""
        return jsonify({'status': 'healthy'})

    @app.errorhandler(404)
    def not_found(error):
        return jsonify({'error': '接口不存在'}), 404

    @app.errorhandler(500)
    def internal_error(error):
        return jsonify({'error': '内部服务器错误'}), 500

    return app


# 命令行启动
if __name__ == '__main__':
    app = create_rest_app()
    app.run(debug=True, host='0.0.0.0', port=5000)
