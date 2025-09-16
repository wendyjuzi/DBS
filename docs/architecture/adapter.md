## 适配器与 API（src/api）

- sql_compiler_adapter.py
  - 执行管线：SQL → Lexer/Parser/Analyzer → Planner → 执行器格式 → HybridExecutor
  - 视图：`CREATE/DROP VIEW`，查询时单表视图展开
  - 索引：`CREATE/DROP (COMPOSITE) INDEX`、`SHOW INDEXES`
  - 导入导出：`IMPORT/EXPORT TABLE`（csv/json），批量导入走 `insert_many`，默认 1000/批
  - 事务：BEGIN/COMMIT/ROLLBACK，autocommit

- unified_api.py / rest_api.py / db_api.py：对外接口封装

注意：
- 导入 CSV 若表不存在，会以首行表头推断并创建 STRING 列
- 导入以列清单 + 字符串字面量写入，避免解析歧义

