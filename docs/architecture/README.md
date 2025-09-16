## 项目架构总览

- 核心思路: 编译器（词法/语法/语义/计划）→ 适配器生成执行计划 → 核心执行引擎（含 C++ 存储/算子）→ 前端 GUI/CLI 展示
- 分布式: 以轻量 Sharding + 分布式执行器为骨架，支持 HASH/RANGE 分片、跨片聚合与合并、慢查询日志

### 目录映射

- modules/sql_compiler/: 编译器（lexical/syntax/semantic/planner/diagnostics）
- src/api/: 适配器与 API（导入导出、视图展开、索引管理、批量导入）
- src/core/: 执行引擎与存储抽象（含 Python 调度 C++）
- src/distributed/: 分片元数据/路由/执行器/监控
- database_gui.py: GUI，含分布式可视化（热力图、时间线、路由测试）


