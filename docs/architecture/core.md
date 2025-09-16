## 核心执行与存储（src/core）

- executor/:
  - _execute_select/_execute_group_by/_execute_aggregate：查询/分组/聚合
  - 聚合回退：无 GROUP BY 时本地聚合（COUNT/SUM/AVG/MIN/MAX）
  - 事务与 WAL：基础日志、MVCC 可见性替换骨架

- storage/: 缓冲池、页、文件、WAL、内存存储抽象
- index/: B+Tree 与索引管理
- hybrid_engine.py：Python 调度 C++ 存储与算子

增强：
- 视图查询展开后生成的计划可直接执行
- select_many/insert_many：批量插入用于导入提速

