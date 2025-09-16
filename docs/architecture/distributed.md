## 分布式（src/distributed）

- sharding.py
  - ShardMetadata: `create_hash_shards(table,n)`、`create_range_shards(table, ranges)`
  - ShardRouter: `locate_by_value(table,key)`、`all_shards(table)`

- executor.py
  - DistributedExecutor: 模板 SQL fan-out，合并结果；示例：分布式 SUM(id)

- monitoring.py
  - SlowQueryLog: 记录每片耗时
  - Metrics: 简单计数/指标

GUI 可视化（database_gui.py）
- 范围/哈希分片初始化；路由测试(key→shard→node)
- 分片热力图（各片行数）
- 并行时间线（每片 SQL 耗时条）

