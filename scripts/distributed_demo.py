#!/usr/bin/env python3
"""
分布式最小示例：分片 + 分布式查询 + 复制 + 2PC（骨架）
运行：python scripts/distributed_demo.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.api.db_api import DatabaseAPI
from src.distributed.sharding import ShardMetadata, ShardRouter
from src.distributed.executor import DistributedExecutor, RemoteNode
from src.distributed.replication import ReplicatedCluster
from src.distributed.transaction import TwoPhaseCoordinator
from src.distributed.monitoring import SlowQueryLog, Metrics
from src.distributed.coordination import MiniZK


def main():
    # 1) 为逻辑表 T 配置 2 份哈希分片
    meta = ShardMetadata()
    meta.create_hash_shards('T', 2)
    router = ShardRouter(meta)

    # 2) 创建两个节点（各自独立的 DatabaseAPI 实例）并注册为 RemoteNode
    slowlog = SlowQueryLog(threshold_ms=0)  # 演示时阈值设为0，全部记录
    metrics = Metrics()
    node_a = RemoteNode(DatabaseAPI(), name="node_a", slowlog=slowlog)
    node_b = RemoteNode(DatabaseAPI(), name="node_b", slowlog=slowlog)
    # 显式开启自动提交，避免事务可见性问题
    node_a.execute("SET AUTOCOMMIT = ON;")
    node_b.execute("SET AUTOCOMMIT = ON;")
    # 分片ID与节点的简单绑定
    shards = router.all_shards('T')
    shard_to_node = {}
    if len(shards) >= 2:
        shard_to_node[shards[0]['id']] = node_a
        shard_to_node[shards[1]['id']] = node_b
    elif len(shards) == 1:
        shard_to_node[shards[0]['id']] = node_a

    # 3) 在所有分片物化表结构
    for s in router.all_shards('T'):
        n = (shard_to_node.get(s['id']) or node_a)
        n.execute("DROP TABLE T;")
        n.execute("CREATE TABLE T(id INT, name STRING);")

    # 4) 通过路由按主键值决定写入哪个分片
    def insert(id_val: int, name: str):
        s = router.locate_by_value('T', str(id_val))
        n = shard_to_node.get(s[0]['id']) if s else node_a
        n.execute(f"INSERT INTO T(id, name) VALUES ({id_val}, '{name}');")
        # 立即提交并验证该分片上行可见
        n.execute("COMMIT;")
        chk = n.execute("SELECT id,name FROM T;")
        print(f"[VERIFY] on {n.name} rows=", len(chk.get('data') or []))

    insert(1, 'A')
    insert(2, 'B')
    insert(3, 'C')

    # 5) 分布式查询与聚合
    dist = DistributedExecutor(router, {'T': 'id'}, shard_to_node, slowlog=slowlog, metrics=metrics)
    res_all = dist.select_all_shards('T', 'SELECT id,name FROM T;')
    print('[DIST] SELECT * FROM T ->', res_all)

    res_sum = dist.distributed_aggregate_sum('T', 'SELECT SUM(id) FROM {table};')
    print('[DIST] SELECT SUM(id) FROM T ->', res_sum)
    res_cnt = dist.distributed_aggregate_count('T', 'SELECT COUNT(id) FROM {table};')
    print('[DIST] SELECT COUNT(id) FROM T ->', res_cnt)
    res_avg = dist.distributed_aggregate_avg('T', 'SELECT SUM(id) FROM {table};', 'SELECT COUNT(id) FROM {table};')
    print('[DIST] SELECT AVG(id) FROM T ->', res_avg)
    res_minmax = dist.distributed_aggregate_minmax('T', 'SELECT MIN(id) FROM {table};', 'SELECT MAX(id) FROM {table};')
    print('[DIST] SELECT MIN/MAX(id) FROM T ->', res_minmax)

    # 6) 复制与读写分离（示例：给 shard0 绑定一个从节点）
    replica0 = RemoteNode(DatabaseAPI(), name="replica0", slowlog=slowlog)
    replica0.execute("SET AUTOCOMMIT = ON;")
    # 初始化副本结构
    replica0.execute("DROP TABLE T;")
    replica0.execute("CREATE TABLE T(id INT, name STRING);")
    shard0 = shards[0]['id'] if shards else 'T_h0'
    cluster0 = ReplicatedCluster(primary=shard_to_node.get(shard0, node_a), replicas=[replica0])
    cluster0.execute_write("INSERT INTO T(id,name) VALUES (100,'R');")
    print('[REPL] read-from-replica:', cluster0.execute_read('SELECT id,name FROM T;', read_from_replicas=True))

    # 7) 2PC（骨架演示）：参与者需实现 prepare/commit/rollback，这里简单用lambda模拟
    class Participant:
        def __init__(self, node):
            self.node = node
        def prepare(self, txid: str) -> bool:
            return True
        def commit(self, txid: str) -> bool:
            return True
        def rollback(self, txid: str) -> None:
            return None

    p1 = Participant(shard_to_node.get(shards[0]['id']) if shards else node_a)
    p2 = Participant(shard_to_node.get(shards[1]['id']) if len(shards) > 1 else node_b)
    coord = TwoPhaseCoordinator([p1, p2])
    txid = coord.begin()
    ok = coord.commit(txid)
    print('[2PC] commit ok =', ok)
    # 7) MiniZK 演示：注册节点与watch
    zk = MiniZK()
    def on_members_change():
        print('[ZK] members changed:', zk.list_members())
    zk.watch_members(on_members_change)
    zk.add_member('node_a')
    zk.add_member('node_b')
    zk.set_config('router.T.strategy', 'HASH')
    print('[ZK] config snapshot:', zk.get_all_config())

    # 8) 打印慢查询日志
    print('[SLOWLOG]', slowlog.list())
    # 9) 打印指标快照
    print('[METRICS]', metrics.snapshot())

    # 10) 额外演示：范围分片并插入查询
    meta2 = ShardMetadata()
    meta2.create_range_shards('R', [(None, '100'), ('100','200'), ('200', None)])
    router2 = ShardRouter(meta2)
    nodes2 = {}
    for s in router2.all_shards('R'):
        # 复用 node_a/node_b
        nodes2[s['id']] = node_a if '0' in s['id'] else node_b
        nodes2[s['id']].execute('DROP TABLE R;')
        nodes2[s['id']].execute('CREATE TABLE R(id INT, name STRING);')
    # 通过router2按范围定位
    for v in [50, 150, 250]:
        sid = router2.locate_by_value('R', str(v))[0]['id']
        nodes2[sid].execute(f"INSERT INTO R(id,name) VALUES ({v}, 'X{v}');")
        nodes2[sid].execute("COMMIT;")
    # 合并读取R
    dist2 = DistributedExecutor(router2, {'R': 'id'}, nodes2, slowlog=slowlog, metrics=metrics)
    print('[DIST] SELECT * FROM R ->', dist2.select_all_shards('R', 'SELECT id,name FROM R;'))


if __name__ == '__main__':
    main()


