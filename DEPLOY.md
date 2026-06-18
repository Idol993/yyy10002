# 分布式缓存分片调度服务 - 部署说明

## 架构概述

```
┌─────────────────────────────────────────────────────┐
│                   CacheService (aiohttp)             │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │
│  │RateLimiter│ │CircuitBrk│ │RetryExec │ │Idempot │ │
│  └──────────┘ └──────────┘ └──────────┘ └────────┘ │
├─────────────────────────────────────────────────────┤
│                   NodeManager                        │
│  ┌──────────────┐  ┌──────────────┐                 │
│  │MigrationMgr  │  │RebalanceTask │                 │
│  └──────────────┘  └──────────────┘                 │
├─────────────────────────────────────────────────────┤
│  ┌────────────────┐  ┌──────────────────┐           │
│  │ConsistentHash  │  │FaultToleranceMgr │           │
│  │Ring            │  │  LocalCache      │           │
│  │  VirtualNodes  │  │  ReplicaTakeover │           │
│  │  WeightMech    │  │  AlertCallback   │           │
│  └────────────────┘  └──────────────────┘           │
├─────────────────────────────────────────────────────┤
│  ┌────────────────┐  ┌──────────────────┐           │
│  │MetadataStore   │  │MetricsCollector  │           │
│  │  LevelDB       │  │  Prometheus      │           │
│  └────────────────┘  └──────────────────┘           │
└─────────────────────────────────────────────────────┘
```

## 分层模块说明

| 层次 | 模块 | 职责 |
|------|------|------|
| 算法层 | `dcache.algorithm.consistent_hash` | 改进版一致性哈希环、虚拟节点权重、分片迁移计划、负载均衡重平衡 |
| 持久化层 | `dcache.persistence.metadata_store` | LevelDB元数据持久化、拓扑快照、故障恢复、JSON降级后备 |
| 容错层 | `dcache.fault_tolerance.manager` | 本地缓存降级、副本接管、故障告警回调 |
| 监控层 | `dcache.monitor.metrics` | Prometheus指标埋点、请求计时器 |
| 网络服务层 | `dcache.service.cache_service` + `resilience` | aiohttp异步HTTP接口、限流、熔断、重试、幂等 |
| 节点管理层 | `dcache.node_manager.manager` | 节点动态上下线、非阻塞迁移、心跳检测、定时重平衡 |

## 环境要求

- Python 3.10+
- LevelDB C库 (plyvel依赖)
- 操作系统: Linux 推荐 / Windows 可用(需LevelDB编译环境)

## 安装步骤

```bash
# 1. 安装LevelDB系统库 (Ubuntu/Debian)
sudo apt-get install libleveldb-dev

# 2. 安装Python依赖
pip install -r requirements.txt

# 3. 验证安装
python -c "from dcache.main import DistributedCacheServer; print('OK')"
```

## 启动服务

```bash
# 基础启动
python -m dcache.main --port 8080

# 完整参数启动
python -m dcache.main \
  --host 0.0.0.0 \
  --port 8080 \
  --db-path /data/dcache/leveldb \
  --vnodes-per-weight 150 \
  --replication-factor 2 \
  --rate-limit 2000 \
  --rebalance-interval 60 \
  --skew-threshold 0.3 \
  --migration-concurrency 4 \
  --log-level INFO
```

## API接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/cache/{key}` | 读取缓存 |
| PUT | `/cache/{key}` | 写入缓存 (body: `{"value": ...}`) |
| DELETE | `/cache/{key}` | 删除缓存 |
| POST | `/cluster/nodes` | 添加节点 (body: `{"node_id": "...", "host": "...", "port": N, "weight": N}`) |
| DELETE | `/cluster/nodes/{node_id}` | 移除节点 |
| GET | `/cluster/status` | 集群状态 |
| POST | `/cluster/heartbeat/{node_id}` | 心跳上报 |
| GET | `/metrics` | Prometheus指标 |
| GET | `/health` | 健康检查 |

## 水平扩展上限

### 理论上限
- **节点数**: 单调度实例可管理约 **200-500** 个缓存节点
- 虚拟节点总数: 建议 **15,000 - 75,000** (节点数 × vnodes_per_weight × 平均权重)
- 超过500节点时, 哈希环查找仍为O(log N), 但迁移开销显著增加

### 实际推荐
| 场景 | 节点数 | vnodes_per_weight | 总虚拟节点 | 重量级节点权重 |
|------|--------|-------------------|-----------|---------------|
| 小型集群 | 3-10 | 100 | 300-3000 | 1-3 |
| 中型集群 | 10-50 | 150 | 3000-15000 | 1-5 |
| 大型集群 | 50-200 | 200 | 15000-60000 | 1-10 |

## 分片数量最优配置

### 关键参数

1. **vnodes_per_weight**: 每权重单位虚拟节点数
   - 越大 → 负载越均匀, 但内存占用更高
   - 推荐值: **150** (平衡点)
   - 下限: 50 (低于此值倾斜严重)
   - 上限: 500 (边际收益递减)

2. **replication_factor**: 副本数
   - 生产环境推荐: **2-3**
   - 值越大, 容错越强, 写放大越高

3. **skew_threshold**: 重平衡倾斜阈值
   - 推荐值: **0.3** (30%偏差触发重平衡)
   - 严格场景: 0.15
   - 宽松场景: 0.5

4. **migration_concurrency**: 并发迁移数
   - 推荐值: **4**
   - 高配集群: 8
   - 低配集群: 2

### 容量规划

```
单节点内存 ≈ (key_count / node_count) × avg_value_size
总虚拟节点 ≈ node_count × vnodes_per_weight × avg_weight
迁移批次大小 = 100 keys/batch (内置默认)
```

示例: 1000万key, 100字节均值, 10节点
- 每节点: 100万key ≈ 100MB
- 虚拟节点: 10 × 150 × 2(平均权重) = 3000
- 节点宕机迁移: 约1-3秒 (非阻塞)

## 运维操作

### 添加节点
```bash
curl -X POST http://localhost:8080/cluster/nodes \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node_4","host":"10.0.1.4","port":6379,"weight":2}'
```

### 移除节点
```bash
curl -X DELETE http://localhost:8080/cluster/nodes/node_4
```

### 心跳上报 (缓存节点定期调用)
```bash
curl -X POST http://localhost:8080/cluster/heartbeat/node_4
```

### 查看集群状态
```bash
curl http://localhost:8080/cluster/status
```

### Prometheus指标
```bash
curl http://localhost:8080/metrics
```

关键指标:
- `dcache_migration_duration_seconds` - 分片迁移耗时
- `dcache_node_hit_rate` - 节点命中率
- `dcache_fault_total` - 故障次数
- `dcache_request_duration_seconds` - 请求延迟分布
- `dcache_active_nodes` - 活跃节点数
- `dcache_rebalance_runs_total` - 重平衡运行次数

## 压力测试

```bash
# 启动服务
python -m dcache.main --port 8080 &

# 运行压测
python tests/stress_test.py --url http://127.0.0.1:8080 --keys 5000 --concurrent 50 --duration 30
```

## 故障恢复

1. **进程崩溃重启**: LevelDB持久化分片拓扑, 自动恢复
2. **节点宕机**: 心跳超时 → 自动标记离线 → 副本接管 → 本地缓存降级
3. **网络分区**: 熔断器开启 → 请求降级 → 半开探测恢复

## 注意事项

- 不使用Redis集群、etcd、Zookeeper等外部协调组件
- 全部分片逻辑自实现, 仅依赖LevelDB做本地持久化
- Windows环境需编译LevelDB C库, 若不可用自动降级为JSON文件存储
- 生产环境建议Linux部署, LevelDB性能更稳定
