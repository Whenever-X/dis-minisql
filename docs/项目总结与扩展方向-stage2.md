# 分布式 MiniSQL 项目总结与扩展方向（Stage 2）

## 1. Stage 2 完成内容

在 Stage 1 已具备分片、多数派写入、WAL/snapshot、跨分片查询合并和动态 rebalance 的基础上，本阶段主要补齐**人机交互**和**运维可观测性**，并修正 rebalance 中 primary 分布不均的问题。

### 1.1 Coordinator Client（集群入口 CLI）

新增 `CoordinatorCli`，通过 `DisMiniSql client <config>` 或 `./run.sh` 启动。

- 与 `CoordinatorServer` **进程分离**：退出 client 不停止后台 Coordinator、DataNode、ZooKeeper。
- 通过 HTTP 访问 `POST /sql`、`GET /nodes`、`GET /metadata`、`GET /admin/health`、`POST /admin/rebalance`、`POST /admin/snapshot`。
- 支持 `shard <key> <sql>` 显式指定分片键，便于单分片路由测试。
- 面向**分布式功能验证**：DDL 广播、写入路由、跨分片读、扩容运维。

### 1.2 DataNode Local CLI（单节点调试 CLI）

新增 `DataNodeCli`，通过 `datanode <config> <nodeId> --cli` 启动。

- 与 DataNode **同进程**，调用 `DataNodeServer.executeLocalSql`，不经过 Coordinator。
- 在 `dataDir/cli-session` 维护持久 MiniSQL 会话：WAL 或 snapshot 压缩点变化时后台重放，交互时**只显示当前 SQL 输出**。
- 面向**副本数据排查**：对比各节点本地表内容、验证恢复后数据是否正确。
- **不写入分布式 WAL**，不能替代 Coordinator 作为写入入口。

### 1.3 MiniSQL 调用双模式

`MiniSqlCli` 区分：

| 模式 | 方法 | 使用场景 | 特点 |
|------|------|----------|------|
| 无状态 | `execute` | HTTP `/execute` | 每次临时目录，全量重放 WAL，保证与日志一致 |
| 会话 | `executeInDirectory` | DataNode Local CLI | 固定 `cli-session` 目录，减少重复重放与输出噪音 |

### 1.4 Rebalance Primary 均衡

`ZkMetadataStore.rebalance` 在轮转计算副本集合后，增加 `assignBalancedPrimaries`：按分片 ID 顺序，在每个分片副本内选择**当前 primary 数量最少**的节点作为 primary，避免多个 shard 的 primary 集中到同一节点（例如排序后总是 `replicas.get(0)`）。

`electPrimary` 在 failover 时同样采用最少 primary 负载策略，结合其他分片已有 primary 计数做选择。

### 1.5 文档与测试配套

- `docs/设计报告.md`：补充两类 Client 架构、CLI 会话路径、rebalance primary 均衡说明。
- `docs/纯手动交互式系统测试.md`、`docs/测试文档.md`：以 Coordinator Client 为主的手工测试流程。

## 2. 当前系统能力边界（Stage 2 视角）

**已经较好覆盖：**

- 中心化 SQL 入口（Coordinator HTTP + Coordinator Client）。
- 分片写入、多数派确认、读副本轮询。
- WAL 追赶恢复、snapshot 压缩、无状态 HTTP 执行。
- 基础跨分片查询合并（聚合、排序、简单 join）。
- 动态扩容 rebalance、primary 大致均衡。
- 单节点本地调试 CLI。

**仍存在的明确局限：**

- DataNode Local CLI 写入不进 WAL，不参与复制，仅用于调试。
- HTTP 路径每次仍启动 MiniSQL 子进程并重放 WAL，吞吐有限。
- Rebalance 只改 ZooKeeper 元数据中的副本列表，**不迁移**已有分片数据。
- Raft 仍是教学版多数派模型，无完整日志复制与成员变更。
- 复杂 SQL 依赖 Coordinator 内存合并，不适合大结果集。

## 3. Stage 3 推荐扩展方向

以下按**投入产出比**和与当前架构的衔接程度排序。每项说明目标、建议落点和与现有代码的关系。

### 3.1 DataNode 常驻 MiniSQL 进程（高优先级）

**目标：** 去掉 HTTP 路径每次 `ProcessBuilder` 启动与全量 WAL 重放的开销。

**建议做法：**

- 在 `MiniSqlCli` 或新类 `MiniSqlSession` 中维护长期运行的 MiniSQL 进程，通过 stdin/stdout 或 Unix domain socket 发送 SQL。
- HTTP `/execute` 仅在启动恢复、snapshot 切换或进程异常退出时做一次性全量重放；正常请求只发送当前 SQL。
- DataNode Local CLI 的 `cli-session` 可复用同一会话实现。

**关联代码：** `MiniSqlCli.java`、`DataNodeServer.execute()`。

### 3.2 分片数据迁移与 Rebalance 状态机（高优先级）

**目标：** 扩容/rebalance 后，新副本不仅更新 ZooKeeper 元数据，还能**真正拥有**分片数据。

**建议做法：**

- 为每个 `(shardId, targetNodeId)` 维护迁移任务：`PENDING` → `COPYING_SNAPSHOT` → `COPYING_WAL` → `CATCHING_UP` → `READY`。
- 新节点从源副本拉取 `GET /recovery-state`（snapshot SQL + WAL），在本地重放至 `commitIndex` 一致后再加入 `replicas` 并标记 `SERVING`。
- Coordinator `rebalance` 改为两阶段：先计算目标布局并创建迁移任务，待任务完成后再切换 primary。
- 在 Coordinator Client 增加 `migration` 或扩展 `health` 输出迁移进度。

**关联代码：** `ZkMetadataStore.rebalance`、`DataNodeServer.recoverFromPeers`、`RecoveryState`。

### 3.3 完整 per-shard Raft（中高优先级）

**目标：** 从「教学版多数派」升级为可演示 leader 选举、日志冲突处理的 Raft group。

**建议做法：**

- 每个分片独立 Raft：leader 接收写入，followers 按 `shardLogIndex` 顺序追加，commit 后执行 MiniSQL。
- Coordinator 写入只发往 leader；`commitIndex` 由 Raft 推进，而非 Coordinator 直接 `updateCommitIndex`。
- failover 与 rebalance 的 primary 选择可与 Raft leader 统一，减少两套语义。

**关联代码：** `CoordinatorServer` 写路径、`ZkMetadataStore.electPrimary`、`ExecuteRequest.replay`。

### 3.4 表级元数据与分片键配置（中优先级）

**目标：** 不再默认 INSERT 第一列为分片键；支持 `CREATE TABLE ... SHARD KEY (col)` 或配置文件声明。

**建议做法：**

- 在 ZooKeeper 增加 `/tables/<name>`，存储字段、主键、分片键、副本策略。
- Coordinator 路由时查表元数据；DDL 时注册元数据。
- Coordinator Client 的 `metadata` 可扩展为 `tables` 子命令。

**关联代码：** `SqlUtils` 分片键推断、`CoordinatorServer` 路由。

### 3.5 结构化结果与 Coordinator 查询下推（中优先级）

**目标：** 减少文本解析脆弱性，支持更大规模跨分片聚合。

**建议做法：**

- DataNode `/execute` 可选返回 JSON `{columns, rows}`（需 MiniSQL 侧配合或 Java 层强化解析）。
- `QueryPostProcessor` 对 `sum/count` 等尽量改写为分片级预聚合 SQL，Coordinator 只做最终合并。
- Coordinator Client 对表格结果做对齐打印。

**关联代码：** `MiniSqlResultParser`、`QueryPostProcessor`、`ExecuteResponse`。

### 3.6 一致性与故障测试自动化（中优先级）

**目标：** 覆盖 Stage 2 新增能力，防止回归。

**建议用例：**

- Coordinator Client：DDL、分片写入、`shard` 单分片读、rebalance 后 primary 分布断言。
- DataNode Local CLI：同一条 `select` 在多个 `--cli` 会话中结果与 Coordinator 读一致（恢复完成后）。
- Rebalance 后统计各节点 primary 数量，断言 `max - min <= 1`。
- 第四节点加入后迁移任务（待 3.2 实现）端到端脚本。

**关联代码：** `scripts/e2e_system_test.py`、新建 `scripts/cli_smoke_test.sh`。

### 3.7 可观测性与运维增强（低中优先级）

**目标：** 便于答辩演示和线上排查。

**建议做法：**

- DataNode/Coordinator 暴露 `/metrics` 或结构化 JSON 日志（WAL 长度、重放耗时、primary 分布）。
- Coordinator Client 增加 `watch health` 或定时刷新分片视图。
- DataNode Local CLI 增加只读命令：`wal stats`、`session status`（不重放即可查看 WAL 条数、compact 点）。

## 4. 推荐实施路线（Stage 3）

若继续迭代，建议按以下顺序推进，每步都可独立演示：

```mermaid
flowchart LR
    A[3.1 常驻 MiniSQL] --> B[3.2 分片迁移]
    B --> C[3.3 per-shard Raft]
    C --> D[3.4 表元数据]
    D --> E[3.5 结构化查询]
    E --> F[3.6 自动化测试]
    F --> G[3.7 可观测性]
```

| 阶段 | 内容 | 预期收益 |
|------|------|----------|
| Step 1 | 常驻 MiniSQL + HTTP 路径增量执行 | 写入/查询延迟明显下降，CLI 与 HTTP 体验一致 |
| Step 2 | Rebalance 数据迁移状态机 | 扩容后新节点真有数据，不再只靠元数据 |
| Step 3 | per-shard Raft | 一致性模型可讲清楚，primary 语义统一 |
| Step 4 | 表元数据 + 结构化结果 | SQL 路由更合理，复杂查询更稳 |
| Step 5 | 测试与 metrics | 回归安全，答辩可展示监控面板 |

## 5. 与 Stage 1 文档的关系

- `docs/项目总结与扩展方向-stage1.md`：记录从单机到分布式的第一阶段能力与当时规划的扩展项；其中多项（snapshot、shardLogIndex、查询合并、rebalance 接口）已在代码中实现。
- 本文档（Stage 2）：记录 **Client 分层、CLI 会话、primary 均衡** 等第二阶段成果，并给出 Stage 3 落地路线。
- `docs/设计报告.md`：面向完整系统设计的权威说明，已同步 Stage 2 架构细节。

## 6. 总体评价

Stage 2 没有改变分布式核心数据路径，但显著改善了**开发调试效率**和**运维可操作性**：用户可以通过 Coordinator Client 完成全部集群级测试，通过 DataNode Local CLI 快速定位单副本问题，rebalance 后的 primary 分布也更加合理。

下一步工作的重点应从「能演示分布式机制」转向「扩容与执行更高效、一致性语义更完整、数据迁移可观测」。优先完成常驻 MiniSQL 与分片迁移，再推进 Raft 与表元数据，可以在保持教学复杂度的同时，让系统更接近真实分布式数据库的演进路径。
