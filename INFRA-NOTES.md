# fd-data-quality 基础设施记录

## NBS 多机执行链路

- coordinator 在 runner（当前为 `xinru-master`）触发：
  `scripts/backfill_nbs.py --mode coordinate --dbcode hgnd,hgyd`。
- coordinator 只负责 schema migration、指标发现、按 indicator 切 shard、写入
  `cnstats.nbs_shard` 和父 run；它不是常驻调度器。
- 每台 Tencent 机器独立运行一个 `fd-data-quality:local` Docker worker。worker 循环
  认领 `pending` 或 lease 已过期的 shard，处理完成后释放 lease。
- 每台 worker 的 NBS HTTP 请求通过数据面 `FD_PROXY_FORWARDER` 的 `/acquire`、
  `/release` 接口选择出口；请求标识为 `source=nbs`。若机器上有本机 host-network
  forwarder，用 `http://127.0.0.1:8080`；当前 canary 使用 xinru-master 上的共享
  Docker 数据面 forwarder `http://100.64.0.3:8080`。中央 `fd-proxy-service` 的
  `:30090` 只用于运维检查，不是 worker 数据请求端点。全局 QPS 和出口并发限制由
  proxy forwarder/Redis 统一控制，worker 使用 `--min-interval 0`，不在本地叠加
  per-worker 限速。
- 当前共享 forwarder 是独立 Docker 容器 `fd-proxy-fw-nbs`（xinru-master，
  `--network host`，镜像来自 Harbor），不修改任何 k3s 资源；DB/Redis 走 mesh
  `100.64.0.3:30432/30380`。
- 不执行 `k3s join`、`k3s merge` 或其他集群合并操作；不修改 cheap 集群。
  各机器只管理自己的 Docker 容器、`.env`、日志和 cron。

## fetch_log 落点

- NBS 观测表是 master 上的 `fd_open_data.fetch_log`，不是 Windows PG 的
  `cnstats` 或 `dq` 库。
- 当前 NBS 客户端在每次请求后以 best-effort 方式写入 `fetch_log`，记录
  `source=nbs`、command、`proxy_id`、classification、状态、延迟和错误详情；
  观测库不可用时只记录警告，不阻塞数据入库。
- 观测 DSN 通过以下变量注入，`FD_PROXY_FETCH_LOG_DSN` 优先：
  - `FD_OPEN_DATA_MCP_DATABASE_URL`
  - `FD_PROXY_FETCH_LOG_DSN`
- 不要把数据库口令写入本文件、README、cron 命令或容器日志。

## shard 表位置

- 数据库：Windows PG `100.64.0.5:5432`
- schema：`cnstats`（SQL 中为 `public.nbs_shard`）
- 表：`nbs_shard`（当前 migration：`003_nbs_shard_execution_identity`，新增
  `regcode/endpoint/target_hash` 和执行身份唯一索引）
- coordinator 独占 migration；worker 只更新 lease、`indicators_done`、计数和
  shard 状态，不执行 DDL。
- 每个 shard 最多 200 个 indicator；`pending`、`failed`、`lease_until` 为空/过期的
  shard 都可由其他 worker 用 `FOR UPDATE SKIP LOCKED` 重认领。checkpoint 与收尾
  更新均校验 `lease_owner`，旧 worker 不能覆盖新 owner 的进度。
- 观测数据仍写入 Windows PG `cnstats.nbs_observation`，回执写入
  `dq.backfill_run`；两张业务表都不因多机部署而复制或迁移。

## worker 机器清单

| 机器 | 角色 | worker 名称建议 |
|---|---|---|
| `xinru-master` | coordinator runner；NBS worker | `fd-nbs-worker-1` |
| `chengsi` | NBS worker | `fd-nbs-worker-2` |
| `zihan` | NBS worker | `fd-nbs-worker-3` |
| `xinru-server1` | NBS worker | `fd-nbs-worker-4` |
| `xinru-server2` | proxy forwarder/出口节点；需要时也可运行 NBS worker | `fd-nbs-worker-5` |

所有 worker 复用本地镜像 `fd-data-quality:local`，并通过各自机器上的
`/opt/fd-data-quality/.env` 注入：

- `FD_PROXY_FORWARDER`
- `FD_CNSTATS_PG_URL`
- `FD_DQ_PG_URL`
- 可选：`FD_OPEN_DATA_MCP_DATABASE_URL`
- 可选：`FD_PROXY_FETCH_LOG_DSN`

## 部署入口

`deploy/install_on_runner.sh` 会构建/刷新 `fd-data-quality:local`、写入
`.env`，并在 runner 上重写 `/etc/cron.d/fd-data-quality`；纯 worker 机器用
`FD_NBS_CRON=0` 跳过 coordinator cron，只保留 worker 容器。NBS coordinator
cron 使用 `--mode coordinate --dbcode hgnd,hgyd --wait`；
weekly 其他库保留独立 coordinator 入口。`--run-key` 默认当天日期，并参与
parent id / `target_hash`，因此每日刷新是新执行世代；`--wait` 负责 shard
全部终态后的父 run 汇总：

```text
20 5 * * 0 root cd /opt/fd-data-quality && ... backfill_nbs.py --endpoint new --mode coordinate --dbcode hgjd,fsnd,fsyd,fsjd,csnd --wait
```

实际 cron 文件由安装脚本生成；不要手工把 DSN 或口令放入 cron 文本。
