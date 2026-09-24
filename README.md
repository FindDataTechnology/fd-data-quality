# fd-data-quality — 数据覆盖度巡检与补齐

本目录实现 OpenSpec change `data-coverage-backfill`：把"缺什么、缺多少、新到什么时候"变成可查询的缺口报告，并提供把缺口补上的执行器。

## 结构

```
fd-data-quality/
  baselines/*.yaml        # 覆盖基线（声明式：新增源只加 YAML，不改代码）
  scripts/coverage_scan.py    # 巡检器：按基线扫描，写 dq 库
  scripts/backfill_wb.py      # World Bank 刷新执行器（wbgapi → Windows world_bank 库）
  scripts/annotate_wb_upstream.py  # 缺口归因：WB 停更缺口 → 上游停发/可补齐（写 coverage_gap.detail.verdict）
  scripts/backfill_nbs.py     # 统计局执行器（/dg/website 新接口 → Windows cnstats 库）
  scripts/reload_check.py     # 文件驱动重灌契约的校验器（GTA/年鉴/城市面板）
  deploy/install_on_runner.sh # runner（xinru-master）部署：docker 镜像 + /etc/cron.d/fd-data-quality
  INFRA-NOTES.md          # NBS 多机链路、观测落点与机器清单
  CONTRACTS.md            # 各执行器 CLI 契约与回执格式
  requirements.txt        # 运行依赖
```

## 数据访问（环境变量）

所有脚本通过环境变量读 DSN；本地开发用 `.env.local`（chmod 600），生产由 runner 的 `/opt/fd-data-quality/.env`（600）经 `docker --env-file` 注入。口令需 URL 百分号编码（`%`→`%25`、`#`→`%23`）。

| 变量 | 指向 |
|---|---|
| `FD_DQ_PG_URL` | Windows `dq` 库（缺口报告与回执） |
| `FD_WB_PG_URL` | Windows `world_bank` 库（World Bank 分析仓） |
| `FD_CNSTATS_PG_URL` | Windows `cnstats` 库（统计局数据与 NBS shard 任务表） |
| `FD_GTA_PG_URL` / `FD_CITY_PG_URL` / `FD_YEARBOOK_PG_URL` | Windows 三个分析库（gta_panel / china_city_panel / yearbook_catalog，各自独立库） |
| `FD_PROXY_FORWARDER` | NBS proxy forwarder 根地址；存在时所有 NBS HTTP 请求经 forwarder，并携带 `source=nbs` |
| `FD_OPEN_DATA_MCP_DATABASE_URL` | 可选：master `fd_open_data` / `fetch_log` DSN |
| `FD_PROXY_FETCH_LOG_DSN` | 可选：优先于上一项的 `fetch_log` DSN；不可用时只记录警告，不阻塞取数 |

## dq 报告表（已建，2026-09-21）

```sql
scan_run      (run_key, source, started_at, finished_at, status, gap_count, critical_count, note)   PK(run_key, source)
coverage_gap  (run_key, source, object, check_kind, key_space, missing_count, missing_sample,
               last_period, severity, detail, updated_at)   PK(run_key, source, object, check_kind, key_space)
backfill_run  (id, run_key, source, executor, mode, started_at, finished_at, status,
               written_rows, receipt jsonb, note)           -- 执行器回执
```

`run_key` = 扫描/执行的日期窗口（默认 `YYYY-MM-DD`，可覆盖）。历史重复行已归档到 `backfill_run_legacy`；当前 `backfill_run.run_key` 由部分唯一索引 `WHERE run_key IS NOT NULL` 保护，NBS 的 coordinator/shard 回执用完整 run key 幂等 upsert。

## 巡检器契约

```bash
python3 scripts/coverage_scan.py --source all            # 或 world_bank / gta_panel / china_city_panel / yearbook_catalog / cnstats
python3 scripts/coverage_scan.py --source world_bank --run-key 2026-09-21 --report   # 打印人读摘要
```

- 退出码：0 = 扫描完成（有缺口也算成功）；非零 = 扫描本身失败（记录日志，不破坏上一轮报告）。
- check 类型：`staleness`（停更）、`missing_cells`（宽表整列/低填充）、`missing_rows`（期间实体数不足）、`volume`（期间体量异常）、`row_count`（最低行数，用于"零数据"源）、`data_quality`（谓词命中，用于解析脏值）。
- 严重度：`info` / `warn` / `critical`，由基线声明 + 缺口语义决定。

## 执行器通用契约

```bash
python3 scripts/backfill_wb.py  --mode rolling|full [--years 3] [--limit N]
python3 scripts/backfill_nbs.py --mode rolling|full [--dbcode hgnd] [--years 5] [--limit N] [--endpoint auto|new|easyquery]
python3 scripts/reload_check.py --source gta_panel --file <path> --dry-run
python3 scripts/annotate_wb_upstream.py [--run-key 2026-09-22-post] [--economies CHN;USA;WLD]
```

- 全部幂等：重复执行不产生重复行；写入用自然键 upsert。
- 每次执行写一条 `dq.backfill_run` 回执（填了多少行、仍缺什么、下次建议）。
- 失败：非零退出 + 日志原因，不阻塞既有数据读取，不影响其它源。
- 源文件不在手的源（GTA/年鉴/城市面板）：回执标注 `待源文件`，不报假成功。

## NBS coordinator / worker 分片回填

NBS 分片模式使用 Windows PG 中的任务表协调，不把本地 JSON 状态当作多机锁。`coordinator` 只负责迁移、发现指标、切分和写入 shard；`worker` 循环认领 shard 并写入观测。worker 之间不合并 k3s，也不修改 cheap 集群；每台 Tencent 机器运行自己的 `fd-data-quality:local` Docker 容器。

### 环境变量

NBS 容器至少需要：

`FD_PROXY_FORWARDER` 指向**数据面 proxy-fw 的 `/acquire` `/release` 端口**，不是中央 `fd-proxy-service` 的 `:30090` 控制端口。若每台机器有自己的 host-network forwarder，用 `http://127.0.0.1:8080`；当前 canary 使用 xinru-master 上的共享数据面 forwarder `http://100.64.0.3:8080`。中央端口只用于运维检查；worker 必须通过数据面 forwarder 获取可路由的 upstream。

```bash
# 本机 host-network proxy-fw：http://127.0.0.1:8080
# 当前共享 canary forwarder：http://100.64.0.3:8080
export FD_PROXY_FORWARDER='http://100.64.0.3:8080'
export FD_CNSTATS_PG_URL='postgresql://<user>:<url-encoded-password>@100.64.0.5:5432/cnstats'
export FD_DQ_PG_URL='postgresql://<user>:<url-encoded-password>@100.64.0.5:5432/dq'
```

可选的观测库 DSN 二选一；`FD_PROXY_FETCH_LOG_DSN` 优先：

```bash
export FD_OPEN_DATA_MCP_DATABASE_URL='postgresql://<user>:<url-encoded-password>@<master-host>:5432/fd_open_data'
# 或
export FD_PROXY_FETCH_LOG_DSN='postgresql://<user>:<url-encoded-password>@<master-host>:5432/fd_open_data'
```

不要把真实口令写入 README、命令历史或日志。安装脚本接受 URL 百分号编码后的 PostgreSQL 口令；`%` 需编码为 `%25`，`#` 需编码为 `%23`。

### coordinator 入口

```bash
sudo docker run --rm \
  --name fd-nbs-coordinator \
  --network host \
  -e TZ=Asia/Shanghai \
  --env-file /opt/fd-data-quality/.env \
  -v /opt/fd-data-quality/.state:/app/.state \
  fd-data-quality:local \
  scripts/backfill_nbs.py --endpoint new --mode coordinate --dbcode hgnd,hgyd \
    --wait --wait-timeout 21600
```

coordinator 会创建/更新 `cnstats.nbs_shard`，每个 shard 最多 200 个 indicator；worker 通过 PostgreSQL lease 自动认领。`--run-key`（默认当天日期）是执行世代，参与 parent id 与 `target_hash`：每日/每周滚动刷新会生成新 shard，而同一世代内 coordinator 重跑保持幂等。`--wait` 让 coordinator 等所有 shard 终态后汇总父 run；不给时退出后 shard 仍可被继续处理，父 run 由下一次同世代 coordinator 汇总。

保留的 weekly 其他库入口：

```bash
sudo docker run --rm \
  --name fd-nbs-coordinator-w \
  --network host \
  -e TZ=Asia/Shanghai \
  --env-file /opt/fd-data-quality/.env \
  -v /opt/fd-data-quality/.state:/app/.state \
  fd-data-quality:local \
  scripts/backfill_nbs.py --endpoint new --mode coordinate --dbcode hgjd,fsnd,fsyd,fsjd,csnd \
    --wait --wait-timeout 86400
```

### worker 入口

在每台机器上使用唯一编号（例如 xinru-master=1、chengsi=2）：

```bash
sudo docker run -d --restart unless-stopped \
  --name fd-nbs-worker-1 \
  --network host \
  -e TZ=Asia/Shanghai \
  --env-file /opt/fd-data-quality/.env \
  -v /opt/fd-data-quality/.state:/app/.state \
  fd-data-quality:local \
  scripts/backfill_nbs.py --endpoint new --mode worker --min-interval 0
```

`--min-interval 0` 是刻意的：proxy forwarder 的 Redis per-source QPS 和 per-address in-flight cap 是全局限速点，不再让每个 worker 叠加本地延迟。查看状态和日志：

```bash
sudo docker ps --filter 'name=^fd-nbs-worker-1$'
sudo docker logs -f fd-nbs-worker-1
```

### 多机部署步骤

1. 在每台 Tencent 机器上同步本目录到 `/opt/fd-data-quality`，确保包含 `Dockerfile`、`scripts/` 和 `deploy/install_on_runner.sh`。
2. 在该机器上设置 `FD_PROXY_FORWARDER`、`FD_CNSTATS_PG_URL`、`FD_DQ_PG_URL`，按需设置 `FD_OPEN_DATA_MCP_DATABASE_URL` 或 `FD_PROXY_FETCH_LOG_DSN`。
3. 运行：

```bash
# runner（仅一台，默认写 coordinator cron）
sudo env \
  FD_PROXY_FORWARDER='<forwarder-root-url>' \
  FD_OPEN_DATA_MCP_DATABASE_URL='<optional-master-fd-open-data-dsn>' \
  bash /opt/fd-data-quality/deploy/install_on_runner.sh '<url-encoded-postgres-password>'

# 其余 worker 机器：不写 NBS coordinator cron，避免重复调度
sudo env \
  FD_NBS_CRON=0 \
  FD_PROXY_FORWARDER='<forwarder-root-url>' \
  FD_OPEN_DATA_MCP_DATABASE_URL='<optional-master-fd-open-data-dsn>' \
  bash /opt/fd-data-quality/deploy/install_on_runner.sh '<url-encoded-postgres-password>'
```

脚本会写入 `/opt/fd-data-quality/.env`（权限 600）、构建 `fd-data-quality:local`；runner 会重写 `/etc/cron.d/fd-data-quality`，worker 机器在 `FD_NBS_CRON=0` 时跳过 coordinator cron，并打印本机 worker 启动模板。脚本不会执行 `k3s join`、`k3s merge` 或任何 cheap 集群变更。

4. 在每台机器上用唯一编号启动 worker（例如 xinru-master=1、chengsi=2、zihan=3、xinru-server1=4）；coordinator cron 只在一台 runner 上运行。

## `nbs_shard` 表

位置：Windows PG `100.64.0.5:5432` 的 `cnstats` 库（`public.nbs_shard`）。coordinator 独占 migration；worker 只读写 shard lease 和完成进度，不执行 DDL。

```sql
CREATE TABLE IF NOT EXISTS nbs_shard (
    id              bigserial PRIMARY KEY,
    parent_run_id   text NOT NULL,
    dbcode          text NOT NULL,
    ind_start       text NOT NULL,
    ind_end         text NOT NULL,
    start_period    text,
    end_period      text,
    regcode         text,
    endpoint        text,
    target_hash     text,
    status          text NOT NULL DEFAULT 'pending',
    lease_owner     text,
    lease_until     timestamptz,
    attempt_count   int NOT NULL DEFAULT 0,
    indicators_done text[],
    counters_json   jsonb NOT NULL DEFAULT '{}'::jsonb,
    indicators_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nbs_shard_status
    ON nbs_shard (status, lease_until);

CREATE UNIQUE INDEX IF NOT EXISTS nbs_shard_execution_uq_idx
    ON nbs_shard (
        dbcode, ind_start, ind_end,
        COALESCE(start_period, ''), COALESCE(end_period, ''),
        COALESCE(regcode, ''), COALESCE(endpoint, ''), COALESCE(target_hash, '')
    );
```

`status` 使用 `pending | running | done | failed`。coordinator 用确定性 parent run id，重跑同一配置不会生成空父 run；`target_hash` 包含目标清单与 `--run-key` 执行世代，每日刷新会生成新 shard。worker 用 `FOR UPDATE SKIP LOCKED` 认领，`pending/failed/lease 过期` 的 shard 都可重认领。checkpoint 与状态收尾都带 `lease_owner` 条件，旧 worker 不能覆盖新 worker 的进度。`indicators_done` 在新接口下保存 `catalog_id:indicator_id`，避免同 UUID 在不同目录下被误跳过。
