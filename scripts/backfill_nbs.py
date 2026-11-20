#!/usr/bin/env python3
"""NBS（国家统计局）数据入库执行器 —— OpenSpec change `data-coverage-backfill` task 3.1-3.3。

契约见 ``openspec/specs/cnstats-ingest-postgres``（表结构 + 幂等 upsert）与
``openspec/specs/cnstats-source``（请求形态、滚动窗口、保守限速）。

**取数通路（2026-09 改版）**：站点已从 easyquery 改版为 ``/dg/website`` SPA，旧接口
``easyquery.htm`` 对所有出口 403 ``reason:UrlACL``。默认 ``--endpoint auto`` 走 SPA 的
免鉴权 JSON 接口（``/dg/website/publicrelease/web/external/...``，见 ``_nbs_client.py``
的 ``NewNBSClient``），旧 easyquery 实现保留为 ``--endpoint easyquery`` 回退。

用法（详见 --help）::

    # 1. 建库建表（幂等）
    python3 scripts/backfill_nbs.py --init

    # 2. 端到端连通性探针（不写库）
    python3 scripts/backfill_nbs.py --probe --dbcode hgnd

    # 3. 滚动窗口入库（近 5 年）
    python3 scripts/backfill_nbs.py --mode rolling --years 5 --dbcode hgnd

    # 4. 全量回补（按指标树逐库，断点续跑）
    python3 scripts/backfill_nbs.py --mode full --dbcode hgnd,hgjd,fsnd

    # 5. 出口被拦截时的离线自测（内置录制响应，验证解析 + 幂等）
    python3 scripts/backfill_nbs.py --self-test

    # 6. 指定通路 / 显式指标（新接口的指标码是 UUID，需带所属目录 id）
    python3 scripts/backfill_nbs.py --endpoint new --mode rolling --limit 3 --dbcode hgnd
    python3 scripts/backfill_nbs.py --indicators <目录id>:<指标id> --dbcode hgnd

落库：Windows PG `cnstats` 库的 ``nbs_indicator / nbs_region / nbs_observation /
nbs_crawl_runs``；回执：Windows PG `dq` 库的 ``backfill_run``（executor=backfill_nbs）。
断点续跑：``fd-data-quality/.state/nbs_backfill.json`` + ``nbs_crawl_runs`` 历史。

退出码：0 = 成功（含 dry-run / selftest）；1 = 失败或部分失败（原因在日志与回执里）。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Optional, Protocol, cast


class ProbeClient(Protocol):
    endpoint_name: str
    endpoint_url: str
    request_count: int
    error_count: int

    def probe(self, dbcode: str) -> dict[str, Any]: ...

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from _env import dsn  # noqa: E402

from _nbs_client import (  # noqa: E402
    CURATED_INDICATORS,
    DBCODE_FREQ,
    DBCODES,
    FULL_START_PERIOD,
    NBSBlockedError,
    NBSError,
    Observation,
    SELF_TEST_FIXTURES,
    make_client,
    parse_new_response,
    parse_response,
    window_start_period,
)

EXECUTOR = "backfill_nbs"
SOURCE = "cnstats"
TARGET_DB = "cnstats"
STATE_PATH = pathlib.Path(__file__).resolve().parent.parent / ".state" / "nbs_backfill.json"
STATE_VERSION = 1
MIGRATION_VERSION = "003_nbs_shard_execution_identity"
BATCH_SIZE = 200
SHARD_MAX_SIZE = 200
LEASE_SECONDS = 30 * 60
SCHEMA_WAIT_DEFAULT_SECONDS = 300.0
SCHEMA_WAIT_POLL_SECONDS = 5.0
PARENT_WAIT_DEFAULT_SECONDS = 300.0
PARENT_WAIT_POLL_SECONDS = 5.0

# ---------------------------------------------------------------------------
# DDL（幂等；按 cnstats-ingest-postgres 规格 + archive/2026-07-26-scrape-cnstats-postgres 设计）
# ---------------------------------------------------------------------------
DDL_COMMON = [
    """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version    text PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS nbs_indicator (
        id          bigserial PRIMARY KEY,
        code        text NOT NULL,
        name        text,
        dbcode      text NOT NULL,
        unit        text,
        source_note text,
        created_at  timestamptz NOT NULL DEFAULT now(),
        updated_at  timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT nbs_indicator_dbcode_code_key UNIQUE (dbcode, code)
    )
    """,
    "CREATE INDEX IF NOT EXISTS nbs_indicator_code_idx ON nbs_indicator (code)",
    """
    CREATE TABLE IF NOT EXISTS nbs_region (
        id         bigserial PRIMARY KEY,
        code       text NOT NULL UNIQUE,
        name       text,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS nbs_crawl_runs (
        run_id           text PRIMARY KEY,
        job_id           text,
        dbcode           text,
        mode             text,
        status           text NOT NULL,
        started_at       timestamptz NOT NULL DEFAULT now(),
        finished_at      timestamptz,
        indicators_total integer,
        indicators_done  integer,
        rows_written     bigint,
        scope_json       jsonb,
        summary_json     jsonb,
        note             text
    )
    """,
    "CREATE INDEX IF NOT EXISTS nbs_crawl_runs_dbcode_started_idx ON nbs_crawl_runs (dbcode, started_at DESC)",
]

DDL_NBS_SHARD = [
    """
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
        attempt_count   integer NOT NULL DEFAULT 0,
        indicators_done text[],
        counters_json   jsonb NOT NULL DEFAULT '{}'::jsonb,
        indicators_json jsonb NOT NULL DEFAULT '[]'::jsonb,
        created_at      timestamptz NOT NULL DEFAULT now(),
        updated_at      timestamptz NOT NULL DEFAULT now()
    )
    """,
    "ALTER TABLE nbs_shard ADD COLUMN IF NOT EXISTS indicators_done text[]",
    "ALTER TABLE nbs_shard ADD COLUMN IF NOT EXISTS counters_json jsonb NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE nbs_shard ADD COLUMN IF NOT EXISTS indicators_json jsonb NOT NULL DEFAULT '[]'::jsonb",
    "ALTER TABLE nbs_shard ADD COLUMN IF NOT EXISTS regcode text",
    "ALTER TABLE nbs_shard ADD COLUMN IF NOT EXISTS endpoint text",
    "ALTER TABLE nbs_shard ADD COLUMN IF NOT EXISTS target_hash text",
    "CREATE INDEX IF NOT EXISTS idx_nbs_shard_status ON nbs_shard (status, lease_until)",
    "CREATE INDEX IF NOT EXISTS idx_nbs_shard_parent_run ON nbs_shard (parent_run_id)",
    # 002 的唯一键缺少执行身份。003 先移除旧索引，再用包含 regcode/endpoint/target_hash
    # 的表达式唯一键：同一指标范围按不同地区、通路或目标清单重跑时是不同的 shard。
    "DROP INDEX IF EXISTS nbs_shard_scope_uq_idx",
    """
    CREATE UNIQUE INDEX IF NOT EXISTS nbs_shard_execution_uq_idx
        ON nbs_shard (
            dbcode, ind_start, ind_end,
            COALESCE(start_period, ''), COALESCE(end_period, ''),
            COALESCE(regcode, ''), COALESCE(endpoint, ''), COALESCE(target_hash, '')
        )
    """,
]

# region_id 可空的唯一键（task 3.1 的显式要求）：PG >= 15 用 UNIQUE NULLS NOT DISTINCT
# （NULL 视为相等，否则 (indicator, NULL, period) 会无限重复）；旧版本回退到两条部分
# 唯一索引 + 分路 upsert（见 DDL_OBSERVATION_PRE15 / UPSERT_OBSERVATION_*_PRE15）。
DDL_OBSERVATION_PG15 = """
    CREATE TABLE IF NOT EXISTS nbs_observation (
        id           bigserial PRIMARY KEY,
        indicator_id bigint NOT NULL REFERENCES nbs_indicator(id) ON DELETE CASCADE,
        region_id    bigint REFERENCES nbs_region(id) ON DELETE CASCADE,
        period       text NOT NULL,
        value        numeric,
        dbcode       text,
        raw_json     jsonb,
        ingested_at  timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT nbs_observation_indicator_region_period_key
            UNIQUE NULLS NOT DISTINCT (indicator_id, region_id, period)
    )
"""
DDL_OBSERVATION_PRE15 = """
    CREATE TABLE IF NOT EXISTS nbs_observation (
        id           bigserial PRIMARY KEY,
        indicator_id bigint NOT NULL REFERENCES nbs_indicator(id) ON DELETE CASCADE,
        region_id    bigint REFERENCES nbs_region(id) ON DELETE CASCADE,
        period       text NOT NULL,
        value        numeric,
        dbcode       text,
        raw_json     jsonb,
        ingested_at  timestamptz NOT NULL DEFAULT now()
    )
"""
DDL_OBSERVATION_PRE15_UQ = [
    """
    CREATE UNIQUE INDEX IF NOT EXISTS nbs_observation_uq_national
        ON nbs_observation (indicator_id, period) WHERE region_id IS NULL
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS nbs_observation_uq_regional
        ON nbs_observation (indicator_id, region_id, period) WHERE region_id IS NOT NULL
    """,
]
DDL_OBSERVATION_INDEXES = [
    "CREATE INDEX IF NOT EXISTS nbs_observation_period_idx ON nbs_observation (period)",
    "CREATE INDEX IF NOT EXISTS nbs_observation_indicator_period_idx ON nbs_observation (indicator_id, period)",
    "CREATE INDEX IF NOT EXISTS nbs_observation_region_idx ON nbs_observation (region_id)",
]

SET_CLAUSE = """
       SET value = EXCLUDED.value,
           dbcode = EXCLUDED.dbcode,
           raw_json = EXCLUDED.raw_json,
           ingested_at = now()
"""
UPSERT_OBSERVATION_PG15 = f"""
    INSERT INTO nbs_observation (indicator_id, region_id, period, value, dbcode, raw_json)
    VALUES %s
    ON CONFLICT (indicator_id, region_id, period) DO UPDATE {SET_CLAUSE}
"""
UPSERT_OBSERVATION_NATIONAL_PRE15 = f"""
    INSERT INTO nbs_observation (indicator_id, region_id, period, value, dbcode, raw_json)
    VALUES %s
    ON CONFLICT (indicator_id, period) WHERE region_id IS NULL DO UPDATE {SET_CLAUSE}
"""
UPSERT_OBSERVATION_REGIONAL_PRE15 = f"""
    INSERT INTO nbs_observation (indicator_id, region_id, period, value, dbcode, raw_json)
    VALUES %s
    ON CONFLICT (indicator_id, region_id, period) WHERE region_id IS NOT NULL DO UPDATE {SET_CLAUSE}
"""

# 由 apply_migrations 设置：是否需要 NULLS NOT DISTINCT（PG >= 15）
_PG15_NULLS_NOT_DISTINCT = True


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _db_error_repr(exc: BaseException) -> str:
    """Windows PG 的服务端消息是 GBK，psycopg2 解码失败会抛 UnicodeDecodeError。"""
    if isinstance(exc, UnicodeDecodeError):
        raw = exc.object if isinstance(exc.object, (bytes, bytearray)) else b""
        return f"PG 服务端错误（非 UTF-8 消息，原始字节：{bytes(raw)!r}）"
    return f"{type(exc).__name__}: {exc}"


def _connect(dsn_or_url: str, *, purpose: str = ""):
    """连 PG，并把 GBK 错误信息转成可读一行。

    为什么需要它：Windows PG 的 `lc_messages` 是中文，认证失败时服务端返回 **GBK 字节**，
    psycopg2 按 UTF-8 解码就抛 `UnicodeDecodeError: 0xd6`。于是真实原因（口令错）
    被一条编码异常完全盖住 —— 2026-11-20 实测该容器因此崩溃循环 3336 次，
    每次日志都只有那个 0xd6，看起来像编码 bug。

    这里把它接住并重新抛出成 SystemExit + 可读消息：worker 会退出（这是对的，
    连不上就不该假装在干活），但退出原因写的是「连不上 PG」而不是「解码失败」。
    """
    import psycopg2

    try:
        return psycopg2.connect(dsn_or_url)
    except UnicodeDecodeError as exc:
        # 用同样的 DSN 再连一次、把消息以 latin-1 取出后按 GBK 解，尽量还原原文。
        hint = ""
        try:
            psycopg2.connect(dsn_or_url, client_encoding="GBK").close()
        except Exception as exc2:
            raw = getattr(exc2, "args", ())
            hint = f"（按 GBK 解码仍是失败：{raw!r}）" if raw else ""
        raise SystemExit(
            f"{purpose}连不上 PG：服务端返回非 UTF-8（GBK）错误消息，"
            f"多半是口令/权限问题而不是编码问题。原始异常：{_db_error_repr(exc)}{hint}"
        ) from None
    except psycopg2.OperationalError as exc:
        raise SystemExit(f"{purpose}连不上 PG：{_db_error_repr(exc)}") from None


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: Optional[datetime]) -> Optional[str]:
    """ISO8601 + Z（回执/状态文件里用，便于跨时区阅读）。"""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


# ---------------------------------------------------------------------------
# 连接 / 建库建表
# ---------------------------------------------------------------------------
def _ingest_dsn() -> str:
    return dsn("FD_CNSTATS_PG_URL")


def _admin_dsn() -> str:
    """把 cnstats DSN 的库名换成 postgres，用于 CREATE DATABASE。"""
    return re.sub(r"/[^/?]+(\?|$)", r"/postgres\1", _ingest_dsn(), count=1)


def ensure_database(name: str = TARGET_DB) -> bool:
    """库不存在则创建（幂等）。返回是否新建。"""
    import psycopg2
    import psycopg2.extensions

    conn = _connect(_admin_dsn(), purpose="建库时")
    try:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
            if cur.fetchone():
                log(f"库 {name} 已存在")
                return False
            cur.execute(
                "SELECT pg_encoding_to_char(encoding), datcollate, datctype "
                "FROM pg_database WHERE datname = 'template0'"
            )
            enc, collate, ctype = cur.fetchone()
            cur.execute(
                f'CREATE DATABASE "{name}" WITH TEMPLATE template0 ENCODING %s LC_COLLATE %s LC_CTYPE %s',
                (enc, collate, ctype),
            )
            log(f"已创建库 {name}（encoding={enc}, collate={collate}）")
            return True
    finally:
        conn.close()


def apply_migrations(conn) -> tuple[list[str], bool]:
    """Apply pending NBS DDL once, under a transaction-scoped advisory lock.

    The migration history is append-only.  Once the current version is committed,
    coordinator retries execute no DDL; workers never call this function and only
    poll ``schema_migrations`` instead.
    """
    global _PG15_NULLS_NOT_DISTINCT

    applied: list[str] = []
    with conn.cursor() as cur:
        cur.execute("SELECT current_setting('server_version_num')::int")
        server_num = int(cur.fetchone()[0])
        _PG15_NULLS_NOT_DISTINCT = server_num >= 150000
        cur.execute(DDL_COMMON[0])
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('nbs_shard_migrations'))")
        cur.execute(
            "SELECT 1 FROM schema_migrations WHERE version = %s",
            (MIGRATION_VERSION,),
        )
        if cur.fetchone():
            conn.commit()
            return applied, _PG15_NULLS_NOT_DISTINCT

        for stmt in DDL_COMMON[1:]:
            cur.execute(stmt)
        if _PG15_NULLS_NOT_DISTINCT:
            cur.execute(DDL_OBSERVATION_PG15)
        else:
            cur.execute(DDL_OBSERVATION_PRE15)
            for stmt in DDL_OBSERVATION_PRE15_UQ:
                cur.execute(stmt)
        for stmt in DDL_OBSERVATION_INDEXES:
            cur.execute(stmt)
        for stmt in DDL_NBS_SHARD:
            cur.execute(stmt)
        cur.execute(
            "INSERT INTO schema_migrations (version) VALUES (%s) "
            "ON CONFLICT DO NOTHING RETURNING version",
            (MIGRATION_VERSION,),
        )
        if cur.fetchone():
            applied.append(MIGRATION_VERSION)
        # 只读角色（沿用既有库的 fdbiz_ro 范式；角色不存在则跳过）
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'fdbiz_ro'")
        if cur.fetchone():
            cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO fdbiz_ro")
    conn.commit()
    return applied, _PG15_NULLS_NOT_DISTINCT


def report_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename LIKE 'nbs%' ORDER BY 1"
        )
        tables = [r[0] for r in cur.fetchall()]
        cur.execute(
            "SELECT (SELECT count(*) FROM nbs_indicator), (SELECT count(*) FROM nbs_region), "
            "(SELECT count(*) FROM nbs_observation), (SELECT count(*) FROM nbs_crawl_runs)"
        )
        ind, reg, obs, runs = cur.fetchone()
    log(f"表：{', '.join(tables)}")
    log(f"行数：nbs_indicator={ind} nbs_region={reg} nbs_observation={obs} nbs_crawl_runs={runs}")


# ---------------------------------------------------------------------------
# 状态文件（断点续跑）
# ---------------------------------------------------------------------------
def load_state(path: Optional[pathlib.Path] = None) -> dict:
    path = path or STATE_PATH
    if not path.exists():
        return {"version": STATE_VERSION, "updated_at": None, "dbcodes": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        log(f"状态文件损坏（{exc}），按空状态继续")
        return {"version": STATE_VERSION, "updated_at": None, "dbcodes": {}}
    data.setdefault("dbcodes", {})
    return data


def save_state(state: dict, path: Optional[pathlib.Path] = None) -> None:
    """原子写：先写临时文件再 os.replace，中断不会留下半个 JSON。"""
    path = path or STATE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    state["version"] = STATE_VERSION
    state["updated_at"] = iso_z(utcnow())
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def seed_state_from_runs(conn, state: dict) -> None:
    """状态文件缺失时，用 nbs_crawl_runs 里最近的成功运行补齐已完成指标（双保险）。"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT dbcode, mode, summary_json
              FROM nbs_crawl_runs
             WHERE status IN ('success', 'partial') AND dbcode IS NOT NULL
             ORDER BY started_at DESC
             LIMIT 20
            """
        )
        rows = cur.fetchall()
    for dbcode, mode, summary in rows:
        entry = state["dbcodes"].get(dbcode)
        if entry and entry.get("completed"):
            continue
        summary = summary or {}
        completed = summary.get("completed") or []
        if not completed:
            continue
        state["dbcodes"][dbcode] = {
            "mode": mode,
            "start_period": summary.get("start_period"),
            "completed": completed,
            "rows": summary.get("rows_written", 0),
            "last_run_at": summary.get("finished_at"),
            "seeded_from": "nbs_crawl_runs",
        }
        log(f"从 nbs_crawl_runs 恢复 {dbcode} 已完成指标 {len(completed)} 个")


def already_covered(state: dict, dbcode: str, code: str, start_period: str) -> bool:
    """该指标在本次（或更宽的）窗口下已完成 -> 可跳过。"""
    entry = (state.get("dbcodes") or {}).get(dbcode) or {}
    if code not in (entry.get("completed") or []):
        return False
    prev = entry.get("start_period") or ""
    if not prev:
        return False
    # 同格式期间字符串：prev <= start_period 说明上次覆盖的窗口不窄于本次
    return prev <= start_period


# ---------------------------------------------------------------------------
# 落库
# ---------------------------------------------------------------------------
def upsert_indicator(
    cur, code: str, dbcode: str, name: Optional[str], unit: Optional[str], source_note: str = "easyquery"
) -> int:
    cur.execute(
        """
        INSERT INTO nbs_indicator (code, name, dbcode, unit, source_note)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (dbcode, code) DO UPDATE
           SET name = COALESCE(EXCLUDED.name, nbs_indicator.name),
               unit = COALESCE(EXCLUDED.unit, nbs_indicator.unit),
               updated_at = now()
        RETURNING id
        """,
        (code, name, dbcode, unit, source_note),
    )
    return cur.fetchone()[0]


def upsert_region(cur, code: str, name: Optional[str]) -> int:
    cur.execute(
        """
        INSERT INTO nbs_region (code, name) VALUES (%s, %s)
        ON CONFLICT (code) DO UPDATE
           SET name = COALESCE(EXCLUDED.name, nbs_region.name),
               updated_at = now()
        RETURNING id
        """,
        (code, name),
    )
    return cur.fetchone()[0]


def _raw_json(obs: Observation) -> dict:
    """保留源记录；自测 fixture 的 `_source` 提到顶层，便于清理与溯源。"""
    raw = dict(obs.raw or {})
    if raw.get("_source"):
        return raw
    data = raw.get("data")
    if isinstance(data, dict) and data.get("_source"):
        raw["_source"] = data["_source"]
    return raw


def _dedupe_observation_rows(rows: list[tuple]) -> tuple[list[tuple], int, int]:
    """按自然键 (region_id, period) 去重。

    新接口的 esData 响应会把同一 指标×地区×期间 的节点重复下发（实测 fsnd 全量
    首指标即触发）；PostgreSQL 的 ``ON CONFLICT DO UPDATE`` 不允许同一命令影响
    同一行两次。重复里有值/空值并存时保留有值行；两个非空值不一致时保留首行，
    计数交给调用方告警，不静默吞掉口径冲突。
    """
    unique: dict[tuple, tuple] = {}
    duplicates = conflicts = 0
    for row in rows:
        key = (row[1], row[2])  # (region_id, period)；indicator_id 在本批内相同
        prev = unique.get(key)
        if prev is None:
            unique[key] = row
            continue
        duplicates += 1
        if prev[3] is None and row[3] is not None:
            unique[key] = row
        elif prev[3] is not None and row[3] is not None and prev[3] != row[3]:
            conflicts += 1
    return list(unique.values()), duplicates, conflicts


def upsert_observations(cur, indicator_id: int, observations: list[Observation]) -> int:
    """批量幂等写观测：ON CONFLICT (indicator_id, region_id, period) DO UPDATE。"""
    from psycopg2.extras import Json, execute_values

    region_ids: dict[str, int] = {}
    rows: list[tuple] = []
    for obs in observations:
        rid = None
        if obs.region_code:
            if obs.region_code not in region_ids:
                region_ids[obs.region_code] = upsert_region(cur, obs.region_code, obs.region_name)
            rid = region_ids[obs.region_code]
        rows.append((indicator_id, rid, obs.period, obs.value, obs.dbcode, Json(_raw_json(obs))))

    rows, duplicates, conflicts = _dedupe_observation_rows(rows)
    if duplicates:
        log(f"    自然键去重：丢弃 {duplicates} 行重复观测（值冲突 {conflicts} 行）")
    if not _PG15_NULLS_NOT_DISTINCT:
        return _upsert_pre15(cur, indicator_id, rows)

    written = 0
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i : i + BATCH_SIZE]
        execute_values(cur, UPSERT_OBSERVATION_PG15, chunk, page_size=len(chunk))
        written += len(chunk)
    return written


def _upsert_pre15(cur, indicator_id: int, rows: list[tuple]) -> int:
    """PG < 15 回退：NULL 行与有地区行分路 upsert（配合两条部分唯一索引）。"""
    from psycopg2.extras import execute_values

    written = 0
    for sql, subset in (
        (UPSERT_OBSERVATION_NATIONAL_PRE15, [r for r in rows if r[1] is None]),
        (UPSERT_OBSERVATION_REGIONAL_PRE15, [r for r in rows if r[1] is not None]),
    ):
        for i in range(0, len(subset), BATCH_SIZE):
            chunk = subset[i : i + BATCH_SIZE]
            if not chunk:
                continue
            execute_values(cur, sql, chunk, page_size=len(chunk))
            written += len(chunk)
    return written


def begin_run(conn, run_id: str, scope: dict, note: str, job_id: Optional[str] = None) -> None:
    """Create a run without resurrecting an already-finished run id."""
    from psycopg2.extras import Json

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO nbs_crawl_runs
                (run_id, job_id, dbcode, mode, status, indicators_total, indicators_done,
                 rows_written, scope_json, note)
            VALUES (%s, %s, %s, %s, 'running', %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO NOTHING
            """,
            (
                run_id,
                job_id,
                scope.get("dbcode"),
                scope.get("mode"),
                scope.get("indicators_total", 0),
                scope.get("indicators_done", 0),
                scope.get("rows_written", 0),
                Json(scope),
                note,
            ),
        )
    conn.commit()


def finish_run(conn, run_id: str, status: str, summary: dict, counters: dict, note: str) -> None:
    """Finish a run idempotently, creating the row if begin_run was retried away."""
    from psycopg2.extras import Json

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO nbs_crawl_runs
                (run_id, dbcode, mode, status, finished_at, indicators_total, indicators_done,
                 rows_written, scope_json, summary_json, note)
            VALUES (
                %s, %s, %s, %s, now(), %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (run_id) DO UPDATE
               SET finished_at = now(),
                   status = EXCLUDED.status,
                   indicators_total = EXCLUDED.indicators_total,
                   indicators_done = EXCLUDED.indicators_done,
                   rows_written = EXCLUDED.rows_written,
                   summary_json = EXCLUDED.summary_json,
                   note = EXCLUDED.note
            """,
            (
                run_id,
                summary.get("dbcode"),
                summary.get("mode"),
                status,
                counters.get("indicators_total"),
                counters.get("indicators_done"),
                counters.get("rows_written"),
                Json(summary),
                Json(summary),
                note,
            ),
        )
    conn.commit()


def ensure_receipt_key_unique() -> bool:
    """Archive historical duplicate run keys, then add the idempotency index.

    The live DQ table predates the executor contract and contains duplicate
    run_key rows.  Keeping those rows in ``backfill_run_legacy`` preserves the
    history while making the NBS receipt path safe for ``ON CONFLICT``.
    """
    import psycopg2

    conn = None
    try:
        conn = _connect(dsn("FD_DQ_PG_URL"), purpose="连 dq 库时")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext('backfill_run_run_key_uq'))"
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS backfill_run_legacy (
                    id            bigint PRIMARY KEY,
                    run_key       text,
                    source        text,
                    executor      text,
                    mode          text,
                    started_at    timestamptz,
                    finished_at   timestamptz,
                    status        text,
                    written_rows  bigint,
                    receipt       jsonb,
                    note          text,
                    archived_at   timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                "ALTER TABLE backfill_run_legacy "
                "ADD COLUMN IF NOT EXISTS archived_at timestamptz NOT NULL DEFAULT now()"
            )
            cur.execute(
                """
                INSERT INTO backfill_run_legacy
                    (id, run_key, source, executor, mode, started_at, finished_at,
                     status, written_rows, receipt, note)
                SELECT id, run_key, source, executor, mode, started_at, finished_at,
                       status, written_rows, receipt, note
                  FROM (
                    SELECT b.*,
                           row_number() OVER (
                               PARTITION BY run_key ORDER BY id DESC
                           ) AS duplicate_rank
                      FROM backfill_run AS b
                     WHERE run_key IS NOT NULL
                  ) AS ranked
                 WHERE duplicate_rank > 1
                   ON CONFLICT (id) DO NOTHING
                """
            )
            cur.execute(
                "DELETE FROM backfill_run "
                "WHERE id IN (SELECT id FROM backfill_run_legacy)"
            )
            # receipt SQL infers the partial unique index below; replace any older
            # full-column index of the same name so PostgreSQL can use ON CONFLICT.
            cur.execute("SELECT indexdef FROM pg_indexes WHERE indexname = 'backfill_run_run_key_uq_idx'")
            indexdef = cur.fetchone()
            if indexdef and "WHERE run_key IS NOT NULL" not in indexdef[0]:
                cur.execute("DROP INDEX backfill_run_run_key_uq_idx")
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS backfill_run_run_key_uq_idx
                    ON backfill_run (run_key)
                 WHERE run_key IS NOT NULL
                """
            )
        conn.commit()
        return True
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        log(f"警告：dq.backfill_run run_key 唯一性迁移失败 -> {_db_error_repr(exc)}")
        return False
    finally:
        if conn is not None:
            conn.close()


def write_receipt(
    run_key: str,
    mode: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    written_rows: int,
    receipt: dict,
    note: str,
    ensure_unique: bool = True,
) -> bool:
    """往 dq.backfill_run 写一条回执；dq 不可达时只告警（不掩盖主流程结论）。"""
    import psycopg2
    from psycopg2.extras import Json

    # The coordinator and legacy single-process paths may repair the historical
    # DQ table once. Workers must never run this DDL; their receipt key is
    # expected to be protected by the coordinator-applied unique index.
    if ensure_unique:
        ensure_receipt_key_unique()
    conn = None
    try:
        conn = _connect(dsn("FD_DQ_PG_URL"), purpose="连 dq 库时")
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO backfill_run
                    (run_key, source, executor, mode, started_at, finished_at,
                     status, written_rows, receipt, note)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_key) WHERE run_key IS NOT NULL DO UPDATE
                   SET source = EXCLUDED.source,
                       executor = EXCLUDED.executor,
                       mode = EXCLUDED.mode,
                       started_at = EXCLUDED.started_at,
                       finished_at = EXCLUDED.finished_at,
                       status = EXCLUDED.status,
                       written_rows = EXCLUDED.written_rows,
                       receipt = EXCLUDED.receipt,
                       note = EXCLUDED.note
                """,
                (
                    run_key, SOURCE, EXECUTOR, mode, started_at, finished_at,
                    status, written_rows, Json(receipt), note,
                ),
            )
        conn.commit()
        log(f"回执已写入 dq.backfill_run（status={status}, written_rows={written_rows}）")
        return True
    except Exception as exc:  # dq 挂了也要把主结论说清楚
        log(f"警告：dq.backfill_run 回执写入失败 -> {_db_error_repr(exc)}")
        return False
    finally:
        if conn is not None:
            conn.close()


# ---------------------------------------------------------------------------
# 指标发现 / 窗口
# ---------------------------------------------------------------------------
@dataclass
class Target:
    """待回补的指标：``code`` 是接口标识（新接口为 UUID，easyquery 为 zbcode）。

    ``catalog_id`` 只在新区接口下需要（esData 的 ``cid``）。
    """

    code: str
    name: Optional[str] = None
    catalog_id: Optional[str] = None
    unit: Optional[str] = None


def resolve_indicators(
    client,
    dbcode: str,
    scope: str,
    explicit: Optional[list[str]],
    limit: Optional[int],
) -> list[Target]:
    """返回待回补指标。

    - ``--indicators``：显式给定；新接口下必须写成 ``目录id:指标id``（否则取数缺 cid）。
    - ``curated``：内置精选清单（zbcode），**只适用于 easyquery**；新接口下自动降级为 tree。
    - ``tree``：走目录树发现（默认）。
    """
    if explicit:
        targets: list[Target] = []
        for raw in explicit:
            cid, _, iid = raw.partition(":")
            if iid:
                targets.append(Target(code=iid, catalog_id=cid))
            else:
                targets.append(Target(code=cid))
        return targets

    is_new = getattr(client, "endpoint_name", "") == "new"
    use_curated = scope == "curated" or (scope == "auto" and dbcode in CURATED_INDICATORS)
    if use_curated and is_new:
        log(f"  {dbcode}：新接口下没有 zbcode 精选清单，--scope {scope} 降级为 tree 发现")
        use_curated = False
    if use_curated:
        items = list(CURATED_INDICATORS.get(dbcode, {}).items())
        return [Target(code=c, name=n) for c, n in (items[:limit] if limit else items)]

    indicators = client.discover_indicators(dbcode, max_nodes=limit)
    return [Target(code=i.code, name=i.name, catalog_id=i.catalog_id, unit=i.unit) for i in indicators]


def build_window(args, dbcode: str) -> tuple[Optional[str], Optional[str]]:
    """返回 (start_period, end_period)；full 模式从最早期间取全部可用历史。"""
    if args.start_period:
        return args.start_period, args.end_period
    freq = DBCODE_FREQ.get(dbcode, "A")
    if args.mode in ("full", "coordinate"):
        return FULL_START_PERIOD[freq], args.end_period
    return window_start_period(dbcode, args.years), args.end_period


def target_to_json(target: Target) -> dict:
    return {
        "code": target.code,
        "name": target.name,
        "catalog_id": target.catalog_id,
        "unit": target.unit,
    }


def targets_from_json(value: Any) -> list[Target]:
    if value is None:
        return []
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, list):
        return []
    targets: list[Target] = []
    for item in value:
        if not isinstance(item, dict) or not item.get("code"):
            continue
        targets.append(
            Target(
                code=str(item["code"]),
                name=item.get("name"),
                catalog_id=item.get("catalog_id"),
                unit=item.get("unit"),
            )
        )
    return targets


def target_key(target: Target) -> str:
    """Checkpoint identity: new-API indicator UUIDs are scoped by catalog id."""
    if target.catalog_id:
        return f"{target.catalog_id}:{target.code}"
    return target.code


def target_chunk_hash(
    targets: list[Target],
    *,
    dbcode: str,
    start_period: Optional[str],
    end_period: Optional[str],
    regcode: Optional[str],
    endpoint: str,
    execution_key: str,
) -> str:
    identity = {
        "dbcode": dbcode,
        "start_period": start_period,
        "end_period": end_period,
        "regcode": regcode,
        "endpoint": endpoint,
        "execution_key": execution_key,
        "targets": [
            {"code": target.code, "catalog_id": target.catalog_id}
            for target in targets
        ],
    }
    raw = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def coordinator_parent_run_id(
    args,
    dbcodes: list[str],
    per_dbcode: dict[str, list[Target]],
    *,
    endpoint: str,
    windows: dict[str, tuple[Optional[str], Optional[str]]],
) -> str:
    """Deterministic parent id: retries of the same allocation share one parent."""
    identity = {
        "version": 2,
        "dbcodes": dbcodes,
        "endpoint": endpoint,
        "execution_key": getattr(args, "run_key", None),
        "regcode": args.regcode,
        "scope": args.scope,
        "indicators": args.indicators,
        "limit": args.limit,
        "shard_size": args.shard_size,
        "start_period": args.start_period,
        "end_period": args.end_period,
        "windows": {db: list(window) for db, window in windows.items()},
        "targets": {
            db: [
                {"code": target.code, "catalog_id": target.catalog_id}
                for target in targets
            ]
            for db, targets in per_dbcode.items()
        },
    }
    raw = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"nbs-coordinate-{digest}"


def query_target_data(client, target: Target, dbcode: str, start_period: Optional[str], end_period: Optional[str], regcode: Optional[str]) -> list[Observation]:
    """Query one target while preserving easyquery compatibility."""
    kwargs = {
        "zbcode": target.code,
        "dbcode": dbcode,
        "start_period": start_period,
        "end_period": end_period,
        "regcode": regcode,
    }
    if getattr(client, "endpoint_name", "") == "new":
        kwargs["catalog_id"] = target.catalog_id
    return client.query_data(**kwargs)


def configure_pg_strategy(conn) -> None:
    """Read the server version without running DDL (workers must not migrate)."""
    global _PG15_NULLS_NOT_DISTINCT

    with conn.cursor() as cur:
        cur.execute("SELECT current_setting('server_version_num')::int")
        server_num = int(cur.fetchone()[0])
    _PG15_NULLS_NOT_DISTINCT = server_num >= 150000


def wait_for_schema(conn, timeout: float = SCHEMA_WAIT_DEFAULT_SECONDS) -> None:
    """Wait until the coordinator has committed the shard migration."""
    import psycopg2

    deadline = time.monotonic() + timeout
    while True:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM schema_migrations WHERE version = %s",
                    (MIGRATION_VERSION,),
                )
                if cur.fetchone():
                    configure_pg_strategy(conn)
                    conn.rollback()
                    return
        except psycopg2.errors.UndefinedTable:
            pass
        conn.rollback()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"等待 NBS shard schema 超过 {timeout:g} 秒（需要版本 {MIGRATION_VERSION}）")
        time.sleep(min(SCHEMA_WAIT_POLL_SECONDS, remaining))


class LeaseLostError(RuntimeError):
    """The current worker no longer owns the shard lease."""


def claim_shard(conn, lease_owner: str, endpoint: Optional[str] = None) -> Optional[dict]:
    """Atomically claim one pending/retryable shard using row locks."""
    sql = """
        WITH candidate AS (
            SELECT id
              FROM nbs_shard
             WHERE (endpoint IS NULL OR %s IS NULL OR endpoint = %s)
               AND (
                    status = 'pending'
                    OR status = 'failed'
                    OR (status = 'running' AND (lease_until IS NULL OR lease_until < now()))
               )
             ORDER BY CASE status WHEN 'pending' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END, id
             FOR UPDATE SKIP LOCKED
             LIMIT 1
        )
        UPDATE nbs_shard AS s
           SET lease_owner = %s,
               lease_until = now() + (%s * interval '1 second'),
               status = 'running',
               attempt_count = s.attempt_count + 1,
               updated_at = now()
          FROM candidate AS c
         WHERE s.id = c.id
         RETURNING s.*
    """
    with conn.cursor() as cur:
        cur.execute(sql, (endpoint, endpoint, lease_owner, LEASE_SECONDS))
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        columns = [description.name for description in cur.description]
        shard = dict(zip(columns, row))
    conn.commit()
    return shard


def renew_shard_lease(conn, shard_id: int, lease_owner: str) -> None:
    """Extend a lease before another worker is allowed to reclaim it."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE nbs_shard
               SET lease_until = now() + (%s * interval '1 second'),
                   updated_at = now()
             WHERE id = %s
               AND lease_owner = %s
               AND status = 'running'
             RETURNING id
            """,
            (LEASE_SECONDS, shard_id, lease_owner),
        )
        if not cur.fetchone():
            raise LeaseLostError(f"shard {shard_id} 的 lease 已丢失或已过期被回收")
    conn.commit()


def update_shard_checkpoint(
    conn,
    shard_id: int,
    lease_owner: str,
    checkpoint: str,
    counters: dict,
) -> None:
    """Commit one indicator checkpoint and its cumulative counters."""
    from psycopg2.extras import Json

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE nbs_shard
               SET indicators_done = CASE
                                         WHEN %s = ANY(COALESCE(indicators_done, ARRAY[]::text[]))
                                             THEN indicators_done
                                         ELSE array_append(COALESCE(indicators_done, ARRAY[]::text[]), %s)
                                     END,
                   counters_json = %s,
                   updated_at = now()
             WHERE id = %s
               AND lease_owner = %s
               AND status = 'running'
             RETURNING id
            """,
            (checkpoint, checkpoint, Json(counters), shard_id, lease_owner),
        )
        if not cur.fetchone():
            raise LeaseLostError(f"shard {shard_id} checkpoint 写入被拒绝：lease 已丢失")
    conn.commit()


def set_shard_finished(
    conn,
    shard_id: int,
    lease_owner: str,
    status: str,
    counters: dict,
) -> None:
    from psycopg2.extras import Json

    if status not in ("done", "failed", "pending"):
        raise ValueError(f"unsupported shard status: {status}")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE nbs_shard
               SET status = %s,
                   lease_owner = NULL,
                   lease_until = NULL,
                   counters_json = %s,
                   updated_at = now()
             WHERE id = %s
               AND lease_owner = %s
               AND status = 'running'
             RETURNING id
            """,
            (status, Json(counters), shard_id, lease_owner),
        )
        if not cur.fetchone():
            raise LeaseLostError(f"shard {shard_id} 状态收尾被拒绝：lease 已丢失")
    conn.commit()


def aggregate_parent_run(conn, parent_run_id: str) -> tuple[dict, str]:
    """Aggregate all shard counters, including progress on active shards."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*),
                   COALESCE(sum(COALESCE((counters_json->>'indicators_total')::int,
                                         jsonb_array_length(indicators_json), 0)), 0),
                   COALESCE(sum(COALESCE((counters_json->>'indicators_done')::int,
                                         cardinality(COALESCE(indicators_done, ARRAY[]::text[])), 0)), 0),
                   COALESCE(sum(COALESCE((counters_json->>'rows_written')::int, 0)), 0),
                   count(*) FILTER (WHERE status = 'done'),
                   count(*) FILTER (WHERE status = 'failed'),
                   count(*) FILTER (WHERE status IN ('pending', 'running'))
              FROM nbs_shard
             WHERE parent_run_id = %s
            """,
            (parent_run_id,),
        )
        shard_count, indicators_total, indicators_done, rows_written, done_count, failed_count, active_count = cur.fetchone()
    counters = {
        "indicators_total": indicators_total or 0,
        "indicators_done": indicators_done or 0,
        "rows_written": rows_written or 0,
        "shard_count": shard_count or 0,
        "done_shards": done_count or 0,
        "failed_shards": failed_count or 0,
    }
    if active_count:
        status = "running"
    elif not shard_count:
        status = "success"
    elif failed_count and done_count:
        status = "partial"
    elif failed_count:
        status = "failed"
    else:
        status = "success"
    return counters, status


def refresh_parent_run(conn, parent_run_id: str, status: str, counters: dict, note: str) -> None:
    """Coordinator-owned parent summary; terminal runs get finished_at."""
    from psycopg2.extras import Json

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE nbs_crawl_runs
               SET status = %s,
                   indicators_total = %s,
                   indicators_done = %s,
                   rows_written = %s,
                   summary_json = COALESCE(summary_json, scope_json, '{}'::jsonb) || %s,
                   note = %s,
                   finished_at = CASE WHEN %s IN ('success', 'partial', 'failed')
                                      THEN now() ELSE NULL END
             WHERE run_id = %s
             RETURNING run_id
            """,
            (
                status,
                counters.get("indicators_total"),
                counters.get("indicators_done"),
                counters.get("rows_written"),
                Json({"status": status, **counters}),
                note,
                status,
                parent_run_id,
            ),
        )
        if not cur.fetchone():
            raise RuntimeError(f"父 run {parent_run_id} 不存在，无法汇总")
    conn.commit()


def wait_for_parent_run(conn, parent_run_id: str, timeout: float) -> tuple[dict, str]:
    """Wait for all shards to become terminal, then return their aggregate counters."""
    deadline = time.monotonic() + timeout
    while True:
        counters, status = aggregate_parent_run(conn, parent_run_id)
        if status != "running":
            return counters, status
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"等待父 run {parent_run_id} 的 shard 汇总超过 {timeout:g} 秒")
        time.sleep(min(PARENT_WAIT_POLL_SECONDS, remaining))


def process_shard(conn, client, args, shard: dict) -> bool:
    """Process one claimed shard; each indicator is committed before checkpointing."""
    shard_id = int(shard["id"])
    parent_run_id = str(shard["parent_run_id"])
    dbcode = str(shard["dbcode"])
    start_period = shard.get("start_period")
    end_period = shard.get("end_period")
    lease_owner = str(shard.get("lease_owner") or "")
    if not lease_owner:
        log(f"shard {shard_id} 缺少 lease_owner，拒绝处理")
        return False
    shard_endpoint = shard.get("endpoint")
    if shard_endpoint and shard_endpoint != client.endpoint_name:
        log(f"shard {shard_id} endpoint={shard_endpoint} 与 worker {client.endpoint_name} 不匹配")
        return False
    regcode = shard.get("regcode") or args.regcode

    targets = targets_from_json(shard.get("indicators_json"))
    if not targets:
        # Older shards may only carry the range endpoints. Resolve that range from
        # the same client so a retry can still make progress.
        discovered = client.discover_indicators(dbcode)
        by_code = {
            target.code: target
            for target in (
                Target(code=item.code, name=item.name, catalog_id=item.catalog_id, unit=item.unit)
                for item in discovered
            )
        }
        codes = sorted(by_code)
        start_code = str(shard.get("ind_start") or "")
        end_code = str(shard.get("ind_end") or "")
        targets = [by_code[code] for code in codes if (not start_code or code >= start_code) and (not end_code or code <= end_code)]
        if not targets and start_code:
            targets = [Target(code=start_code)]

    recorded = {str(value) for value in (shard.get("indicators_done") or [])}
    done = {
        target_key(target)
        for target in targets
        if target_key(target) in recorded or target.code in recorded
    }
    prior_counters = shard.get("counters_json") or {}
    if isinstance(prior_counters, str):
        prior_counters = json.loads(prior_counters)
    try:
        prior_rows_written = int(prior_counters.get("rows_written", 0) or 0)
    except (AttributeError, TypeError, ValueError):
        prior_rows_written = 0
    counters = {
        "indicators_total": len(targets),
        "indicators_done": len(done),
        "rows_written": prior_rows_written,
    }
    run_id = str(uuid.uuid4())
    scope = {
        "mode": "worker",
        "dbcode": dbcode,
        "parent_run_id": parent_run_id,
        "shard_id": shard_id,
        "ind_start": shard.get("ind_start"),
        "ind_end": shard.get("ind_end"),
        "start_period": start_period,
        "end_period": end_period,
        "regcode": regcode,
        "endpoint": shard_endpoint or client.endpoint_name,
        "target_hash": shard.get("target_hash"),
        "attempt_count": shard.get("attempt_count"),
        "run_id": run_id,
    }
    receipt_key = f"nbs-shard:{parent_run_id}:{shard_id}"
    shard_started_at = utcnow()
    data_complete = False
    observations_committed = False
    checkpoint_committed = False
    try:
        begin_run(conn, run_id, scope, f"worker shard {shard_id}")
        for target in targets:
            checkpoint = target_key(target)
            if checkpoint in done:
                continue
            renew_shard_lease(conn, shard_id, lease_owner)
            observations = query_target_data(
                client, target, dbcode, start_period, end_period, regcode
            )
            ind_name = target.name or next((o.indicator_name for o in observations if o.indicator_name), None)
            ind_unit = target.unit or next((o.unit for o in observations if o.unit), None)
            with conn.cursor() as cur:
                ind_id = upsert_indicator(cur, target.code, dbcode, ind_name, ind_unit, client.endpoint_name)
                written = upsert_observations(cur, ind_id, observations)
            # The commit may have reached PostgreSQL even when its acknowledgement
            # was lost. Treat that boundary as reclaimable progress, not failure.
            observations_committed = True
            conn.commit()
            done.add(checkpoint)
            counters["indicators_done"] = len(done)
            counters["rows_written"] += written
            update_shard_checkpoint(conn, shard_id, lease_owner, checkpoint, counters)
            checkpoint_committed = True
            observations_committed = False
            checkpoint_committed = False
            log(f"  shard={shard_id} {dbcode}/{target.code} {ind_name or ''} -> {written} 行")

        data_complete = True
        status = "success"
        note = f"worker shard {shard_id} 完成 {counters['indicators_done']}/{counters['indicators_total']} 个指标"
        summary = dict(scope)
        summary.update({"status": status, **counters})
        finish_run(conn, run_id, status, summary, counters, note)
        receipt = dict(summary)
        receipt["run_id"] = run_id
        write_receipt(
            receipt_key,
            "worker",
            shard_started_at,
            utcnow(),
            status,
            counters["rows_written"],
            receipt,
            note,
            ensure_unique=False,
        )
        set_shard_finished(conn, shard_id, lease_owner, "done", counters)
        log(f"shard {shard_id} 完成：{note}")
        return True
    except Exception as exc:
        conn.rollback()
        status = "failed"
        note = f"worker shard {shard_id} 失败：{type(exc).__name__}: {exc}"
        summary = dict(scope)
        summary.update({"status": status, **counters, "error": note})

        if isinstance(exc, LeaseLostError):
            # A new owner now owns the shard. This attempt must not touch shard
            # state or overwrite the retry's deterministic shard receipt key.
            log(f"警告：shard {shard_id} lease 已丢失，停止本 attempt：{_db_error_repr(exc)}")
            try:
                finish_run(conn, run_id, status, summary, counters, note)
            except Exception as inner:
                conn.rollback()
                log(f"警告：shard {shard_id} run 状态写入失败 -> {_db_error_repr(inner)}")
            return False

        has_progress = bool(done) or observations_committed or checkpoint_committed
        retry_status = "pending" if has_progress else "failed"
        try:
            set_shard_finished(conn, shard_id, lease_owner, retry_status, counters)
        except Exception as inner:
            conn.rollback()
            log(f"警告：shard {shard_id} 状态写入失败，等待 lease 过期回收 -> {_db_error_repr(inner)}")

        try:
            finish_run(conn, run_id, status, summary, counters, note)
        except Exception as inner:
            conn.rollback()
            log(f"警告：shard {shard_id} run 状态写入失败 -> {_db_error_repr(inner)}")
        try:
            receipt = dict(summary)
            receipt["run_id"] = run_id
            write_receipt(
                receipt_key,
                "worker",
                shard_started_at,
                utcnow(),
                status,
                counters["rows_written"],
                receipt,
                note,
                ensure_unique=False,
            )
        except Exception as inner:
            log(f"警告：shard {shard_id} 回执写入失败 -> {_db_error_repr(inner)}")
        if data_complete:
            log(f"警告：shard {shard_id} 数据已完成但收尾失败，已按 {retry_status} 保留重试")
        log(f"shard {shard_id} 失败：{exc}")
        return False


def cmd_coordinate(args) -> int:
    """Discover indicators once and enqueue idempotent, bounded shards."""
    import psycopg2
    from psycopg2.extras import Json, execute_values

    started_at = utcnow()
    dbcodes: list[str] = args.dbcode
    client = make_client(
        args.endpoint,
        min_interval=args.min_interval,
        retries=args.retries,
        timeout=args.timeout,
        logger=log,
    )
    endpoint = client.endpoint_name
    windows: dict[str, tuple[Optional[str], Optional[str]]] = {}
    per_dbcode: dict[str, list[Target]] = {}
    chunks_by_dbcode: dict[str, list[list[Target]]] = {}
    total_indicators = 0

    try:
        log(f"取数通路：{client.endpoint_name}（{client.endpoint_url}）")
        for dbcode in dbcodes:
            window = build_window(args, dbcode)
            windows[dbcode] = window
            targets = resolve_indicators(client, dbcode, args.scope, args.indicators, args.limit)
            targets = sorted(targets, key=lambda t: (t.code, t.catalog_id or "", t.name or ""))
            unique_by_identity: dict[tuple[str, Optional[str]], Target] = {}
            for target in targets:
                unique_by_identity.setdefault((target.code, target.catalog_id), target)
            targets = [unique_by_identity[key] for key in sorted(unique_by_identity, key=lambda item: (item[0], item[1] or ""))]
            chunks = [targets[i : i + args.shard_size] for i in range(0, len(targets), args.shard_size)]
            windows[dbcode] = window
            per_dbcode[dbcode] = targets
            chunks_by_dbcode[dbcode] = chunks
            total_indicators += len(targets)

        parent_run_id = coordinator_parent_run_id(
            args,
            dbcodes,
            per_dbcode,
            endpoint=endpoint,
            windows=windows,
        )
        planned_shards = sum(len(chunks) for chunks in chunks_by_dbcode.values())
        per_dbcode_summary = {
            dbcode: {"indicators": len(targets), "shards": len(chunks_by_dbcode[dbcode])}
            for dbcode, targets in per_dbcode.items()
        }

        if args.dry_run:
            log(
                f"coordinate dry-run：{len(dbcodes)} 个 dbcode，{total_indicators} 个指标，"
                f"预计 {planned_shards} 个 shard，parent_run_id={parent_run_id}"
            )
            return 0

        scope = {
            "mode": "coordinate",
            "dbcode": ",".join(dbcodes),
            "dbcodes": dbcodes,
            "parent_run_id": parent_run_id,
            "endpoint": endpoint,
            "regcode": args.regcode,
            "run_key": args.run_key,
            "indicators_total": total_indicators,
            "shard_count": planned_shards,
            "per_dbcode": per_dbcode_summary,
            "windows": {db: list(window) for db, window in windows.items()},
            "start_period": args.start_period,
            "end_period": args.end_period,
            "run_id": parent_run_id,
        }

        ensure_database()
        conn = _connect(_ingest_dsn(), purpose="连 cnstats 时")
        try:
            applied, pg15 = apply_migrations(conn)
            log(f"迁移：{'新应用 ' + ', '.join(applied) if applied else '无变化（已是最新）'}"
                f"；唯一键策略={'NULLS NOT DISTINCT' if pg15 else '部分唯一索引'}")
            begin_run(
                conn,
                parent_run_id,
                scope,
                "coordinator shard allocation",
                job_id=os.environ.get("SCRAPYD_JOB") or os.environ.get("JOB_ID"),
            )

            insert_sql = """
                INSERT INTO nbs_shard
                    (parent_run_id, dbcode, ind_start, ind_end, start_period, end_period,
                     regcode, endpoint, target_hash, indicators_json, indicators_done, counters_json)
                VALUES %s
                ON CONFLICT (
                    dbcode, ind_start, ind_end,
                    COALESCE(start_period, ''), COALESCE(end_period, ''),
                    COALESCE(regcode, ''), COALESCE(endpoint, ''), COALESCE(target_hash, '')
                ) DO UPDATE
                   SET parent_run_id = CASE
                                           WHEN nbs_shard.status = 'done'
                                               THEN nbs_shard.parent_run_id
                                           ELSE EXCLUDED.parent_run_id
                                       END,
                       status = CASE
                                    WHEN nbs_shard.status IN ('running', 'done')
                                        THEN nbs_shard.status
                                    ELSE 'pending'
                                END,
                       lease_owner = CASE
                                         WHEN nbs_shard.status = 'running'
                                             THEN nbs_shard.lease_owner
                                         ELSE NULL
                                     END,
                       lease_until = CASE
                                         WHEN nbs_shard.status = 'running'
                                             THEN nbs_shard.lease_until
                                         ELSE NULL
                                     END,
                       indicators_done = nbs_shard.indicators_done,
                       counters_json = nbs_shard.counters_json,
                       indicators_json = EXCLUDED.indicators_json,
                       regcode = EXCLUDED.regcode,
                       endpoint = EXCLUDED.endpoint,
                       target_hash = EXCLUDED.target_hash,
                       updated_at = now()
            """
            rows: list[tuple] = []
            for dbcode in dbcodes:
                start_period, end_period = windows[dbcode]
                for chunk in chunks_by_dbcode[dbcode]:
                    rows.append(
                        (
                            parent_run_id,
                            dbcode,
                            chunk[0].code,
                            chunk[-1].code,
                            start_period,
                            end_period,
                            args.regcode,
                            endpoint,
                            target_chunk_hash(
                                chunk,
                                dbcode=dbcode,
                                start_period=start_period,
                                end_period=end_period,
                                regcode=args.regcode,
                                endpoint=endpoint,
                                execution_key=args.run_key,
                            ),
                            Json([target_to_json(target) for target in chunk]),
                            [],
                            Json({}),
                        )
                    )
            if rows:
                with conn.cursor() as cur:
                    execute_values(cur, insert_sql, rows, page_size=len(rows))
            # Workers poll PostgreSQL from separate connections: publish the
            # claimed shards before this coordinator enters --wait.
            conn.commit()
            for dbcode in dbcodes:
                log(f"  {dbcode}：{len(per_dbcode[dbcode])} 个指标 -> {len(chunks_by_dbcode[dbcode])} 个 shard")

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*), COALESCE(sum(jsonb_array_length(indicators_json)), 0)
                      FROM nbs_shard
                     WHERE parent_run_id = %s
                    """,
                    (parent_run_id,),
                )
                actual_shards, actual_indicators = cur.fetchone()
            if actual_shards != planned_shards or actual_indicators != total_indicators:
                raise RuntimeError(
                    f"shard 分配校验失败：期望 {planned_shards}/{total_indicators}，"
                    f"实际 {actual_shards}/{actual_indicators}"
                )

            if total_indicators == 0:
                counters = {"indicators_total": 0, "indicators_done": 0, "rows_written": 0}
                parent_status = "success"
                receipt_note = "coordinator 未发现指标"
            elif getattr(args, "wait", False):
                wait_timeout = float(getattr(args, "wait_timeout", PARENT_WAIT_DEFAULT_SECONDS))
                try:
                    counters, parent_status = wait_for_parent_run(conn, parent_run_id, wait_timeout)
                except TimeoutError as exc:
                    counters, _ = aggregate_parent_run(conn, parent_run_id)
                    parent_status = "running"
                    receipt_note = str(exc)
                    refresh_parent_run(conn, parent_run_id, parent_status, counters, receipt_note)
                    receipt = dict(scope)
                    receipt.update(counters)
                    receipt["run_id"] = parent_run_id
                    receipt["status"] = parent_status
                    write_receipt(
                        f"nbs-coordinate:{parent_run_id}",
                        "coordinate",
                        started_at,
                        utcnow(),
                        parent_status,
                        int(counters["rows_written"]),
                        receipt,
                        receipt_note,
                    )
                    log(f"父 run {parent_run_id} 等待 worker 超时，保留 running 状态")
                    return 1
                receipt_note = (
                    f"coordinator shard 汇总完成：{counters['done_shards']}/{counters['shard_count']} "
                    f"个 shard 完成，{counters['failed_shards']} 个失败"
                )
            else:
                counters, parent_status = aggregate_parent_run(conn, parent_run_id)
                receipt_note = "coordinator 已分配 shard，等待 worker"

            refresh_parent_run(conn, parent_run_id, parent_status, counters, receipt_note)
            receipt = dict(scope)
            receipt.update(counters)
            receipt["run_id"] = parent_run_id
            receipt["status"] = parent_status
            write_receipt(
                f"nbs-coordinate:{parent_run_id}",
                "coordinate",
                started_at,
                utcnow(),
                parent_status,
                int(counters["rows_written"]),
                receipt,
                receipt_note,
            )
            log(f"父 run {parent_run_id}：status={parent_status}，汇总={json.dumps(counters, ensure_ascii=False)}")
            if not getattr(args, "wait", False):
                return 0
            return 0 if parent_status == "success" else 1
        finally:
            conn.close()
    finally:
        client.close()


def cmd_worker(args) -> int:
    """Claim and process shards without executing any DDL."""
    import psycopg2

    conn = _connect(args.shard_url or _ingest_dsn(), purpose="worker 启动时")
    client = None
    try:
        try:
            wait_for_schema(conn, timeout=args.schema_timeout)
        except Exception as exc:
            log(f"worker schema 检查失败：{_db_error_repr(exc)}")
            return 1
        configure_pg_strategy(conn)
        client = make_client(
            args.endpoint,
            min_interval=args.min_interval,
            retries=args.retries,
            timeout=args.timeout,
            logger=log,
        )
        log(f"worker 取数通路：{client.endpoint_name}（{client.endpoint_url}）")
        lease_owner = f"{os.uname().nodename}:{os.getpid()}:{uuid.uuid4()}"
        consecutive_failures = 0
        while True:
            shard = claim_shard(conn, lease_owner, endpoint=client.endpoint_name)
            if shard is None:
                if args.once:
                    if consecutive_failures:
                        log(f"worker：本次 attempt 失败（连续 {consecutive_failures} 个 shard）")
                        return 1
                    log("worker：没有可认领的 shard")
                    return 0
                log(f"worker：没有可认领的 shard，{args.poll_interval:g} 秒后重试")
                time.sleep(args.poll_interval)
                continue
            ok = process_shard(conn, client, args, shard)
            consecutive_failures = 0 if ok else consecutive_failures + 1
            if args.once:
                return 0 if ok else 1
            if (
                consecutive_failures
                and args.max_failed_shards
                and consecutive_failures >= args.max_failed_shards
            ):
                log(f"worker：连续失败达到上限 {args.max_failed_shards}，退出")
                return 1
    finally:
        conn.close()
        if client is not None:
            client.close()


# ---------------------------------------------------------------------------
# 子命令
# ---------------------------------------------------------------------------
def cmd_init(args) -> int:
    import psycopg2

    ensure_database()
    conn = _connect(_ingest_dsn(), purpose="连 cnstats 时")
    try:
        applied, pg15 = apply_migrations(conn)
        log(f"迁移：{'新应用 ' + ', '.join(applied) if applied else '无变化（已是最新）'}"
            f"；唯一键策略={'NULLS NOT DISTINCT' if pg15 else '部分唯一索引'}") 
        report_tables(conn)
    finally:
        conn.close()
    return 0


def cmd_probe(args) -> int:
    client = make_client(
        args.endpoint,
        min_interval=args.min_interval,
        retries=args.retries,
        timeout=args.timeout,
        logger=log,
    )
    dbcode = args.dbcode[0]
    started = time.monotonic()
    probe_client = cast(ProbeClient, client)
    try:
        summary = probe_client.probe(dbcode=dbcode)
    except NBSError as exc:
        log(f"连通性探针失败：{type(exc).__name__}: {exc}")
        return 1
    finally:
        client.close()
    summary["elapsed_s"] = round(time.monotonic() - started, 2)
    summary.setdefault("endpoint", client.endpoint_name)
    summary.setdefault("url", client.endpoint_url)
    log("连通性探针成功：" + json.dumps(summary, ensure_ascii=False))
    return 0


def cmd_self_test(args) -> int:
    """离线自测：内置录制响应 -> 解析 -> 幂等落库 -> 回执（不联网、不写断点状态）。"""
    import psycopg2

    ensure_database()
    run_id = str(uuid.uuid4())
    started_at = utcnow()
    t0 = time.monotonic()
    counters = {"indicators_total": 0, "indicators_done": 0, "rows_written": 0, "rows_seen": 0}
    per_dbcode: dict[str, dict] = {}
    scope = {"mode": "selftest", "dbcode": None, "fixtures": list(SELF_TEST_FIXTURES)}

    conn = _connect(_ingest_dsn(), purpose="连 cnstats 时")
    try:
        wait_for_schema(conn, timeout=args.schema_timeout)
        begin_run(conn, run_id, scope, "离线自测（内置录制响应，未联网）")
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM nbs_observation")
            before = cur.fetchone()[0]

        for fixture_name, fixture in SELF_TEST_FIXTURES.items():
            dbcode = fixture["dbcode"]
            parser = parse_new_response if fixture.get("parser") == "new" else parse_response
            observations = parser(fixture["response"], dbcode)
            counters["rows_seen"] += len(observations)
            by_code: dict[str, list[Observation]] = {}
            for obs in observations:
                by_code.setdefault(obs.indicator_code, []).append(obs)
            stats = per_dbcode.setdefault(
                dbcode, {"indicators_total": 0, "indicators_done": 0, "rows_written": 0, "fixtures": []}
            )
            stats["fixtures"].append(fixture_name)
            for code, obs_list in sorted(by_code.items()):
                counters["indicators_total"] += 1
                stats["indicators_total"] += 1
                name = next((o.indicator_name for o in obs_list if o.indicator_name), None)
                unit = next((o.unit for o in obs_list if o.unit), None)
                with conn.cursor() as cur:
                    ind_id = upsert_indicator(cur, code, dbcode, name, unit, "selftest_fixture")
                    written = upsert_observations(cur, ind_id, obs_list)
                conn.commit()
                counters["indicators_done"] += 1
                counters["rows_written"] += written
                stats["indicators_done"] += 1
                stats["rows_written"] += written
                log(f"  [{fixture_name}] {dbcode}/{code} {name or ''} -> {written} 行")

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM nbs_observation")
            after = cur.fetchone()[0]

        elapsed = round(time.monotonic() - t0, 2)
        summary = {
            "selftest": True,
            "fixtures": list(SELF_TEST_FIXTURES),
            "observations_parsed": counters["rows_seen"],
            "rows_written": counters["rows_written"],
            "rows_before": before,
            "rows_after": after,
            "rows_delta": after - before,
            "elapsed_s": elapsed,
            "per_dbcode": per_dbcode,
            "cleanup_hint": "DELETE FROM nbs_observation WHERE raw_json->>'_source' = 'selftest_fixture'",
        }
        note = (
            f"离线自测：解析 {counters['rows_seen']} 个 datanode，写出 {counters['rows_written']} 行，"
            f"nbs_observation {before} -> {after}（+{after - before}）；"
            "数据来自内置录制响应（raw_json._source=selftest_fixture），非本轮联网抓取"
        )
        finish_run(conn, run_id, "selftest", summary, counters, note)
    finally:
        conn.close()

    receipt = dict(summary)
    receipt.update({"run_id": run_id, "mode": "selftest", "endpoint": "offline://fixture"})
    write_receipt(args.run_key, "selftest", started_at, utcnow(), "selftest", counters["rows_written"], receipt, note)
    log(f"自测完成：nbs_observation 行数 {summary['rows_before']} -> {summary['rows_after']}"
        f"（清理：{summary['cleanup_hint']}）")
    return 0


def cmd_backfill(args) -> int:
    import psycopg2

    started_at = utcnow()
    t0 = time.monotonic()
    dbcodes: list[str] = args.dbcode
    job_id = os.environ.get("SCRAPYD_JOB") or os.environ.get("JOB_ID")
    run_id = str(uuid.uuid4())
    scope = {
        "mode": args.mode,
        "years": args.years if args.mode == "rolling" else None,
        "dbcodes": dbcodes,
        "indicator_scope": "explicit" if args.indicators else args.scope,
        "limit": args.limit,
        "regcode": args.regcode,
        "min_interval": args.min_interval,
        "endpoint": args.endpoint,
        "run_id": run_id,
    }
    client = make_client(
        args.endpoint,
        min_interval=args.min_interval,
        retries=args.retries,
        timeout=args.timeout,
        logger=log,
    )
    scope["endpoint_resolved"] = client.endpoint_name
    log(f"取数通路：{client.endpoint_name}（{client.endpoint_url}）")
    counters = {"indicators_total": 0, "indicators_done": 0, "rows_written": 0, "rows_seen": 0}
    per_dbcode: dict[str, dict] = {}
    failures: list[dict] = []
    state = load_state()
    resume_used = False
    status = "failed"
    note = ""
    receipt: dict = {}
    conn = None
    periods_by_db: dict[str, list[str]] = {}

    try:
        if not args.dry_run:
            ensure_database()
            conn = _connect(_ingest_dsn(), purpose="连 cnstats 时")
            wait_for_schema(conn, timeout=args.schema_timeout)
            if not (state.get("dbcodes") or {}):
                seed_state_from_runs(conn, state)
            begin_run(conn, run_id, scope, f"backfill_nbs {args.mode}", job_id=job_id)

        for dbcode in dbcodes:
            start_period, end_period = build_window(args, dbcode)
            stats = per_dbcode.setdefault(
                dbcode,
                {
                    "indicators_total": 0,
                    "indicators_done": 0,
                    "indicators_skipped": 0,
                    "indicators_failed": 0,
                    "rows_written": 0,
                    "start_period": start_period,
                    "end_period": end_period,
                    "period_min": None,
                    "period_max": None,
                    "elapsed_s": 0.0,
                },
            )
            seen_periods = periods_by_db.setdefault(dbcode, [])
            t_db = time.monotonic()
            log(f"=== {dbcode}（{DBCODES.get(dbcode, dbcode)}）窗口 {start_period} -> {end_period or '最新'} ===")
            indicators = resolve_indicators(client, dbcode, args.scope, args.indicators, args.limit)
            stats["indicators_total"] = len(indicators)
            counters["indicators_total"] += len(indicators)
            log(f"指标 {len(indicators)} 个" + (f"（--limit {args.limit}）" if args.limit else ""))

            for target in indicators:
                code = target.code
                name = target.name
                if args.resume and not args.dry_run and already_covered(state, dbcode, code, start_period or ""):
                    stats["indicators_skipped"] += 1
                    resume_used = True
                    log(f"  跳过 {dbcode}/{code}（断点续跑：上次已覆盖窗口 {start_period}）")
                    continue
                try:
                    observations = query_target_data(
                        client, target, dbcode, start_period, end_period, args.regcode
                    )
                except NBSBlockedError:
                    raise
                except NBSError as exc:  # 单指标失败不拖垮整轮
                    stats["indicators_failed"] += 1
                    failures.append({"dbcode": dbcode, "indicator": code, "error": str(exc)})
                    log(f"  指标 {code} 失败：{exc}")
                    continue

                counters["rows_seen"] += len(observations)
                if args.dry_run:
                    counters["indicators_done"] += 1
                    stats["indicators_done"] += 1
                    for obs in observations[:2]:
                        log(f"  [dry-run] {dbcode}/{obs.indicator_code} {obs.period} = {obs.value}")
                    continue

                ind_name = name or next((o.indicator_name for o in observations if o.indicator_name), None)
                ind_unit = next((o.unit for o in observations if o.unit), None)
                assert conn is not None
                with conn.cursor() as cur:
                    ind_id = upsert_indicator(cur, code, dbcode, ind_name, ind_unit, client.endpoint_name)
                    written = upsert_observations(cur, ind_id, observations)
                conn.commit()
                counters["indicators_done"] += 1
                counters["rows_written"] += written
                stats["indicators_done"] += 1
                stats["rows_written"] += written
                seen_periods.extend(o.period for o in observations)

                entry = state["dbcodes"].setdefault(dbcode, {"mode": args.mode, "completed": []})
                if code not in entry["completed"]:
                    entry["completed"].append(code)
                entry["mode"] = args.mode
                entry["start_period"] = start_period
                entry["rows"] = counters["rows_written"]
                entry["last_run_at"] = iso_z(utcnow())
                entry.pop("seeded_from", None)
                log(f"  {dbcode}/{code} {ind_name or ''} -> {written} 行（{len(observations)} 个节点）")

            periods = sorted(set(seen_periods))
            stats["period_min"] = periods[0] if periods else None
            stats["period_max"] = periods[-1] if periods else None
            stats["elapsed_s"] = round(time.monotonic() - t_db, 2)
            stats["requests"] = client.request_count
            if not args.dry_run:
                save_state(state)

        elapsed = round(time.monotonic() - t0, 2)
        if args.dry_run:
            status = "dry-run"
        elif not failures:
            status = "success"
        elif counters["indicators_done"]:
            status = "partial"
        else:
            status = "failed"
        receipt = {
            "run_id": run_id,
            "mode": args.mode,
            "endpoint": client.endpoint_url,
            "per_dbcode": per_dbcode,
            "requests": client.request_count,
            "errors": client.error_count,
            "rows_seen": counters["rows_seen"],
            "rows_written": counters["rows_written"],
            "indicators_total": counters["indicators_total"],
            "indicators_done": counters["indicators_done"],
            "failed_indicators": failures[:50],
            "elapsed_s": elapsed,
            "state_file": str(STATE_PATH),
            "resume_used": resume_used,
        }
        note = (
            f"{args.mode} 模式：{len(dbcodes)} 库，指标 {counters['indicators_done']}/"
            f"{counters['indicators_total']} 完成，写 {counters['rows_written']} 行，"
            f"{client.request_count} 次请求，{elapsed}s"
            + (f"，失败 {len(failures)} 个指标" if failures else "")
            + ("（dry-run，未写库）" if args.dry_run else "")
        )
        if not args.dry_run and conn is not None:
            summary = dict(receipt)
            summary["finished_at"] = iso_z(utcnow())
            summary["completed_by_dbcode"] = {
                db: sorted((state["dbcodes"].get(db) or {}).get("completed") or []) for db in dbcodes
            }
            first = per_dbcode.get(dbcodes[0]) or {}
            summary["start_period"] = first.get("start_period")
            summary["rows_written"] = counters["rows_written"]
            finish_run(conn, run_id, status, summary, counters, note)
        log(note)

    except NBSBlockedError as exc:
        note = f"被 NBS 拦截（403）：{exc}"
        receipt = {
            "run_id": run_id,
            "mode": args.mode,
            "endpoint": client.endpoint_url,
            "blocked": True,
            "error": str(exc),
            "requests": client.request_count,
            "elapsed_s": round(time.monotonic() - t0, 2),
            "per_dbcode": per_dbcode,
        }
        log(f"失败：{note}")
        if conn is not None:
            try:
                finish_run(conn, run_id, "failed", dict(receipt), counters, note)
            except Exception as inner:
                log(f"警告：nbs_crawl_runs 状态更新失败 -> {_db_error_repr(inner)}")
    except NBSError as exc:
        note = f"执行失败：{type(exc).__name__}: {exc}"
        receipt = {
            "run_id": run_id,
            "mode": args.mode,
            "endpoint": client.endpoint_url,
            "error": note,
            "requests": client.request_count,
            "elapsed_s": round(time.monotonic() - t0, 2),
            "per_dbcode": per_dbcode,
        }
        log(f"失败：{note}")
        if conn is not None:
            try:
                finish_run(conn, run_id, "failed", dict(receipt), counters, note)
            except Exception as inner:
                log(f"警告：nbs_crawl_runs 状态更新失败 -> {_db_error_repr(inner)}")
    except Exception as exc:  # 落库/系统错误也要留下 failed 回执，不能只抛栈后消失
        note = f"执行失败：{type(exc).__name__}: {exc}"
        receipt = {
            "run_id": run_id,
            "mode": args.mode,
            "endpoint": client.endpoint_url,
            "error": note,
            "requests": client.request_count,
            "elapsed_s": round(time.monotonic() - t0, 2),
            "per_dbcode": per_dbcode,
        }
        log(f"失败：{note}")
        if conn is not None:
            try:
                conn.rollback()
                finish_run(conn, run_id, "failed", dict(receipt), counters, note)
            except Exception as inner:
                conn.rollback()
                log(f"警告：nbs_crawl_runs 状态更新失败 -> {_db_error_repr(inner)}")
    finally:
        if conn is not None:
            conn.close()
        client.close()

    if not args.dry_run:
        write_receipt(
            args.run_key, args.mode, started_at, utcnow(), status,
            counters["rows_written"], receipt, note,
        )
    if status in ("success", "dry-run"):
        return 0
    log(f"非零退出（status={status}）：{note}")
    return 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="NBS（国家统计局）easyquery -> Windows PG cnstats 库 幂等入库",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--mode",
        choices=["rolling", "full", "coordinate", "worker"],
        default="rolling",
        help="滚动窗口、全量回补、shard 协调分配或 worker 认领处理",
    )
    p.add_argument(
        "--dbcode",
        default="hgnd",
        help="库代码，逗号分隔：hgyd,hgjd,hgnd,fsyd,fsjd,fsnd,csyd,csjd,csnd（默认 hgnd）",
    )
    p.add_argument("--years", type=int, default=5, help="rolling 模式的年数（默认 5）")
    p.add_argument("--start-period", help="显式起始期间（覆盖 --years，如 202101 / 2021A / 2021）")
    p.add_argument("--end-period", help="显式结束期间（仅用于防御性过滤）")
    p.add_argument("--limit", type=int, help="限定指标数（测试用）")
    p.add_argument(
        "--shard-size",
        type=int,
        default=SHARD_MAX_SIZE,
        help=f"coordinator 每 shard 指标数（默认 {SHARD_MAX_SIZE}，最大 {SHARD_MAX_SIZE}）",
    )
    p.add_argument(
        "--scope",
        choices=["tree", "curated", "auto"],
        default="tree",
        help="指标发现方式：GetTree 指标树（默认）/ 内置精选清单 / auto（有精选清单则用清单）",
    )
    p.add_argument("--indicators", help="显式指标码，逗号分隔（覆盖 --scope）；新接口写成 目录id:指标id")
    p.add_argument("--regcode", help="地区码（分省/分市库用，如 110000；新接口下为客户端过滤）")
    p.add_argument(
        "--endpoint",
        choices=["auto", "new", "easyquery"],
        default="auto",
        help="取数通路：new=/dg/website 新接口（改版后可用）；easyquery=旧接口（现多被 403 拦截）；"
        "auto=先用新接口探一次，失败回退 easyquery（默认）",
    )
    p.add_argument("--run-key", default=date.today().isoformat(), help="回执 run_key（默认当天 YYYY-MM-DD）")
    p.add_argument("--min-interval", type=float, default=1.0, help="请求最小间隔秒（默认 1.0）")
    p.add_argument("--retries", type=int, default=3, help="瞬时错误重试次数（默认 3）")
    p.add_argument("--timeout", type=float, default=30.0, help="单请求超时秒（默认 30）")
    p.add_argument("--no-resume", dest="resume", action="store_false", help="忽略断点续跑状态，强制重取")
    p.add_argument("--dry-run", action="store_true", help="只抓取解析，不写库、不写回执")
    p.add_argument("--init", action="store_true", help="建库建表（幂等）后退出")
    p.add_argument("--probe", action="store_true", help="端到端连通性探针（目录树->指标->一次真实取数，不写库）")

    p.add_argument("--self-test", action="store_true", help="离线自测：内置录制响应，验证解析 + 幂等落库")
    p.add_argument("--shard-url", help="worker 的 cnstats PostgreSQL DSN（默认使用 FD_CNSTATS_PG_URL）")
    p.add_argument("--schema-timeout", type=float, default=SCHEMA_WAIT_DEFAULT_SECONDS, help="worker 等待 schema 的秒数")
    p.add_argument("--poll-interval", type=float, default=SCHEMA_WAIT_POLL_SECONDS, help="worker 无 shard 时的轮询秒数")
    p.add_argument("--once", action="store_true", help="worker 处理一个 shard 后退出")
    p.add_argument("--wait", action="store_true", help="coordinator 等待 shard 完成后汇总父 run")
    p.add_argument("--wait-timeout", type=float, default=PARENT_WAIT_DEFAULT_SECONDS, help="coordinator 等待 shard 汇总的秒数")
    p.add_argument("--max-failed-shards", type=int, default=0, help="worker 连续失败退出上限（0=不限制）")
    p.set_defaults(resume=True)
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    args.dbcode = [d.strip() for d in str(args.dbcode).split(",") if d.strip()]
    bad = [d for d in args.dbcode if d not in DBCODES]
    if bad:
        print(f"未知 dbcode：{bad}（可选：{', '.join(DBCODES)}）", file=sys.stderr)
        return 2
    if args.indicators:
        args.indicators = [c.strip() for c in args.indicators.split(",") if c.strip()]
    if not 1 <= args.shard_size <= SHARD_MAX_SIZE:
        print(f"--shard-size 必须在 1..{SHARD_MAX_SIZE} 之间", file=sys.stderr)
        return 2
    if args.max_failed_shards < 0:
        print("--max-failed-shards 不能小于 0", file=sys.stderr)
        return 2
    if args.schema_timeout < 0:
        print("--schema-timeout 不能小于 0", file=sys.stderr)
        return 2
    if args.poll_interval <= 0:
        print("--poll-interval 必须大于 0", file=sys.stderr)
        return 2
    if args.wait_timeout < 0:
        print("--wait-timeout 不能小于 0", file=sys.stderr)
        return 2

    if args.init:
        return cmd_init(args)
    if args.probe:
        return cmd_probe(args)
    if args.self_test:
        return cmd_self_test(args)
    if args.mode == "coordinate":
        return cmd_coordinate(args)
    if args.mode == "worker":
        return cmd_worker(args)
    return cmd_backfill(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n已中断", file=sys.stderr)
        sys.exit(130)
