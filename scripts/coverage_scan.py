#!/usr/bin/env python3
"""覆盖度巡检器：按 baselines/*.yaml 扫描受管数据源，把缺口写入 dq 库。

用法:
    python3 scripts/coverage_scan.py --source all
    python3 scripts/coverage_scan.py --source world_bank --report
    python3 scripts/coverage_scan.py --source all --run-key 2026-09-21

退出码: 0 = 扫描完成（有缺口也算成功）；非零 = 扫描本身失败（不破坏上一轮报告）。

check kind：staleness（组间停更） / freshness（整源停更，水位对 wall clock） /
missing_cells / missing_rows / missing_entity / volume / row_count（可选 sum_col 按列
求行数规模） / data_quality。全部接受可选 ``where`` 行级过滤（共享表按源巡检）。
"""
import argparse
import datetime as dt
import json
import statistics
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _env import dsn  # noqa: E402

BASE = Path(__file__).resolve().parent.parent
BASELINES = BASE / "baselines"

SEV_ORDER = {"info": 0, "warn": 1, "critical": 2}


# ---------------------------------------------------------------- 工具


def where_sql(check) -> str:
    """可选的行级过滤（基线人工声明，与 data_quality.predicate 同一信任级）。

    共享表按源巡检需要它：证券业的财务/指标/六爬虫单元都落在
    semantic_observations，只能用 source_used / concept_id 区分。
    """
    cond = (check.get("where") or "").strip()
    return f" WHERE ({cond})" if cond else ""


def period_agg(check) -> str:
    """staleness 的期间聚合表达式。period_type=date 时把 varchar 日期归一为 date。"""
    period = check["period_col"]
    if check.get("period_type") == "date":
        return f"max({period}::date)"
    return f"max({period})"


def lag_of(expected, mx):
    """lag 单位：数值型为原值差；date/timestamp 型为天数（日历日）。"""
    if isinstance(expected, dt.date) or isinstance(expected, dt.datetime):
        return (expected - mx).days
    return expected - mx


def as_of_moment(check):
    """新鲜度判定的参照时刻：check.as_of（drill 用）或当前 UTC。"""
    raw = check.get("as_of")
    if raw in (None, ""):
        return dt.datetime.now(dt.timezone.utc)
    if isinstance(raw, dt.datetime):
        parsed = raw
    elif isinstance(raw, dt.date):
        return dt.datetime(raw.year, raw.month, raw.day)
    else:
        text = str(raw).strip()
        try:
            parsed = dt.datetime.fromisoformat(text)
        except ValueError:
            parsed = dt.datetime.combine(dt.date.fromisoformat(text), dt.time())
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def parse_period(value, ptype):
    """把水位归一为 date/datetime（varchar 日期形态混杂：YYYY / YYYY-MM / YYYY-MM-DD）。"""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.date):
        return value
    text = str(value).strip()
    if ptype == "timestamp":
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return dt.datetime.strptime(text[:19], fmt)
            except ValueError:
                continue
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m"):
        try:
            parsed = dt.datetime.strptime(text, fmt)
            return parsed.date()
        except ValueError:
            continue
    if len(text) == 4 and text.isdigit():  # 年度水位：按年初（保守）
        return dt.date(int(text), 1, 1)
    return None


def age_days_of(as_of, latest):
    """as_of 与水位之差（天，取整）；date 与 datetime 混算一律折算到 UTC 日历。"""
    if latest is None:
        return None
    if isinstance(latest, dt.datetime):
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=dt.timezone.utc)
        return (as_of - latest).total_seconds() / 86400.0
    if isinstance(latest, dt.date):
        base = as_of.date() if isinstance(as_of, dt.datetime) else as_of
        if isinstance(latest, dt.datetime):
            latest = latest.date()
        return float((base - latest).days)
    # 数值/字符串水位（如 202609）不参与 freshness（按未定义处理）
    return None


def columns_of(cur, table: str):
    cur.execute(
        """SELECT column_name FROM information_schema.columns
           WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position""",
        (table,),
    )
    return [r[0] for r in cur.fetchall()]


def table_exists(cur, table: str) -> bool:
    cur.execute(
        """SELECT 1 FROM information_schema.tables
           WHERE table_schema='public' AND table_name=%s""",
        (table,),
    )
    return cur.fetchone() is not None


def gaps_staleness(cur, check):
    table, period = check["table"], check["period_col"]
    keys = check.get("key_cols") or []
    allow = float(check.get("allow_lag", 2))
    cmult = float(check.get("critical_mult", 2))
    sel = ", ".join(keys + [period_agg(check)])
    grp = f"GROUP BY {', '.join(keys)}" if keys else ""
    cur.execute(f"SELECT {sel} FROM {table}{where_sql(check)} {grp}")
    rows = cur.fetchall()
    if not rows:
        return []
    non_null = [r[-1] for r in rows if r[-1] is not None]
    expected = max(non_null) if non_null else None
    out = []
    for r in rows:
        *key_vals, mx = r
        if mx is None:
            lag = None
            sev = "critical"
            ks = ", ".join(f"{k}={v}" for k, v in zip(keys, key_vals)) or "all"
        else:
            lag = lag_of(expected, mx) if expected is not None else None
            if lag is not None and lag <= allow:
                continue
            sev = "critical" if (lag is not None and lag > allow * cmult) else "warn"
            ks = ", ".join(f"{k}={v}" for k, v in zip(keys, key_vals)) or "all"
        out.append(
            dict(
                check_kind="staleness",
                key_space=ks[:500],
                # A group whose period column is entirely NULL has no usable
                # period at all — it is *at least* one period short, so 0 is a
                # lie. 0 also sorts last in the report's `ORDER BY
                # missing_count DESC`, which buries the worst case (audit 04 §5).
                missing_count=(int(lag) if lag is not None else 1),
                missing_sample=None,
                last_period=str(mx),
                severity=sev,
                detail={"expected_max": str(expected), "allow_lag": allow, "lag": lag,
                        "period_type": check.get("period_type", "number")},
            )
        )
    return out


def gaps_missing_cells(cur, check):
    table = check["table"]
    excl = set(check.get("exclude_cols") or [])
    min_cov = float(check.get("min_coverage", 0.9))
    cols = [c for c in columns_of(cur, table) if c not in excl]
    if not cols:
        return []
    # 宽表逐列填充率：jsonb 展开一次
    ks = sorted(excl) or ["id"]
    # 用 jsonb_each_text 展开所有非排除列
    sub = f"""SELECT (kv).key AS key, (kv).value AS val
              FROM {table} t, LATERAL jsonb_each_text(to_jsonb(t) - ARRAY[{", ".join("'" + k + "'" for k in ks)}]::text[]) AS kv{where_sql(check)}"""
    cur.execute(
        f"""WITH cells AS ({sub})
            SELECT key, count(*) AS cells, count(*) FILTER (WHERE val IS NOT NULL AND val <> '') AS filled
            FROM cells GROUP BY key"""
    )
    out = []
    total_cells = 0
    for key, cells, filled in cur.fetchall():
        total_cells += cells
        cov = filled / cells if cells else 0
        if cov >= min_cov:
            continue
        out.append(
            dict(
                check_kind="missing_cells",
                key_space=f"column={key}"[:500],
                missing_count=cells - filled,
                missing_sample=None,
                last_period=None,
                severity="critical" if filled == 0 else "warn",
                detail={"cells": cells, "filled": filled, "coverage": round(cov, 4), "min_coverage": min_cov},
            )
        )
    return out


def gaps_missing_rows(cur, check):
    table = check["table"]
    ent, per = check["entity_col"], check["period_col"]
    ratio = float(check.get("min_ratio", 0.9))
    cur.execute(
        f"SELECT {per}, count(DISTINCT {ent}) FROM {table}{where_sql(check)} GROUP BY 1 ORDER BY 1"
    )
    rows = cur.fetchall()
    if not rows:
        return []
    best = max(r[1] for r in rows)
    out = []
    for p, n in rows:
        if n >= ratio * best:
            continue
        out.append(
            dict(
                check_kind="missing_rows",
                key_space=f"{per}={p}"[:500],
                missing_count=best - n,
                missing_sample=None,
                last_period=str(p),
                severity="critical" if n == 0 else "warn",
                detail={"expected": best, "actual": n, "min_ratio": ratio},
            )
        )
    return out


def gaps_volume(cur, check):
    table, per = check["table"], check["period_col"]
    lo, hi = check.get("period_range", [None, None])
    ratio = float(check.get("min_ratio", 0.4))
    conds = []
    if lo is not None and hi is not None:
        conds.append(f"{per} BETWEEN {int(lo)} AND {int(hi)}")
    extra = (check.get("where") or "").strip()
    if extra:
        conds.append(f"({extra})")
    where = f" WHERE {' AND '.join(conds)}" if conds else ""
    cur.execute(f"SELECT {per}, count(*) FROM {table}{where} GROUP BY 1 ORDER BY 1")
    rows = cur.fetchall()
    if not rows:
        return []
    med = statistics.median(n for _, n in rows)
    out = []
    for p, n in rows:
        if n >= ratio * med:
            continue
        out.append(
            dict(
                check_kind="volume",
                key_space=f"{per}={p}"[:500],
                missing_count=int(med - n),
                missing_sample=None,
                last_period=str(p),
                severity="warn",
                detail={"count": n, "median": med, "min_ratio": ratio},
            )
        )
    return out


def gaps_row_count(cur, check):
    table = check["table"]
    min_rows = int(check.get("min_rows", 1))
    # sum_col：按列求和当「行数规模」（concept_source_stats.rows 是精确维护的
    # 每源行数，避免对 semantic_observations 做全表 count）。
    agg = f"COALESCE(sum({check['sum_col']}),0)" if check.get("sum_col") else "count(*)"
    cur.execute(f"SELECT {agg} FROM {table}{where_sql(check)}")
    n = cur.fetchone()[0]
    if n is None:
        n = 0
    if n >= min_rows:
        return []
    return [
        dict(
            check_kind="row_count",
            key_space="table",
            missing_count=int(min_rows - n),
            missing_sample=None,
            last_period=None,
            severity=check.get("severity", "critical"),
            detail={"rows": int(n), "min_rows": min_rows,
                    "agg": "sum" if check.get("sum_col") else "count",
                    "note": check.get("note")},
        )
    ]


def gaps_freshness(cur, check):
    """停更判定：最新数据期（水位）距今不得超过 max_age_days。

    与 ``staleness`` 的分工：staleness 是「组间落后」（同一源内实体互相比较），
    freshness 是「整源停更」（水位对 wall clock）。证券业基线逐源一条，
    容忍窗按源的产出节奏声明（日更 7 天 / 月更 60 天 / 季更 120 天）。
    ``as_of`` 可把参照时刻前移，用于人工停更演练（把当前水位当作过期）。
    """
    table, period = check["table"], check["period_col"]
    ptype = check.get("period_type", "date")
    max_age = float(check["max_age_days"])
    # 水位列可能是混合形态 varchar（YYYY / YYYY-MM / YYYY-MM-DD）——聚合取文本
    # 最大值后在 Python 归一日历，避免 ::date 被脏形态炸掉整源扫描。
    if ptype == "timestamp":
        agg = f"max({period}::timestamp)"
    else:
        agg = f"max({period})"
    cur.execute(f"SELECT {agg}, count(*) FROM {table}{where_sql(check)}")
    raw_latest, n = cur.fetchone()
    latest = parse_period(raw_latest, ptype)
    as_of = as_of_moment(check)
    if not n:
        return [
            dict(
                check_kind="freshness",
                key_space="latest=<none>",
                # No rows at all is the *worst* coverage state, not "nothing
                # missing". Reporting 0 made a dead source sort below healthy
                # ones in the critical listing (ORDER BY missing_count DESC) and
                # made sum(missing_count) treat it as complete. 1 matches the
                # convention `gaps_missing_entity` already uses for an absent
                # entity: at least one period is missing. `detail.reason` still
                # says "no rows in scope" for whoever needs the distinction.
                missing_count=1,
                missing_sample=None,
                last_period=None,
                severity=check.get("severity", "critical"),
                detail={"latest": None, "rows": 0, "max_age_days": max_age,
                        "as_of": as_of.isoformat(), "reason": "no rows in scope",
                        "note": check.get("note")},
            )
        ]
    age = age_days_of(as_of, latest)
    if age is None:
        return [
            dict(
                check_kind="freshness",
                key_space=f"latest={raw_latest}"[:500],
                missing_count=1,
                missing_sample=None,
                last_period=str(raw_latest),
                severity="warn",
                detail={"latest": str(raw_latest), "rows": int(n),
                        "age_days": None, "max_age_days": max_age,
                        "as_of": as_of.isoformat(),
                        "reason": "watermark unparseable", "note": check.get("note")},
            )
        ]
    if age <= max_age:
        return []
    return [
        dict(
            check_kind="freshness",
            key_space=f"latest={raw_latest}"[:500],
            missing_count=int(age - max_age),
            missing_sample=None,
            last_period=str(raw_latest),
            severity=check.get("severity", "critical"),
            detail={"latest": str(raw_latest), "rows": int(n), "age_days": round(age, 2),
                    "max_age_days": max_age, "as_of": as_of.isoformat(),
                    "note": check.get("note")},
        )
    ]


def gaps_data_quality(cur, check):
    table = check["table"]
    pred = check["predicate"]
    extra = (check.get("where") or "").strip()
    cond = f"({pred}) AND ({extra})" if extra else pred
    cur.execute(f"SELECT count(*) FROM {table} WHERE {cond}")
    n = cur.fetchone()[0]
    if n == 0:
        return []
    return [
        dict(
            check_kind="data_quality",
            key_space=f"predicate:{pred}"[:500],
            missing_count=n,
            missing_sample=None,
            last_period=None,
            severity=check.get("severity", "critical"),
            detail={"predicate": pred, "note": check.get("note")},
        )
    ]


def gaps_missing_entity(cur, check):
    table, ent = check["table"], check["entity_col"]
    expected = check.get("expected")
    if expected is None and check.get("expected_from"):
        cur.execute(check["expected_from"])
        expected = [r[0] for r in cur.fetchall()]
    if not expected:
        return []
    cur.execute(f"SELECT DISTINCT {ent}::text FROM {table}{where_sql(check)}")
    present = {r[0] for r in cur.fetchall()}
    out = []
    for e in expected:
        if str(e) in present:
            continue
        out.append(
            dict(
                check_kind="missing_entity",
                key_space=f"{ent}={e}"[:500],
                missing_count=1,
                missing_sample=None,
                last_period=None,
                severity=check.get("severity", "warn"),
                detail={"expected_count": len(expected), "present_count": len(present)},
            )
        )
    return out


KINDS = {
    "staleness": gaps_staleness,
    "freshness": gaps_freshness,
    "missing_cells": gaps_missing_cells,
    "missing_rows": gaps_missing_rows,
    "missing_entity": gaps_missing_entity,
    "volume": gaps_volume,
    "row_count": gaps_row_count,
    "data_quality": gaps_data_quality,
}


# ---------------------------------------------------------------- 主流程


def scan_source(baseline: dict, run_key: str, conn_dq, verbose: bool):
    source = baseline["source"]
    db_env = baseline["db_env"]
    all_gaps = []
    status, note = "success", None
    try:
        conn = psycopg2.connect(dsn(db_env))
        conn.autocommit = True
        cur = conn.cursor()
        if baseline.get("report_only"):
            all_gaps.append(
                dict(
                    check_kind="executor",
                    key_space="report_only",
                    missing_count=0,
                    missing_sample=None,
                    last_period=None,
                    severity="info",
                    detail={
                        "note": "仅报告：loader/源文件在外部，补齐需按 CONTRACTS.md 重灌",
                        "expected_source": baseline.get("expected_source"),
                    },
                )
            )
        for check in baseline.get("checks", []):
            kind = check["kind"]
            fn = KINDS.get(kind)
            if fn is None:
                raise SystemExit(f"未知 check kind: {kind}")
            table = check.get("table")
            if table and not table_exists(cur, table):
                all_gaps.append(
                    dict(
                        check_kind=kind + ":table_missing",
                        key_space=f"table={table}"[:500],
                        missing_count=0,
                        missing_sample=None,
                        last_period=None,
                        severity="critical",
                        detail={"note": "表不存在", "table": table},
                    )
                )
                continue
            if verbose:
                print(f"  [{source}] {kind} on {table} ...", flush=True)
            all_gaps.extend(fn(cur, check))
        conn.close()
    except Exception as e:  # noqa: BLE001
        status, note = "failed", f"{type(e).__name__}: {e}"

    # 写报告：同 run_key+source 先清后写（幂等）。
    # **只有本轮真的成功才动 coverage_gap**：README 承诺"扫描本身失败不破坏上一轮报告"，
    # 而失败时 all_gaps 往往只攒了一半甚至为空，照常清写就等于把上一轮的证据抹掉——
    # 看板会显示"没有缺口"，恰好在最不健康的时候报健康。
    dc = conn_dq.cursor()
    if status == "success":
        dc.execute("DELETE FROM coverage_gap WHERE run_key=%s AND source=%s", (run_key, source))
        for g in all_gaps:
            dc.execute(
                """INSERT INTO coverage_gap
                   (run_key, source, object, check_kind, key_space, missing_count,
                    missing_sample, last_period, severity, detail, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                   ON CONFLICT (run_key, source, object, check_kind, key_space) DO UPDATE
                     SET missing_count=EXCLUDED.missing_count, missing_sample=EXCLUDED.missing_sample,
                         last_period=EXCLUDED.last_period, severity=EXCLUDED.severity,
                         detail=EXCLUDED.detail, updated_at=now()""",
                (
                    run_key,
                    source,
                    baseline["object"],
                    g["check_kind"],
                    g["key_space"],
                    g["missing_count"],
                    json.dumps(g["missing_sample"], ensure_ascii=False) if g["missing_sample"] else None,
                    g["last_period"],
                    g["severity"],
                    json.dumps(g["detail"], ensure_ascii=False),
                ),
            )
    crit = sum(1 for g in all_gaps if g["severity"] == "critical")
    dc.execute(
        """INSERT INTO scan_run (run_key, source, started_at, finished_at, status, gap_count, critical_count, note)
           VALUES (%s,%s,now(),now(),%s,%s,%s,%s)
           ON CONFLICT (run_key, source) DO UPDATE
             SET finished_at=now(), status=EXCLUDED.status, gap_count=EXCLUDED.gap_count,
                 critical_count=EXCLUDED.critical_count, note=EXCLUDED.note""",
        (run_key, source, status, len(all_gaps), crit, note),
    )
    conn_dq.commit()
    return status, len(all_gaps), crit, note


def print_report(conn_dq, run_key):
    cur = conn_dq.cursor()
    cur.execute(
        """SELECT source, gap_count, critical_count, status, note FROM scan_run
           WHERE run_key=%s ORDER BY source""",
        (run_key,),
    )
    print(f"\n=== 扫描摘要 run_key={run_key} ===")
    for src, n, crit, status, note in cur.fetchall():
        flag = "" if status == "success" else f"  !! {status}: {note}"
        print(f"{src:20s} gaps={n:<5d} critical={crit:<4d}{flag}")
    cur.execute(
        """SELECT source, check_kind, severity, count(*) FROM coverage_gap
           WHERE run_key=%s GROUP BY 1,2,3 ORDER BY 1,2,3""",
        (run_key,),
    )
    rows = cur.fetchall()
    if rows:
        print("\n--- 缺口分布（source / kind / severity / 条数）---")
        for src, kind, sev, n in rows:
            print(f"  {src:20s} {kind:22s} {sev:9s} {n}")
    cur.execute(
        """SELECT source, check_kind, key_space, missing_count, last_period, severity
           FROM coverage_gap WHERE run_key=%s AND severity='critical'
           ORDER BY source, check_kind, missing_count DESC LIMIT 20""",
        (run_key,),
    )
    crit = cur.fetchall()
    if crit:
        print("\n--- critical 样例（最多 20 条）---")
        for src, kind, ks, mc, lp, sev in crit:
            print(f"  {src:16s} {kind:22s} {ks[:60]:60s} miss={mc} last={lp}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="all")
    ap.add_argument("--run-key", default=dt.date.today().isoformat())
    ap.add_argument("--report", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    files = sorted(BASELINES.glob("*.yaml"))
    baselines = [yaml.safe_load(p.read_text(encoding="utf-8")) for p in files]
    if args.source != "all":
        baselines = [b for b in baselines if b["source"] == args.source]
        if not baselines:
            print(f"没有匹配的基线: {args.source}", file=sys.stderr)
            return 2

    conn_dq = psycopg2.connect(dsn("FD_DQ_PG_URL"))
    failed = 0
    for b in baselines:
        print(f"扫描 {b['source']} ...", flush=True)
        status, n, crit, note = scan_source(b, args.run_key, conn_dq, args.verbose)
        print(f"  -> gaps={n} critical={crit} status={status}" + (f" ({note})" if note else ""))
        if status != "success":
            failed += 1
    if args.report:
        print_report(conn_dq, args.run_key)
    conn_dq.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
