#!/usr/bin/env python3
"""给 World Bank 的停更缺口做上游归因：区分"我们缺"与"上游停发"。

做法：对某次扫描报告里的 staleness 缺口，逐指标向上游 API 要一次最新年份，
与库内最新年份比对后写回 coverage_gap.detail：
  - verdict=upstream_limited ：上游最新年 <= 库内最新年（上游已停发/未更新，补不了）
  - verdict=fillable         ：上游最新年 >  库内最新年（真缺口，可补齐）
每次运行写一条 dq.backfill_run 回执（executor='annotate_wb_upstream'）。

用法:
    python3 scripts/annotate_wb_upstream.py [--run-key 2026-09-21] [--limit N] [--batch-size 25]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import socket
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras
import wbgapi

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _env import dsn  # noqa: E402

sys.path.insert(0, str(HERE))
from backfill_wb import span_arg  # noqa: E402  复用窗口参数构造


def latest_run_key(cur, source: str) -> str:
    cur.execute("SELECT max(run_key) FROM coverage_gap WHERE source=%s", (source,))
    return cur.fetchone()[0]


def upstream_max_years(codes: list[str], ours: dict, batch: int, end: int, economies: str = "CHN;USA;WLD", chunk_years: int = 8):
    """按"每个指标从自己库内最新年往前一点"的窄窗口分组拉取。

    不要对全部指标用一个大窗口：37 年 × 全经济体的分页量会让 wbgapi 长时间无响应
    （实测卡死且 socket 超时挡不住持续到达的分页）。窄窗口 + 8 年分块与执行器同一思路。
    """
    out: dict[str, int | None] = {}
    groups: dict[int, list[str]] = {}
    for c in codes:
        our = ours.get(c)
        start = max(1960, (our - 1) if our else 1990)
        groups.setdefault(start, []).append(c)

    total = len(codes)
    done = 0
    for start, group in sorted(groups.items()):
        for i in range(0, len(group), batch):
            chunk = group[i : i + batch]
            y = start
            while y <= end:
                span = (y, min(y + chunk_years - 1, end))
                try:
                    for row in wbgapi.data.fetch(
                        series=chunk, economy=economies, time=span_arg(span), skipBlanks=True, db=2
                    ):
                        code = row["series"]
                        year = int(row["time"][2:])
                        if out.get(code) is None or year > out[code]:
                            out[code] = year
                except Exception as exc:  # noqa: BLE001
                    # 未发布年份会返回 XML 而非 JSON（wbgapi 抛 JSON 解析错），按"该窗口无数据"处理
                    if "JSON decoding error" not in str(exc):
                        print(f"  !! 窗口 {span} 批次失败：{type(exc).__name__}: {exc}", flush=True)
                y = span[1] + 1
            for c in chunk:
                out.setdefault(c, None)
            done += len(chunk)
            print(f"  ...已核 {done}/{total} 个指标（窗口起点 {start}）", flush=True)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="world_bank")
    ap.add_argument("--run-key", default=None)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--batch-size", type=int, default=25)
    ap.add_argument("--start-year", type=int, default=1990)
    ap.add_argument("--end-year", type=int, default=dt.date.today().year + 1)
    ap.add_argument("--http-timeout", type=int, default=60)
    ap.add_argument("--economies", default="CHN;USA;WLD",
                    help="探测用经济体（默认 CHN;USA;WLD；判断上游是否新发数据用代表经济体即可，全经济体分页太慢）")
    args = ap.parse_args()
    wbgapi.get_options["timeout"] = args.http_timeout  # requests 层超时（socket 默认值对它无效）
    socket.setdefaulttimeout(args.http_timeout)

    dq = psycopg2.connect(dsn("FD_DQ_PG_URL"))
    cur = dq.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    run_key = args.run_key or latest_run_key(cur, args.source)
    if not run_key:
        print("没有可归因的扫描报告", file=sys.stderr)
        return 2
    cur.execute(
        """SELECT key_space, last_period, detail FROM coverage_gap
           WHERE run_key=%s AND source=%s AND check_kind='staleness' ORDER BY key_space""",
        (run_key, args.source),
    )
    gaps = cur.fetchall()
    if args.limit:
        gaps = gaps[: args.limit]
    print(f"归因对象：run_key={run_key} source={args.source} staleness 缺口 {len(gaps)} 条")

    codes, ours = [], {}
    for g in gaps:
        code = g["key_space"].split("=", 1)[-1]
        codes.append(code)
        ours[code] = int(g["last_period"]) if g["last_period"] and g["last_period"].isdigit() else None

    ups = upstream_max_years(codes, ours, args.batch_size, args.end_year, args.economies)

    limited = fillable = unknown = 0
    for code in codes:
        up, our = ups.get(code), ours.get(code)
        if up is None:
            verdict, unknown = "probe_no_data", unknown + 1
        elif our is not None and up <= our:
            verdict, limited = "upstream_limited", limited + 1
        else:
            verdict, fillable = "fillable", fillable + 1
        cur.execute(
            """UPDATE coverage_gap
                  SET detail = coalesce(detail,'{}'::jsonb)
                               || jsonb_build_object('upstream_max_year', %s, 'verdict', %s,
                                                     'checked_at', now()::text),
                      updated_at = now()
                WHERE run_key=%s AND source=%s AND check_kind='staleness' AND key_space=%s""",
            (up, verdict, run_key, args.source, f"indicator_code={code}"),
        )

    print(f"归因结果：上游停发 {limited}｜可补齐 {fillable}｜探测经济体无数据 {unknown}（指标可能只覆盖特定国家，需人工判定）")
    dq.commit()
    dc = dq.cursor()
    dc.execute(
        """INSERT INTO backfill_run (run_key, source, executor, mode, started_at, finished_at,
                                     status, written_rows, receipt, note)
           VALUES (%s,%s,'annotate_wb_upstream','annotate', now(), now(), 'success', %s, %s, %s)""",
        (
            run_key,
            args.source,
            fillable,
            json.dumps(
                {"gaps": len(codes), "upstream_limited": limited, "fillable": fillable, "probe_no_data": unknown},
                ensure_ascii=False,
            ),
            f"上游归因完成：可补齐 {fillable} 个（可用 --indicators 定向跑）",
        ),
    )
    dq.commit()
    return 0


if __name__ == "__main__":
    sys.exit(main())
