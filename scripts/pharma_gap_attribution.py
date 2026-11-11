#!/usr/bin/env python3
"""医药缺口归因：给 coverage_gap 的每处缺口写上可查询的 verdict（任务 5.4）。

为什么单独一支脚本
------------------
``coverage_scan.py`` 只负责"报缺口"，不负责"解释缺口"——两者混在一起会让解释沉淀成
写死在扫描器里的猜测。既有先例是 ``annotate_wb_upstream.py``（把 verdict 写进
``coverage_gap.detail``），本脚本沿用同一位置与同一风格。

verdict 取值（在既有三值基础上补一个，理由见下）
--------------------------------------------
- ``fillable``          上游有、库里没有 → 可分片补齐
- ``upstream_limited``  上游已停发/未更新 → 补不了
- ``probe_no_data``     探测本身无数据 → 无法判定
- ``not_yet_scheduled`` **本脚本新增**：源已合入且有绿 golden，但尚未进入调度，
  从未跑过 → 缺口是我们自己的排期问题，不是上游问题

为什么必须加第四个值：本批两个医药源的 `row_count=0` 缺口既不是"上游停发"（上游
数据充足，golden 刚跑绿过），也不是"可分片补齐"（没点亮就补不了），更不是"口径
变更"。硬塞进既有三值就是给缺口编一个错的原因，而错的原因会让后续修复走错方向。

判定依据（每条都可复算，不靠人工记忆）
------------------------------------
读 ``crawl_runs``：该 source 从未有 status='ok' 的运行 → ``not_yet_scheduled``。
本脚本只判定这一种情形；其余情形留给 ``annotate_wb_upstream`` 那类探测式归因，
不在这里猜。

用法
----
    python3 scripts/pharma_gap_attribution.py            # 打印判定
    python3 scripts/pharma_gap_attribution.py --apply     # 写进 coverage_gap.detail

需要 ``FD_DQ_PG_URL`` 与 ``FD_CRAWL_DB_URL``。
"""
from __future__ import annotations

import argparse
import json
import sys

import psycopg2

sys.path.insert(0, "scripts")
from _env import dsn  # noqa: E402

PHARMA_SOURCES = ("pharma-clinicaltrials-cn", "pharma-nrdl-national-catalog")
VERDICTS = ("fillable", "upstream_limited", "probe_no_data", "not_yet_scheduled")


def ok_runs(crawl_conn, source: str, unit: str) -> int:
    """该单元在 crawl_runs 里的成功运行次数（按 manifest 的 name，不带 pharma- 前缀）。"""
    cur = crawl_conn.cursor()
    cur.execute(
        "SELECT count(*) FROM crawl_runs WHERE source = %s AND status = 'ok'", (unit,)
    )
    return cur.fetchone()[0]


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)

    crawl = psycopg2.connect(dsn("FD_CRAWL_DB_URL"))
    dq = psycopg2.connect(dsn("FD_DQ_PG_URL"))
    dq.autocommit = False
    cur = dq.cursor()

    cur.execute(
        "SELECT run_key, source, check_kind, key_space, detail FROM coverage_gap"
        " WHERE source = ANY(%s) ORDER BY source, check_kind",
        (list(PHARMA_SOURCES),),
    )
    rows = cur.fetchall()
    if not rows:
        print("没有医药缺口行（先跑 coverage_scan.py --source pharma-* --report）")
        return 0

    updated = 0
    for run_key, source, check_kind, key_space, detail in rows:
        detail = detail or {}
        if check_kind == "executor":
            # report_only 说明行，本身不是缺口，不归因
            continue
        unit = source.removeprefix("pharma-")
        runs = ok_runs(crawl, source, unit)
        if runs == 0:
            verdict, reason = "not_yet_scheduled", (
                f"单元 {unit} 在 crawl_runs 中无 status='ok' 运行；源已合入且有绿 golden，"
                "未进调度。点亮 schedule 后本条应转为 fillable 或消失"
            )
        elif detail.get("rows") == 0:
            verdict, reason = "upstream_limited", (
                f"单元 {unit} 有 {runs} 次成功运行但落地 0 行——需查上游是否改版/拦截"
            )
        else:
            verdict, reason = "fillable", (
                f"单元 {unit} 有 {runs} 次成功运行，落地行数低于基线下限，属可分片补齐"
            )
        assert verdict in VERDICTS
        new_detail = dict(detail, verdict=verdict, verdict_reason=reason)
        print(f"{source:<32} {check_kind:<12} -> {verdict}")
        if args.apply:
            cur.execute(
                "UPDATE coverage_gap SET detail = %s::jsonb, updated_at = now()"
                " WHERE run_key = %s AND source = %s AND check_kind = %s",
                (json.dumps(new_detail, ensure_ascii=False), run_key, source, check_kind),
            )
            updated += 1

    if args.apply:
        dq.commit()
        print(f"APPLIED: 写入 {updated} 条 verdict")
    else:
        dq.rollback()
        print("DRY-RUN（加 --apply 落库）")
    crawl.close()
    dq.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
