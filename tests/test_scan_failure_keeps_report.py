"""扫描失败不许抹掉上一轮报告——README 就是这么承诺的。

`scan_source` 把整个巡检包在 try/except 里，异常时只把 `status` 置成 `"failed"`，
然后**照常**走写报告那段：`DELETE FROM coverage_gap WHERE run_key=%s AND source=%s`，
再把 `all_gaps`（失败时通常只攒了一半甚至空）写回去。

于是同一个 `run_key` 的第二次扫描只要断一次网，上一轮那批 gap 行就被删掉，
看板显示"没有缺口"——`scripts/coverage_scan.py` 的 README 原文写的是
"非零 = 扫描本身失败（记录日志，不破坏上一轮报告）"。

修法：只有 `status == "success"` 才写 `coverage_gap`；失败时只落 `scan_run`。

真实签名：`scan_source(baseline, run_key, conn_dq, verbose)`——`conn_dq` 由调用方传入，
源侧连接由函数自己 `psycopg2.connect(dsn(...))` 建。本文件把 `psycopg2.connect`
换成一个会抛异常的假对象来模拟"断网"，再断言那条 DELETE 根本没被执行。
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

import coverage_scan  # noqa: E402


class _Cur:
    """记录执行过的 SQL；select 类查询回放预设结果。"""

    def __init__(self, rows=None):
        self.sqls: list[str] = []
        self._rows = rows or []

    def execute(self, sql, params=None):
        self.sqls.append(" ".join(sql.split()))
        return self

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass


class _Conn:
    def __init__(self, cur):
        self._cur = cur
        self.autocommit = False
        self.closed = False
        self.commits = 0

    def cursor(self):
        return self._cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def _sqls_after_failure(monkeypatch):
    """让源侧连接在巡检中途断掉，返回 report 连接收到的全部 SQL。"""
    report_cur = _Cur()
    report_conn = _Conn(report_cur)

    class _Boom(Exception):
        pass

    def fake_connect(*a, **kw):
        raise _Boom("connection reset by peer")

    monkeypatch.setattr(coverage_scan.psycopg2, "connect", fake_connect)
    monkeypatch.setattr(coverage_scan, "dsn", lambda env: "postgresql://x/y", raising=False)
    monkeypatch.setitem(coverage_scan.KINDS, "staleness",
                        lambda cur, check: (_ for _ in ()).throw(_Boom("boom")))

    baseline = {"source": "world_bank", "db_env": "X", "checks": [{"kind": "staleness", "table": "t"}]}
    coverage_scan.scan_source(baseline, "2026-11-08", report_conn, False)
    return report_cur.sqls


def test_failed_scan_does_not_delete_the_previous_report(monkeypatch):
    """断网导致扫描失败时，绝不能删掉上一轮的 coverage_gap。"""
    sqls = _sqls_after_failure(monkeypatch)
    deletes = [s for s in sqls if s.startswith("DELETE FROM coverage_gap")]
    assert not deletes, (
        "扫描失败后仍然执行了 DELETE —— 上一轮报告被抹掉，看板会显示「没有缺口」。"
        f"实际执行的语句：{deletes}"
    )


def test_failed_scan_still_records_the_failed_run(monkeypatch):
    """失败本身要留痕（scan_run 里 status=failed），只是不能动 coverage_gap。"""
    sqls = _sqls_after_failure(monkeypatch)
    assert any("scan_run" in s for s in sqls), f"失败没有写 scan_run：{sqls}"
