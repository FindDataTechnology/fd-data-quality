"""缺数据不能报成 missing_count=0——那是"什么都不缺"的意思。

`coverage_scan` 里有两处把"未知/完全没有"写成了 0：

* `gaps_freshness`：整源零行 → `missing_count=0` + `severity=critical`；
* `gaps_staleness`：某组的期间列全 NULL → `lag=None` → 同样 0。

而报告自己按 `ORDER BY missing_count DESC` 列 critical 行（`coverage_scan.py` 末尾），
于是**最坏的情况排在最后**；任何 `sum(missing_count)` 也把它当成完整的源。

审计 04 §5 描述的是这个现象，但它把位置指到了 `gaps_staleness` 的"零行组"——
`GROUP BY` 根本不会产出空组，而"整个实体没有数据"是 `gaps_missing_entity` 的职责，
那里本来就报 `missing_count=1`。真正要修的是 freshness 的零行分支（以及 staleness
里期间列全 NULL 的那个角落）。

本文件用假游标直接喂这两种输入，不需要真 PG（`fd-data-quality` 没有 tests 目录，
也没有 pytest 配置，这是新增的第一份）。
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

import coverage_scan  # noqa: E402


class _Cur:
    """最小假游标：只回放预设结果，并记录执行过的 SQL。"""

    def __init__(self, *, one=None, many=None):
        self._one = one
        self._many = many
        self.sqls: list[str] = []

    def execute(self, sql, params=None):
        self.sqls.append(sql)
        return self

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many or []


# ---------------------------------------------------------------- freshness


def test_a_source_with_zero_rows_is_not_reported_as_nothing_missing():
    """整源零行 = 最坏覆盖状态，不许报 missing_count=0。"""
    cur = _Cur(one=(None, 0))
    gaps = coverage_scan.gaps_freshness(cur, {
        "table": "t", "period_col": "d", "period_type": "date", "max_age_days": 7,
    })
    assert len(gaps) == 1
    g = gaps[0]
    assert g["severity"] == "critical"
    assert g["missing_count"] >= 1, (
        f"零行源被报成 missing_count={g['missing_count']}——"
        "按 missing_count DESC 排序时它会沉到最底下"
    )
    # 仍然保留"为什么是 critical"的可读原因
    assert g["detail"]["reason"] == "no rows in scope"


def test_healthy_source_reports_no_gap():
    """正常源不该产出任何缺口（别把修复做成'全都报缺'）。"""
    from datetime import date, timedelta
    recent = (date.today() - timedelta(days=1)).isoformat()
    cur = _Cur(one=(recent, 100))
    gaps = coverage_scan.gaps_freshness(cur, {
        "table": "t", "period_col": "d", "period_type": "date", "max_age_days": 7,
    })
    assert gaps == []


# ---------------------------------------------------------------- staleness


def test_a_group_with_no_usable_period_is_not_reported_as_nothing_missing():
    """期间列全 NULL 的组：lag 未知，但"缺"是确定存在的。"""
    # 假对象必须 mirror 真实驱动：period_type=date 时 SQL 是 max(period::date)，
    # psycopg2 返回 datetime.date，不是字符串。
    cur = _Cur(many=[("DEU", None), ("USA", dt.date(2024, 3, 1))])
    gaps = coverage_scan.gaps_staleness(cur, {
        "table": "t", "period_col": "d", "key_cols": ["region"],
        "period_type": "date", "allow_lag": 2,
    })
    zero = [g for g in gaps if g["key_space"] == "region=DEU"]
    assert zero, f"DEU 组应当产出缺口，实际 {gaps}"
    assert zero[0]["severity"] == "critical"
    assert zero[0]["missing_count"] >= 1, (
        f"未知 lag 被报成 missing_count={zero[0]['missing_count']}"
    )
    # detail 里仍然如实写 lag 未知，别假装算出来了
    assert zero[0]["detail"]["lag"] is None


def test_staleness_still_skips_groups_within_the_allowance():
    """组间差距在容忍窗内的不报（原有行为不许退化）。"""
    cur = _Cur(many=[("DEU", dt.date(2024, 3, 1)), ("USA", dt.date(2024, 3, 2))])
    gaps = coverage_scan.gaps_staleness(cur, {
        "table": "t", "period_col": "d", "key_cols": ["region"],
        "period_type": "date", "allow_lag": 2,
    })
    assert gaps == []


# ---------------------------------------------------------------- 一致性


def test_absent_entity_uses_the_same_convention():
    """整实体缺失本来就用 1，本次把另两处对齐到同一约定。"""
    cur = _Cur(many=[("CHN",), ("USA",)])
    gaps = coverage_scan.gaps_missing_entity(cur, {
        "table": "t", "entity_col": "region", "expected": ["CHN", "USA", "DEU"],
    })
    assert [g["key_space"] for g in gaps] == ["region=DEU"]
    assert gaps[0]["missing_count"] == 1
