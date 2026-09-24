#!/usr/bin/env python3
"""World Bank 刷新执行器（OpenSpec change `data-coverage-backfill` 任务 2.1–2.3）。

拉取 wbgapi（WDI, db=2）→ 幂等 upsert 进 Windows PG `world_bank` 库的
`public.observation`，并补齐 `public.indicator` 维度；每次执行写一条
`dq.public.backfill_run` 回执。

用法
----
  python3 scripts/backfill_wb.py --mode rolling                       # 全指标 × 最近 3 年
  python3 scripts/backfill_wb.py --mode rolling --years 5 --limit 20  # 冒烟/抽样
  python3 scripts/backfill_wb.py --mode rolling --indicators NY.GDP.PCAP.CD --years 3
  python3 scripts/backfill_wb.py --mode full                          # 全量 1,499 指标（手动窗口，可断点续跑）
  python3 scripts/backfill_wb.py --mode full --restart                # 忽略断点，重头再来

契约
----
- 幂等：`ON CONFLICT (country_code, indicator_code, year) DO UPDATE SET value=EXCLUDED.value
  WHERE observation.value IS DISTINCT FROM EXCLUDED.value`，重复执行行数不变、值以最新为准。
- 断点：`.state/wb_backfill.json` 的 `done_indicators` 只在单个指标成功落库（COMMIT 完成）后推进；
  重跑跳过已完成指标，`--restart` 忽略状态。state 里 mode/窗口 与本次不一致时视为失效（重新拉）。
- 上游无数据（指标停发、该年无观测）不算失败：记入 `no_data_indicators`，回执如实标注。
  请求失败（重试 3 次仍失败）与落库失败算失败：回执 status='failed' + note 记原因 + 退出码 1。
- 只写非空值（与目标表现状一致：`observation.value` 现有 0 个 NULL），不会把已有值改成 NULL。
- 不打印任何凭据（DSN/口令）。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import socket
import sys
import time
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import wbgapi  # noqa: E402
import requests  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402

from _wb_helpers import PgConnError, brief, chunks, connect, decode_pg_error, with_retries  # noqa: E402

# ---------------------------------------------------------------- 常量

SOURCE = "world_bank"
EXECUTOR = "backfill_wb"
WB_DB = 2                      # World Bank 数据源 2 = WDI（indicator.category 现值为 'wdi'）
WB_CATEGORY = "wdi"            # 新增指标时的 category 默认值
WB_SOURCE_NAME = "World Bank"  # 新增指标时的 source 默认值
TARGET_SCHEMA = "public"
OBS_TABLE = f"{TARGET_SCHEMA}.observation"
IND_TABLE = f"{TARGET_SCHEMA}.indicator"

STATE_PATH = HERE.parent / ".state" / "wb_backfill.json"
YEAR_RE = re.compile(r"(\d{3,4})")
MIN_YEAR = 1900
DEFAULT_START_YEAR = 1960      # WDI 最早年份（目标库 observation 实测也是 1960 起）
MAX_BATCH = 50                 # 单批指标数上限（规格：每批 ≤50）
CONSECUTIVE_DB_ERROR_LIMIT = 3  # 目标库连续写失败多少次就中止（大概率连接坏了）

UPSERT_OBS_SQL = f"""
INSERT INTO {OBS_TABLE} (country_code, indicator_code, year, value)
VALUES %s
ON CONFLICT (country_code, indicator_code, year)
DO UPDATE SET value = EXCLUDED.value
WHERE {OBS_TABLE}.value IS DISTINCT FROM EXCLUDED.value
"""

# 只补空：绝不覆盖已有值（尤其 name_cn —— 现库 1,499 指标里 19 个有人工中文名，
# 而 wbgapi 只给英文名，覆盖就等于把中文名冲成空白/英文）
UPSERT_IND_SQL = f"""
INSERT INTO {IND_TABLE} (code, name_en, source, category)
VALUES %s
ON CONFLICT (code) DO UPDATE SET
  name_en = COALESCE(NULLIF({IND_TABLE}.name_en, ''), EXCLUDED.name_en),
  source  = COALESCE(NULLIF({IND_TABLE}.source,  ''), EXCLUDED.source)
"""


# ---------------------------------------------------------------- 日志 / 状态


def log(msg: str) -> None:
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


class State:
    """断点状态文件：`{mode, years, window, done_indicators, started_at, updated_at, …}`。"""

    def __init__(self, path: Path, mode: str, years, window, batch_size: int, restart: bool):
        self.path = path
        self.mode = mode
        self.years = years
        self.window = list(window) if window else None
        self.now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        self.data = {
            "mode": mode,
            "years": years,
            "window": self.window,
            "batch_size": batch_size,
            "done_indicators": [],
            "failed_indicators": {},
            "no_data_indicators": [],
            "started_at": self.now,
            "updated_at": self.now,
            "runs": 0,
        }
        self._done: set[str] = set()
        self._loaded = False
        self._load(restart)

    def _load(self, restart: bool) -> None:
        if restart:
            log("状态文件：--restart，忽略既有断点")
            return
        if not self.path.exists():
            return
        try:
            prev = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            log(f"状态文件不可读（按空状态继续）：{brief(exc)}")
            return
        if prev.get("mode") != self.mode or prev.get("window") != self.window:
            log(f"状态文件与本次窗口不一致（旧 {prev.get('mode')}/{prev.get('window')} → "
                f"新 {self.mode}/{self.window}），视为失效，不跳过任何指标")
            return
        self.data["done_indicators"] = list(prev.get("done_indicators") or [])
        self.data["failed_indicators"] = dict(prev.get("failed_indicators") or {})
        self.data["no_data_indicators"] = list(prev.get("no_data_indicators") or [])
        self.data["started_at"] = prev.get("started_at") or self.now
        self.data["runs"] = int(prev.get("runs") or 0)
        self._done = set(self.data["done_indicators"])
        self._loaded = True
        log(f"状态文件：已完成 {len(self._done)} 个指标（mode={self.mode}, window={self.window}）")

    @property
    def restored(self) -> bool:
        return self._loaded and bool(self._done)

    def is_done(self, code: str) -> bool:
        return code in self._done

    def mark_done(self, code: str, *, no_data: bool = False) -> None:
        if not self.is_done(code):
            self.data["done_indicators"].append(code)
            self._done.add(code)
        if no_data and code not in self.data["no_data_indicators"]:
            self.data["no_data_indicators"].append(code)
        self.data["failed_indicators"].pop(code, None)

    def mark_failed(self, code: str, reason: str) -> None:
        self.data["failed_indicators"][code] = reason[:400]

    def save(self) -> None:
        self.data["updated_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(tmp, self.path)  # 原子替换：中断也不会留下半个 JSON


# ---------------------------------------------------------------- 参数


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog="backfill_wb.py",
        description="World Bank 刷新执行器：wbgapi → world_bank.observation（幂等 upsert + 回执 + 断点续跑）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("用法\n----\n", 1)[-1],
    )
    p.add_argument("--mode", choices=("rolling", "full"), required=True,
                   help="rolling=只拉最近 --years 年的期间；full=按指标全历史回补")
    p.add_argument("--years", type=int, default=3, help="rolling 模式的年数（默认 3）")
    p.add_argument("--limit", type=int, default=None, help="只处理前 N 个指标（便于冒烟测试）")
    p.add_argument("--indicators", default=None, help="逗号分隔的指标代码，覆盖默认指标集合（全部/前 N 个）")
    p.add_argument("--restart", action="store_true", help="忽略 .state/wb_backfill.json 的断点，重头跑")
    p.add_argument("--batch-size", type=int, default=25,
                   help=f"每批指标数（1–{MAX_BATCH}，默认 25；上限取自规格「每批 ≤50」）")
    p.add_argument("--year-chunk", type=int, default=10,
                   help="单次 API 请求覆盖的年数（默认 10；full 模式靠它把 66 年切成小块，失败重试代价可控）")
    p.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR,
                   help=f"full 模式的起始年（默认 {DEFAULT_START_YEAR}）")
    p.add_argument("--sleep", type=float, default=0.75, help="批间限速秒数（默认 0.75）")
    p.add_argument("--retries", type=int, default=3, help="单次请求失败后的重试次数（默认 3，指数退避 2/4/8s）")
    p.add_argument("--http-timeout", type=int, default=60,
                   help="单次 HTTP 请求的 socket 超时秒数（默认 60；wbgapi 自身不设超时，靠它防挂死）")
    p.add_argument("--run-key", default=None, help="回执 run_key（默认启动日期 YYYY-MM-DD）")
    args = p.parse_args(argv)

    if args.years < 1:
        p.error("--years 必须 ≥ 1")
    if args.batch_size < 1:
        p.error("--batch-size 必须 ≥ 1")
    if args.batch_size > MAX_BATCH:
        log(f"--batch-size {args.batch_size} 超过规格上限 {MAX_BATCH}，按 {MAX_BATCH} 执行")
        args.batch_size = MAX_BATCH
    if args.year_chunk < 1:
        p.error("--year-chunk 必须 ≥ 1")
    args.indicators = [c.strip() for c in (args.indicators or "").split(",") if c.strip()]
    return args


# ---------------------------------------------------------------- 目标库读取


def db_scalar(conn, sql, params=None, default=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else default
    finally:
        cur.close()


def load_selected_indicators(conn, args) -> list[str]:
    """指标集合：--indicators 优先，否则取 world_bank.indicator 全量（再套 --limit）。"""
    if args.indicators:
        codes, seen = [], set()
        for c in args.indicators:
            if c not in seen:
                seen.add(c)
                codes.append(c)
    else:
        cur = conn.cursor()
        cur.execute(f"SELECT code FROM {IND_TABLE} ORDER BY code")
        codes = [r[0] for r in cur.fetchall()]
        cur.close()
        log(f"指标集合：{IND_TABLE} 全量 {len(codes)} 个")
    if args.limit is not None:
        if args.limit < 0:
            raise SystemExit("--limit 不能为负")
        dropped = max(0, len(codes) - args.limit)
        codes = codes[: args.limit]
        if dropped:
            log(f"指标集合：--limit {args.limit} 截断，忽略 {dropped} 个")
    if not codes:
        raise SystemExit("指标集合为空：检查 --indicators / --limit 或 world_bank.indicator 是否有数据")
    return codes


def load_country_codes(conn) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"SELECT code FROM {TARGET_SCHEMA}.country")
    codes = {r[0] for r in cur.fetchall()}
    cur.close()
    return codes


# ---------------------------------------------------------------- 窗口 / 抓取


def resolve_window(args, db_max_year):
    """返回 (mode, years, window)。rolling 的窗口 = [anchor-years+1, anchor]，anchor 取
    「当前年」与「目标库最大年」的较大者（保证最新已有年份一定落在窗口内）。"""
    if args.mode == "full":
        end = max(dt.datetime.now(dt.timezone.utc).year, db_max_year or 0)
        start = min(args.start_year, end)
        return args.mode, None, (start, end)
    if db_max_year:
        log(f"目标库现有最大年：{db_max_year}")
    anchor = max(dt.datetime.now(dt.timezone.utc).year, db_max_year or 0)
    return args.mode, args.years, (anchor - args.years + 1, anchor)


def year_spans(window, chunk: int) -> list[tuple[int, int]]:
    start, end = window
    return [(s, min(s + chunk - 1, end)) for s in range(start, end + 1, chunk)]


def span_arg(span):
    """wbgapi 的 time 参数：闭区间 range（wbgapi 会转成 `start:end`）。"""
    return range(span[0], span[1] + 1)


def upstream_no_data(url: str, timeout: int = 15) -> bool:
    """该 URL 的响应体是否为 WB 的「Data not found」XML 错误（= 该期间上游无数据）。

    WB 对「没有任何匹配行」的请求返回 **HTTP 200 + XML 错误体**（实测 211 字节
    ``<wb:error …><wb:message id="160" key="Data not found.">…``），wbgapi 按 JSON
    解析失败抛 `APIResponseError('JSON decoding error')`。只看异常类型会把「上游无数据」
    误判成「请求失败」——例如窗口锚在当前年（2026）时，该年尚未发布，指标会被错记成失败。
    因此这里回看一次响应体（很小）再下结论；探针本身失败则保守地按「非无数据」处理。
    """
    if not url:
        return False
    try:
        body = requests.get(url, timeout=timeout).text or ""
    except Exception:  # noqa: BLE001 — 探针失败不影响主判定（按请求失败处理）
        return False
    return "<wb:error" in body and "Data not found" in body


class FetchError(RuntimeError):
    """某个年段里部分（或全部）指标抓取失败。

    ``data`` 保存同一批里**已成功抓到**的指标，``failed`` 保存「失败指标 → 原因」。
    拆半重试时前半可能成功、后半失败，两者必须一起带回调用方：否则已抓到的数据被丢掉，
    而没抓到的指标会被当成「该指标所有年段都成功」而推进断点，之后被永久跳过。
    """

    def __init__(self, span, failed, data=None, cause=None):
        codes = sorted(failed)
        head = ", ".join(codes[:3]) + ("…" if len(codes) > 3 else "")
        reason = failed[codes[0]] if codes else brief(cause) if cause else "未知原因"
        super().__init__(f"抓取失败 {len(codes)} 指标 codes={head} "
                         f"years={span[0]}-{span[1]}：{reason}")
        self.codes = codes
        self.failed = dict(failed)
        self.data = dict(data or {})
        self.span = span
        self.cause = cause


def fetch_span(codes, span, args) -> dict:
    """抓一批指标 × 一个年段，返回 {code: [(country_code, year, value), …]}。

    用 `wbgapi.data.fetch()`（逐行生成器）而不是 `data.DataFrame()`：实测同一份数据
    DataFrame 要 284s（它逐格 df.loc 赋值），fetch 只要 72s，且返回形状正好是
    `{'series','economy','time':'YR2025','value'}`，不需要再从 (economy × YRxxxx) 矩阵拆回长表。
    """

    def call():
        out = defaultdict(list)
        try:
            for row in wbgapi.data.fetch(series=list(codes), economy="all", time=span_arg(span),
                                         skipBlanks=True, db=WB_DB):
                code = row.get("series")
                country = row.get("economy")
                value = row.get("value")
                match = YEAR_RE.search(str(row.get("time") or ""))
                if not code or not country or value is None or match is None:
                    continue
                out[code].append((country, int(match.group(1)), float(value)))
        except wbgapi.APIResponseError as exc:
            # 一行都没拿到 + 响应体是「Data not found」XML ⇒ 该年段上游无数据，不是失败
            # （也不该重试：这是确定性结果，重试只是白等 2/4/8s）
            if not out and upstream_no_data(getattr(exc, "url", "") or ""):
                log(f"    上游无数据（{len(codes)} 指标 × {span[0]}-{span[1]}）："
                    f"WB 返回 XML «Data not found»，按空结果处理")
                return out
            raise
        return out

    label = f"抓取 {len(codes)} 指标 × {span[0]}-{span[1]}"
    try:
        return with_retries(call, retries=args.retries, label=label, log=log)
    except Exception as exc:  # noqa: BLE001
        if len(codes) > 1:
            # 整批失败：对半拆开重来，避免一个坏指标连坐整批（拆到单指标仍失败才算失败）
            log(f"    ! {label} 整批失败，拆半重试：{brief(exc)}")
            mid = len(codes) // 2
            merged, failed = {}, {}
            for part in (codes[:mid], codes[mid:]):
                try:
                    merged.update(fetch_span(part, span, args))
                except FetchError as sub:
                    # 前半失败不能吞掉后半：合并「已抓到的数据」与「失败清单」一起带回
                    merged.update(sub.data)
                    failed.update(sub.failed)
            if failed:
                raise FetchError(span, failed, data=merged, cause=exc) from exc
            return merged
        raise FetchError(span, {list(codes)[0]: brief(exc)}, cause=exc) from exc


# ---------------------------------------------------------------- 落库


def upsert_indicator_meta(conn, codes) -> int:
    """补齐 indicator 维度：只填空值，绝不覆盖已有 name_cn/topic/unit/category。"""
    names = {}
    try:
        for item in wbgapi.series.info(list(codes), db=WB_DB).items:
            if item.get("value"):
                names[item["id"]] = item["value"]
    except Exception as exc:  # noqa: BLE001 — 元数据拿不到不影响观测落库
        log(f"    ! 指标元数据（series.info）拉取失败，跳过维度补齐：{brief(exc)}")
        return 0
    rows = [(c, names.get(c), WB_SOURCE_NAME, WB_CATEGORY) for c in codes]
    cur = conn.cursor()
    try:
        execute_values(cur, UPSERT_IND_SQL, rows, template="(%s, %s, %s, %s)", page_size=500)
        affected = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    finally:
        cur.close()
    return affected


def upsert_observations(conn, code, rows) -> int:
    """单指标落库并提交；返回真正写入/更新的行数（值未变的冲突行不计）。"""
    if not rows:
        return 0
    payload = [(country, code, year, value) for (country, year, value) in rows]
    cur = conn.cursor()
    try:
        # execute_values 的 cursor.rowcount 只是最后一页的结果，用 RETURNING 汇总真实影响行数
        result = execute_values(cur, UPSERT_OBS_SQL + " RETURNING 1", payload,
                                template="(%s, %s, %s, %s)", page_size=1000, fetch=True)
    finally:
        cur.close()
    return len(result or ())


def stale_summary(conn, codes, reference_year, sample_size=20):
    """复核口径之一：本次处理过的指标里，最大年份仍落后全库最新年的有哪些（上游未发布/停发）。"""
    if not codes or not reference_year:
        return {"reference_year": reference_year, "count": None, "sample": []}
    cur = conn.cursor()
    try:
        cur.execute("SET LOCAL statement_timeout = '240s'")
        cur.execute(
            f"""SELECT indicator_code, max(year) AS max_year FROM {OBS_TABLE}
                WHERE indicator_code = ANY(%s) GROUP BY 1 HAVING max(year) < %s ORDER BY 2, 1""",
            (list(codes), reference_year),
        )
        rows = cur.fetchall()
    except Exception as exc:  # noqa: BLE001 — 统计失败不能让整轮回执变失败
        conn.rollback()
        return {"reference_year": reference_year, "count": None, "sample": [],
                "error": brief(exc)}
    finally:
        cur.close()
    conn.rollback()
    return {"reference_year": reference_year, "count": len(rows),
            "sample": [r[0] for r in rows[:sample_size]],
            "truncated": len(rows) > sample_size}


# ---------------------------------------------------------------- 回执


def open_receipt(conn, run_key, mode, note):
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO backfill_run (run_key, source, executor, mode, status, note)
               VALUES (%s, %s, %s, %s, 'running', %s) RETURNING id""",
            (run_key, SOURCE, EXECUTOR, mode, note),
        )
        return cur.fetchone()[0]
    finally:
        cur.close()


def close_receipt(conn, receipt_id, status, written_rows, receipt, note):
    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE backfill_run SET finished_at = now(), status = %s, written_rows = %s,
                   receipt = %s, note = %s WHERE id = %s""",
            (status, written_rows, json.dumps(receipt, ensure_ascii=False, default=str), note, receipt_id),
        )
    finally:
        cur.close()


# ---------------------------------------------------------------- 主流程


def run(args) -> int:
    started = time.time()
    run_key = args.run_key or dt.date.today().isoformat()
    # 先建回执通道：即使目标库连不上，也要在 dq.backfill_run 留下 failed 记录
    dq = connect("FD_DQ_PG_URL", autocommit=True)
    receipt_id = None
    status, note = "failed", ""
    written_rows = 0
    receipt: dict = {}
    target = None

    try:
        receipt_id = open_receipt(dq, run_key, args.mode,
                                  f"启动：--mode {args.mode} --years {args.years}"
                                  + (f" --limit {args.limit}" if args.limit is not None else ""))
        target = connect("FD_WB_PG_URL")
        db_max_year = db_scalar(target, f"SELECT max(year) FROM {OBS_TABLE}", default=None)
        mode, years, window = resolve_window(args, db_max_year)
        if mode == "full" and args.years != 3:
            log("提示：full 模式忽略 --years（窗口由 --start-year 决定）")
        state = State(STATE_PATH, mode, years, window, args.batch_size, args.restart)
        state.data["runs"] = int(state.data.get("runs") or 0) + 1

        codes = load_selected_indicators(target, args)
        skipped_codes = [c for c in codes if state.is_done(c)]
        todo = [c for c in codes if not state.is_done(c)]
        log(f"模式 {mode}｜窗口 {window[0]}-{window[1]}｜选中 {len(codes)} 指标｜"
            f"断点跳过 {len(skipped_codes)}｜待处理 {len(todo)}")

        receipt.update({
            "run_key": run_key, "mode": mode, "years": years,
            "window": {"start": window[0], "end": window[1]},
            "batch_size": args.batch_size, "restart": bool(args.restart),
            "state_file": str(STATE_PATH),
            "indicators_selected": len(codes),
            "indicators_skipped_by_checkpoint": len(skipped_codes),
            "skipped_sample": skipped_codes[:20],
        })

        countries = load_country_codes(target)
        spans = year_spans(window, args.year_chunk)
        receipt["year_chunks"] = [[s[0], s[1]] for s in spans]
        per_indicator: dict[str, dict] = {}
        totals = {"fetched": 0, "upserted": 0, "affected": 0}
        failed: dict[str, str] = {}
        no_data: list[str] = []
        unknown_countries: dict[str, int] = defaultdict(int)
        consecutive_db_errors = 0

        for batch_no, batch in enumerate(chunks(todo, args.batch_size), start=1):
            log(f"批次 {batch_no}：{len(batch)} 指标（{batch[0]} … {batch[-1]}）")
            progress = {c: {"fetched": 0, "upserted": 0, "affected": 0, "spans_ok": 0,
                            "spans_failed": 0, "t0": time.time()} for c in batch}
            try:
                changed = upsert_indicator_meta(target, batch)
                target.commit()
                if changed:
                    log(f"  维度 indicator：补齐/新增 {changed} 行")
            except Exception as exc:  # noqa: BLE001
                target.rollback()
                log(f"  ! 维度补齐失败（不阻塞观测落库）：{brief(exc)}")

            for span in spans:
                if len(spans) > 1:
                    log(f"  年段 {span[0]}-{span[1]}")
                data, failures = {}, {}
                try:
                    data = fetch_span(batch, span, args)
                except FetchError as exc:
                    data = exc.data          # 同批里已成功的指标照常落库，不丢数据
                    failures = exc.failed
                    log(f"  ! {brief(exc)}")

                for code in batch:
                    info = progress[code]
                    if code in failures:
                        info["spans_failed"] += 1
                        info.setdefault("error", failures[code])
                        continue
                    rows = []
                    for country, year, value in data.get(code, ()):
                        if country not in countries:
                            unknown_countries[country] += 1
                            continue
                        if not (MIN_YEAR <= year <= window[1] + 1):
                            continue
                        rows.append((country, year, value))
                    info["fetched"] += len(rows)
                    if not rows:
                        info["spans_ok"] += 1
                        continue
                    try:
                        affected = upsert_observations(target, code, rows)
                        target.commit()
                        info["upserted"] += len(rows)
                        info["affected"] += affected
                        info["spans_ok"] += 1
                        consecutive_db_errors = 0
                    except Exception as exc:  # noqa: BLE001
                        target.rollback()
                        info["spans_failed"] += 1
                        info.setdefault("error", f"落库失败：{brief(exc)}")
                        consecutive_db_errors += 1
                        log(f"  ! {code} 落库失败：{brief(exc)}")
                        if consecutive_db_errors >= CONSECUTIVE_DB_ERROR_LIMIT:
                            raise RuntimeError(
                                f"目标库连续 {consecutive_db_errors} 次写入失败，中止本轮：{brief(exc)}")

            # 断点推进：上面每个指标都已单独 COMMIT，这里逐指标落盘（断点只在成功落库后前进）
            for code in batch:
                info = progress[code]
                info["elapsed_s"] = round(time.time() - info.pop("t0"), 1)
                per_indicator[code] = info
                totals["fetched"] += info["fetched"]
                totals["upserted"] += info["upserted"]
                totals["affected"] += info["affected"]
                written_rows += info["affected"]
                if info["spans_failed"]:
                    reason = info.get("error") or "抓取失败"
                    failed[code] = reason
                    state.mark_failed(code, reason)
                else:
                    if info["upserted"] == 0:
                        no_data.append(code)
                    state.mark_done(code, no_data=info["upserted"] == 0)
                state.save()
                log(f"  · {code}: fetched={info['fetched']} upserted={info['upserted']} "
                    f"affected={info['affected']}" + ("" if not info["spans_failed"] else " [FAILED]"))
            # 每批都要落一次回执快照：中途被杀也能看到进度
            receipt.update({"indicators_processed": len(per_indicator), "totals": dict(totals),
                            "indicators_failed": len(failed), "failed": dict(failed),
                            "indicators_no_data": len(no_data)})
            if len(per_indicator) < len(todo):
                time.sleep(max(0.0, args.sleep))

        # 复核口径自检（正式复核由 1.3 的 coverage_scan.py 执行）
        reference_year = db_max_year
        try:
            after_max = db_scalar(target, f"SELECT max(year) FROM {OBS_TABLE}", default=reference_year)
            reference_year = max(reference_year or 0, after_max or 0) or None
        except Exception:  # noqa: BLE001
            target.rollback()
        still = stale_summary(target, sorted(per_indicator), reference_year)

        elapsed = round(time.time() - started, 1)
        status = "failed" if failed else "success"
        if failed:
            note = (f"{len(failed)} 个指标失败：{list(failed)[:5]}；"
                    f"其余 {len(per_indicator) - len(failed)} 个指标已落库")
        else:
            note = (f"{mode} 窗口 {window[0]}-{window[1]} 完成：处理 {len(per_indicator)} 指标"
                    f"（上游无数据 {len(no_data)}），跳过 {len(skipped_codes)}，耗时 {elapsed}s")
        receipt.update({
            "totals": dict(totals),
            "per_indicator": {c: {"fetched": v["fetched"], "upserted": v["upserted"],
                                  "affected": v["affected"], "elapsed_s": v["elapsed_s"]}
                              for c, v in sorted(per_indicator.items())},
            "no_data_sample": no_data[:20],
            "failed": dict(failed),
            "dropped_rows": {"unknown_country_codes": dict(unknown_countries)},
            "stale_after_run": still,
            "elapsed_s": elapsed,
            "next": ("仍落后全库最新年 %s 的指标 %s 个（多为上游停发/该年未发布），"
                     "复核请用 scripts/coverage_scan.py --source world_bank；"
                     "如欲放宽窗口用 --years 5 或 --mode full 重跑"
                     % (reference_year, still.get("count"))),
        })
        log(f"完成：写入/更新 {written_rows} 行（提交 {totals['upserted']} 行），"
            f"无数据 {len(no_data)} 指标，失败 {len(failed)} 指标，耗时 {elapsed}s")
        return 0 if not failed else 1

    except KeyboardInterrupt:
        status, note = "failed", "中断（KeyboardInterrupt）：断点已保留，重跑将从此处继续"
        log(note)
        return 130
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 — 任何未捕获异常：回执记原因 + 非零退出
        status = "failed"
        note = f"执行失败：{brief(exc)}"[:2000]
        log(f"{note}\n{decode_pg_error(exc)}")
        if not isinstance(exc, PgConnError):
            import traceback
            traceback.print_exc()
        return 1
    finally:
        if receipt_id is not None:
            try:
                receipt.setdefault("elapsed_s", round(time.time() - started, 1))
                close_receipt(dq, receipt_id, status, written_rows, receipt,
                              note or ("执行完成" if status == "success" else "执行失败"))
            except Exception as exc:  # noqa: BLE001
                log(f"! 回执写入失败（backfill_run id={receipt_id}）：{brief(exc)}")
        for conn in (target, dq):
            try:
                if conn is not None:
                    conn.close()
            except Exception:  # noqa: BLE001
                pass


def main(argv=None) -> int:
    args = parse_args(argv)
    # wbgapi 走 requests（不是 urllib），且默认不带 timeout：网络挂起会无限等待
    # （实测 full 模式卡死 8 分钟、rolling 种子卡死 10+ 分钟）。
    # socket.setdefaulttimeout 对 requests 无效，必须设 wbgapi.get_options['timeout']；
    # 两者都设以覆盖 wbgapi 内部的 direct requests 调用路径。
    wbgapi.get_options['timeout'] = args.http_timeout
    socket.setdefaulttimeout(args.http_timeout)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
