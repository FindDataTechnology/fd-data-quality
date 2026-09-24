#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""reload_check.py — 文件驱动重灌契约的校验器（OpenSpec change data-coverage-backfill task 4.3）。

对象：三个"源文件在外部、loader 不在仓库"的源
  - gta_panel        国泰安 dta  → gta_panel.panel_c0 / panel_c1 / panel_c2（+ variables 字典）
  - yearbook_catalog 年鉴解析产物 → yearbook_catalog.indicator_values
  - china_city_panel 城市统计资料 → china_city_panel.panel_raw（+ city_dim / variable_catalog）

契约见同目录上级的 CONTRACTS.md（每个源的输入文件契约 / 校验清单 / 幂等键 / 回执 / 仅报告约定）。

本脚本**只读**：
  - 默认 dry-run（--dry-run 是默认行为，加不加都一样）；
  - 绝不写任何表（--apply 会被明确拒绝，见 docs/CONTRACTS.md §回执）；
  - --db-baseline 只对既有表做 SELECT，把"期望形状"落到 .state/reload_baseline_<source>.json。

用法
----
# 1) 从既有库推导期望形状（真连 Windows PG，只读；需 fd-data-quality/.env.local 的 FD_ANALYSIS_PG_URL）
python3 scripts/reload_check.py --source gta_panel --db-baseline
python3 scripts/reload_check.py --source all --db-baseline

# 2) 校验待重灌的源文件（默认 dry-run，不写任何表）
python3 scripts/reload_check.py --source gta_panel --file /path/FS_Combas.dta          # c0 键=(id_org,year)
python3 scripts/reload_check.py --source china_city_panel --file /path/城市面板.xlsx --sheet 原始数据
python3 scripts/reload_check.py --source yearbook_catalog --file /path/2024版.csv --edition-year 2024

# 3) 源文件不在手 = 仅报告（不报成功）
python3 scripts/reload_check.py --source yearbook_catalog

退出码
------
0  校验通过（可能存在 WARN）
1  校验失败（FAIL：行数/键/期间/字段等不满足契约）→ 不得重灌
2  用法或环境错误（参数非法、DSN 缺失、连库失败、--apply 被拒）
3  源文件缺失 → "待源文件 / 仅报告"，不产生任何"已补齐"结论

幂等 upsert 键（与 CONTRACTS.md 一致）
--------------------------------------
gta_panel        c0 = (id_org, year)；c1/c2 = (id, year)（id = id_org::bigint，既有库实测 100% 成立）
yearbook_catalog = (indicator_id, table_id, region, data_year, edition_year, dims 归一化)，region/dims 用空串/
                   空对象哨兵；既有表在该键上不唯一（4.35 行/键），重灌按版本整体替换最稳
china_city_panel = (city_code, year)，严格网格 297×25=7,425
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import pathlib
import re
import sys
import unicodedata
from datetime import datetime, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
try:
    from _env import dsn  # 复用既有 DSN 加载（fd-data-quality/.env.local）
except Exception:  # pragma: no cover - _env.py 缺失时的退化路径
    def dsn(name: str) -> str:  # type: ignore
        v = os.environ.get(name)
        if not v:
            raise SystemExit(f"缺少环境变量 {name}（本地见 fd-data-quality/.env.local，生产由 Secret 注入）")
        return v

ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_STATE_DIR = ROOT / ".state"
ANALYSIS_DB_ENV = "FD_ANALYSIS_PG_URL"
DQ_DB_ENV = "FD_DQ_PG_URL"
SCHEMA_VERSION = 1
CHUNK_SIZE = 20000
MAX_KEY_SET = 3_000_000          # 键集合上限（超过则唯一性检查降级为"部分"）
DUP_EXAMPLE_WINDOW = 20000       # 保留前 N 行的原始键，用于重复样例展示
VALUE_SAMPLE_MAX = 5000          # 抽样对比保留的文件行数上限
CURRENT_YEAR = 2026              # 契约窗上界（与 baselines/*.yaml 保持一致，2026-09-21 口径）

# --------------------------------------------------------------------------------------
# 契约默认值（无 .state baseline 时的兜底；数字来自 2026-09-21 对既有库的实测，见 CONTRACTS.md）
# --------------------------------------------------------------------------------------
CONTRACT: dict[str, dict] = {
    "gta_panel": {
        "db": "gta_panel",
        "objects": ["panel_c0", "panel_c1", "panel_c2"],
        "exts": [".dta", ".csv", ".parquet"],
        "reject_exts": {".pdf": "国泰安 PDF 不是重灌输入，请先在本地导出 dta（loader 不在仓库，见 CONTRACTS.md §GTA）"},
        "required_cols": ["id", "year"],
        "key": ["id", "year"],
        # 幂等 upsert 键按表区分：c0 有 id_org（6 位证券代码 text）→ 用源文件里的真实身份做键；
        # c1/c2 没有 id_org → 只能用 id。既有库实测 id = id_org::bigint 100% 成立、三表 (id,year)
        # 全连接 = 96,029 行，所以 id 就是 id_org 的整数形式，loaders 必须保住这个映射。
        "keys_by_table": {
            "panel_c0": ["id_org", "year"],
            "panel_c1": ["id", "year"],
            "panel_c2": ["id", "year"],
        },
        "required_cols_by_table": {
            "panel_c0": ["year"],          # id_org / id 至少有一个，见下方专门检查
            "panel_c1": ["id", "year"],
            "panel_c2": ["id", "year"],
        },
        "period_col": "year",
        "period_range": [1990, 2025],
        "periods_expected": None,        # baseline 推导
        "row_range": [91227, 153646],    # 单表（实测 96,029 × [0.95, 1.6]；baseline 会按实测行数刷新）
        "strict_grid": False,
        "entity_col": "id",
        "entity_count_range": [5900, 6000],   # 实测 5,939 家
        "receipt_executor": "reload_gta_panel",
        "note": "三表同网格（96,029 行 × 1990–2025）；c0 键=(id_org,year)，c1/c2 键=(id,year)",
    },
    "china_city_panel": {
        "db": "china_city_panel",
        "objects": ["panel_raw"],
        "exts": [".xlsx", ".xlsm", ".xls", ".csv"],
        "reject_exts": {".pdf": "PDF 不是重灌输入，请先转 xlsx/xls/csv"},
        "required_cols": ["city_code", "year"],
        "key": ["city_code", "year"],
        "period_col": "year",
        "period_range": [2000, 2024],
        "periods_expected": list(range(2000, 2025)),
        "row_range": [7425, 7425],       # 严格 297 城 × 25 年
        "strict_grid": True,
        "entity_col": "city_code",
        "entity_count_range": [297, 297],
        "receipt_executor": "reload_china_city_panel",
        "note": "严格网格 297×25=7,425 行；209 个变量列 v001..v209 由 variable_catalog 定义",
    },
    "yearbook_catalog": {
        "db": "yearbook_catalog",
        "objects": ["indicator_values"],
        "exts": [".csv", ".xlsx", ".xls", ".htm", ".html", ".parquet"],
        "reject_exts": {".pdf": "PDF 不入重灌（既有 ingest_files 里 .pdf 一律 skipped_pdf），请先解析成 xls/xlsx/htm/csv"},
        "required_cols": ["indicator_id", "table_id", "region", "data_year", "edition_year", "value"],
        "key": ["indicator_id", "table_id", "region", "data_year", "edition_year", "dims"],
        "period_col": "edition_year",
        "period_range": [1980, 2026],
        "periods_expected": None,
        "row_range": [300000, 1500000],  # 无 baseline 时的保守兜底；有 baseline 时按单版中位推导（见 baseline_for_source）
        "hard_floor": 100000,            # 低于此值 = 半版（2021 版实测 115,963 即半版；2000 版 58,567 为早期小版）
        "strict_grid": False,
        "entity_col": "indicator_id",
        "entity_count_range": [1000, 12000],
        # 年鉴天生「一版一文件」：单版文件不可能覆盖全部版本，期间覆盖检查必须切到单版模式
        # （--edition-year 显式指定，或文件里只有一个 edition_year 时自动切换）。
        "single_edition_ok": True,
        "receipt_executor": "reload_yearbook_catalog",
        "note": "单版值数量级 30 万–150 万；edition_year 必须落在 1980–2026（既有库有 2026–2100 越界脏值）；"
                "既有表在自然键上不唯一（见 dedup_note），重灌按版本整体替换或先判重",
        "dedup_note": "既有 indicator_values 24,332,692 行只有 5,589,074 个自然键（≈4.35 行/键）；"
                      "2024 版 813,749 行只有 211,253 个唯一键，且去掉表头脏值后仍约 4.5 倍重复——"
                      "说明解析器把同一 sheet 的不同单元格压到了同一个 (indicator_id,table_id,region,data_year,dims) 上，"
                      "该键不足以区分行。文件侧必须键唯一（本脚本 FAIL），库侧按版本整体替换最稳。",
    },
}

# 年鉴 region 列里已知的表头脏值（实测 '地  区' 54.6 万行 / 全库 68.7 万 NULL region）
REGION_HEADER_WORDS = {
    "地区", "地　区", "行业", "指标", "项目", "年份", "单位", "名称", "栏目",
    "行　业", "指　标", "项　目", "年　份", "单　位", "名　称", "栏　目",
}
YEARBOOK_DIMS_KEYS = ["industry", "admin_region", "registration", "reg_type", "scale"]
# 键里这些列按整数解析（id_org 是 text 证券代码，不在此列）
INT_KEY_COLS = {"id", "year", "city_code", "data_year", "edition_year", "indicator_id", "table_id"}

LEVEL_PASS, LEVEL_WARN, LEVEL_FAIL = "PASS", "WARN", "FAIL"


# --------------------------------------------------------------------------------------
# 通用工具
# --------------------------------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def sha256_file(path: pathlib.Path, buf_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(buf_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def is_null(v) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and v != v:  # NaN
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    try:  # numpy nan / pandas NA
        import math
        return isinstance(v, float) and math.isnan(v)
    except Exception:
        return False


def norm_text(v) -> str:
    """地区/维度文本归一化：NFKC → 空白压成单空格 → strip。"""
    if v is None:
        return ""
    s = unicodedata.normalize("NFKC", str(v))
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_region_key(v) -> str:
    """地区归一化（与 CONTRACTS.md 一致）：NULL/空 → 空串哨兵，否则 NFKC + 空白压单空格。"""
    s = norm_text(v)
    return s


def norm_dims_key(v) -> str:
    """dims 归一化串：dict/list → jsonb 语义（键排序、去 null、无多余空格）；空 → '{}'。

    与 SQL 侧 `jsonb_strip_nulls(coalesce(dims,'{}'::jsonb))::text` 对齐。
    """
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return "{}"
    obj = v
    if isinstance(v, str):
        try:
            obj = json.loads(v)
        except Exception:
            return norm_text(v)
    if isinstance(obj, dict):
        obj = {k: v2 for k, v2 in obj.items() if v2 is not None and v2 != ""}
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def to_int(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        if v != v:
            return None
        return int(v)
    s = str(v).strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def to_float(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return None if v != v else float(v)
    s = str(v).strip().replace(",", "")
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None


def key_digest(parts: list) -> bytes:
    return hashlib.blake2b("\x1f".join("" if p is None else str(p) for p in parts).encode("utf-8"),
                           digest_size=8).digest()


class Result:
    __slots__ = ("level", "code", "title", "detail")

    def __init__(self, level: str, code: str, title: str, detail: str = ""):
        self.level = level
        self.code = code
        self.title = title
        self.detail = detail

    def to_dict(self) -> dict:
        return {"level": self.level, "code": self.code, "title": self.title, "detail": self.detail}


class Findings:
    def __init__(self):
        self.items: list[Result] = []

    def add(self, level: str, code: str, title: str, detail: str = "") -> Result:
        r = Result(level, code, title, detail)
        self.items.append(r)
        return r

    def fail(self, code, title, detail=""):
        return self.add(LEVEL_FAIL, code, title, detail)

    def warn(self, code, title, detail=""):
        return self.add(LEVEL_WARN, code, title, detail)

    def ok(self, code, title, detail=""):
        return self.add(LEVEL_PASS, code, title, detail)

    def counts(self) -> dict:
        c = {LEVEL_PASS: 0, LEVEL_WARN: 0, LEVEL_FAIL: 0}
        for i in self.items:
            c[i.level] = c.get(i.level, 0) + 1
        return c

    def has_fail(self) -> bool:
        return any(i.level == LEVEL_FAIL for i in self.items)


# --------------------------------------------------------------------------------------
# 文件读取层（流式；依赖缺失给安装提示而不是崩溃）
# --------------------------------------------------------------------------------------
class Chunk:
    """一列一序列的块（pandas 后端给 numpy 数组，csv 后端给 list）。"""

    __slots__ = ("columns", "n", "_data", "_cache")

    def __init__(self, columns: list[str], data: dict, n: int):
        self.columns = columns
        self._data = data
        self.n = n
        self._cache: dict[str, list] = {}

    def values(self, name: str) -> list:
        if name in self._cache:
            return self._cache[name]
        v = self._data.get(name)
        if v is None:
            out: list = [None] * self.n
        elif isinstance(v, list):
            out = v
        else:  # numpy array / pandas Series
            out = list(v)
        if len(out) < self.n:
            out = out + [None] * (self.n - len(out))
        self._cache[name] = out
        return out

    def has(self, name: str) -> bool:
        return name in self._data


class Stream:
    def __init__(self, fmt: str, columns: list[str], chunks, n_hint=None, notes=None, sheet=None):
        self.fmt = fmt
        self.columns = columns
        self._chunks = chunks
        self.n_hint = n_hint
        self.notes = notes or []
        self.sheet = sheet

    def __iter__(self):
        return iter(self._chunks)


def _pandas():
    try:
        import pandas as pd  # noqa: F401
        return pd
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "缺少 pandas（读 dta/xlsx/xls/htm 需要）：\n"
            "  pip install -r fd-data-quality/requirements.txt   # 或 pip install pandas openpyxl xlrd lxml"
        ) from e


def _iter_csv(path: pathlib.Path, encoding: str, chunk_size: int):
    with path.open("r", encoding=encoding, newline="") as f:
        sample = f.read(65536)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        except csv.Error:
            dialect = csv.excel
        reader = csv.reader(f, dialect)
        try:
            header = next(reader)
        except StopIteration:
            raise RuntimeError(f"CSV 为空（无表头）：{path}")
        cols = [c.strip().lstrip("\ufeff") for c in header]
        data = {c: [] for c in cols}
        n = 0
        for row in reader:
            if len(row) != len(cols):
                row = (row + [""] * len(cols))[: len(cols)]
            for c, v in zip(cols, row):
                data[c].append(v)
            n += 1
            if n >= chunk_size:
                yield Chunk(cols, data, n)
                data = {c: [] for c in cols}
                n = 0
        if n:
            yield Chunk(cols, data, n)


def _csv_encoding(path: pathlib.Path, forced=None):
    if forced:
        return forced, []
    notes = []
    for enc in ("utf-8-sig", "gbk"):
        try:
            with path.open("r", encoding=enc) as f:
                f.read(1 << 20)
            if enc != "utf-8-sig":
                notes.append(f"CSV 不是 UTF-8，按 {enc} 解码（请在契约里统一 UTF-8，见 CONTRACTS.md）")
            return enc, notes
        except UnicodeDecodeError:
            continue
    notes.append("CSV 既非 UTF-8 也非 GBK，按 latin-1 兜底解码（高风险：请检查源文件编码）")
    return "latin-1", notes


def _iter_df(df, cols, chunk_size: int):
    n = len(df)
    for i in range(0, n, chunk_size):
        sub = df.iloc[i:i + chunk_size]
        yield Chunk(cols, {c: sub[c].to_numpy() for c in cols}, len(sub))


def open_stream(path: pathlib.Path, *, sheet=None, encoding=None, chunk_size=CHUNK_SIZE) -> Stream:
    ext = path.suffix.lower()
    notes: list[str] = []
    if ext in (".csv", ".tsv", ".txt"):
        enc, notes = _csv_encoding(path, encoding)
        cols_holder: list[list[str]] = []

        def gen():
            for ch in _iter_csv(path, enc, chunk_size):
                if not cols_holder:
                    cols_holder.append(ch.columns)
                yield ch

        it = gen()
        first = next(it, None)
        cols = first.columns if first else (cols_holder[0] if cols_holder else [])

        def gen2():
            if first is not None:
                yield first
            yield from it

        return Stream(f"csv/{enc}", cols, gen2(), notes=notes)
    if ext == ".dta":
        pd = _pandas()
        reader = pd.io.stata.StataReader(str(path))
        cols = list(reader.variable_labels().keys())
        n_hint = None
        for attr in ("nobs", "_nobs"):
            if hasattr(reader, attr):
                try:
                    n_hint = int(getattr(reader, attr))
                    break
                except Exception:
                    pass
        if n_hint is None and hasattr(reader, "_get_nobs"):
            try:
                n_hint = int(reader._get_nobs())
            except Exception:
                n_hint = None
        it = pd.read_stata(str(path), chunksize=chunk_size, convert_categoricals=False)
        return Stream("dta", cols, (_chunk_from_df(df, cols) for df in it), n_hint=n_hint, notes=notes)
    if ext in (".xlsx", ".xlsm", ".xls"):
        pd = _pandas()
        try:
            xls = pd.ExcelFile(str(path))
        except ImportError as e:
            raise RuntimeError(_excel_hint(ext)) from e
        names = list(xls.sheet_names)
        target = sheet
        if target is None:
            if len(names) == 1:
                target = names[0]
            else:
                raise RuntimeError(
                    f"工作簿含 {len(names)} 个 sheet，必须显式 --sheet 指定：{names}"
                )
        if target not in names:
            raise RuntimeError(f"sheet {target!r} 不存在；可用：{names}")
        df = pd.read_excel(xls, sheet_name=target)
        if len(names) > 1:
            notes.append(f"工作簿含多个 sheet，本次只校验 {target!r}（其余：{[n for n in names if n != target]}）")
        cols = [str(c).strip() for c in df.columns]
        df.columns = cols
        return Stream(f"excel/{ext}", cols, _iter_df(df, cols, chunk_size), n_hint=len(df),
                      notes=notes, sheet=target)
    if ext in (".htm", ".html"):
        pd = _pandas()
        try:
            tables = pd.read_html(str(path))
        except ImportError as e:
            raise RuntimeError(
                "读 HTML 表需要 lxml/bs4：pip install lxml beautifulsoup4 html5lib"
            ) from e
        if not tables:
            raise RuntimeError(f"HTML 里没找到表格：{path}")
        df = tables[0]
        if len(tables) > 1:
            notes.append(f"HTML 含 {len(tables)} 张表，本次只校验第 1 张（其余 {len(tables)-1} 张需分别落文件）")
        cols = [str(c).strip() for c in df.columns]
        df.columns = cols
        return Stream("html", cols, _iter_df(df, cols, chunk_size), n_hint=len(df), notes=notes)
    if ext == ".parquet":
        pd = _pandas()
        try:
            df = pd.read_parquet(str(path))
        except ImportError as e:
            raise RuntimeError("读 parquet 需要 pyarrow：pip install pyarrow") from e
        cols = [str(c) for c in df.columns]
        df.columns = cols
        return Stream("parquet", cols, _iter_df(df, cols, chunk_size), n_hint=len(df), notes=notes)
    raise RuntimeError(f"不支持的扩展名 {ext}（契约允许：见 CONTRACTS.md）")


def _chunk_from_df(df, cols):
    d = {c: df[c].to_numpy() for c in cols if c in df.columns}
    return Chunk(list(df.columns), d, len(df))


def _excel_hint(ext: str) -> str:
    if ext == ".xls":
        return "读 .xls 需要 pandas + xlrd：pip install pandas xlrd"
    return "读 .xlsx 需要 pandas + openpyxl：pip install pandas openpyxl"


# --------------------------------------------------------------------------------------
# 与既有库的抽样对比 + 幂等预演（只读）
# --------------------------------------------------------------------------------------
def pg_connect(dbname: str, statement_timeout_ms: int = 300000):
    """连 Windows PG（host/user/password 取自 FD_ANALYSIS_PG_URL，库名可换）。

    密码优先 DSN 自带；DSN 无密码时依次回退 PGPASSWORD / WIN_PG_PASSWORD / INFRA-NOTES.md 的
    PG_PASSWORD=（与 dashboards/sql/win_pg.sh 同一约定）。

    注意：urlparse().password 返回的仍是 percent-encoded 形式（如 `%25%23`），必须 unquote，
    否则密码不对 → 服务器以 GBK 中文报错，表现为 UnicodeDecodeError（实测踩过）。
    """
    import urllib.parse
    import psycopg2
    raw = dsn(ANALYSIS_DB_ENV)
    u = urllib.parse.urlparse(raw)
    password = urllib.parse.unquote(u.password) if u.password else None
    username = urllib.parse.unquote(u.username) if u.username else "postgres"
    if not password:
        password = os.environ.get("PGPASSWORD") or os.environ.get("WIN_PG_PASSWORD")
    if not password:
        notes = ROOT.parent / "INFRA-NOTES.md"
        if notes.exists():
            for line in notes.read_text(encoding="utf-8", errors="replace").splitlines():
                if line.startswith("PG_PASSWORD="):
                    password = line.split("=", 1)[1].strip()
                    break
    try:
        return psycopg2.connect(
            host=u.hostname, port=u.port or 5432, user=username, dbname=dbname,
            password=password, connect_timeout=20,
            options=f"-c statement_timeout={statement_timeout_ms}",
        )
    except UnicodeDecodeError as e:
        raise RuntimeError(
            "连接 %s 失败，且服务器返回的是 GBK 中文错误信息（本机 Windows 中文 locale）。"
            "最常见原因是密码缺失/错误：请检查 FD_ANALYSIS_PG_URL 是否带密码，或设置 PGPASSWORD"
            "（dashboards/sql/win_pg.sh 从 INFRA-NOTES.md 的 PG_PASSWORD= 取）。原始错误：%s" % (dbname, e)
        ) from e


def gta_pick_table(file_cols: list[str], baseline: dict, override: str | None):
    """按列集匹配判 dta 属于哪张表（panel_c0/c1/c2）。"""
    if override:
        return override, 1.0, []
    objs = (baseline or {}).get("objects", {})
    best, best_score, best_unknown = None, -1.0, []
    fset = set(file_cols)
    for name, o in objs.items():
        cols = set(o.get("columns", []))
        if not cols:
            continue
        inter = len(fset & cols)
        score = inter / max(1, len(cols)) - 0.5 * len(fset - cols) / max(1, len(fset))
        if score > best_score:
            best, best_score, best_unknown = name, score, sorted(fset - cols)
    return best, best_score, best_unknown


def norm_id_org(v):
    """证券代码归一化：'000501' / 501 / 501.0 → '000501'（既有库 id_org 一律 6 位 text，实测 100%）。

    dta 里的 id_org 可能是数值型（国泰安导出常见），必须补零到 6 位才能和既有库对上。
    """
    if is_null(v):
        return None
    s = str(v).strip()
    f = to_float(s)
    if f is not None and f == int(f):
        s = str(int(f))
    if s.isdigit():
        return s.zfill(6)
    return s


def compare_gta(conn, table: str, file_keys: list, file_rows: dict, cols: list, findings: Findings,
                diff_cap: float, sample: int, baseline: dict, key_cols: list):
    """抽样对比：c0 用 id_org（text）做第一键，c1/c2 用 id（bigint）。"""
    cur = conn.cursor()
    k0 = key_cols[0]
    k0vals = [norm_id_org(k[0]) if k0 == "id_org" else to_int(k[0]) for k in file_keys]
    years = [to_int(k[1]) for k in file_keys]
    cast = "text" if k0 == "id_org" else "bigint"
    sel_cols = ", ".join(f'"{c}"' for c in cols) if cols else ""
    sql = (f'SELECT "{k0}", "year"{", " + sel_cols if sel_cols else ""} FROM {table} '
           f"WHERE ({k0}, year) IN (SELECT * FROM unnest(%s::{cast}[], %s::bigint[]))")
    cur.execute(sql, (k0vals, years))
    db_rows = {}
    names = [k0, "year"] + cols
    for r in cur.fetchall():
        db_rows[(r[0], r[1])] = dict(zip(names, r))
    norm_keys = list(zip(k0vals, years))
    missing_keys = [k for k in norm_keys if k not in db_rows]
    diff = same = 0
    for k, fk in zip(norm_keys, file_keys):
        if k not in db_rows:
            continue
        fr = file_rows.get(fk, {})
        dr = db_rows[k]
        for c in cols:
            a, b = to_float(fr.get(c)), to_float(dr.get(c))
            if a is None and b is None:
                same += 1
            elif a is None or b is None:
                diff += 1
            elif abs(a - b) <= max(1e-9, 1e-6 * abs(b)):
                same += 1
            else:
                diff += 1
    total_cmp = same + diff
    rate = (diff / total_cmp) if total_cmp else 0.0
    findings.ok("db_sample_keys", f"抽样 {len(norm_keys)} 键（{k0},year）命中既有库 {len(db_rows)} 行",
                f"未命中（= 本次新增）{len(missing_keys)} 键" + (f"，样例 {missing_keys[:3]}" if missing_keys else ""))
    if total_cmp:
        lvl = LEVEL_FAIL if rate > diff_cap * 2 else (LEVEL_WARN if rate > diff_cap else LEVEL_PASS)
        findings.add(lvl, "db_value_diff",
                    f"同名键旧值 vs 新值差异率 {rate:.2%}（上限 {diff_cap:.0%}）",
                    f"比较 {total_cmp} 个单元格（{len(cols)} 个变量列），差异 {diff}")
    return len(missing_keys)


def compare_city_full(conn, findings: Findings, diff_cap: float, baseline: dict):
    """城市面板整网格对比（7,425 行，代价可接受）。"""
    cur = conn.cursor()
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' "
                "AND table_name='panel_raw' ORDER BY ordinal_position")
    cols = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT " + ", ".join(f'"{c}"' for c in cols) + " FROM panel_raw")
    db = {}
    for r in cur.fetchall():
        d = dict(zip(cols, r))
        db[(to_int(d.get("city_code")), to_int(d.get("year")))] = d
    findings.ok("db_grid", f"既有库 panel_raw 网格 {len(db)} 行（期望 297×25=7,425）", "")
    return cols, db


def compare_yearbook(conn, file_key_rows: list, findings: Findings, diff_cap: float, batch=200):
    """年鉴按抽样键对比：命中 / 同键多值（既有库无唯一约束）/ 值差异率。"""
    cur = conn.cursor()
    hits = 0
    multi = 0
    diff = same = 0
    for i in range(0, len(file_key_rows), batch):
        chunk = file_key_rows[i:i + batch]
        ind = [c[0] for c in chunk]
        tab = [c[1] for c in chunk]
        reg = [c[2] for c in chunk]
        yr = [c[3] for c in chunk]
        ed = [c[4] for c in chunk]
        dm = [c[5] for c in chunk]
        cur.execute(
            """
            SELECT indicator_id, table_id, coalesce(region,''), data_year, edition_year,
                   jsonb_strip_nulls(coalesce(dims,'{}'::jsonb))::text, count(*), min(value), min(value_num)
              FROM indicator_values
             WHERE (indicator_id, table_id, data_year, edition_year) IN
                   (SELECT * FROM unnest(%s::bigint[], %s::bigint[], %s::int[], %s::int[]))
             GROUP BY 1,2,3,4,5,6
            """, (ind, tab, yr, ed),
        )
        got = {}
        for r in cur.fetchall():
            # 两侧必须用同一套归一化：region 走 norm_region_key（NFKC/空白压缩）、dims 走 norm_dims_key
            #（库里 jsonb::text 是 `{"a": 1}`，带空格；不回炉就永远对不上、命中率假 0）。
            k = (r[0], r[1], norm_region_key(r[2]), r[3], r[4], norm_dims_key(r[5]))
            got[k] = (r[6], r[7], r[8])
        for c in chunk:
            k = tuple(c[:6])          # c = 6 个键 + 1 个值（见 check_file 的 key_rows 构造）
            if k in got:
                hits += 1
                cnt, _mv, _mn = got[k]
                if cnt > 1:
                    multi += 1
                fnum = c[6] if len(c) > 6 else None
                if fnum is not None and _mn is not None:
                    if abs(float(fnum) - float(_mn)) <= max(1e-9, 1e-6 * abs(float(_mn))):
                        same += 1
                    else:
                        diff += 1
    n = len(file_key_rows)
    findings.ok("db_sample_keys", f"抽样 {n} 键命中既有库 {hits} 行",
                f"未命中 {n - hits}（= 新增或 region/dims 归一化不一致，重点看命中率）")
    if hits and multi / hits > 0.05:
        findings.add(LEVEL_FAIL if multi / hits > 0.3 else LEVEL_WARN, "db_key_multi",
                     f"抽样键中 {multi}/{hits}（{multi/hits:.0%}）在既有库有同键多行",
                     "既有 indicator_values 无自然键唯一约束（实测全表 24,332,692 行只有 5,589,074 个唯一键；"
                     "2024 版 813,749 行只有 211,253 个唯一键）；重灌前必须先决定去重口径，见 CONTRACTS.md §年鉴")
    total = same + diff
    if total:
        rate = diff / total
        lvl = LEVEL_FAIL if rate > diff_cap * 2 else (LEVEL_WARN if rate > diff_cap else LEVEL_PASS)
        findings.add(lvl, "db_value_diff", f"同键值差异率 {rate:.2%}（上限 {diff_cap:.0%}）",
                     f"比较 {total} 个值，差异 {diff}")
    return {"inserted": n - hits, "updated": hits}


# --------------------------------------------------------------------------------------
# 文件校验主流程
# --------------------------------------------------------------------------------------
def check_file(args, findings: Findings, baseline: dict | None) -> dict:
    src = args.source
    spec = dict(CONTRACT[src])
    if baseline:
        exp = baseline.get("expectations") or {}
        for k in ("row_range", "period_range", "periods_expected", "key", "required_cols", "strict_grid",
                  "keys_by_table", "required_cols_by_table", "hard_floor", "entity_count_range",
                  "entity_col", "single_edition_ok"):
            if k in exp and exp[k] is not None:
                spec[k] = exp[k]

    path = pathlib.Path(args.file).expanduser()
    ext = path.suffix.lower()
    state = {"source": src, "file": str(path), "dry_run": True}

    if not path.exists():
        findings.fail("file_exists", "源文件不存在", str(path))
        return state
    if path.is_dir():
        findings.fail("file_is_file", "给定路径是目录，不是文件", str(path))
        return state
    if path.stat().st_size == 0:
        findings.fail("file_nonempty", "文件为空（0 字节）", str(path))
        return state
    if ext in spec["reject_exts"]:
        findings.fail("file_ext", f"扩展名 {ext} 不接受", spec["reject_exts"][ext])
        return state
    if ext not in spec["exts"]:
        findings.fail("file_ext", f"扩展名 {ext} 不在契约允许集合 {spec['exts']}", "见 CONTRACTS.md")
        return state
    digest = sha256_file(path)
    size_mb = path.stat().st_size / 1024 / 1024
    state["sha256"] = digest
    state["size_bytes"] = path.stat().st_size
    findings.ok("file_readable", f"文件可读：{path.name}",
                f"{size_mb:.1f} MB  sha256={digest[:16]}…  扩展名 {ext}")

    try:
        stream = open_stream(path, sheet=args.sheet, encoding=args.csv_encoding)
    except RuntimeError as e:
        findings.fail("file_parse", "解析失败（依赖或格式问题）", str(e))
        return state
    except Exception as e:  # noqa: BLE001
        findings.fail("file_parse", f"解析失败：{type(e).__name__}", str(e))
        return state

    for note in stream.notes:
        findings.warn("file_note", "读取提示", note)
    state["fmt"] = stream.fmt
    if stream.sheet:
        state["sheet"] = stream.sheet

    cols = list(stream.columns)
    findings.ok("file_columns", f"表头 {len(cols)} 列", ", ".join(cols[:12]) + ("…" if len(cols) > 12 else ""))

    src_spec = dict(spec)
    detected_table = None
    if src == "gta_panel":
        detected_table, score, unknown = gta_pick_table(cols, baseline or {}, args.table)
        state["table"] = detected_table
        if score < 0.95 and not args.table:
            findings.fail("file_table_match", f"无法判定该文件属于哪张表（最佳匹配 {detected_table}，得分 {score:.2f}）",
                          "列集与 variables 字典对不上；请用 --table 显式指定")
        else:
            findings.ok("file_table_match",
                        f"判定目标表 {detected_table}（列集匹配度 {score:.2%}）",
                        "以 variables 字典为准，见 CONTRACTS.md §GTA")
            if unknown:
                findings.warn("file_unknown_cols", f"文件含 {len(unknown)} 个字典外列", ", ".join(unknown[:10]))
        # 幂等键按表切换：c0 = (id_org, year)，c1/c2 = (id, year)
        key_by_table = src_spec.get("keys_by_table") or {}
        if detected_table in key_by_table:
            src_spec["key"] = list(key_by_table[detected_table])
        findings.ok("file_key", f"幂等 upsert 键 = {src_spec['key']}",
                    "c0 用 id_org（6 位证券代码 text）；c1/c2 无 id_org，用 id（= id_org::bigint，既有库实测 100% 成立）")
        # c0 必须能定位证券：有 id_org 最好，只有 id 时可确定性地反推（id::text）
        if detected_table == "panel_c0":
            if "id_org" in cols:
                findings.ok("id_org_present", "panel_c0 含 id_org（源身份列）", "")
            elif "id" in cols:
                findings.warn("id_org_missing", "panel_c0 缺 id_org，只有 id",
                              "可反推：id_org = lpad(id::text, 6, '0')（既有库 id = id_org::bigint 100% 成立）；"
                              "loader 必须在装载时补出 id_org，否则 (id_org,year) 幂等键失效")
            else:
                findings.fail("id_org_missing", "panel_c0 既无 id_org 也无 id", f"实际列：{cols[:12]}")
        req_by_table = src_spec.get("required_cols_by_table") or {}
        req_cols = req_by_table.get(detected_table, src_spec["required_cols"])
        missing_req = [c for c in req_cols if c not in cols]
        if missing_req:
            findings.fail("file_cols_required", f"{detected_table} 缺必需列 {missing_req}", f"实际列：{cols[:20]}")
        else:
            findings.ok("file_cols_required", f"{detected_table} 必需列齐全（{req_cols}）")
        if baseline:
            exp_cols = set((baseline.get("objects", {}).get(detected_table) or {}).get("columns", []))
            if exp_cols:
                missing = sorted(exp_cols - set(cols))
                if missing:
                    findings.fail("file_cols_missing", f"缺 {len(missing)} 个既有列（列集必须与 variables 对齐）",
                                  ", ".join(missing[:12]))
                extra = sorted(set(cols) - exp_cols)
                if extra:
                    findings.warn("file_cols_extra", f"多 {len(extra)} 个既有表没有的列", ", ".join(extra[:12]))
    else:
        req = [c for c in src_spec["required_cols"] if c not in cols]
        if req:
            findings.fail("file_cols_required", f"缺必需列 {req}", f"实际列：{cols[:20]}")
        else:
            findings.ok("file_cols_required", f"必需列齐全（{src_spec['required_cols']}）")
        if src == "china_city_panel":
            if baseline:
                known = set((baseline.get("objects", {}).get("panel_raw") or {}).get("columns", []))
                if known:
                    unknown = sorted(set(cols) - known)
                    if unknown:
                        findings.warn("file_cols_extra", f"含 {len(unknown)} 个 panel_raw 没有的列", ", ".join(unknown[:12]))

    # ---------------- 单遍流式聚合 ----------------
    n_rows = 0
    key_seen: set[bytes] = set()
    dup_count = 0
    dup_examples: list[list] = []
    raw_key_window: list[tuple] = []
    key_null = 0
    key_set_truncated = False
    period_counts: dict[int, int] = {}
    entity_seen: set = set()
    null_required: dict[str, int] = {}
    header_region_rows = 0
    region_null_rows = 0
    dims_null_rows = 0
    edition_out_of_range = 0
    year_gt_edition = 0
    value_num_missing = 0
    sample_rows: list[dict] = []
    sample_keys: list[tuple] = []
    key_cols = list(src_spec["key"])
    period_col = src_spec["period_col"]
    # 抽样：流式读入时不知道总行数，所以用"步长自适应"——先逐行收，收满 sample 条后步长翻倍，
    # 这样小文件全收（不会只抽到 1 条），大文件也能铺满整个文件、总条数有界。
    sample_target = max(1, min(int(getattr(args, "sample", 300) or 300), VALUE_SAMPLE_MAX))
    fixed_step = int(getattr(args, "sample_step", 0) or 0)
    stride = fixed_step if fixed_step > 0 else 1
    next_at = 0

    for chunk in stream:
        n_rows += chunk.n
        years = chunk.values(period_col) if chunk.has(period_col) else [None] * chunk.n
        for i in range(chunk.n):
            kparts: list = []
            bad = False
            for kc in key_cols:
                # 年鉴的 region / dims 用空串 / 空对象哨兵：NULL 在唯一索引里不去重（会让幂等失效），
                # 所以空值在这里是合法哨兵，不算"键为空"。
                if src == "yearbook_catalog" and kc == "dims":
                    dims_v = chunk.values("dims")[i] if chunk.has("dims") else None
                    if is_null(dims_v) or dims_v == {} or dims_v == "{}":
                        dims_null_rows += 1
                    kparts.append(norm_dims_key(dims_v))
                    continue
                if src == "yearbook_catalog" and kc == "region":
                    v = chunk.values("region")[i] if chunk.has("region") else None
                    if is_null(v):
                        region_null_rows += 1
                    elif norm_region_key(v).replace(" ", "") in REGION_HEADER_WORDS:
                        header_region_rows += 1
                    kparts.append(norm_region_key(v))
                    continue
                v = chunk.values(kc)[i] if chunk.has(kc) else None
                if kc == "id_org":
                    v = norm_id_org(v)
                elif kc in INT_KEY_COLS:
                    v = to_int(v)
                if is_null(v):
                    bad = True
                kparts.append(v)
            if bad:
                key_null += 1
            d = key_digest(kparts)
            if d in key_seen:
                dup_count += 1
                if len(dup_examples) < 5:
                    dup_examples.append(kparts)
            elif len(key_seen) < MAX_KEY_SET:
                key_seen.add(d)
            else:
                key_set_truncated = True
            if len(raw_key_window) < DUP_EXAMPLE_WINDOW:
                raw_key_window.append(tuple(kparts))
            yr = to_int(years[i]) if i < len(years) else None
            if yr is not None:
                period_counts[yr] = period_counts.get(yr, 0) + 1
            ent = kparts[0] if kparts else None
            if ent is not None and len(entity_seen) < 2_000_000:
                entity_seen.add(ent)
            for rc in src_spec["required_cols"]:
                if chunk.has(rc):
                    v = chunk.values(rc)[i]
                    if is_null(v):
                        null_required[rc] = null_required.get(rc, 0) + 1
            if src == "yearbook_catalog":
                ed = to_int(chunk.values("edition_year")[i]) if chunk.has("edition_year") else None
                if ed is None or ed < 1980 or ed > CURRENT_YEAR:
                    edition_out_of_range += 1
                dy = to_int(chunk.values("data_year")[i]) if chunk.has("data_year") else None
                if ed is not None and dy is not None and dy > ed:
                    year_gt_edition += 1
                if chunk.has("value_num") and is_null(chunk.values("value_num")[i]):
                    value_num_missing += 1
            abs_i = n_rows - chunk.n + i
            if abs_i >= next_at and len(sample_rows) < VALUE_SAMPLE_MAX:
                row = {c: (chunk.values(c)[i] if chunk.has(c) else None) for c in
                       (key_cols + [period_col] + ([c for c in ("value", "value_num", "value_text", "v001") if chunk.has(c)]))}
                for c in cols[:40]:
                    row.setdefault(c, chunk.values(c)[i] if chunk.has(c) else None)
                sample_rows.append(row)
                sample_keys.append(tuple(kparts))
                if fixed_step > 0:
                    next_at = abs_i + stride
                elif len(sample_rows) >= sample_target:
                    stride = min(stride * 2, 1 << 20)
                    next_at = abs_i + stride
                else:
                    next_at = abs_i + 1

    if key_set_truncated:
        findings.warn("key_set_truncated", f"键集合超过 {MAX_KEY_SET:,}，唯一性只在已扫描部分内判定", "")

    # ---------------- 单版模式（年鉴：一版一文件，不可能覆盖整窗） ----------------
    base_ed = ((baseline or {}).get("expectations", {}).get("per_edition", {}) or {})
    single_edition = None
    if src == "yearbook_catalog" and src_spec.get("single_edition_ok"):
        got_p = sorted(period_counts)
        if getattr(args, "edition_year", None):
            single_edition = int(args.edition_year)
        elif len(got_p) == 1:
            single_edition = got_p[0]
    state["single_edition"] = single_edition
    if single_edition is not None:
        findings.ok("single_edition", f"按单版文件校验：edition_year={single_edition}",
                    "整窗覆盖由巡检器负责；重灌只对这一版做行数/键/字段校验（见 CONTRACTS.md §年鉴）")
        if getattr(args, "edition_year", None) and period_counts and set(period_counts) != {single_edition}:
            findings.fail("edition_mismatch",
                          f"--edition-year {single_edition} 与文件实际版本 {sorted(period_counts)} 不符",
                          "文件必须只含声明的这一版，否则覆盖判定会错")

    # ---------------- 行数 ----------------
    lo, hi = src_spec["row_range"]
    exp_ed = base_ed.get(str(single_edition)) if single_edition is not None else None
    if exp_ed:
        # 单版：区间换成"该版既有行数下限（可少不允许低于容忍度）/ 上限放宽到 2 倍（重解析可能补回漏行）"
        lo = int(exp_ed * (1 - args.year_tolerance))
        hi = int(exp_ed * 2)
        state["expected_rows_for_edition"] = exp_ed
    state["rows"] = n_rows
    state["row_range"] = [lo, hi]
    if n_rows == 0:
        findings.fail("rows_range", "文件 0 数据行", "空文件不得重灌")
    elif n_rows < lo:
        findings.fail("rows_range", f"行数严重不足：{n_rows:,} < 下限 {lo:,}",
                      f"契约区间 {lo:,}–{hi:,}" + (f"（既有 {single_edition} 版 {exp_ed:,} 行）" if exp_ed else ""))
    elif n_rows > hi:
        findings.warn("rows_range", f"行数超上限：{n_rows:,} > {hi:,}", f"契约区间 {lo:,}–{hi:,}")
    else:
        findings.ok("rows_range", f"行数 {n_rows:,} 在契约区间 {lo:,}–{hi:,}"
                    + (f"（既有 {single_edition} 版 {exp_ed:,} 行）" if exp_ed else ""))
    floor = getattr(args, "min_rows", None) or src_spec.get("hard_floor")
    if floor and exp_ed and exp_ed < floor:
        findings.warn("rows_hard_floor", f"{single_edition} 版既有量 {exp_ed:,} 本就低于硬下限 {floor:,}，本版不套用硬下限",
                      "早期版本（如 2000 版 58,567）天然小于现代版本；硬下限只对 2001+ 版有意义")
        floor = None
    if floor and n_rows < floor:
        findings.fail("rows_hard_floor", f"行数低于硬下限 {floor:,}（半版/截断）",
                      "该阈值即已知半版样本（2021 版 115,963）的下侧；早期版本请用 --min-rows 显式放低")
    if src_spec["strict_grid"] and n_rows != lo:
        findings.fail("rows_strict_grid", f"严格网格要求恰好 {lo:,} 行，实际 {n_rows:,}",
                      "城市面板 = 297 城 × 25 年，缺一行即缺口")

    # ---------------- 主键/键组合 ----------------
    if key_null:
        findings.fail("key_null", f"键列存在 {key_null} 行空值", f"键 = {key_cols}，键列不得为空")
    else:
        findings.ok("key_null", f"键列无空值（键 = {key_cols}）", "")
    if dup_count:
        findings.fail("key_unique", f"键组合重复 {dup_count:,} 行",
                      f"样例：{dup_examples[:3]}；键 = {key_cols}")
    else:
        findings.ok("key_unique", f"键组合唯一（{len(key_seen):,} 个键 / {n_rows:,} 行）", f"键 = {key_cols}")

    # ---------------- 期间覆盖 ----------------
    p_lo, p_hi = src_spec["period_range"]
    if period_counts:
        got = sorted(period_counts)
        state["periods"] = {"min": got[0], "max": got[-1], "count": len(got)}
        out_of_range = [y for y in got if y < p_lo or y > p_hi]
        missing = [y for y in (src_spec.get("periods_expected") or []) if y not in period_counts]
        findings.ok("period_range", f"{period_col} 覆盖 {got[0]}–{got[-1]}（{len(got)} 个期间）",
                    f"期望窗 {p_lo}–{p_hi}")
        if out_of_range:
            findings.fail("period_out_of_range", f"{len(out_of_range)} 个期间越界：{out_of_range[:8]}",
                          f"期望窗 {p_lo}–{p_hi}（年鉴既有库的 2026–2100 即此类脏值）")
        if single_edition is not None:
            # 单版文件只需落在窗内；整窗缺版不是本次重灌的结论（由巡检器出缺口）
            if single_edition < p_lo or single_edition > p_hi:
                findings.fail("edition_window", f"该版 {single_edition} 越出契约窗 {p_lo}–{p_hi}", "")
            else:
                findings.ok("period_cover", f"单版 {single_edition} 在契约窗 {p_lo}–{p_hi} 内（不做整窗覆盖判定）", "")
        else:
            if got[0] > p_lo:
                findings.fail("period_head", f"期间头部缺失：文件最早 {got[0]} > 期望起点 {p_lo}", "")
            if got[-1] < p_hi:
                findings.fail("period_tail", f"期间尾部缺失：文件最新 {got[-1]} < 期望终点 {p_hi}",
                              "重灌不得让最新期间回退")
            if missing:
                findings.fail("period_missing", f"整年缺失 {len(missing)} 个：{missing[:10]}", "期间集合必须覆盖期望窗")
            if not missing and not out_of_range and got[0] <= p_lo and got[-1] >= p_hi:
                findings.ok("period_cover", f"期间覆盖完整（{p_lo}–{p_hi} 无整年缺失）", "")
        base_pp = ((baseline or {}).get("objects", {}).get(
            (state.get("table") or src_spec["objects"][0])) or {}).get("rows_per_period") or {}
        if base_pp:
            shrink = []
            for y, c in sorted(period_counts.items()):
                bc = base_pp.get(str(y))
                if bc and c < bc * (1 - args.year_tolerance):
                    shrink.append((y, c, bc))
            if shrink:
                detail = "; ".join(f"{y}: {c:,} < {bc:,}" for y, c, bc in shrink[:6])
                lvl = LEVEL_WARN if args.allow_shrink else LEVEL_FAIL
                findings.add(lvl, "period_shrink",
                            f"{len(shrink)} 个期间行数比既有库少（容忍 {args.year_tolerance:.0%}）", detail)
            else:
                findings.ok("period_shrink", "各期间行数均不低于既有库（未发现历史行丢失）", "")
    else:
        findings.fail("period_range", f"无法从文件读出期间列 {period_col}", f"检查列名与取值格式")

    # ---------------- 实体数 ----------------
    if entity_seen:
        elo, ehi = src_spec["entity_count_range"]
        state["entities"] = len(entity_seen)
        if len(entity_seen) < elo:
            findings.fail("entity_count", f"{src_spec['entity_col']} 实体数不足：{len(entity_seen):,} < {elo:,}", "")
        elif len(entity_seen) > ehi and single_edition is None:
            findings.warn("entity_count", f"{src_spec['entity_col']} 实体数 {len(entity_seen):,} > {ehi:,}", "")
        else:
            findings.ok("entity_count", f"{src_spec['entity_col']} 实体数 {len(entity_seen):,}"
                        + (f"（单版文件，不套用整表上限 {ehi:,}）" if single_edition is not None and len(entity_seen) > ehi
                           else f" 在契约区间 {elo:,}–{ehi:,}"))

    # ---------------- 源特有 ----------------
    if src == "yearbook_catalog":
        if edition_out_of_range:
            findings.fail("edition_range", f"edition_year 越界 {edition_out_of_range:,} 行",
                          f"必须落在 1980–{CURRENT_YEAR}（既有库 2100 版 370.9 万行即此类脏值，不得再灌）")
        else:
            findings.ok("edition_range", f"edition_year 全部落在 1980–{CURRENT_YEAR}", "")
        total = max(1, n_rows)
        if header_region_rows and not args.allow_header_values:
            findings.add(LEVEL_FAIL if header_region_rows / total > 0.001 else LEVEL_WARN, "region_header",
                         f"region 命中表头脏值 {header_region_rows:,} 行（{header_region_rows/total:.2%}）",
                         "如 '地  区'/'行  业' 等表头词混进了地区列；既有库 2024 版就有 30,038 行同类脏值"
                         "（占该版 3.7%），重灌不应再引入")
        if region_null_rows:
            findings.warn("region_null", f"region 空值 {region_null_rows:,} 行（{region_null_rows/total:.1%}）",
                          "契约要求 region 归一化为空串哨兵（NULL 在唯一索引里不去重，会让幂等失效）；"
                          "既有库全表 687,266 行 region 为 NULL")
        if dims_null_rows:
            findings.warn("dims_null", f"dims 为空 {dims_null_rows:,} 行（{dims_null_rows/total:.1%}）",
                          "契约要求 dims 归一化为 '{}' 空对象哨兵；既有库全表 14,472,586 行 dims 为空（59%）")
        if year_gt_edition / max(1, n_rows) > 0.02:
            findings.warn("year_gt_edition", f"data_year > edition_year 占 {year_gt_edition/n_rows:.1%}",
                          "疑似解析错行（年鉴数据年份通常 ≤ 版本年）")
        if value_num_missing:
            findings.warn("value_num_missing", f"value_num 空 {value_num_missing:,} 行（{value_num_missing/total:.1%}）",
                          "既有库全表 3,090,788 行 value_num 为空（12.7%，纯文本值），重灌应保持同口径或显式声明")
    if src == "gta_panel":
        findings.ok("gta_grid", f"目标表 {state.get('table')} 行数 {n_rows:,}",
                    "三表应同网格（既有库 c0/c1/c2 各 96,029 行 × 1990–2025 × 5,939 家）")
    if src == "china_city_panel" and baseline:
        known = (baseline.get("objects", {}).get("panel_raw") or {}).get("columns", [])
        vcols = [c for c in known if re.fullmatch(r"v\d+", str(c))]
        if vcols:
            missing_v = sorted(set(vcols) - set(cols), key=lambda s: int(str(s)[1:]))
            if missing_v:
                findings.fail("city_vcols", f"缺 {len(missing_v)} 个变量列：{missing_v[:12]}",
                              "变量列由 china_city_panel.variable_catalog（v001..v209）定义")

    # ---------------- 与既有库抽样/整网格对比 ----------------
    state["compare_db"] = bool(args.compare_db)
    preview = {"inserted": None, "updated": None}
    if args.compare_db:
        try:
            conn = pg_connect(src_spec["db"])
        except Exception as e:  # noqa: BLE001
            findings.add(LEVEL_FAIL if args.require_db else LEVEL_WARN, "db_connect",
                         "无法连既有库做抽样对比", str(e)[:400])
            conn = None
        if conn is not None:
            try:
                if src == "gta_panel" and sample_keys:
                    t = state.get("table") or src_spec["objects"][0]
                    cmp_cols = [c for c in cols if c not in ("id", "id_org", "year", "stknme")][: args.compare_cols]
                    new_keys = compare_gta(conn, t, sample_keys, {k: r for k, r in zip(sample_keys, sample_rows)},
                                           cmp_cols, findings, args.value_diff_cap, args.sample, baseline,
                                           src_spec["key"])
                    preview = {"inserted": new_keys, "updated": len(sample_keys) - new_keys}
                    findings.ok("idempotent_preview", f"幂等预演（抽样 {len(sample_keys)} 键 {src_spec['key']}）："
                                                      f"预计新增 {new_keys} / 命中既有 {len(sample_keys)-new_keys}",
                                "同一文件重复重灌不产生重复行（键命中即 UPDATE）")
                elif src == "china_city_panel":
                    db_cols, db = compare_city_full(conn, findings, args.value_diff_cap, baseline)
                    file_keys = set(sample_keys)
                    hit = sum(1 for k in file_keys if k in db)
                    preview = {"inserted": len(file_keys) - hit, "updated": hit}
                    findings.ok("idempotent_preview", f"幂等预演：网格键命中既有 {hit}/{len(file_keys)}（整表 7,425 键）",
                                "命中即 UPDATE，未命中即 INSERT；同一文件重复重灌不产生重复行"
                                "；panel_interp / panel_arima 是派生表，重灌 panel_raw 后必须重跑插补")
                elif src == "yearbook_catalog" and sample_keys:
                    key_rows = []
                    for k, row in zip(sample_keys, sample_rows):
                        vnum = to_float(row.get("value_num"))
                        if vnum is None:
                            vnum = to_float(row.get("value"))
                        key_rows.append(list(k) + [vnum])
                    preview = compare_yearbook(conn, key_rows, findings, args.value_diff_cap)
                    findings.ok("idempotent_preview",
                                f"幂等预演（抽样 {len(key_rows)} 键）：命中即 UPDATE，未命中即 INSERT", "")
            except Exception as e:  # noqa: BLE001
                findings.add(LEVEL_FAIL if args.require_db else LEVEL_WARN, "db_compare_failed",
                             f"抽样对比失败：{type(e).__name__}", str(e)[:400])
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
    else:
        findings.warn("db_compare_skipped", "已跳过与既有库的抽样对比（--no-compare-db）",
                      "契约校验清单包含该项；正式重灌前应至少跑一次")

    # ---------------- 回执片段 ----------------
    gaps_after = [f"{i.code}: {i.title}" for i in findings.items if i.level == LEVEL_FAIL]
    state["verdict"] = "fail" if findings.has_fail() else ("warn" if findings.counts()[LEVEL_WARN] else "pass")
    state["preview"] = preview
    state["gaps_after"] = gaps_after
    state["receipt"] = {
        "executor": src_spec["receipt_executor"],
        "source": src,
        "mode": "file",
        "status": "validated" if not findings.has_fail() else "rejected",
        "report_only": False,
        "file": str(path),
        "sha256": digest,
        "bytes": state.get("size_bytes"),
        "rows": n_rows,
        "key": src_spec["key"],
        "table": state.get("table") or src_spec["objects"][0],
        "period": state.get("periods"),
        "edition_year": single_edition,
        "inserted": preview["inserted"],
        "updated": preview["updated"],
        "verdict": state["verdict"],
        "gaps_after": gaps_after,
        "note": "inserted/updated 由 --compare-db 的键命中数推得（抽样口径）；实际写入由 loader 完成后覆盖。"
                "FAIL 时不得重灌（status=rejected）。",
    }
    return state


# --------------------------------------------------------------------------------------
# --db-baseline：从既有库推导"期望形状"
# --------------------------------------------------------------------------------------
PERIOD_COL = {"panel_c0": "year", "panel_c1": "year", "panel_c2": "year",
              "panel_raw": "year", "indicator_values": "edition_year"}
# 探针用的键 = 契约键（c0 用 id_org，c1/c2 用 id；两者与 PK(id,year) 等价，实测均为 96,029 唯一）
KEY_COLS = {"panel_c0": ["id_org", "year"], "panel_c1": ["id", "year"], "panel_c2": ["id", "year"],
            "panel_raw": ["city_code", "year"],
            "indicator_values": ["indicator_id", "table_id", "region", "data_year", "edition_year", "dims"]}
ENTITY_COL = {"panel_c0": "id", "panel_c1": "id", "panel_c2": "id", "panel_raw": "city_code",
              "indicator_values": "indicator_id"}


def baseline_for_source(source: str, args, findings: Findings) -> dict:
    spec = CONTRACT[source]
    out = {
        "schema_version": SCHEMA_VERSION,
        "source": source,
        "generated_at": now_iso(),
        "db": spec["db"],
        "env": ANALYSIS_DB_ENV,
        "host": None,
        "read_only": True,
        "note": spec["note"],
        "objects": {},
        "expectations": {},
    }
    try:
        import urllib.parse
        out["host"] = urllib.parse.urlparse(dsn(ANALYSIS_DB_ENV)).hostname
    except SystemExit as e:
        raise RuntimeError(str(e))
    conn = pg_connect(spec["db"])
    try:
        cur = conn.cursor()

        def one(sql, params=None):
            cur.execute(sql, params)
            r = cur.fetchone()
            return r[0] if r else None

        for obj in spec["objects"]:
            o: dict = {"object": f"{spec['db']}.public.{obj}"}
            cur.execute(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position", (obj,))
            colinfo = cur.fetchall()
            o["columns"] = [c[0] for c in colinfo]
            o["column_types"] = {c[0]: c[1] for c in colinfo}
            o["row_count"] = one(f'SELECT count(*) FROM public."{obj}"')
            pcol = PERIOD_COL[obj]
            o["period_col"] = pcol
            cur.execute(f'SELECT "{pcol}", count(*) FROM public."{obj}" GROUP BY 1 ORDER BY 1')
            rpp = {str(r[0]): r[1] for r in cur.fetchall() if r[0] is not None}
            o["rows_per_period"] = rpp
            o["periods"] = {"min": min(int(k) for k in rpp) if rpp else None,
                            "max": max(int(k) for k in rpp) if rpp else None,
                            "count": len(rpp)}
            ecol = ENTITY_COL[obj]
            o["entity_col"] = ecol
            o["entity_count"] = one(f'SELECT count(distinct "{ecol}") FROM public."{obj}"')
            kcols = KEY_COLS[obj]
            kexpr = ", ".join(f'coalesce("{c}"::text,\'\')' for c in kcols)
            if o["row_count"] and o["row_count"] <= (200_000 if args.quick else 10 ** 9):
                o["unique_keys"] = one(f'SELECT count(*) FROM (SELECT 1 FROM public."{obj}" GROUP BY {kexpr}) x')
                o["duplicate_key_rows"] = (o["row_count"] or 0) - (o["unique_keys"] or 0)
            else:
                o["unique_keys"] = None
                o["duplicate_key_rows"] = None
                o["skipped"] = ["unique_keys(表太大或 --quick；年鉴已知无自然键唯一约束)"]
            # 键列空值率
            nulls = {}
            for c in set(kcols) | set(spec["required_cols"]):
                if c == "dims":
                    n = one(f'SELECT count(*) FROM public."{obj}" WHERE dims IS NULL')
                else:
                    n = one(f'SELECT count(*) FROM public."{obj}" WHERE "{c}" IS NULL')
                if n:
                    nulls[c] = {"rows_null": n, "rate": round(n / max(1, o["row_count"]), 4)}
            o["null_stats"] = nulls
            # 唯一索引 / 约束
            cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='public' AND tablename=%s "
                        "ORDER BY indexname", (obj,))
            o["indexes"] = [{"name": r[0], "def": r[1]} for r in cur.fetchall()]
            out["objects"][obj] = o
            findings.ok("baseline_object", f"{spec['db']}.{obj}：{o['row_count']:,} 行",
                        f"{pcol} {o['periods']['min']}–{o['periods']['max']}（{o['periods']['count']} 期），"
                        f"{ecol} {o['entity_count']:,} 个，列 {len(o['columns'])}")

        # ---- 期望形状 ----
        if source == "yearbook_catalog":
            rpp = out["objects"]["indicator_values"]["rows_per_period"]
            real = {int(k): v for k, v in rpp.items() if 1980 <= int(k) <= CURRENT_YEAR and v >= 1000}
            vals = sorted(real.values())
            median = vals[len(vals) // 2] if vals else 0
            out["expectations"] = {
                "key": CONTRACT[source]["key"],
                "required_cols": CONTRACT[source]["required_cols"],
                "row_range": [int(median * 0.4), int(median * 1.6)],
                "row_range_basis": f"单版值中位 {median:,}（{len(real)} 个 1980–{CURRENT_YEAR} 版）；"
                                   f"最小 {min(vals) if vals else 0:,}，最大 {max(vals) if vals else 0:,}",
                "hard_floor": CONTRACT[source]["hard_floor"],
                "period_col": "edition_year",
                "period_range": [min(real) if real else 1980, max(real) if real else CURRENT_YEAR],
                "periods_expected": sorted(real),
                "strict_grid": False,
                "single_edition_ok": True,
                "per_edition": {str(k): v for k, v in sorted(real.items())},
                "known_dirty": {str(k): v for k, v in sorted(rpp.items()) if not (1980 <= int(k) <= CURRENT_YEAR)},
                # 该表在契约键上不唯一（重灌前必须处理，见 CONTRACT["yearbook_catalog"]["dedup_note"]）
                "natural_key_note": CONTRACT["yearbook_catalog"]["dedup_note"],
            }
            out["key_uniqueness"] = {
                "rows": out["objects"]["indicator_values"]["row_count"],
                "unique_keys": out["objects"]["indicator_values"].get("unique_keys"),
                "duplicate_key_rows": out["objects"]["indicator_values"].get("duplicate_key_rows"),
            }
        elif source == "china_city_panel":
            o = out["objects"]["panel_raw"]
            cities = one("SELECT count(*) FROM city_dim")
            out["dimension"] = {"city_dim_rows": cities}
            cur.execute("SELECT min(col_no), max(col_no), count(*) FROM variable_catalog")
            out["dimension"]["variable_catalog"] = dict(zip(("min_col_no", "max_col_no", "n_vars"), cur.fetchone()))
            out["expectations"] = {
                "key": CONTRACT[source]["key"],
                "required_cols": CONTRACT[source]["required_cols"],
                "row_range": [o["row_count"], o["row_count"]],
                "row_range_basis": f"既有库严格网格 {o['entity_count']} 城 × {o['periods']['count']} 年 = {o['row_count']:,}",
                "period_col": "year",
                "period_range": [o["periods"]["min"], o["periods"]["max"]],
                "periods_expected": sorted(int(k) for k in o["rows_per_period"]),
                "strict_grid": True,
                "entity_col": "city_code",
                "entity_count_range": [o["entity_count"], o["entity_count"]],
            }
        else:  # gta_panel
            r0 = out["objects"]["panel_c0"]
            grid_ok = all(out["objects"][t]["row_count"] == r0["row_count"] for t in
                          ("panel_c0", "panel_c1", "panel_c2"))
            rows = r0["row_count"] or 96029
            out["expectations"] = {
                "key": CONTRACT[source]["key"],
                "keys_by_table": CONTRACT[source]["keys_by_table"],
                "required_cols": CONTRACT[source]["required_cols"],
                "required_cols_by_table": CONTRACT[source]["required_cols_by_table"],
                # 单表文件：下限 95%（历史行丢失必须 FAIL）；上限放宽到 1.6 倍（新增年份/公司是正常增长）
                "row_range": [int(rows * 0.95), int(rows * 1.6)],
                "row_range_basis": f"三表实测各 {rows:,} 行（同网格={grid_ok}）；"
                                   f"年份/公司维度另由 periods_expected 与 entity_count_range 约束",
                "period_col": "year",
                "period_range": [r0["periods"]["min"], r0["periods"]["max"]],
                "periods_expected": sorted(int(k) for k in r0["rows_per_period"]),
                "strict_grid": False,
                "entity_col": "id",
                "entity_count_range": [int(r0["entity_count"] * 0.99), int(r0["entity_count"] * 1.6)],
                "tables_same_grid": grid_ok,
                "variables_dict": {
                    t: len(out["objects"].get(t, {}).get("columns", [])) for t in ("panel_c0", "panel_c1", "panel_c2")
                },
            }
            # variables 字典（列 → 表）留给文件校验判定归属
            cur.execute("SELECT name, table_name, pg_type FROM variables")
            vd = {r[0]: {"table": r[1], "pg_type": r[2]} for r in cur.fetchall()}
            out["variables"] = vd
            findings.ok("variables_dict", f"variables 字典 {len(vd)} 个变量名", "用于判定 dta 归属与列集对齐")

        if source == "yearbook_catalog":
            cur.execute("SELECT status, count(*) FROM ingest_files GROUP BY 1 ORDER BY 2 DESC")
            rows = cur.fetchall()
            out["ingest_files_by_status"] = {r[0]: r[1] for r in rows}
            findings.ok("ingest_status", f"ingest_files 状态：{out['ingest_files_by_status']}",
                        "pending_* = 源文件在目录里但尚未解析（本轮只报告）")
    finally:
        conn.close()
    return out


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
def print_findings(findings: Findings, title: str):
    c = findings.counts()
    print(f"\n== {title} ==")
    for i in findings.items:
        mark = {LEVEL_PASS: "[PASS]", LEVEL_WARN: "[WARN]", LEVEL_FAIL: "[FAIL]"}[i.level]
        print(f"{mark} {i.code}: {i.title}")
        if i.detail:
            print(f"        {i.detail}")
    print(f"---- 小结：PASS {c[LEVEL_PASS]} / WARN {c[LEVEL_WARN]} / FAIL {c[LEVEL_FAIL]}")


def print_no_source(args, findings: Findings) -> int:
    """源文件缺失 = 仅报告：给回执片段（status=no_source_file），绝不产生"已补齐"。"""
    src = args.source
    srcs = list(CONTRACT) if src == "all" else [src]
    receipts = {}
    print("== 待源文件（仅报告）==")
    for s in srcs:
        spec = CONTRACT[s]
        kb = spec.get("keys_by_table")
        key_txt = ("c0=" + str(kb["panel_c0"]) + " / c1/c2=" + str(kb["panel_c1"])) if kb else str(spec["key"])
        print(f"[REPORT-ONLY] {s}: 无可用源文件 → 不执行重灌、不报成功")
        print(f"        期望输入：{'/'.join(spec['exts'])}；契约键 {key_txt}；既有对象 "
              f"{spec['db']}." + " / ".join(spec["objects"]))
        receipts[s] = {
            "executor": spec["receipt_executor"],
            "source": s,
            "mode": "file",
            "status": "no_source_file",
            "report_only": True,
            "file": None,
            "sha256": None,
            "rows": 0,
            "key": spec["key"],
            "inserted": 0,
            "updated": 0,
            "verdict": "report_only",
            "gaps_after": ["no_source_file: 待源文件，缺口只能报告不能补"],
            "note": "源文件不在手 → 标注『待源文件』；不得写成功回执、不得报已补齐（OpenSpec data-backfill 需求『文件驱动重灌契约』）",
        }
        findings.add(LEVEL_WARN, "no_source_file", f"{s}: 待源文件，标注为仅报告", "禁止写 dq.backfill_run 的成功回执")
    print("\n== 回执片段（status=no_source_file；不是成功回执）==")
    print(json.dumps(receipts if src == "all" else receipts[src], ensure_ascii=False, indent=2))
    return 3


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="reload_check.py",
        description="文件驱动重灌契约的校验器（只读、默认 dry-run；契约见 CONTRACTS.md）",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True, choices=sorted(CONTRACT) + ["all"])
    ap.add_argument("--file", help="待校验的源文件；省略则该源标注为『待源文件/仅报告』")
    ap.add_argument("--db-baseline", action="store_true", help="从既有库推导期望形状并写 .state/reload_baseline_<source>.json")
    ap.add_argument("--table", help="GTA：强制指定目标表（panel_c0/c1/c2），默认按列集自动判定")
    ap.add_argument("--sheet", help="Excel：指定 sheet 名（城市面板源工作簿含 原始数据/线性插值/ARIMA填补(慎用)）")
    ap.add_argument("--csv-encoding", help="CSV 编码，默认自动探测 utf-8-sig → gbk")
    ap.add_argument("--edition-year", type=int,
                    help="年鉴：声明该文件只有这一版（一版一文件），整窗覆盖检查切到单版模式；"
                         "文件里只有一个 edition_year 时自动切换")
    ap.add_argument("--min-rows", type=int,
                    help="覆盖硬下限（早期版本如年鉴 2000 版仅 58,567 行，需显式放低）")
    ap.add_argument("--state-dir", default=str(DEFAULT_STATE_DIR))
    ap.add_argument("--json", dest="json_out", help="把结果 JSON 写到该路径（默认仅打印）")
    ap.add_argument("--receipt-out", help="把回执片段 JSON 写到该路径（loader 用；本脚本不写库）")
    ap.add_argument("--sample", type=int, default=300, help="与既有库抽样对比的键数（默认 300）")
    ap.add_argument("--sample-step", type=int, default=0, help="文件抽样步长（默认自动）")
    ap.add_argument("--compare-cols", type=int, default=8, help="对比的变量列数上限（默认 8）")
    ap.add_argument("--value-diff-cap", type=float, default=0.05, help="旧值/新值差异率上限（默认 5%%）")
    ap.add_argument("--year-tolerance", type=float, default=0.02, help="单期行数相对既有库的容忍度（默认 2%%）")
    ap.add_argument("--allow-shrink", action="store_true", help="允许单期行数比既有库少（默认 FAIL）")
    ap.add_argument("--allow-header-values", action="store_true", help="允许 region 出现表头词（默认 FAIL）")
    ap.add_argument("--compare-db", dest="compare_db", action="store_true", default=True,
                    help="与既有库抽样对比（默认开启；需 FD_ANALYSIS_PG_URL 可达）")
    ap.add_argument("--no-compare-db", dest="compare_db", action="store_false", help="跳过连库对比")
    ap.add_argument("--require-db", action="store_true", help="连库失败视为 FAIL（默认 WARN）")
    ap.add_argument("--quick", action="store_true", help="--db-baseline：跳过重查询（大表去重计数）")
    ap.add_argument("--dry-run", action="store_true", default=True, help="默认行为，不写任何表")
    ap.add_argument("--apply", action="store_true", help="（被拒）本校验器不写库")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)

    if args.apply:
        print("[FAIL] reload_check.py 不写库：本脚本是重灌前的校验器（默认 dry-run）。\n"
              "       实际写入由 loader / 执行器完成，回执格式见 CONTRACTS.md §回执。", file=sys.stderr)
        return 2
    if args.file and args.source == "all":
        print("[FAIL] --file 必须与具体 --source 搭配（all 仅用于 --db-baseline）", file=sys.stderr)
        return 2
    if args.file and args.db_baseline:
        print("[FAIL] --file 与 --db-baseline 是两条独立路径，不能同时用：\n"
              "       先 `--db-baseline` 生成 .state/reload_baseline_<source>.json，再 `--file` 校验。", file=sys.stderr)
        return 2

    findings = Findings()
    state_dir = pathlib.Path(args.state_dir).expanduser()
    result: dict = {"ts": now_iso(), "source": args.source, "dry_run": True}

    # ---- --db-baseline ----
    if args.db_baseline:
        sources = list(CONTRACT) if args.source == "all" else [args.source]
        baselines = {}
        rc = 0
        for s in sources:
            try:
                b = baseline_for_source(s, args, findings)
            except Exception as e:  # noqa: BLE001
                findings.fail("baseline_failed", f"{s}: 推导基线失败：{type(e).__name__}", str(e)[:500])
                rc = 2
                continue
            baselines[s] = b
            state_dir.mkdir(parents=True, exist_ok=True)
            p = state_dir / f"reload_baseline_{s}.json"
            p.write_text(json.dumps(b, ensure_ascii=False, indent=2), encoding="utf-8")
            findings.ok("baseline_written", f"{s}: 期望形状已写入 {p}",
                        f"row_range={b['expectations'].get('row_range')} "
                        f"period={b['expectations'].get('period_range')} "
                        f"key={b['expectations'].get('keys_by_table') or b['expectations'].get('key')}")
        result["db_baseline"] = {k: v["expectations"] for k, v in baselines.items()}
        print_findings(findings, f"--db-baseline（只读连库，{ANALYSIS_DB_ENV}）")
        print("\n== 期望形状摘要（供报告引用）==")
        for s, b in baselines.items():
            print(json.dumps({"source": s, "expectations": b["expectations"],
                              "objects": {k: {"row_count": v["row_count"], "entity_count": v["entity_count"],
                                              "periods": v["periods"], "n_columns": len(v["columns"]),
                                              "duplicate_key_rows": v.get("duplicate_key_rows")}
                                          for k, v in b["objects"].items()}},
                             ensure_ascii=False, indent=2))
        if args.json_out:
            pathlib.Path(args.json_out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return rc

    # ---- --file ----
    if args.file:
        baseline = None
        bp = state_dir / f"reload_baseline_{args.source}.json"
        if bp.exists():
            baseline = json.loads(bp.read_text(encoding="utf-8"))
            findings.ok("baseline_loaded", f"载入既有库基线 {bp}", f"生成于 {baseline.get('generated_at')}")
        else:
            findings.warn("baseline_missing", f"未找到 {bp}，用契约默认值兜底",
                          f"建议先跑：--source {args.source} --db-baseline")
        st = check_file(args, findings, baseline)
        result["file_check"] = st
        result["findings"] = [i.to_dict() for i in findings.items]
        print_findings(findings, f"文件校验（dry-run，未写任何表）：{args.file}")
        print("\n== 回执片段（loader 填完 written_rows 后写 dq.backfill_run；格式见 CONTRACTS.md §回执）==")
        print(json.dumps(st.get("receipt", {}), ensure_ascii=False, indent=2))
        if args.receipt_out:
            pathlib.Path(args.receipt_out).write_text(
                json.dumps({"source": args.source, "executor": st.get("receipt", {}).get("executor"),
                            "status": st.get("receipt", {}).get("status"),
                            "written_rows": None,
                            "receipt": st.get("receipt", {})}, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"\n（回执片段已写 {args.receipt_out}；本脚本不写库，回执入库由 loader 执行）")
        if args.json_out:
            pathlib.Path(args.json_out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return 1 if findings.has_fail() else 0

    # ---- 无文件、无 baseline → 仅报告 ----
    rc = print_no_source(args, findings)
    result["report_only"] = True
    result["status"] = "no_source_file"
    result["findings"] = [i.to_dict() for i in findings.items]
    print_findings(findings, "仅报告")
    if args.json_out:
        pathlib.Path(args.json_out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return rc


if __name__ == "__main__":
    sys.exit(main())
