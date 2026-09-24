"""国家统计局（NBS）客户端 —— 两条取数通路，供 scripts/backfill_nbs.py 使用。

本模块自包含（不 import fd_world），不依赖浏览器。

**新接口（默认，2026 改版后）** —— ``NewNBSClient``
    站点已从 easyquery 改版为 ``/dg/website`` 的 SPA，后端换成一组免鉴权 JSON 接口
    （base ``https://data.stats.gov.cn/dg/website/publicrelease``）：

    - ``GET  /web/external/new/queryIndexTreeAsync?pid=&code=<tab>``  指标目录树
      （``code``：1 月度 / 2 季度 / 3 年度 / 4-6 分省 / 7-8 主要城市 …）
    - ``GET  /web/external/new/queryIndicatorsByCid?cid=<目录id>``     目录下的叶子指标
    - ``POST /web/external/stream/esData``                            取数（核心）
      body ``{cid, indicatorIds, daCatalogId, das, showType, dt, dts, rootId}``；
      ``dt``/``dts`` 为期间区间 ``"<起>-<止>"``，后缀 ``YY``/``MM``/``SS``；
      ``showType=1`` 单地区（``das`` 取首个），``showType=3`` 全地区（期间为行、地区为列）。
    - ``GET  /web/external/getDaCatalogTreeByIndicatorCid?indicatorCid=<目录id>``  地区目录树
    - ``GET  /web/external/getDasByDaCatalogId?daCid=<地区目录id>``                 目录下地区清单

    目录与指标都用不透明 UUID 标识（不再是 zbcode）。改版后 easyquery 的 403 与此无关，
    新接口实测全部 200 且无需登录/token/签名。

**旧接口（回退，``--endpoint easyquery``）** —— ``NBSClient``
    POST ``https://data.stats.gov.cn/easyquery.htm``（m/dbcode/rowcode/colcode/wds/
    dfwds/k1）。2026 起该 URL 对所有出口返回 403 ``reason:UrlACL``，仅在出口未被封时可用。

两者共用的行为：浏览器请求头、TLS 校验关闭、代理走 HTTP_PROXY/HTTPS_PROXY、保守限速
（每次请求间隔 >= min_interval 秒）、瞬时故障退避重试、403/WAF 快速失败。

- ``discover_indicators``：递归走目录树到叶子，展开叶子目录下的指标。
- ``query_data``：按指标 + 期间窗口取回数据，归一化为 ``Observation``（指标码/名、
  dbcode、期间、Decimal 值、单位、地区码/名、原始记录）。

期间格式（两条通路归一化到同一套，便于与既有行共存）：月度 ``YYYYMM``、季度
``YYYYA``-``YYYYD``、年度 ``YYYY``。新接口的 ``202602SS`` -> ``2026B``，
``2025YY`` -> ``2025``，``202608MM`` -> ``202608``。

``SELF_TEST_FIXTURES``：真实录制响应形态，用于在出口不可达时验证解析与幂等落库路径。
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Optional

import requests
from urllib3.exceptions import InsecureRequestWarning

# spec cnstats-source 明确要求对 NBS 关闭 TLS 校验，这里同时静音 urllib3 的告警噪声
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

EASYQUERY_URL = "https://data.stats.gov.cn/easyquery.htm"
REFERER = "https://data.stats.gov.cn/easyquery.htm?cn=A01"

# --- 新接口（/dg/website SPA 后端） ---------------------------------------
NEW_API_BASE = "https://data.stats.gov.cn/dg/website/publicrelease"
NEW_TREE_URL = f"{NEW_API_BASE}/web/external/new/queryIndexTreeAsync"
NEW_INDICATORS_URL = f"{NEW_API_BASE}/web/external/new/queryIndicatorsByCid"
NEW_ES_DATA_URL = f"{NEW_API_BASE}/web/external/stream/esData"
NEW_DA_TREE_URL = f"{NEW_API_BASE}/web/external/getDaCatalogTreeByIndicatorCid"
NEW_DA_LIST_URL = f"{NEW_API_BASE}/web/external/getDasByDaCatalogId"
NEW_PAGE_REFERER = "https://data.stats.gov.cn/dg/website/page.html"

# dbcode -> queryIndexTreeAsync 的 code（取自 SPA 的 tabsCode 常量表）
NEW_TAB_CODE = {
    "hgyd": "1",  # 月度数据
    "hgjd": "2",  # 季度数据
    "hgnd": "3",  # 年度数据
    "fsyd": "4",  # 分省月度数据
    "fsjd": "5",  # 分省季度数据
    "fsnd": "6",  # 分省年度数据
    "csyd": "7",  # 主要城市月度数据
    "csnd": "8",  # 主要城市年度数据
    # csjd（城市季度）在新站点没有对应页签，调用时显式报错
}

# 带地区维度的库：走 daCatalogId + showType=3（一次拿全地区）
AREA_DBCODES = frozenset({"fsyd", "fsjd", "fsnd", "csyd", "csjd", "csnd"})

# 全国维度的伪地区码（新接口用它表示"全国"，落库时映射为 region_id = NULL）
NATIONAL_AREA_CODE = "000000000000"

# 季度在新接口里是 YYYY0QSS（01-04），落库转回旧记法的 A-D
QUARTER_LETTERS = "ABCD"

DBCODES = {
    "hgyd": "宏观月度",
    "hgjd": "宏观季度",
    "hgnd": "宏观年度",
    "fsyd": "分省月度",
    "fsjd": "分省季度",
    "fsnd": "分省年度",
    "csyd": "城市月度",
    "csjd": "城市季度",
    "csnd": "城市年度",
}

# dbcode -> 频率（决定滚动窗口的起始期间拼法，见 spec cnstats-source: 默认窗口场景）
DBCODE_FREQ = {
    "hgyd": "M", "hgjd": "Q", "hgnd": "A",
    "fsyd": "M", "fsjd": "Q", "fsnd": "A",
    "csyd": "M", "csjd": "Q", "csnd": "A",
}

# 全量回补的起始期间（"取全部可用历史"）
FULL_START_PERIOD = {"M": "194901", "Q": "1949A", "A": "1949"}

# 精选指标（沿用 fd-world COMMON_INDICATORS 的 zbcode，按 dbcode 归组）。
# 不做 tree 全量发现时（--scope curated）使用；tree 发现是默认的更全路径。
CURATED_INDICATORS: dict[str, dict[str, str]] = {
    "hgnd": {
        "A0201": "国内生产总值",
        "A0202": "第一产业增加值",
        "A0203": "第二产业增加值",
        "A0204": "第三产业增加值",
        "A0301": "年末总人口",
    },
    "hgjd": {
        "A0A0A": "国内生产总值(绝对额)",
        "A0A0B": "国内生产总值(同比增速)",
    },
    "hgyd": {
        "A010101": "居民消费价格指数(上年同月=100)",
        "A010801": "工业生产者出厂价格指数(上年同月=100)",
        "A0201": "工业增加值增长速度",
        "A0401": "固定资产投资",
        "A0501": "社会消费品零售总额",
        "A0601": "进出口贸易总额",
        "A0D01": "货币供应量",
    },
}

BROWSER_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://data.stats.gov.cn",
    "Referer": REFERER,
    "Host": "data.stats.gov.cn",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
}

PROXY_SOURCE = "nbs"


@dataclass
class Acquisition:
    """Forwarder selection result for one upstream fetch."""

    upstream_url: Optional[str]
    addr_id: Optional[int]
    provider: Optional[str]


class _ForwarderClient:
    """Small acquire/release client for ``fd-proxy-service``.

    The forwarder owns proxy selection, Redis QPS accounting and circuit state.
    The NBS client still terminates TLS so it can classify the decrypted 403/429
    response before returning the outcome to the forwarder.
    """

    def __init__(self, timeout: float = 3.0) -> None:
        self.timeout = timeout

    @property
    def base_url(self) -> str:
        return (os.environ.get("FD_PROXY_FORWARDER") or "").rstrip("/")

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    def _post(self, path: str, payload: dict) -> Optional[dict]:
        if not self.base_url:
            return None
        body = json.dumps(payload).encode("utf-8")
        # This control call intentionally bypasses HTTP_PROXY/HTTPS_PROXY: the
        # forwarder is the control-plane endpoint, not an upstream data proxy.
        try:
            with requests.Session() as session:
                session.trust_env = False
                resp = session.post(
                    f"{self.base_url}{path}", data=body,
                    headers={"Content-Type": "application/json"},
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                return resp.json()
        except (requests.RequestException, ValueError):
            return None

    def acquire(self, exclude: Optional[list[int]] = None) -> Optional[Acquisition]:
        body = self._post("/acquire", {"source": PROXY_SOURCE, "exclude": exclude or []})
        if body is None:
            return None
        return Acquisition(
            upstream_url=body.get("upstream_url"),
            addr_id=body.get("addr_id"),
            provider=body.get("provider"),
        )

    def release(self, acq: Acquisition, outcome: str) -> None:
        if not self.enabled or acq.addr_id is None:
            return
        self._post("/release", {
            "source": PROXY_SOURCE,
            "addr_id": acq.addr_id,
            "provider": acq.provider,
            "outcome": outcome,
        })


class _FetchLogWriter:
    """Best-effort writer for the scraw ``fetch_log`` table.

    ``FD_OPEN_DATA_MCP_DATABASE_URL`` (or ``FD_PROXY_FETCH_LOG_DSN``) points at
    the existing master ``fd_open_data`` database. Logging is deliberately
    non-blocking: a missing/unreachable observability database must not stop an
    NBS ingestion run.
    """

    def __init__(self, logger: Optional[Callable[[str], None]] = None) -> None:
        self.log = logger or (lambda _msg: None)
        self.dsn = os.environ.get("FD_PROXY_FETCH_LOG_DSN") or os.environ.get(
            "FD_OPEN_DATA_MCP_DATABASE_URL"
        )
        self.conn = None
        self.disabled = False

    @staticmethod
    def _psycopg_dsn(url: str) -> str:
        return url.replace("postgresql+psycopg2://", "postgresql://", 1)

    def record(
        self,
        command: str,
        proxy_id: Optional[int],
        classification: str,
        latency_ms: int,
        status: str,
        detail: Optional[str],
    ) -> None:
        if not self.dsn or self.disabled:
            return
        try:
            if self.conn is None or getattr(self.conn, "closed", True):
                import psycopg2

                self.conn = psycopg2.connect(self._psycopg_dsn(self.dsn))
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO fetch_log
                        (source, latency_ms, status, detail, proxy_id,
                         classification, real_source, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                    """,
                    (
                        PROXY_SOURCE,
                        max(0, int(latency_ms)),
                        status,
                        f"command={command}; {detail}" if detail else f"command={command}",
                        proxy_id,
                        classification,
                        "NBS",
                    ),
                )
            self.conn.commit()
        except Exception as exc:  # observability must never block ingestion
            if self.conn is not None:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
            self.disabled = True
            self.log(f"警告：fetch_log 写入失败，后续请求不再重试：{type(exc).__name__}: {exc}")

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None


class NBSError(RuntimeError):
    """NBS 客户端错误基类。"""


class NBSBlockedError(NBSError):
    """403/WAF 拦截（如 ``reason:UrlACL``）——不可重试，需换国内出口。"""


class NBSTransientError(NBSError):
    """可重试的瞬时错误（5xx/408/429/连接超时）。"""


@dataclass
class Node:
    """GetTree 节点。"""

    id: str
    name: str
    pid: Optional[str] = None
    is_parent: bool = False


@dataclass
class Indicator:
    """叶子指标。

    ``code`` 在新接口下是目录里的指标 UUID，在 easyquery 下是 zbcode。
    ``catalog_id`` 只在新区接口下有值（esData 请求要带 ``cid``）。
    """

    code: str
    name: str
    dbcode: str
    unit: Optional[str] = None
    parent_code: Optional[str] = None
    catalog_id: Optional[str] = None


@dataclass
class Observation:
    """归一化观测。"""

    indicator_code: str
    indicator_name: Optional[str]
    dbcode: str
    period: str
    value: Optional[Decimal] = None
    unit: Optional[str] = None
    region_code: Optional[str] = None
    region_name: Optional[str] = None
    raw: dict = field(default_factory=dict)


def proxies_from_env() -> Optional[dict]:
    http = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    https = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if http or https:
        return {"http": http or "", "https": https or https or ""}
    return None


def parse_value(raw: Any) -> Optional[Decimal]:
    """``strdata``/``data`` 容错解析：空、非数值、负数占位 -> None。"""
    if raw is None:
        return None
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return Decimal(str(raw))
    s = str(raw).strip()
    if not s or s in ("-", "--", "…", "None", "null", "nan"):
        return None
    s = s.replace(",", "")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


_YEAR_RE = re.compile(r"^(\d{4})")


def period_year(period: str) -> Optional[int]:
    m = _YEAR_RE.match(period or "")
    return int(m.group(1)) if m else None


def window_start_period(dbcode: str, years: int, now: Optional[int] = None) -> str:
    """滚动窗口起始期间：近 ``years`` 个自然年（含今年）的首期。"""
    freq = DBCODE_FREQ.get(dbcode, "A")
    this_year = now if now is not None else time.localtime().tm_year
    start_year = this_year - max(1, years) + 1
    if freq == "M":
        return f"{start_year}01"
    if freq == "Q":
        return f"{start_year}A"
    return f"{start_year}"


# ---------------------------------------------------------------------------
# 期间格式互转（旧 easyquery 记法 <-> 新接口 YY/MM/SS 记法）
# ---------------------------------------------------------------------------
def to_new_period(period: Optional[str], freq: str) -> Optional[str]:
    """旧记法 -> 新接口期间码。

    ``A``：``2022`` -> ``2022YY``；``M``：``202201`` -> ``202201MM``；
    ``Q``：``2022A`` -> ``202201SS``（``2022``/``202201`` 也接受）。已经是
    ``YY``/``MM``/``SS`` 后缀的原样返回。
    """
    if not period:
        return None
    p = str(period).strip()
    if p.endswith(("YY", "MM", "SS")):
        return p
    if freq == "A":
        if re.fullmatch(r"\d{4}", p):
            return f"{p}YY"
    elif freq == "M":
        if re.fullmatch(r"\d{6}", p):
            return f"{p}MM"
    elif freq == "Q":
        m = re.fullmatch(r"(\d{4})([A-Da-d])", p)
        if m:
            q = QUARTER_LETTERS.index(m.group(2).upper()) + 1
            return f"{m.group(1)}{q:02d}SS"
        if re.fullmatch(r"\d{6}", p):
            return f"{p}SS"
    raise NBSError(f"无法把期间 {period!r} 转成新接口格式（freq={freq}）")


def from_new_period(code: Optional[str]) -> str:
    """新接口期间码 -> 旧记法（落库/断点续跑沿用同一套期间字符串）。

    ``2025YY`` -> ``2025``；``202608MM`` -> ``202608``；``202602SS`` -> ``2026B``。
    """
    c = str(code or "").strip()
    if c.endswith(("YY", "MM")):
        return c[:-2]
    if c.endswith("SS"):
        year, q = c[:4], c[4:6]
        if q.isdigit() and 1 <= int(q) <= 4:
            return f"{year}{QUARTER_LETTERS[int(q) - 1]}"
        return c[:-2]
    return c


def new_period_now(freq: str, now: Optional[time.struct_time] = None) -> str:
    """当前期间的新接口期间码（区间的右端，保证"取到最新"）。"""
    t = now or time.localtime()
    if freq == "M":
        return f"{t.tm_year}{t.tm_mon:02d}MM"
    if freq == "Q":
        return f"{t.tm_year}{(t.tm_mon - 1) // 3 + 1:02d}SS"
    return f"{t.tm_year}YY"


def normalize_region_code(code: Optional[str]) -> Optional[str]:
    """新接口地区码 ``110000000000`` -> ``110000``（GB/T 2260）；全国伪码 -> None。"""
    if not code:
        return None
    c = str(code).strip()
    if c == NATIONAL_AREA_CODE:
        return None
    if len(c) == 12 and c.endswith("000000"):
        return c[:6]
    return c


def parse_wdnodes(payload: dict) -> tuple[dict, dict, dict]:
    """返回 (zb 码->名, reg 码->名, zb 码->单位/元信息)。"""
    zb_map: dict[str, str] = {}
    reg_map: dict[str, str] = {}
    zb_meta: dict[str, dict] = {}
    returndata = payload.get("returndata") or {}
    for node in returndata.get("wdnodes") or []:
        wdcode = node.get("wdcode")
        for item in node.get("nodes") or []:
            code = item.get("code")
            if not code:
                continue
            name = item.get("cname") or item.get("name") or code
            if wdcode == "zb":
                zb_map[code] = name
                zb_meta[code] = {k: item[k] for k in ("unit", "exp", "dw") if item.get(k)}
            elif wdcode == "reg":
                reg_map[code] = name
    return zb_map, reg_map, zb_meta


def parse_response(
    payload: dict,
    dbcode: str,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
) -> list[Observation]:
    """把一次 QueryData 响应解析为观测列表（含地区维度、容错缺值）。

    期间的 sj valuecode 语义是"起始期间"，但响应里可能混入更早/更新的稀疏节点，
    因此按 ``start_period``/``end_period`` 的年窗口做一次防御性过滤。
    """
    if not isinstance(payload, dict):
        raise NBSError(f"响应不是 JSON 对象：{type(payload).__name__}")
    code = payload.get("returncode")
    if code not in (None, 200, "200"):
        raise NBSError(f"NBS 返回码 {code}: {payload.get('errormsg') or '未知错误'}")

    zb_map, reg_map, zb_meta = parse_wdnodes(payload)
    returndata = payload.get("returndata") or {}
    start_year = period_year(start_period) if start_period else None
    end_year = period_year(end_period) if end_period else None

    out: list[Observation] = []
    for node in returndata.get("datanodes") or []:
        wd = {w.get("wdcode"): w.get("valuecode") for w in (node.get("wds") or [])}
        zb = wd.get("zb") or ""
        sj = wd.get("sj") or ""
        if not sj:
            continue
        year = period_year(sj)
        if start_year is not None and year is not None and year < start_year:
            continue
        if end_year is not None and year is not None and year > end_year:
            continue

        data = node.get("data") or {}
        value = parse_value(data.get("strdata"))
        if value is None and data.get("hasdata") is not False:
            inner = data.get("data")
            if isinstance(inner, list) and inner:
                value = parse_value(inner[0])
            elif isinstance(inner, (str, int, float)):
                value = parse_value(inner)

        reg_code = wd.get("reg")
        meta = zb_meta.get(zb) or {}
        out.append(
            Observation(
                indicator_code=zb,
                indicator_name=zb_map.get(zb),
                dbcode=dbcode,
                period=str(sj),
                value=value,
                unit=meta.get("unit") or meta.get("dw"),
                region_code=reg_code,
                region_name=reg_map.get(reg_code) if reg_code else None,
                raw=node,
            )
        )
    return out


def parse_new_response(
    payload: dict,
    dbcode: str,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
) -> list[Observation]:
    """把一次 ``stream/esData`` 响应解析为观测列表。

    响应形态（实测）::

        {"success": true, "state": 20000, "message": "成功",
         "data": [{"code": "2025YY", "name": "2025年",
                   "values": [{"_id": "<指标UUID>", "_name": "国内生产总值",
                               "value": "1401879.2", "du_name": "亿元",
                               "da": "000000000000", "da_name": "全国"}]}]}

    地区列模式下（``showType=3``）用 ``areaCode``/``area`` 而不是 ``da``/``da_name``。
    期间 ``code`` 归一化回旧记法（见 ``from_new_period``）；缺值（``value`` 为
    ``""``）保留为 ``value=None``，与 easyquery 通路一致。
    """
    if not isinstance(payload, dict):
        raise NBSError(f"响应不是 JSON 对象：{type(payload).__name__}")
    if payload.get("success") is False:
        raise NBSError(
            f"新接口返回失败（state={payload.get('state')}）：{payload.get('message') or '未知错误'}"
        )

    start_year = period_year(start_period) if start_period else None
    end_year = period_year(end_period) if end_period else None

    out: list[Observation] = []
    for row in payload.get("data") or []:
        if not isinstance(row, dict):
            continue
        period = from_new_period(row.get("code"))
        if not period:
            continue
        year = period_year(period)
        if start_year is not None and year is not None and year < start_year:
            continue
        if end_year is not None and year is not None and year > end_year:
            continue
        for val in row.get("values") or []:
            if not isinstance(val, dict):
                continue
            region_code = normalize_region_code(val.get("areaCode") or val.get("da"))
            region_name = val.get("area") or val.get("da_name") or None
            if region_code is None:
                region_name = None
            out.append(
                Observation(
                    indicator_code=str(val.get("_id") or ""),
                    indicator_name=val.get("_name"),
                    dbcode=dbcode,
                    period=period,
                    value=parse_value(val.get("value")),
                    unit=val.get("du_name") or None,
                    region_code=region_code,
                    region_name=region_name,
                    raw=val,
                )
            )
    return out


class _BaseClient:
    """限速 / 重试 / 计数 的公共底座（两条通路共用）。

    ``sleep`` 可注入（测试用）；``min_interval`` 是任意两次请求之间的最小间隔秒数。
    """

    endpoint_name = "base"
    endpoint_url = ""

    def __init__(
        self,
        min_interval: float = 1.0,
        retries: int = 3,
        timeout: float = 30.0,
        verify: bool = False,
        session: Optional[requests.Session] = None,
        sleep: Callable[[float], None] = time.sleep,
        logger: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.min_interval = max(0.0, min_interval)
        self.retries = max(1, retries)
        self.timeout = timeout
        self.verify = verify
        self.session = session or requests.Session()
        self.session.headers.update(BROWSER_HEADERS)
        self.proxies = proxies_from_env()
        self.sleep = sleep
        self.log = logger or (lambda _msg: None)
        self._last_request_at: Optional[float] = None
        self.request_count = 0
        self.error_count = 0
        self.forwarder = _ForwarderClient(timeout=min(3.0, timeout))
        self.fetch_log_writer = _FetchLogWriter(logger=self.log)
        if self.forwarder.enabled:
            # The forwarder owns the shared Redis QPS gate. A second local delay
            # would unnecessarily serialize workers that are already globally gated.
            self.min_interval = 0.0

    def close(self) -> None:
        """Release the HTTP session and best-effort fetch-log connection."""
        try:
            self.session.close()
        finally:
            self.fetch_log_writer.close()

    def _throttle(self) -> None:
        if self._last_request_at is None:
            return
        wait = self.min_interval - (time.monotonic() - self._last_request_at)
        if wait > 0:
            self.sleep(wait)

    def _blocked(self, resp: requests.Response) -> NBSBlockedError:
        reason = "WAF 403"
        m = re.search(r"reason:([A-Za-z]+)", resp.text or "")
        if m:
            reason = m.group(1)
        return NBSBlockedError(
            f"{self.endpoint_name} 返回 403（reason:{reason}）——NBS 按出口 IP/URL 拦截，"
            "需从国内出口且未被封的机器执行"
        )

    @staticmethod
    def _classify_status(status_code: int) -> str:
        if status_code == 403:
            return "ban"
        if status_code in (408, 429) or 500 <= status_code < 600:
            return "transient"
        if 200 <= status_code < 300:
            return "ok"
        return "blocked"

    def _execute_once(
        self,
        method: str,
        url: str,
        *,
        params: Optional[dict] = None,
        data: Optional[dict] = None,
        body: Optional[dict] = None,
        headers: Optional[dict] = None,
        command: str,
    ) -> requests.Response:
        """Send one upstream attempt, optionally through the proxy forwarder."""
        self._throttle()
        started = time.monotonic()
        self.request_count += 1
        acq: Optional[Acquisition] = None
        proxy_id: Optional[int] = None
        try:
            if self.forwarder.enabled:
                acq = self.forwarder.acquire()
                if acq is None or not acq.upstream_url:
                    detail = "forwarder acquire failed" if acq is None else "forwarder returned no upstream"
                    self.fetch_log_writer.record(
                        command=command,
                        proxy_id=None,
                        classification="transient",
                        latency_ms=int((time.monotonic() - started) * 1000),
                        status="acquire_error",
                        detail=detail,
                    )
                    raise NBSTransientError(f"{detail}；proxy 模式禁止回退直连")
                proxy_id = acq.addr_id
                request_proxies = {"http": acq.upstream_url, "https": acq.upstream_url}
            else:
                request_proxies = self.proxies

            request_kwargs: dict[str, Any] = {
                "params": params,
                "headers": headers,
                "verify": self.verify,
                "timeout": self.timeout,
            }
            if method.upper() == "POST":
                if body is not None:
                    request_kwargs["json"] = body
                elif data is not None:
                    request_kwargs["data"] = data

            try:
                resp = self.session.request(
                    method,
                    url,
                    proxies=request_proxies,
                    **request_kwargs,
                )
            except requests.RequestException:
                if acq is not None:
                    self.forwarder.release(acq, "transient")
                self.fetch_log_writer.record(
                    command=command,
                    proxy_id=proxy_id,
                    classification="transient",
                    latency_ms=int((time.monotonic() - started) * 1000),
                    status="error",
                    detail="upstream request exception",
                )
                raise

            classification = self._classify_status(resp.status_code)
            outcome = classification if classification in ("ok", "transient", "ban") else "blocked"
            if acq is not None:
                self.forwarder.release(acq, outcome)
            self.fetch_log_writer.record(
                command=command,
                proxy_id=proxy_id,
                classification=classification,
                latency_ms=int((time.monotonic() - started) * 1000),
                status=str(resp.status_code),
                detail=resp.text[:500] if classification != "ok" else None,
            )
            return resp
        finally:
            self._last_request_at = time.monotonic()


class NBSClient(_BaseClient):
    """带限速/重试的 easyquery 客户端（旧通路，2026 起多被 403 UrlACL 拦截）。

    ``sleep`` 可注入（测试用）；``min_interval`` 是任意两次请求之间的最小间隔秒数。
    """

    endpoint_name = "easyquery"
    endpoint_url = EASYQUERY_URL

    # ---- 传输层 -----------------------------------------------------------
    def _post_once(self, params: dict) -> requests.Response:
        return self._execute_once(
            "POST",
            EASYQUERY_URL,
            data=params,
            headers=BROWSER_HEADERS,
            command=f"easyquery:{params.get('m', 'QueryData')}",
        )

    def _request(self, **params: Any) -> Any:
        """POST easyquery 并返回 JSON；瞬时错误退避重试，403 直接失败。"""
        params.setdefault("m", "QueryData")
        params.setdefault("rowcode", "zb")
        params.setdefault("colcode", "sj")
        params.setdefault("wds", "[]")
        params.setdefault("dfwds", "[]")
        delay = max(1.0, self.min_interval)
        last_err: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            params["k1"] = str(int(time.time() * 1000))
            try:
                resp = self._post_once(params)
            except NBSTransientError as exc:
                last_err = exc
                self.error_count += 1
                self.log(f"  请求异常（第 {attempt}/{self.retries} 次）：{type(exc).__name__}: {exc}")
                if attempt < self.retries:
                    self.sleep(delay)
                    delay *= 2
                    continue
                raise
            except requests.RequestException as exc:  # 连接/超时
                last_err = exc
                self.error_count += 1
                self.log(f"  请求异常（第 {attempt}/{self.retries} 次）：{type(exc).__name__}: {exc}")
                if attempt < self.retries:
                    self.sleep(delay)
                    delay *= 2
                    continue
                raise NBSTransientError(f"请求失败已重试 {self.retries} 次：{exc}") from exc

            if resp.status_code == 403:
                self.error_count += 1
                raise self._blocked(resp)
            if resp.status_code in (408, 429) or 500 <= resp.status_code < 600:
                self.error_count += 1
                last_err = NBSTransientError(f"HTTP {resp.status_code}")
                self.log(f"  HTTP {resp.status_code}（第 {attempt}/{self.retries} 次），退避 {delay:.1f}s")
                if attempt < self.retries:
                    self.sleep(delay)
                    delay *= 2
                    continue
                raise NBSTransientError(f"重试 {self.retries} 次后仍返回 HTTP {resp.status_code}")
            if resp.status_code != 200:
                self.error_count += 1
                raise NBSError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            if not resp.text.strip():
                # NBS 偶发空响应：按瞬时错误处理
                self.error_count += 1
                last_err = NBSTransientError("空响应体")
                if attempt < self.retries:
                    self.sleep(delay)
                    delay *= 2
                    continue
                raise NBSTransientError("重试后仍返回空响应体")
            try:
                return resp.json()
            except ValueError as exc:
                self.error_count += 1
                raise NBSError(f"响应不是 JSON：{resp.text[:200]}") from exc
        raise NBSTransientError(str(last_err) if last_err else "请求失败")

    # ---- 业务层 -----------------------------------------------------------
    def get_tree(self, node_id: str = "zb", dbcode: str = "hgnd") -> list[Node]:
        payload = self._request(m="GetTree", dbcode=dbcode, id=node_id, wds="[]", dfwds="[]")
        nodes = payload if isinstance(payload, list) else (payload.get("nodes") or [])
        out: list[Node] = []
        for n in nodes:
            nid = n.get("id")
            if not nid:
                continue
            out.append(
                Node(
                    id=nid,
                    name=n.get("cname") or n.get("name") or nid,
                    pid=n.get("pid"),
                    is_parent=bool(n.get("isParent")),
                )
            )
        return out

    def discover_indicators(
        self,
        dbcode: str = "hgnd",
        max_nodes: Optional[int] = None,
        max_depth: int = 8,
    ) -> list[Indicator]:
        """GetTree 递归发现叶子指标（``isParent=false``）。

        ``max_nodes`` 限定指标数（测试用）；``max_depth`` 防止意外深树。
        """
        found: list[Indicator] = []
        queue: list[tuple[str, int, Optional[str]]] = [("zb", 0, None)]
        seen: set[str] = set()
        while queue:
            node_id, depth, parent = queue.pop(0)
            if node_id in seen or depth > max_depth:
                continue
            seen.add(node_id)
            for node in self.get_tree(node_id, dbcode):
                if node.is_parent:
                    queue.append((node.id, depth + 1, node.id))
                    continue
                found.append(
                    Indicator(code=node.id, name=node.name, dbcode=dbcode, parent_code=node.pid or parent)
                )
                if max_nodes is not None and len(found) >= max_nodes:
                    return found
        return found

    def query_data(
        self,
        zbcode: str,
        dbcode: str = "hgnd",
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        regcode: Optional[str] = None,
    ) -> list[Observation]:
        """查一个指标：``dfwds`` 带 zb + sj（sj 为起始期间）。"""
        dfwds = [{"wdcode": "zb", "valuecode": zbcode}]
        if start_period:
            dfwds.append({"wdcode": "sj", "valuecode": start_period})
        wds = [{"wdcode": "reg", "valuecode": regcode}] if regcode else []
        payload = self._request(
            m="QueryData",
            dbcode=dbcode,
            wds=json.dumps(wds, ensure_ascii=False),
            dfwds=json.dumps(dfwds, ensure_ascii=False),
        )
        return parse_response(payload, dbcode, start_period=start_period, end_period=end_period)

    def probe(self, dbcode: str = "hgnd", node_id: str = "zb") -> dict:
        """最小连通性探针：一次 GetTree，返回摘要（供 --probe 用）。"""
        nodes = self.get_tree(node_id, dbcode)
        return {
            "endpoint": self.endpoint_name,
            "url": self.endpoint_url,
            "dbcode": dbcode,
            "dbname": DBCODES.get(dbcode, dbcode),
            "node": node_id,
            "node_count": len(nodes),
            "sample": [{"id": n.id, "name": n.name, "isParent": n.is_parent} for n in nodes[:10]],
            "requests": self.request_count,
        }


# ---------------------------------------------------------------------------
# 自测 fixture：真实录制响应的形态（结构取自 fd-cn-report/test_fixtures/ministry/
# nbs_gdp.json 的 datanodes/wdnodes；全国年度 GDP 数值为 NBS 已发布值，亿元）。
# 仅在出口被 403 拦截、无法真跑时用于验证解析 + 幂等落库路径；落库行的 raw_json
# 带 `_source=selftest_fixture`，便于一键清理：
#   DELETE FROM nbs_observation WHERE raw_json->>'_source' = 'selftest_fixture';
# ---------------------------------------------------------------------------
SELF_TEST_FIXTURES: dict[str, dict] = {
    "hgnd_national": {
        "parser": "easyquery",
        "dbcode": "hgnd",
        "response": {
            "returncode": 200,
            "returndata": {
                "datanodes": [
                    {
                        "code": f"A0201_{p}",
                        "wds": [
                            {"wdcode": "zb", "valuecode": "A0201"},
                            {"wdcode": "sj", "valuecode": p},
                        ],
                        "data": {"hasdata": True, "data": [v], "strdata": str(v), "_source": "selftest_fixture"},
                    }
                    for p, v in (
                        ("2021", 1149237.0),
                        ("2022", 1210207.2),
                        ("2023", 1260582.1),
                        ("2024", 1349084.0),
                    )
                ]
                + [
                    {
                        "code": "A0201_2025",
                        "wds": [
                            {"wdcode": "zb", "valuecode": "A0201"},
                            {"wdcode": "sj", "valuecode": "2025"},
                        ],
                        "data": {"hasdata": False, "strdata": "", "_source": "selftest_fixture"},
                    }
                ],
                "wdnodes": [
                    {
                        "wdcode": "zb",
                        "nodes": [{"code": "A0201", "cname": "国内生产总值", "unit": "亿元"}],
                    },
                    {
                        "wdcode": "sj",
                        "nodes": [
                            {"code": p, "cname": f"{p}年"}
                            for p in ("2021", "2022", "2023", "2024", "2025")
                        ],
                    },
                ],
            },
        },
    },
    "fsnd_provincial": {
        "parser": "easyquery",
        "dbcode": "fsnd",
        "response": {
            "returncode": 200,
            "returndata": {
                "datanodes": [
                    {
                        "code": f"A0201_{reg}_{p}",
                        "wds": [
                            {"wdcode": "zb", "valuecode": "A0201"},
                            {"wdcode": "reg", "valuecode": reg},
                            {"wdcode": "sj", "valuecode": p},
                        ],
                        "data": {"hasdata": False, "strdata": "", "_source": "selftest_fixture"},
                    }
                    for reg in ("110000", "440000")
                    for p in ("2023", "2024")
                ],
                "wdnodes": [
                    {
                        "wdcode": "zb",
                        "nodes": [{"code": "A0201", "cname": "地区生产总值", "unit": "亿元"}],
                    },
                    {
                        "wdcode": "reg",
                        "nodes": [
                            {"code": "110000", "cname": "北京市"},
                            {"code": "440000", "cname": "广东省"},
                        ],
                    },
                    {
                        "wdcode": "sj",
                        "nodes": [{"code": p, "cname": f"{p}年"} for p in ("2023", "2024")],
                    },
                ],
            },
        },
    },
    # 新接口（/dg/website）实测录制：全国年度 GDP，一次 esData 返回多个期间。
    # 期间码 YY 后缀 -> 落库归一化为 "2025"；value 为 NBS 已发布值（亿元）。
    "new_hgnd_national": {
        "parser": "new",
        "dbcode": "hgnd",
        "response": {
            "success": True,
            "state": 20000,
            "message": "成功",
            "data": [
                {
                    "code": f"{p}YY",
                    "name": f"{p}年",
                    "values": [
                        {
                            "_id": "7dc6a2ee6c614960b7059991e0cc4d96",
                            "_name": "国内生产总值",
                            "value": v,
                            "du_name": "亿元",
                            "da": "000000000000",
                            "da_name": "全国",
                            "_source": "selftest_fixture",
                        }
                    ],
                }
                for p, v in (
                    ("2022", "1234029.4"),
                    ("2023", "1294271.7"),
                    ("2024", "1348066.2"),
                    ("2025", "1401879.2"),
                    ("2026", ""),
                )
            ],
        },
    },
    # 新接口地区维度（showType=3）：一次返回目录内全部地区，值里是 area/areaCode。
    "new_fsnd_provincial": {
        "parser": "new",
        "dbcode": "fsnd",
        "response": {
            "success": True,
            "state": 20000,
            "message": "成功",
            "data": [
                {
                    "code": f"{p}YY",
                    "name": f"{p}年",
                    "values": [
                        {
                            "_id": "aff57de5ee994283974705914fbed246",
                            "_name": "国内生产总值",
                            "value": v,
                            "du_name": "亿元",
                            "area": area,
                            "areaCode": code,
                            "_source": "selftest_fixture",
                        }
                        for code, area, v in (
                            ("110000000000", "北京市", "52073.4"),
                            ("440000000000", "广东省", "146482.6"),
                        )
                    ],
                }
                for p in ("2025",)
            ],
        },
    },
}


# ---------------------------------------------------------------------------
# 新接口客户端（2026 改版后 /dg/website SPA 的后端）
# ---------------------------------------------------------------------------
class NewNBSClient(_BaseClient):
    """``/dg/website/publicrelease`` 免鉴权 JSON 接口客户端。

    与 easyquery 的差异：指标与目录用不透明 UUID 标识；取数按「指标 id + 期间区间」，
    一次请求可返回多个期间（区间端点用 ``YY``/``MM``/``SS`` 后缀）；地区维度走
    ``daCatalogId`` + ``showType=3``（一次拿全地区）。

    目录树 / 地区目录按 dbcode 与 catalog_id 缓存，避免重复请求。
    """

    endpoint_name = "new"
    endpoint_url = NEW_ES_DATA_URL

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.session.headers["Referer"] = NEW_PAGE_REFERER
        self._root_ids: dict[str, str] = {}
        self._da_catalogs: dict[str, str] = {}
        self._area_names: dict[str, dict[str, str]] = {}

    # ---- 传输层 -----------------------------------------------------------
    @staticmethod
    def _parse_json_any(text: str) -> Any:
        """esData 名义上是流式响应；正常返回完整 JSON，这里兼容 NDJSON 分片。"""
        text = (text or "").strip()
        if not text:
            raise NBSError("空响应体")
        try:
            return json.loads(text)
        except ValueError:
            pass
        for line in reversed([ln.strip() for ln in text.splitlines() if ln.strip()]):
            try:
                return json.loads(line)
            except ValueError:
                continue
        raise NBSError(f"响应不是 JSON：{text[:200]}")

    def _fetch(self, method: str, url: str, *, params: Optional[dict] = None, body: Optional[dict] = None) -> Any:
        """一次请求 + 瞬时错误退避重试；403/WAF 直接失败。"""
        delay = max(1.0, self.min_interval)
        last_err: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                headers = {"Content-Type": "application/json;charset=UTF-8"} if method.upper() == "POST" else None
                resp = self._execute_once(
                    method,
                    url,
                    params=params,
                    body=body,
                    headers=headers,
                    command=f"new:{method.upper()}:{url}",
                )
            except NBSTransientError as exc:
                last_err = exc
                self.error_count += 1
                self.log(f"  请求异常（第 {attempt}/{self.retries} 次）：{type(exc).__name__}: {exc}")
                if attempt < self.retries:
                    self.sleep(delay)
                    delay *= 2
                    continue
                raise
            except requests.RequestException as exc:  # 连接/超时
                last_err = exc
                self.error_count += 1
                self.log(f"  请求异常（第 {attempt}/{self.retries} 次）：{type(exc).__name__}: {exc}")
                if attempt < self.retries:
                    self.sleep(delay)
                    delay *= 2
                    continue
                raise NBSTransientError(f"请求失败已重试 {self.retries} 次：{exc}") from exc

            if resp.status_code == 403:
                self.error_count += 1
                raise self._blocked(resp)
            if resp.status_code in (408, 429) or 500 <= resp.status_code < 600:
                self.error_count += 1
                last_err = NBSTransientError(f"HTTP {resp.status_code}")
                self.log(f"  HTTP {resp.status_code}（第 {attempt}/{self.retries} 次），退避 {delay:.1f}s")
                if attempt < self.retries:
                    self.sleep(delay)
                    delay *= 2
                    continue
                raise NBSTransientError(f"重试 {self.retries} 次后仍返回 HTTP {resp.status_code}")
            if resp.status_code != 200:
                self.error_count += 1
                raise NBSError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            return self._parse_json_any(resp.text)
        raise NBSTransientError(str(last_err) if last_err else "请求失败")

    # ---- 目录树 -----------------------------------------------------------
    @staticmethod
    def _tab_code(dbcode: str) -> str:
        code = NEW_TAB_CODE.get(dbcode)
        if not code:
            raise NBSError(
                f"新接口没有 {dbcode}（{DBCODES.get(dbcode, dbcode)}）对应的页签"
                f"（可用：{', '.join(sorted(NEW_TAB_CODE))}）；请改用 --endpoint easyquery"
            )
        return code

    def query_tree(self, pid: str = "", dbcode: str = "hgnd") -> list[Node]:
        """一级目录 / 某目录的子目录。``pid=""`` 取根。"""
        payload = self._fetch(
            "GET", NEW_TREE_URL, params={"pid": pid or "", "code": self._tab_code(dbcode)}
        )
        nodes = (payload or {}).get("data") or []
        out: list[Node] = []
        for n in nodes:
            nid = n.get("_id")
            if not nid:
                continue
            out.append(
                Node(
                    id=nid,
                    name=n.get("_name") or nid,
                    pid=n.get("treeinfo_pid"),
                    is_parent=not n.get("isLeaf", False),
                )
            )
        return out

    def get_root_id(self, dbcode: str = "hgnd") -> str:
        """该 dbcode 的根目录 id（esData 的 ``rootId``）。"""
        if dbcode not in self._root_ids:
            nodes = self.query_tree("", dbcode)
            if not nodes:
                raise NBSError(f"新接口目录树为空（dbcode={dbcode}）")
            self._root_ids[dbcode] = nodes[0].id
        return self._root_ids[dbcode]

    def query_indicators(self, catalog_id: str, name: str = "") -> list[Indicator]:
        """某叶子目录下的指标（``/new/queryIndicatorsByCid``）。"""
        payload = self._fetch(
            "GET", NEW_INDICATORS_URL, params={"cid": catalog_id, "dt": "", "name": name}
        )
        items = ((payload or {}).get("data") or {}).get("list") or []
        out: list[Indicator] = []
        for it in items:
            iid = it.get("_id")
            if not iid:
                continue
            out.append(
                Indicator(
                    code=iid,
                    name=it.get("i_showname") or it.get("_name") or iid,
                    dbcode="",
                    unit=it.get("du_name") or None,
                    parent_code=catalog_id,
                    catalog_id=catalog_id,
                )
            )
        return out

    def discover_indicators(
        self, dbcode: str = "hgnd", max_nodes: Optional[int] = None, max_depth: int = 8
    ) -> list[Indicator]:
        """BFS 走目录树，展开叶子目录下的指标。

        ``max_nodes`` 一达到就停（``--limit`` 时只走前几个叶子目录，省请求）。
        """
        self._tab_code(dbcode)
        found: list[Indicator] = []
        queue: list[tuple[str, int]] = [("", 0)]
        seen: set[str] = set()
        leaves = 0
        while queue:
            node_id, depth = queue.pop(0)
            if node_id in seen or depth > max_depth:
                continue
            seen.add(node_id)
            for node in self.query_tree(node_id, dbcode):
                if node.is_parent:
                    queue.append((node.id, depth + 1))
                    continue
                leaves += 1
                if leaves % 20 == 0:
                    self.log(f"  ...已走 {leaves} 个叶子目录，累计 {len(found)} 个指标，{self.request_count} 次请求")
                for ind in self.query_indicators(node.id):
                    ind.dbcode = dbcode
                    found.append(ind)
                    if max_nodes is not None and len(found) >= max_nodes:
                        return found
        return found

    # ---- 地区目录 ---------------------------------------------------------
    def get_area_catalog(self, catalog_id: str) -> str:
        """指标目录 -> 地区目录 id（「全部地区」），供 ``daCatalogId`` 用。"""
        if catalog_id not in self._da_catalogs:
            payload = self._fetch("GET", NEW_DA_TREE_URL, params={"indicatorCid": catalog_id})
            nodes = (payload or {}).get("data") or []
            if not nodes:
                raise NBSError(f"新接口没有返回地区目录（indicatorCid={catalog_id}）")
            pick = next((n for n in nodes if n.get("_name") == "全部地区"), nodes[0])
            self._da_catalogs[catalog_id] = pick.get("_id") or ""
        return self._da_catalogs[catalog_id]

    def list_areas(self, da_catalog_id: str) -> list[dict]:
        """地区目录下的地区清单（``[{name_value, show_name, ...}]``）。"""
        payload = self._fetch("GET", NEW_DA_LIST_URL, params={"daCid": da_catalog_id})
        return (payload or {}).get("data") or []

    def get_area_names(self, da_catalog_id: str) -> dict[str, str]:
        """地区码 -> 中文名（``getDasByDaCatalogId`` 的 ``show_name``）。

        esData 的 ``area`` 字段对部分目录会返回英文名（如 ``Beijing``），这里统一用
        地区目录里的中文名覆盖。按 ``da_catalog_id`` 缓存（实测各指标目录共用同一个
        地区目录，整轮只会多花一次请求）。
        """
        if da_catalog_id not in self._area_names:
            try:
                items = self.list_areas(da_catalog_id)
            except NBSError as exc:
                self.log(f"  地区名称获取失败（{exc}），沿用 esData 返回的地区名")
                items = []
            self._area_names[da_catalog_id] = {
                normalize_region_code(it.get("name_value")) or "": (
                    it.get("show_name") or it.get("name_text") or ""
                )
                for it in items
                if it.get("name_value")
            }
        return self._area_names[da_catalog_id]

    # ---- 取数 -------------------------------------------------------------
    def build_dt(
        self, dbcode: str, start_period: Optional[str], end_period: Optional[str] = None
    ) -> str:
        """构造 ``dt`` 区间串。两端缺省分别是「全部历史起点」与「当前期间」。"""
        freq = DBCODE_FREQ.get(dbcode, "A")
        start = (
            to_new_period(start_period, freq)
            if start_period
            else to_new_period(FULL_START_PERIOD[freq], freq)
        )
        end = to_new_period(end_period, freq) if end_period else new_period_now(freq)
        return f"{start}-{end}"

    def query_data(
        self,
        zbcode: str,
        dbcode: str = "hgnd",
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        regcode: Optional[str] = None,
        catalog_id: Optional[str] = None,
    ) -> list[Observation]:
        """查一个指标的一段期间。

        ``catalog_id`` 必填（指标所属目录 id，作为 esData 的 ``cid``）；带地区维度的库
        自动走 ``daCatalogId`` + ``showType=3``，一次取回目录内全部地区。

        ``regcode`` 在新接口下是客户端过滤（接口本身整目录返回）。
        """
        if not catalog_id:
            raise NBSError(
                "新接口取数需要 catalog_id（指标所属目录 id）——"
                "请用 --scope tree 发现指标，或用 --indicators <目录id>:<指标id>"
            )
        dt = self.build_dt(dbcode, start_period, end_period)
        body: dict[str, Any] = {
            "cid": catalog_id,
            "indicatorIds": [zbcode],
            "daCatalogId": "",
            "das": [],
            "showType": 1,
            "dt": dt,
            "dts": [dt],
            "rootId": self.get_root_id(dbcode),
        }
        if dbcode in AREA_DBCODES:
            da_catalog_id = self.get_area_catalog(catalog_id)
            body["daCatalogId"] = da_catalog_id
            body["showType"] = 3
            area_names = self.get_area_names(da_catalog_id)
        else:
            body["das"] = [{"text": "全国", "value": NATIONAL_AREA_CODE}]
            area_names = {}
        payload = self._fetch("POST", NEW_ES_DATA_URL, body=body)
        observations = parse_new_response(
            payload, dbcode, start_period=start_period, end_period=end_period
        )
        for obs in observations:
            if obs.region_code and area_names.get(obs.region_code):
                obs.region_name = area_names[obs.region_code]
        if regcode:
            want = normalize_region_code(regcode) or regcode
            observations = [o for o in observations if o.region_code == want]
        return observations

    # ---- 探针 -------------------------------------------------------------
    def probe(self, dbcode: str = "hgnd", node_id: str = "zb") -> dict:
        """端到端探针：目录树 -> 叶子目录 -> 指标 -> 一次真实取数（供 --probe 用）。"""
        summary: dict[str, Any] = {
            "endpoint": self.endpoint_name,
            "url": self.endpoint_url,
            "dbcode": dbcode,
            "dbname": DBCODES.get(dbcode, dbcode),
            "tab_code": self._tab_code(dbcode),
        }
        top = self.query_tree("", dbcode)
        summary["root"] = {"id": top[0].id, "name": top[0].name} if top else None
        summary["top_catalogs"] = [{"id": n.id, "name": n.name} for n in top[:10]]
        if not top:
            return summary

        leaf: Optional[Node] = None
        for start in top[:3]:
            node = start
            for _ in range(4):
                if not node.is_parent:
                    leaf = node
                    break
                kids = self.query_tree(node.id, dbcode)
                if not kids:
                    break
                node = kids[0]
            if leaf:
                break
        if not leaf:
            summary["note"] = "没有找到叶子目录"
            return summary
        summary["leaf_catalog"] = {"id": leaf.id, "name": leaf.name}

        indicators = self.query_indicators(leaf.id)
        summary["indicator_count"] = len(indicators)
        if not indicators:
            summary["note"] = "叶子目录下没有指标"
            return summary
        target = indicators[0]
        summary["sample_indicator"] = {"id": target.code, "name": target.name, "unit": target.unit}

        observations = self.query_data(
            zbcode=target.code,
            dbcode=dbcode,
            start_period=window_start_period(dbcode, 3),
            catalog_id=leaf.id,
        )
        with_value = [o for o in observations if o.value is not None]
        summary["observations"] = len(observations)
        summary["observations_with_value"] = len(with_value)
        summary["sample"] = [
            {"period": o.period, "value": str(o.value), "unit": o.unit, "region": o.region_name}
            for o in sorted(with_value, key=lambda o: o.period, reverse=True)[:5]
        ]
        summary["requests"] = self.request_count
        return summary


def make_client(
    endpoint: str = "auto",
    *,
    min_interval: float = 1.0,
    retries: int = 3,
    timeout: float = 30.0,
    logger: Optional[Callable[[str], None]] = None,
) -> _BaseClient:
    """按 ``--endpoint`` 造客户端。

    ``new`` = 只用新接口；``easyquery`` = 只用旧接口；``auto`` = 先用新接口探一次
    （一次 queryIndexTreeAsync），失败再退回 easyquery，并把选择写进日志。
    """
    log = logger or (lambda _msg: None)
    kwargs = dict(min_interval=min_interval, retries=retries, timeout=timeout, logger=logger)
    if endpoint == "easyquery":
        return NBSClient(**kwargs)
    if endpoint == "new":
        return NewNBSClient(**kwargs)
    if endpoint != "auto":
        raise NBSError(f"未知 endpoint：{endpoint}")

    client = NewNBSClient(**kwargs)
    try:
        client.query_tree("", "hgnd")
    except NBSError as exc:
        client.close()
        if "proxy 模式禁止回退直连" in str(exc):
            raise
        log(f"新接口探测失败（{type(exc).__name__}: {exc}），回退 easyquery")
        return NBSClient(**kwargs)
    log("endpoint=auto -> 使用新接口 /dg/website（easyquery 已 403）")
    return client
