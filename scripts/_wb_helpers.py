"""fd-data-quality 执行器共享小工具：DSN 规范化、安全连接、分批与重试。

单独成模块的两个原因（都是实测踩到的坑，不属于某个执行器的业务逻辑）：

1. `fd-data-quality/.env.local` 里的口令是 **URL 百分号编码**写入的，而 psycopg2 2.9.12
   （libpq 14）把 DSN 串里的 userinfo 原样使用、不做 unquote —— 直接 connect(dsn) 会
   认证失败。这里统一 unquote 后再按 keyword 连接，并在认证失败时回退到「原样口令」
   以兼容未编码的 DSN（生产 Secret 可能是任意一种）。
2. Windows PG（<db-host>，PostgreSQL 18.6 on x86_64-windows）在**客户端编码协商之前**
   的错误消息是 GBK 编码的（如「用户 "postgres" Password 认证失败」）。psycopg2 按 utf-8
   解码这段字节，抛 UnicodeDecodeError 把真实原因盖掉。这里把原始字节按 GBK 解回来，
   让失败原因可见（且消息里不含口令）。

本模块不打印、不返回口令原文以外的任何敏感信息。
"""
from __future__ import annotations

import pathlib
import sys
import time
import urllib.parse

import psycopg2

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _env import dsn as _env_dsn  # noqa: E402  统一走 _env 读环境变量，不硬编码口令

APPLICATION_NAME = "fd-data-quality"
DEFAULT_CONNECT_TIMEOUT = 20

# 认证类失败的判据：这类失败才值得用「另一种口令解码方式」重试
_AUTH_HINTS = (
    "password authentication failed",
    "认证失败",
    "no password supplied",
    "password authentication",
)


class PgConnError(RuntimeError):
    """连接失败（消息已按服务端编码解码，不含口令）。"""


def decode_pg_error(exc: BaseException) -> str:
    """把 psycopg2 抛出的（可能是原始字节的）服务端消息解成可读文本。"""
    obj = getattr(exc, "object", None)
    if isinstance(obj, (bytes, bytearray)):
        for enc in ("gbk", "utf-8"):
            try:
                return bytes(obj).decode(enc)
            except UnicodeDecodeError:
                continue
        return bytes(obj).decode("utf-8", "replace")
    return str(exc)


def brief(exc: BaseException, limit: int = 240) -> str:
    """一行版异常描述（用于日志与回执 note）。"""
    if isinstance(exc, UnicodeDecodeError):
        text = decode_pg_error(exc)
    else:
        text = str(exc) or exc.__class__.__name__
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def dsn_kwargs(url: str, *, decode: bool = True) -> dict:
    """把 URL 形式的 DSN 拆成 psycopg2 的 keyword 参数。

    decode=True 对 userinfo 做 URL 解码（标准语义）；decode=False 原样保留，
    用于兼容「口令里恰好含 %」却没有被编码的 DSN。
    """
    u = urllib.parse.urlsplit(url)
    if not u.scheme.startswith("postgres"):
        raise ValueError(f"不支持的 DSN scheme: {u.scheme!r}")

    def pick(part):
        part = part or ""
        return urllib.parse.unquote(part) if decode else part

    kw = {
        "host": u.hostname,
        "port": u.port,
        "user": pick(u.username),
        "password": pick(u.password),
        "dbname": (u.path or "").lstrip("/") or None,
    }
    # 保留 DSN 上的附加参数（sslmode / options / connect_timeout …）
    for k, v in urllib.parse.parse_qsl(u.query, keep_blank_values=False):
        kw.setdefault(k, v)
    return {k: v for k, v in kw.items() if v not in (None, "")}


def connect(env_name: str, *, autocommit: bool = False, connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
            application_name: str = APPLICATION_NAME):
    """按环境变量名连库；认证失败时自动尝试另一种口令解码方式。"""
    url = _env_dsn(env_name)
    kw = dsn_kwargs(url, decode=True)
    kw.setdefault("connect_timeout", connect_timeout)
    kw.setdefault("application_name", application_name)

    last_error = ""
    for candidate in (kw, _fallback_kwargs(url, kw)):
        if candidate is None:
            continue
        try:
            conn = psycopg2.connect(**candidate)
            break
        except (UnicodeDecodeError, psycopg2.OperationalError) as exc:
            last_error = decode_pg_error(exc)
            # 只有认证类失败才换口令解码方式重试；连不上/超时直接报错，不重复等待
            if not any(h in last_error.lower() for h in _AUTH_HINTS):
                raise PgConnError(f"{env_name}: {brief(RuntimeError(last_error))}") from None
    else:
        raise PgConnError(f"{env_name}: {brief(RuntimeError(last_error))}")

    conn.set_client_encoding("UTF8")
    conn.autocommit = autocommit
    return conn


def _fallback_kwargs(url: str, primary: dict):
    """认证失败时的备选组合：未解码口令 / 未指定口令。"""
    raw = dsn_kwargs(url, decode=False)
    if raw.get("password") and raw.get("password") != primary.get("password"):
        raw.setdefault("connect_timeout", primary.get("connect_timeout"))
        raw.setdefault("application_name", primary.get("application_name"))
        return raw
    return None


def chunks(seq, size: int):
    """把序列切成定长块。"""
    size = max(1, int(size))
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def with_retries(fn, *, retries: int = 3, base_delay: float = 2.0, label: str = "", log=print):
    """带指数退避的重试：共 1 + retries 次尝试，间隔 base_delay * 2^(k-1) 秒。"""
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # noqa: BLE001 — 调用方决定是否/如何重试
            if attempt > retries:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            log(f"    ! {label} 第 {attempt} 次失败：{brief(exc)}；{delay:.0f}s 后重试")
            time.sleep(delay)
