"""共享环境加载：从 fd-data-quality/.env.local 读 DSN（生产由环境变量直接注入）。"""
import os
import pathlib

ENV_FILE = pathlib.Path(__file__).resolve().parent.parent / ".env.local"


def load_env(path=None):
    p = pathlib.Path(path) if path else ENV_FILE
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def dsn(name: str) -> str:
    load_env()
    url = os.environ.get(name)
    if not url:
        raise SystemExit(f"缺少环境变量 {name}（本地见 fd-data-quality/.env.local，生产由 Secret 注入）")
    return url


def connect(name: str):
    import psycopg2
    return psycopg2.connect(dsn(name))
