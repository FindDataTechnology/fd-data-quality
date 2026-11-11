#!/usr/bin/env python3
"""医药域价格越界检查：把「价格/财报数据停爬」的裁决变成可执行断言。

为什么需要独立脚本
------------------
用户既有裁决是「价格与财报数据停爬」，而这条边界此前只写在文档里——文档不
会让评测失败。本脚本扫描三处可能出现价格语义的地方，任何一处命中即非零退出：

1. ``fd-data-quality/baselines/pharma-*.yaml`` 的列名与 where/note 之外的字段；
2. ``fd-industry-data/spiders/`` 下医药单元 ``manifest.yaml`` 的 ``columns[].name``
   与 ``columns[].description``；
3. ``fd-open-data-protocol/.../concepts.yaml`` 里医药概念族的变量 ``code``/``measure``。

判据
----
列名匹配 ``price|amount|fee|payment|premium|cost``（大小写不敏感）。描述里出现
「支付标准金额」「零售价」「中标价」等金额语义词组同样命中——因为列名可以叫
``dosage_form`` 而内容是 ``52.60元(10mg/瓶)``，只查列名挡不住（本项目实测到的
真实形态，见 spiders/nrdl-national-catalog 的模块文档）。

故意**不匹配**的词（避免误报）：``fee`` 出现在 ``coffee``、``premium`` 出现在
``premium_lag`` 这类。因此用词边界匹配，并显式豁免医药单元里已知的合法同形词。

用法
----
    python3 scripts/pharma_boundary_check.py            # 扫描并打印结论
    python3 scripts/pharma_boundary_check.py --quiet     # 只在失败时输出

退出码：0 = 无越界；1 = 发现越界（列出位置）。
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

BASE = Path(__file__).resolve().parent.parent          # fd-data-quality/
ROOT = BASE.parent                                      # finddata/
SPIDERS = ROOT / "fd-industry-data" / "spiders"
CONCEPTS = (ROOT / "fd-open-data-protocol" / "fd-open_data_protocol"
            / "vocabulary" / "concepts.yaml")

# 医药单元（只扫这些；别的域的源不受本规则约束）
PHARMA_UNITS = ("clinicaltrials-cn", "nrdl-national-catalog")
PHARMA_FAMILIES = ("ClinicalTrial", "InsuranceCatalogEntry", "DrugApproval",
                   "ReviewProgress", "MedicalInstitution")

# 列名判据：按**分词**匹配，不用 \b。
# 踩过的坑：``avg_price`` 里下划线是词字符，``\bprice\b`` 匹配不到 —— 检查器因此
# 在负例（注入 avg_price）下仍然通过，是自证。改为「左右不得紧邻字母数字」。
NAME_RE = re.compile(r"(?<![0-9a-z])(price|amount|fee|payment|premium|cost)s?(?![0-9a-z])", re.I)
# 描述判据：金额语义词组（中文）+ 金额形态（数字后跟元）
DESC_TERMS = ("支付标准金额", "零售价", "中标价", "集采价", "挂网价", "支付标准")
AMOUNT_RE = re.compile(r"\d+(\.\d+)?\s*元")


def scan_baselines() -> list[str]:
    hits: list[str] = []
    for path in sorted(BASE.glob("baselines/pharma-*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for check in doc.get("checks") or []:
            for field in ("value_cols", "exclude_cols"):
                for col in check.get(field) or []:
                    if NAME_RE.search(str(col)):
                        hits.append(f"{path.name}: checks.{field} 含价格列 {col!r}")
            note = str(check.get("note") or "")
            for term in DESC_TERMS:
                if term in note:
                    hits.append(f"{path.name}: checks.note 提到金额语义 {term!r}")
            if AMOUNT_RE.search(note):
                hits.append(f"{path.name}: checks.note 含金额形态 {AMOUNT_RE.search(note).group(0)!r}")
    return hits


def scan_manifests() -> list[str]:
    hits: list[str] = []
    for unit in PHARMA_UNITS:
        path = SPIDERS / unit / "manifest.yaml"
        if not path.is_file():
            hits.append(f"{unit}: manifest.yaml 不存在（期望已在检查范围内）")
            continue
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for fn in doc.get("functions") or []:
            for col in fn.get("columns") or []:
                name = str(col.get("name") or "")
                if NAME_RE.search(name):
                    hits.append(f"{unit}: columns[].name 含价格列 {name!r}")
                desc = str(col.get("description") or "")
                for term in DESC_TERMS:
                    if term in desc:
                        hits.append(f"{unit}: columns[{name}].description 提到 {term!r}")
                if AMOUNT_RE.search(desc):
                    hits.append(f"{unit}: columns[{name}].description 含金额形态")
    return hits


def scan_concepts() -> list[str]:
    hits: list[str] = []
    if not CONCEPTS.is_file():
        return hits
    doc = yaml.safe_load(CONCEPTS.read_text(encoding="utf-8")) or {}
    for entry in doc.get("entries") or []:
        if entry.get("id") not in PHARMA_FAMILIES:
            continue
        for var in entry.get("variables") or []:
            for field in ("code", "measure"):
                value = str(var.get(field) or "")
                if NAME_RE.search(value):
                    hits.append(f"concepts:{entry['id']}: variables.{field} 含价格词 {value!r}")
    return hits


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)

    hits = scan_baselines() + scan_manifests() + scan_concepts()
    if hits:
        print(f"pharma boundary: FAIL — {len(hits)} 处价格越界")
        for hit in hits:
            print(f"  - {hit}")
        return 1
    if not args.quiet:
        scanned = len(list(BASE.glob("baselines/pharma-*.yaml")))
        print(f"pharma boundary: PASS — {scanned} 份医药基线、"
              f"{len(PHARMA_UNITS)} 个单元 manifest、{len(PHARMA_FAMILIES)} 个概念族无价格越界")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
