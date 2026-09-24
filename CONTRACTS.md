# 文件驱动重灌契约（GTA 面板 / 年鉴 / 城市面板）

本文件是 OpenSpec change `data-coverage-backfill` task 4.1 的交付物，覆盖三个"**源文件在外部、loader 不在仓库**"的源：
`gta_panel`、`yearbook_catalog`、`china_city_panel`。它定义「文件 → 既有表」的重灌契约：输入文件要求、校验清单、
幂等 upsert 键、回执格式，以及**源文件不在手时的"仅报告"约定**。

配套校验器：`scripts/reload_check.py`（**只读、默认 dry-run，永不写库**；`--apply` 会被明确拒绝）。
所有数字来自 2026-09-21 对 Windows PG（`FD_ANALYSIS_PG_URL`）的实测，并已落成
`.state/reload_baseline_<source>.json`（`--db-baseline` 生成，可随时重生成）。

> 总则：**校验通过才允许重灌；同一文件重复重灌必须不产生重复行；源文件缺失标"待源文件"，不得报成功。**
> 本轮（change 执行期）三个源都是 `report_only: true`（见 `baselines/*.yaml`）：只巡检 + 出契约，不重灌。

---

## 0. 通用约定

### 0.1 校验器用法

```bash
# 1) 从既有库推导期望形状（真连 Windows PG，只读）
python3 scripts/reload_check.py --source all --db-baseline           # 三源一次跑完
python3 scripts/reload_check.py --source gta_panel --db-baseline     # 单源 → .state/reload_baseline_gta_panel.json

# 2) 校验待重灌文件（默认 dry-run + 默认开启连库抽样对比）
python3 scripts/reload_check.py --source gta_panel --file /path/FS_Combas.dta
python3 scripts/reload_check.py --source china_city_panel --file /path/城市面板.xlsx --sheet 原始数据
python3 scripts/reload_check.py --source yearbook_catalog --file /path/2024版.csv --edition-year 2024

# 3) 源文件不在手 = 仅报告
python3 scripts/reload_check.py --source yearbook_catalog
```

| 退出码 | 含义 | 处置 |
|---|---|---|
| 0 | 校验通过（可能有 WARN） | 可以重灌 |
| 1 | 校验失败（行数/键/期间/字段不满足契约） | **不得重灌**（回执 `status=rejected`） |
| 2 | 用法或环境错误（参数非法、DSN 缺失、连库失败、`--apply` 被拒） | 修环境后重跑 |
| 3 | 源文件缺失 → 待源文件 / 仅报告 | 不产生任何"已补齐"结论 |

常用开关：`--min-rows`（放低硬下限，早期版本用）、`--edition-year`（年鉴单版模式）、`--table`（GTA 强制指定表）、
`--no-compare-db`（跳连库对比，仅离线自检）、`--value-diff-cap`（旧值/新值差异率上限，默认 5%）、
`--year-tolerance`（单期行数相对既有库的容忍度，默认 2%）、`--sample`（抽样键数，默认 300）、
`--receipt-out`（导出回执片段 JSON 给 loader）。

### 0.2 校验清单（三源统一顺序）

1. **可读**：文件存在、非目录、非 0 字节、扩展名在契约集合内、sha256 可算。
2. **解析**：依赖缺失只报提示（`pip install -r requirements.txt`），不崩栈。
3. **字段**：必需列齐全；GTA 还要与 `variables` 字典对齐；城市面板要 209 个变量列齐全。
4. **行数**：落在契约区间（单版/严格网格另算）；低于硬下限即 FAIL。
5. **键唯一**：契约键在**文件内**必须唯一且非空（重复即 FAIL，附重复样例）。
6. **期间覆盖**：期间列落在期望窗内、无整年缺失、各期行数不低于既有库（默认 2% 容忍）。
7. **与既有库抽样对比**：抽样键的命中率（= 本次新增/更新预演）、同键值差异率 ≤ 上限、同键多行比例。

### 0.3 回执：写 `dq.backfill_run`

校验器只**产出回执片段**（`--receipt-out`）；真正入库由 loader 执行（`dq` 库表结构见 `README.md`）：

```sql
INSERT INTO backfill_run (run_key, source, executor, mode, started_at, finished_at,
                          status, written_rows, receipt, note)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
ON CONFLICT ... ;   -- 幂等口径由 loader 决定；同一 run_key 重复执行不产生重复行
```

- `executor` 固定为 `reload_<source>`：`reload_gta_panel` / `reload_yearbook_catalog` / `reload_china_city_panel`。
- `receipt` 必含：`file`、`sha256`、`bytes`、`rows`、`key`、`table`、`period`、`edition_year`、
  **`inserted`（新增）/ `updated`（更新）**、`gaps_after`（**仍缺项**）、`verdict`、`note`。
- `status` 取值：`validated`（校验通过，待 loader 填 `written_rows`）、`rejected`（校验 FAIL，不得重灌）、
  `no_source_file`（待源文件）、`written` / `partial`（loader 落地后自填）。
- **`gaps_after` 不允许为空话**：它就是 `FAIL` 行的代码+标题（如 `rows_range: 行数严重不足`），未收敛的缺口必须留着。

### 0.4 "仅报告"约定（源文件缺失 / loader 不在仓库）

- 命令：`python3 scripts/reload_check.py --source <src>`（不给 `--file`）→ 退出码 **3**。
- 回执：`status=no_source_file`、`report_only=true`、`written_rows` 不填 0 假装成功，`receipt.note` 写明"源文件不在手"。
- **禁止**写 `status=written` 的成功回执；缺口报告里该源一律标"**仅报告 / 待源文件**"。
- 三个源在 `baselines/*.yaml` 中标了 `report_only: true`：本轮巡检只出缺口，不补数。

---

## 1. GTA 面板（`gta_panel`）

国泰安（CSMAR）上市公司面板，**源文件是国泰安 dta 导出，loader 不在仓库**（当年离线导入）。

### 1.1 既有库形状（实测）

| 对象 | 行数 | 列数 | 说明 |
|---|---|---|---|
| `gta_panel.panel_c0` | 96,029 | 190 | 含 `id` / `id_org` / `year` / `stknme` |
| `gta_panel.panel_c1` | 96,029 | 851 | 无 `id_org` |
| `gta_panel.panel_c2` | 96,029 | 431 | 无 `id_org` |
| `gta_panel.variables` | 1,468 | 10 | 变量字典：`dta_colno,name,name_src,label_cn,stata_type,pg_type,table_name,dict_no,dict_missing,sublib_code`；按 `table_name` 分组 190/849/429 |
| `gta_panel.value_labels` | 439 | — | 取值标签 |

- 三表**同一网格**：96,029 行 × 1990–2025（36 年）× 5,939 家公司；`(id, year)` 是既有 PK，三表全连接 = 96,029。
- `panel_c0.id_org` 是 6 位证券代码 text（'000501'）；**实测 `id = id_org::bigint` 100% 成立**（0 行例外），
  故 `(id_org, year)` 与 `(id, year)` 都是 96,029 唯一。
- 每年公司数不等：1990 年 12 家 → 2023 年 5,711、2024 年 5,665、2025 年 5,582（**按年行数是比总行数更硬的约束**）。

### 1.2 输入文件契约

| 项 | 要求 |
|---|---|
| 扩展名 | `.dta`（首选，国泰安原始导出）/ `.csv` / `.parquet`；**`.pdf` 明确拒绝**（先在本地导出 dta） |
| 命名建议 | `panel_c0.dta` / `panel_c1.dta` / `panel_c2.dta`（**一张表一个文件**，不要三表合一个文件） |
| 列 | 必须与 `variables` 字典对齐（190 / 851 / 431 列）；c0 需 `id_org` 或 `id` 之一 + `year`；c1/c2 需 `id` + `year` |
| 行数区间 | **91,227 – 153,646**（= 96,029 × [0.95, 1.6]；下限是历史行丢失的硬线，上限留给新增年份/公司） |
| 公司数 | 5,879 – 9,502（= 5,939 × [0.99, 1.6]） |
| 期间 | 1990–2025 全 36 年，缺一年即 FAIL；各年行数不得比既有库少 2% 以上 |

### 1.3 幂等 upsert 键

| 表 | 键 | 说明 |
|---|---|---|
| `panel_c0` | **`(id_org, year)`** | c0 有源身份列 `id_org`（6 位代码），优先用它。若文件只有数值型 `id`：`id_org = lpad(id::text, 6, '0')`（实测等价），loader 必须补出 `id_org` |
| `panel_c1` / `panel_c2` | **`(id, year)`** | c1/c2 **没有** `id_org` 列（既有库实测），只能用 `id`；因此 `id` 必须保持"同一 `id_org` 的整数形式"，否则三表 join 断裂——这是本契约的已知弱点，靠 loader 保证 |

`id` 归一化：数值型（dta 常见）→ 整数；`id_org` 文本/数值统一补零到 6 位（`501` → `000501`）。

### 1.4 校验清单

可读 → 列集与 `variables` 字典对齐（多列 WARN、缺列 FAIL）→ 行数区间 → 键唯一/非空 →
1990–2025 期间覆盖 + 各年行数 shrink（>2% FAIL，`--allow-shrink` 降级）→ 公司数区间 →
**抽样对比既有库**（默认 300 键 × 8 个变量列，差异率 >5% WARN、>10% FAIL；未命中键 = 本次新增）。

---

## 2. 年鉴（`yearbook_catalog`）

全国/地方统计年鉴解析产物，**解析器不在仓库**：源文件仍躺在目录里（`ingest_files` 实测
`pending_xls` 266,962 / `pending_html` 261,850 / `skipped_other` 186,320 / `skipped_pdf` 5,012），本轮只报告。

### 2.1 既有库形状（实测）

| 对象 | 行数 | 说明 |
|---|---|---|
| `yearbook_catalog.indicator_values` | 24,332,692 | 列：`id, indicator_id, table_id, region, data_year, value, unit, note, dims(jsonb), edition_year, value_num, created_at` |
| `yearbook_catalog.tables` / `indicators` | 805,726 / 53,925 | 维度表（`table_id` / `indicator_id` 的出处） |

- `edition_year` 有 133 个取值，**大量越界脏值**：1900–1954 的解析残渣 + **2100 版 3,709,280 行**；
  真实版本为 **2000–2025（26 版）**，单版中位 **813,749** 行（最小 2000 版 58,567；最大 2014 版 1,376,978）。
- 空值：`region` 687,266 行（2.8%）、`dims` 为空 14,472,586 行（59%）、`value_num` 3,090,788 行（12.7%，纯文本值）。
- **已知半版**：2021 版仅 115,963 行（约为邻版 1/8），是"半版/截断"的实证样本。
- ⚠️ **该表在契约键上不唯一**：24,332,692 行只有 5,589,074 个自然键（≈**4.35 行/键**）；2024 版 813,749 行
  只有 211,253 个唯一键；**去掉表头脏值后仍有约 4.5 倍重复**（42,181 干净行 → 9,391 键）。
  根因是解析器把同一 sheet 的不同单元格压到了同一 `(indicator_id, table_id, region, data_year, dims)` 上——
  即该键不足以区分行。**这不是本契约能修的历史问题，但重灌前必须先定去重口径**（见 2.3）。

### 2.2 输入文件契约

| 项 | 要求 |
|---|---|
| 扩展名 | `.csv` / `.xlsx` / `.xls` / `.htm` / `.html` / `.parquet`；**`.pdf` 拒绝**（历史上 5,012 个 pdf 全被 skip，先把 PDF 解析成 xls/htm） |
| 命名建议 | `yearbook_values_<edition_year>.<ext>`；**一版一文件**（多版合并文件请先拆） |
| 列 | 必需 `indicator_id, table_id, region, data_year, edition_year, value`；建议带 `unit, dims, value_num` |
| 行数区间 | **单版口径**：以该版既有行数为基准 `[0.98×N, 2×N]`（重解析可能补回漏行，上限放宽）；整表兜底区间 **325,499 – 1,301,998**（= 单版中位 813,749 × [0.4, 1.6]，来自 baseline；无 baseline 时脚本用 300,000–1,500,000 的保守兜底）；硬下限 **100,000**（`--min-rows` 可放低；既有量本身低于硬下限的版本，如 2000 版 58,567，自动不套用） |
| 期间 | `edition_year` ∈ **1980–2026**（越界即 FAIL）；**单版模式**下不要求覆盖 2000–2025 整窗（整窗缺版由巡检器出缺口，不是单次重灌的锅） |
| 归一化 | `region`：NFKC + 空白压单空格，空 → **空串哨兵**；`dims`：键排序、去 null、空 → **`{}` 空对象哨兵**（NULL 在唯一索引里不去重，会让幂等失效） |

### 2.3 幂等 upsert 键

**契约键**：`(indicator_id, table_id, region归一化, data_year, edition_year, dims归一化)`，`region`/`dims` 用哨兵。

**但既有表在该键上不唯一（4.35 行/键）**，所以两种可接受的落地口径：

1. **按版本整体替换（推荐）**：`DELETE FROM indicator_values WHERE edition_year = :edition` 后在**单事务**内整版插入；
   幂等性来自"版本粒度"，重灌同一文件结果一致、不累积重复。适用于新解析要**替换**旧解析的场景（含清洗脏值）。
2. **键级 upsert + 先判重**：仅当新文件在契约键上唯一（校验器已 FAIL 档把住）且业务接受"同键多行保留"时使用。

无论哪种：**文件侧键必须唯一**（校验器直接把文件内重复当 FAIL），**`edition_year` 越界行一律不得入库**。

### 2.4 校验清单

可读 → 必需列 → 行数（单版区间 + 硬下限）→ **键唯一/非空（跳过 region/dims 哨兵）** →
`edition_year` 落在 1980–2026（越界行计数，>0 即 FAIL）→ 单版模式判定（`--edition-year` 或文件只含一版时自动）→
`region` 表头脏值（如 `地  区`/`行  业`，占比 >0.1% 即 FAIL；实测 2024 版就有 30,038 行 = 3.7%，不得再引入）→
`data_year > edition_year` 占比（>2% WARN）→ `value_num` 空值率 WARN → **抽样对比既有库**
（命中率=新增/更新预演；同键多行比例 >5% WARN、>30% FAIL；同键值差异率 ≤5%）。

---

## 3. 城市面板（`china_city_panel`）

城市统计资料整理 + 插补，**插补/重灌脚本不在仓库**。

### 3.1 既有库形状（实测）

| 对象 | 行数 | 列数 | 说明 |
|---|---|---|---|
| `china_city_panel.panel_raw` | 7,425 | 211 | 键 `(city_code, year)`（PK 为 `(year, city_code)`），列 = `year, city_code, v001…v209` |
| `china_city_panel.panel_interp` | 7,425 | 211 | 派生：线性插值 |
| `china_city_panel.panel_arima` | 7,425 | 211 | 派生：ARIMA 填补（慎用） |
| `china_city_panel.city_dim` | 297 | 6 | `city_code, city_name, province, province_code, region, hu_line_side` |
| `china_city_panel.variable_catalog` | 209 | 6 | `var_id('v001'), col_no(从 8 起), name_cn, unit, classification, note` |

- **严格网格**：297 城 × 25 年（2000–2024）= 7,425 行，一行都不能少（缺一行即缺口）。
- 变量列名由 `variable_catalog.var_id` 定义（`v001`…`v209`），`col_no` 是源工作簿里的列序。

### 3.2 输入文件契约

| 项 | 要求 |
|---|---|
| 扩展名 | `.xlsx` / `.xlsm` / `.xls` / `.csv`；`.pdf` 拒绝（先转 xlsx/csv） |
| 命名建议 | `city_panel_raw_<yyyyMM>.xlsx`；**工作簿多 sheet 时必须 `--sheet`**（原始数据 / 线性插值 / ARIMA填补(慎用) 分表） |
| 列 | 必需 `city_code`（int）、`year`（int）；**必须含 `v001`…`v209` 全部 209 个变量列**（缺列即 FAIL，以 `variable_catalog` 为准） |
| 行数 | **恰好 7,425**（严格网格，容不得近似）；城市数 **恰好 297**；年份 2000–2024 全 25 年 |
| 期间 | 逐年行数不得比既有库少 2% 以上（等于某年整城丢失即 FAIL） |

### 3.3 幂等 upsert 键

**`(city_code, year)`**（既有 PK 是 `(year, city_code)`，包含同一对列；顺序不影响幂等）。
命中即 UPDATE、未命中即 INSERT；同一文件重复重灌不产生重复行。

⚠️ **派生表**：`panel_interp` / `panel_arima` 由 `panel_raw` 派生，重灌 `panel_raw` 后二者立即过期，
必须重跑插补（脚本不在仓库）——回执 `gaps_after` 必须留下这条，否则等于假成功。`panel_raw` 填充率约 50.3%（设计文档口径），不是校验失败项。

### 3.4 校验清单

可读 → 必需列 + 209 变量列齐全 → **行数严格等于 7,425** → 键唯一/非空 → 2000–2024 全覆盖 + 逐年 shrink ≤2% →
城市数 297 → **整网格对比既有库**（7,425 行很小，直接全量对比而非抽样：命中率 = 新增/更新预演，同键值差异率 ≤5%）。

---

## 4. 已知限制

1. **三个源都还没有源文件**：本契约 + 校验器本轮只能用既有库数据自测；真实 dta/年鉴/城市文件到位后，
   以 `.state/reload_baseline_<source>.json` 为准重跑校验（`--db-baseline` 可随时刷新基线）。
2. **实际写入不在本仓库**：`reload_check.py` 只校验、只出回执片段，`dq.backfill_run` 的写入由 loader 完成；
   否则就是"校验器偷偷写库"，与 dry-run 约定冲突。
3. **年鉴自然键不足**：既有表 4.35 行/键是解析器历史问题的沉淀，契约只能要求"文件侧键唯一 + 按版本整体替换"，
   **不能**保证键级 upsert 在既有表上收敛（见 2.3）。
4. **GTA c1/c2 没有 `id_org`**：其幂等键只能用 `(id, year)`，跨表 join 正确性依赖 loader 保住 `id = id_org::bigint`。
5. **依赖**：读 dta/xlsx/htm 需 pandas + openpyxl/xlrd/lxml（`requirements.txt` 未列全，按提示装）；
   缺依赖报安装提示而非崩溃。
6. **`--db-baseline` 对年鉴较重**：24M 行的自然键去重需要数分钟（`--quick` 可跳过），建议错峰跑。
7. **Windows PG 是家宽机器**（延迟抖动 ~212ms）：连库对比失败默认降级为 WARN（`--require-db` 可升级为 FAIL）。
