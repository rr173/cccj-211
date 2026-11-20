# annotation_qc — 公文标注离线质检入仓管道

把多家标注团队对同一份已转录公文交回的片段提议、已审核区间与引用关系，
按策略声明做结构校验、争议区域归并、覆盖资格统计、双方计票与引用归票，
产出：

- **正式片段表** `official_fragment`（仅候选胜出时写入）；
- **正式引用表** `official_relation`（确认存在的引用，不悬挂）；
- **复核工作库** `review_item`（区域待核、关系待核、被阻断的原始关系）；
- 以及完整的证据链（原文摘要、字节坐标、包/文件/行/记录序号、审核区间）。

仅依赖 Python 3.11 标准库与 SQLite，可在 `python -I` 下运行；不做 OCR、
文本修订、自动抽取或标注界面。

## 目录
```
qc/
  util.py       稳定标识、规范化 JSON、结构错误、UTF-8 边界判定
  policy.py     策略声明解析（层/标签/关系端点层/最少支持团队数≥2）
  source.py     原文清单、字节级只读存取、SHA-256 与字符边界核对
  packages.py   标注包发现与 JSONL 流式读取、记录形状校验
  build.py      构建主流程：校验落库 → 归组 → 覆盖 → 计票 → 引用 → 原子发布
  query.py      只读查询、证据展开、覆盖缺口复算、流式导出
  cli.py        命令行入口
  schema.sql    SQLite 库结构
tools/
  make_fixtures.py  可重建小夹具（含独立手算表 hand_calc.json）
  scalegen.py       运行时规模生成器（产物不提交）
tests/
  test_pipeline.py  29 个 unittest 用例
docs/usage.md       入口与数据格式说明
```

## 核心规则实现

### 争议区域归并（无平方级边）
同层提议按 `(doc, layer, start, end)` 排序后**单遍扫描线**处理：
- 正长度交叠（`a.start < b.end 且 b.start < a.end`）传递合并为一组；
- 首尾相碰（`prev_end == next_start`）不算重叠，开新组；
- 组区间取成员边界包络。
不显式枚举提议两两交叠，不生成 O(n²) 交叠边；工作内存只保留当前
`(doc, layer)` 上的活跃组。

### 覆盖资格与分母
- 每个包的审核区间按层取出、有序合并（紧邻可接上）；
- 对区域用 `coverage_gaps()` 复算未覆盖缺口：中间只差一个字节也会
  显式报告（如 `[0,5)+[6,9)` 对 `[0,9)` 给出缺口 `[[5,6]]`），
  返回的是缺口列表而非布尔值；
- 只有**完整覆盖**的团队进分母；部分覆盖团队的提议不投票，作为
  「被排除意见」保留并附缺口；没有提议、只有审核覆盖的地方不生成区域。

### 区域计票
- 候选按 `(起止字节, 标签)` 认同，包内局部 ID 仅用于回查；
- 完整覆盖且无提议 → 投「无片段」；只提一个不同候选 → 投该候选
  （同候选多个局部 ID 仍只一票）；提多个不同候选 → 自相矛盾，
  不给任何候选加票但仍在分母；
- 候选（含「无片段」）须**同时**达到该层 `min_support` 与分母严格多数
  （`denominator//2 + 1`）才胜出；0 个或多个达标者都判待核（并列无胜出）；
- 「无片段」胜出记为确认不入仓，不写正式片段。

### 引用解析与归票
- 包内关系端点只解析到该包**实际提议**的候选；不会因为两个候选同处
  一个争议区域就把票转移到胜出候选上；
- 端点待定 / 确认无片段 / 另一候选胜出 → 阻断，保留原始关系记录与原因
  （`endpoint_region_pending` / `endpoint_no_fragment_confirmed` /
  `endpoint_other_candidate_won`）；
- 两端都是正式片段的记录，按 `(doc, 关系类型, 起点片段, 终点片段)` 归票；
  反向关系天然是另一个键，单独核算，不并入正向；
- 只纳入明确投票的不同团队，不因团队看过两个片段就补反对票；
- 同团队经不同局部 ID 多次表态只计一次；赞成与反对都出现则不计任一方
  但仍进分母；
- 赞成或反对达到该类型 `min_support` 且严格多数 → 确认引用 / 确认无引用，
  否则进复核。

## 有界工作内存
- 标注包逐行流式读取，原始记录直接落 SQLite，不整包/整文档驻留；
- 每个包两遍流式：先收集轻量元组并合并完整审核区间，再校验片段与关系，
  因此记录物理顺序（引用先于片段、片段先于审核）不影响结果；
- 归组是排序 + 扫描线；覆盖按包合并后逐区域推进，不为每个区域反复
  全表扫描审核记录；
- 计票与引用解析走带索引的 SQL；导出用服务端游标逐行产出；
- 构建在输出旁的临时库进行，`wal_checkpoint` 后复制并 `os.replace` 原子发布。

实测（12 团队、约 4.8 万条记录、200KB 原文）：峰值 RSS ≈ 66MB、约 12 秒；
提议数 ×4 时耗时近似 ×4 而内存仅小幅上升，未见平方级工作量。

## 失败语义
- 任何结构错误都不发布；输出路径上的既有完整结果不会被替换；
- 拒收报告逐项给出错误码、包、文件相对名、行号、记录序号与原因。
- 错误码包括：`SOURCE_DIGEST_MISMATCH`、`WRONG_SOURCE_DIGEST`、
  `DUPLICATE_PACKAGE`、`DUPLICATE_LOCAL_ID`、`DUPLICATE_RECORD`、
  `DANGLING_RELATION`、`OUTSIDE_REVIEWED_SPAN`、`BAD_RANGE`、
  `SEEN_TEXT_MISMATCH`、`UNKNOWN_LAYER`、`UNKNOWN_LABEL`、
  `UNKNOWN_RELATION_TYPE`、`RELATION_LAYER_MISMATCH`、`PACKAGE_FORMAT` 等。

## 重建与测试
```bash
# 1. 生成可重建小夹具（含手算表）
python -I tools/make_fixtures.py --root fixtures

# 2. 构建
python -I -c "import sys;sys.path.insert(0,'.');from qc.cli import main;raise SystemExit(main())" \
  build --manifest fixtures/manifest.json --packages fixtures/packages \
  --policy fixtures/policy.json --out out/warehouse.db

# 3. 全部测试（29 用例）
python -I -m unittest discover -s tests -v

# 4. 规模生成与构建实测（产物写到临时目录，不提交）
python -I tools/scalegen.py --root /tmp/scale --teams 12 \
  --proposals 4000 --doc-bytes 200000 --build
```

## 稳定身份与重排
所有业务标识均为内容派生（见 `docs/usage.md` 第 7 节）。测试
`test_reorder_records_and_files` 把包目录反序、包内记录行逆序后重建，
断言区域、候选、票、正式片段/引用与复核项的内容快照完全一致；
物理行号允许变化，但不参与任何身份计算。
