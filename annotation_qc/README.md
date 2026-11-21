# annotation_qc — 公文标注离线质检入仓管道

把多家标注团队对同一份已转录公文交回的片段提议、已审核区间与引用关系，
按策略声明做结构校验、争议区域归并、覆盖资格统计、双方计票与引用归票，
产出：

- **正式片段表** `official_fragment`（仅候选胜出时写入）；
- **正式引用表** `official_relation`（确认存在的引用，不悬挂）；
- **复核工作库** `review_item`（区域待核、关系待核、被阻断的原始关系）；
- 以及完整的证据链（原文摘要、字节坐标、包/文件/行/记录序号、审核区间）。

在此之上，**裁决工作区**（`docs/adjudication.md`）把复核意见变成可合并的
裁决：从同一构建库初始化独立只读底稿，导入各复核员的工单，按三方合并把
两份工单一起合入审定稿，再按公文批次登记审定发布。它不改变自动质检的
计票结果，也不重新读取标注团队的交付包。

再之上，**分仓台账**（`docs/ledger.md`）以裁决工作区为只读上游，把
「同一底稿 / 复用条款 / 人工同源」的公文归成连通组，产出训练、评测、
隔离三张语料清单：选入按「档号 + 发布 ID」读取发布快照，交付确认按档号
永久锁仓并留存交付摘要，冲突组整组隔离；只做离线台账，不做训练、
相似度打分，也不改动质检、工单与发布规则。

再之上，**发布后独立盲审**（`docs/audit.md`）从已登记发布中按种子确定性地
抽取公文，连同该发布的全部正式片段与有向引用交给审核员盲审；审核结论按
盲审标识登记为独立审计记录。审计库独立，不回写质检结果、裁决工作区或
发布内容。

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
  ws_schema.sql 裁决工作区库结构
  ws_common.py  工作区连接/事务/文件锁/稳定标识
  ws_init.py    用现有构建库初始化独立工作区
  ws_state.py   按版本读取判定、待办与证据
  ws_source.py  按库中摘要核对的原文存取
  ws_ticket.py  工单解析与端点解析
  ws_propose.py 按基版构造可合并提案并自洽预检
  ws_import.py  工单导入（稳定身份、幂等）
  ws_merge.py   三方合批、依赖检查、原子写版本、幂等回执
  ws_release.py 发布登记、待办阻挡、交付快照与流式导出
  ws_view.py    按版本查看与证据展开
  ws_cli.py     工作区命令行（qc-ws）
  ledger_schema.sql 分仓台账库结构
  ledger_common.py 台账连接/事务/文件锁/错误对象
  ledger_upstream.py 只读上游存取（发布/审定片段/公文身份）
  ledger.py     变更批、交付确认、清单、解释、连通组局部重算
  ledger_cli.py 台账命令行（qc-ledger）
  audit_schema.sql 发布后盲审库结构
  audit_common.py  盲审连接/事务/文件锁/请求台账/错误对象
  audit_upstream.py 只读上游存取（发布/发布片段/发布引用/公文身份）
  audit_source.py  按批次绑定的底稿摘要核对证据坐标与原文
  audit.py      确定性抽样、盲审批次快照、审核提交、只读查询与导出
  audit_cli.py  盲审命令行（qc-audit）
tools/
  make_fixtures.py  可重建小夹具（含独立手算表 hand_calc.json）
  scalegen.py       运行时规模生成器（产物不提交）
  ws_fixture.py     可复跑的裁决小样本（只留生成方式）
  fanoutgen.py      运行时高扇出引用生成器（产物不提交）
  ledger_fixture.py 台账演示小样本生成（真实管道；只留生成方式）
  ledger_demo.py    台账可复跑验收示例（甲/乙/丙/丁全场景）
  ledger_scalegen.py 台账规模生成与更新工作量实测（产物不提交）
  audit_fixture.py  盲审小样本生成（真实管道；含逆序重建；只留生成方式）
  audit_reference.py 不依赖产品实现的参考抽样（独立复算选中集合）
  audit_scalegen.py 运行时大批文档生成（产物不提交）
  audit_demo.py     盲审可复跑验收示例（逐条断言）
tests/
  test_pipeline.py  31 个 unittest 用例
  test_workspace.py 26 个裁决工作区用例
  test_ledger.py    28 个分仓台账用例
  test_audit.py     25 个发布后盲审用例
docs/usage.md       入口与数据格式说明
docs/adjudication.md 裁决工作区、工单合批与审定发布
docs/ledger.md      分仓台账：证据、仓位规则、变更批与诊断
docs/audit.md       发布后独立盲审：抽样、盲审标识、提交与导出台账
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

## 裁决工作区速览
```bash
# 初始化独立工作区（原构建库只读）；固定人工规则
qc-ws init --workspace ws/ --base-db out/warehouse.db --policy fixtures/policy.json
# 查看指定版本的区域/片段/引用/待办，导入并预览工单
qc-ws view    --workspace ws/ --rev HEAD --doc D1
qc-ws import  --workspace ws/ --ticket t.json --manifest fixtures/manifest.json
qc-ws preview --workspace ws/ --ticket t.json --manifest fixtures/manifest.json
# 合批（三方合并；冲突整份不生效；幂等回执）
qc-ws merge   --workspace ws/ --ticket t.json --manifest fixtures/manifest.json
# 发布：点名版本与公文；有待办则只列阻挡项
qc-ws release --workspace ws/ --rev HEAD --doc D1
qc-ws export  --workspace ws/ --release <id> --what relations
```
详见 `docs/adjudication.md`。

## 分仓台账速览
```bash
# 建账：固定标注层、片段最短字节数与豁免表；上游为裁决工作区（只读）
qc-ledger init  --ledger L --workspace ws/ --layer clause --min-bytes 16 \
  --exempt exempt.json
# 变更批：选入/改选/撤出/人工同源证据，整批原子入账，按批后状态算仓位
qc-ledger apply   --ledger L --batch batch.json
# 确认某版清单中的一部分已对外交付：按档号永久锁仓 + 留存交付摘要
qc-ledger confirm --ledger L --request confirm.json
# 导出训练/评测/隔离清单；查分组与不可交付原因；取历史交付凭据
qc-ledger lists   --ledger L --out out/
qc-ledger explain --ledger L --doc D1
qc-ledger credentials --ledger L --doc D1
```
详见 `docs/ledger.md`。

## 发布后盲审速览
```bash
# 从已登记发布里确定性抽样，建立可重开的盲审批次
qc-audit create   --audit A --workspace ws/ --request create.json
qc-audit blind    --audit A --batch <batch_id> --out blind.jsonl   # 审核员盲审导出
qc-audit source   --audit A --batch <batch_id> --blind-doc bd_… --start 0 --end 8
# 提交审核结论（按盲审标识；问题证据按底稿摘要核对坐标与原文）
qc-audit submit   --audit A --request submit.json --manifest fixtures/manifest.json
# 档案只读：抽中范围、未审记录、审计结论与可定位证据
qc-audit coverage --audit A --batch <batch_id>
qc-audit results  --audit A --batch <batch_id>
```
详见 `docs/audit.md`。

## 重建与测试
```bash
# 1. 生成可重建小夹具（含手算表）
python -I tools/make_fixtures.py --root fixtures

# 2. 构建
python -I -c "import sys;sys.path.insert(0,'.');from qc.cli import main;raise SystemExit(main())" \
  build --manifest fixtures/manifest.json --packages fixtures/packages \
  --policy fixtures/policy.json --out out/warehouse.db

# 3. 全部测试（110 用例：管道 31 + 裁决工作区 26 + 分仓台账 28 + 盲审 25）
python -I -m unittest discover -s tests -v

# 3b. 裁决小样本与高扇出生成（产物不提交）
python -I tools/ws_fixture.py --root /tmp/wsample \
  --workspace /tmp/wsample/ws --tickets-dir /tmp/wsample/tickets
python -I tools/fanoutgen.py --root /tmp/fanout --fanout 20000

# 3c. 分仓台账验收示例与规模实测（产物不提交）
python -I tools/ledger_demo.py --root /tmp/ldemo
python -I tools/ledger_scalegen.py --root /tmp/lscale \
  --groups 1500 --large-group 2000

# 3d. 盲审验收示例与参考抽样（产物不提交；--large-docs 追加有界内存实测）
python -I tools/audit_demo.py --root /tmp/audit-demo --large-docs 20000
python -I tools/audit_reference.py --workspace /tmp/audit-demo/src/ws \
  --release <release_id> --sample-size 3 --seed 20261121 \
  --audit /tmp/audit-demo/audit --batch <batch_id>

# 4. 规模生成与构建实测（产物写到临时目录，不提交）
python -I tools/scalegen.py --root /tmp/scale --teams 12 \
  --proposals 4000 --doc-bytes 200000 --build
```

## 稳定身份与重排
所有业务标识均为内容派生（见 `docs/usage.md` 第 7 节）。测试
`test_reorder_records_and_files` 把包目录反序、包内记录行逆序后重建，
断言区域、候选、票、正式片段/引用与复核项的内容快照完全一致；
物理行号允许变化，但不参与任何身份计算。
