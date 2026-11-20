# 裁决工作区（审稿合批与审定发布）

自动质检把多家标注团队的意见整理成正式片段、正式引用与待复核项之后，
两名复核员可能从**同一个版本**分别处理工单，再回来合入同一份审定稿。
「一人把条款拆成两段、另一人同时为旧条款补了一条引用」时，他们并没有
修改同一条记录，但两份工单也不能直接叠加。这一层为此提供离线的裁决
工作区、工单合批与审定发布，沿用 Python、SQLite 与命令行。

不改动自动质检的计票结果，也不重新读取标注团队的交付包：工作区从既有
构建库复制一份只读底稿，之后一切决策都在工作区内完成。

## 1. 目录与模块

```
qc/ws_schema.sql    工作区库结构（底稿快照 + 判定时间行 + 工单/批次/发布）
qc/ws_common.py     连接、显式事务、跨进程文件锁、稳定标识、版本定位
qc/ws_init.py       用现有构建库初始化独立工作区（原库只读）
qc/ws_state.py      按版本读取生效的判定；待办与无效关联；证据查询
qc/ws_source.py     按库中摘要核对的原文边界与取文（不接受工单自报切片）
qc/ws_ticket.py     工单解析、结构校验、端点到审定片段的解析
qc/ws_propose.py    按工单基版把裁决解析成提案，并先在自己的基版上成立
qc/ws_import.py     工单导入：稳定身份、基版绑定、幂等重入
qc/ws_merge.py      三方合批、依赖检查、原子写入新版本、幂等回执
qc/ws_release.py    发布登记与交付快照、待办阻挡、流式导出
qc/ws_view.py       按版本查看与证据展开
qc/ws_cli.py        命令行入口（qc-ws）
tools/ws_fixture.py 可重建的小样本（只保留生成方式，不提交产物）
tools/fanoutgen.py  运行时高扇出引用生成器（产物不提交）
```

## 2. 初始化：独立工作区，原库只读

```bash
python -I -c "import sys;sys.path.insert(0,'.');from qc.ws_cli import main;raise SystemExit(main())" \
  init --workspace ws/ --base-db out/warehouse.db --policy fixtures/policy.json
```

- 原构建库以 `mode=ro` 打开，复制为工作区内的 `base.db`，设为只读并记录
  原库路径与摘要；此后工作区不再触碰原库。
- 初版（第 0 版）原样接收：
  - 区域判定（胜出 / 确认无片段 / 待定）；
  - 引用归票判定（存在 / 不存在 / 待定）；
  - 被阻断的原始关系记录。
  并保留可定位回原库的证据键（区域、候选、归票组、原始记录、包与记录位置）。
- 人工规则（层、标签、关系端点层约束）在初始化时提供并**固定**。构建库
  只保存计票结果，不保存策略全文，因此不要求旧库有未保存的策略。若固定的
  策略未覆盖构建库实际使用的层/关系类型，初始化被拒绝（`POLICY_BINDING_MISMATCH`）。

## 3. 查看与导入

```bash
qc-ws view    --workspace ws/ --rev HEAD --doc D1 --layer clause
qc-ws import  --workspace ws/ --ticket alice.json --manifest manifest.json
qc-ws preview --workspace ws/ --ticket alice.json --manifest manifest.json
```

- `view` 给出指定版本的区域、片段、引用与待办（含无效关联）。
- 工单有稳定身份：`sha256(reviewer, base_rev, 规范化内容)`；同一工单重入返回
  同一身份，不重复登记。
- 导入只做「提案先在自己的基版上成立」：原文摘要、UTF-8 边界、坐标在区域
  包络内、同区不重叠、端点层约束、引用退役/依赖等都在这一步校验；任何一处
  不成立即拒收，且不改动任何审定稿。
- `preview` 按工单基版预览整批提案（新片段、引用与办结的关联后果），不改 HEAD。

## 4. 工单格式

```json
{
  "reviewer": "alice",
  "title": "拆分 X 并退役旧引用",
  "base": "rev_…",
  "regions": [
    {"region": "<region_uid>", "decision": "fragments",
     "reason": "X 拆为 X1/X2",
     "fragments": [
       {"ref": "X1", "start": 0, "end": 4, "label": "L1"},
       {"ref": "X2", "start": 4, "end": 10, "label": "L1"}
     ]}
  ],
  "relations": [
    {"doc": "D1", "type": "ref", "from": "X1", "to": "<fragment_uid>",
     "decision": "present", "reason": "人工确认"},
    {"doc": "D1", "type": "ref", "from": "<fragment_uid>", "to": "<fragment_uid>",
     "decision": "retired", "reason": "端点移除，旧判定失效"}
  ],
  "blocked": [
    {"record": "<record_uid>", "disposition": "adopted",
     "relation": {"doc": "D1", "type": "ref", "from": "X2", "to": "<fragment_uid>"},
     "reason": "端点区域一致，采用"}
  ]
}
```

- **区域**提交该区域完整的审定片段集合：可判空（`decision:"none"`），可选现有
  片段（`fragment` 指向基版审定片段），或在区域包络内人工划出一个或多个片段
  （`start/end/label`）。同区片段不得正长度重叠，相邻可并存。
- 人工改划的坐标由调用者提供，但**不采用工单自报的切片**：工作区按库中摘要
  核对原文，校验边界落在 UTF-8 字符边界，并从原文取出 `seen_text`。
- 审定片段身份取决于底稿、文档、层、区间与标签
  （`sha256(doc, layer, start, end, label)`），与工单局部名、提交顺序无关；
  同一输入片段沿用稳定身份。
- 工单可在**同一批裁决**中引用自己新划出的片段（`ref`）。
- **引用裁决**用有序端点与类型定键：
  - `present` 确认存在；`absent` 确认不存在（只对现存两个端点）；
  - `retired` 明确退役因撤掉端点而失效的既有判定；它不是否定票，也不会因
    端点后来重现而自动生效。
  - 若替换片段使旧引用判定失去端点，工单**必须**显式退役或另立新引用，不能
    在合批时静默扔掉任何原有判定（见 `TICKET_RELATION_MUST_RETIRE`）。
- **阻断记录**单独办结：`rejected`（不采纳）或 `adopted`（关联一条**已人工
  确认存在**的审定引用）。关联要求文档、类型和两个方向上对应的原端点区域
  必须匹配；被关联的引用不新增团队票。原始阻断记录仍可查。若后续改动使关联
  失效，须在同一工单 `reopen` 使其重新待办，或重新作出有效办结。
- 每项裁决必须给出非空 `reason`；可选 `evidence` 列证据指针（候选 / 原始
  记录 / 复核项），引用证据不改变其自动判定。

## 5. 合批：两份工单如何一起进审定稿

```bash
qc-ws merge --workspace ws/ --ticket alice.json --manifest manifest.json
```

三方比较 **工单基版 / 当前审定稿 / 工单提案**：

| 情形 | 处理 |
| --- | --- |
| 只一边相对基版变更 | 取该变更 |
| 两边给出相同业务值 | 合并，保留双方理由与证据 |
| 两边给出不同业务值 | 冲突，整份工单不生效 |
| 提案未改动（= 自己的基版） | 不是删除，也不产生空版本 |

- 冲突返回可定位的冲突对象与原因；依赖冲突指出受影响的引用或办结项及其
  发生变化的端点。
- **对象键完全不重叠也要查依赖**：用拟合批的结果检查所有仍应存在的引用与
  已办结事项，其端点必须落在合批后仍有效的片段上；不能从同区另找一个片段
  顶替端点。
- 相同裁决的证据随版本留存；同一工单重试不会重复追加理由/证据。
- 合批成功生成可按 ID 重开的版本，保留前一审定稿、工单及其基版的关系。
- 复核员可依据当前版另做工单，原草稿保留。

### 安全性与规模

- 每个合批在单个 `BEGIN IMMEDIATE` 事务内完成，并持有工作区文件锁；
  两个进程同时合批不会互相覆盖。失败、强制退出或重试都**不会出现可见的半版**。
- 成功后同一工单重试返回**原结果**（幂等回执），即使成功时未收到回执。
- 常规合批只读写本批触及的文档；不复制或遍历不相干的公文。
- 单篇公文引用很多时，依赖检查走带索引的服务端游标逐行核对，不把整张关系网
  载入内存；冲突报告随处理写出，不整体驻留。
- 判定以「时间行」保存：每个版本只追加发生变化的事项，闭合旧行
  （`seq_removed`），不复制整份审定稿。

## 6. 发布：交接给下游

```bash
qc-ws blockers --workspace ws/ --rev HEAD --doc D1
qc-ws release  --workspace ws/ --rev HEAD --doc D1 --doc D2
qc-ws export   --workspace ws/ --release <release_id> --what fragments
qc-ws export   --workspace ws/ --release <release_id> --what relations
qc-ws export   --workspace ws/ --release <release_id> --what provenance
```

- 发布点名审定版本及一组公文。只要所选文档还有**待定区域、待定引用、未办结
  的阻断事项或无效关联**，就列出挡住发布的事项，不登记发布；其他文档的待办
  不阻挡本批。
- 登记成功的发布标识固定那一版和公文集合。发布是只读快照：后续合批、退役引用
  或发布新批次都不改变过去发布可读到的内容。
- 下游可据此**流式**取审定片段与存在的有向引用（服务端游标逐行产出）。
- 每条交付记录可回查是自动承接（`source_kind:"auto"`）还是人工裁决
  （`source_kind:"manual"`），以及相应版本、工单理由与证据。
- 否定、退役和未采纳的记录留在审定历史（`*_state`），不混进交付表
  （`release_fragment` / `release_relation` / `release_blocked`）。

## 7. 稳定标识

| 对象 | 身份 |
| --- | --- |
| 工单 | `sha256(reviewer, base_rev, 规范化内容)` |
| 审定片段 | `sha256(doc, layer, start, end, label)`（与构建阶段一致） |
| 引用判定 | 有序端点 `(doc, type, from_fragment, to_fragment)` |
| 版本 | `sha256(seq, parent_rev, ticket_uid, batch_id)` |
| 批次 | `sha256(ticket_uid, base_rev, current_rev, 内容摘要)` |
| 发布 | `sha256(rev_id, 文档集合, 发布序号)` |

不依赖工单局部名、提交顺序、数据库自增号或到达时间。

## 8. 可复跑例子

```bash
# 1) 生成小样本（构建库 + 甲/乙工单）
python -I tools/ws_fixture.py --root /tmp/wsample \
  --workspace /tmp/wsample/ws --tickets-dir /tmp/wsample/tickets

# 2) 交换先后合批：两个相同工作区
cp -a /tmp/wsample/ws /tmp/wsample/ws-a
cp -a /tmp/wsample/ws /tmp/wsample/ws-b
# ws-a：甲先乙后；ws-b：乙先甲后
# 甲成功；乙（在另一份已成形的审定稿上）报告新引用与被移除的旧端点冲突，
# 且不留下任何变更。
python -I -c "import sys;sys.path.insert(0,'.');from qc.ws_cli import main;raise SystemExit(main())" \
  merge --workspace /tmp/wsample/ws-a --ticket /tmp/wsample/tickets/alice.json \
  --manifest /tmp/wsample/manifest.json
python -I -c "import sys;sys.path.insert(0,'.');from qc.ws_cli import main;raise SystemExit(main())" \
  merge --workspace /tmp/wsample/ws-a --ticket /tmp/wsample/tickets/bob.json \
  --manifest /tmp/wsample/manifest.json   # -> conflict（endpoint）

# 3) 在已接受乙的工作区里：新工单明确退役两条旧引用、选 X2->Y，
#    并办结同类型同端点区域的阻断记录，再发布。
#    发布只导出人工确认的 X2->Y，不会为 X1 复制旧边。

# 4) 高扇出端点撤除：运行时生成，不入仓库
python -I tools/fanoutgen.py --root /tmp/fanout --fanout 20000
```

测试：`python -I -m unittest discover -s tests -v`（既有 31 例 + 裁决工作区 26 例）。
