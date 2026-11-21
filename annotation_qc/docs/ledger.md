# 分仓台账（partition ledger）

档案馆要用已审定的公文制作两份标注语料：一份训练、一份独立评测。
不同档号下可能存着同一底稿、或复用了同一条款；按档号随机分配会让
评测见到训练中已有的材料。台账把「哪些公文不能分开」的证据管起来，
产出**训练 / 评测 / 隔离**三张清单，并保证两边已交付的公文不会因为
语料更新而互换。

台账是离线组件：以裁决工作区为**只读上游**，不做模型训练、相似度打分，
也不改动原工作区的质检、工单与发布规则。

## 1. 模块与目录

```
qc/ledger_schema.sql    台账库结构
qc/ledger_common.py     连接/事务/文件锁/错误对象（与裁决工作区同款约定）
qc/ledger_upstream.py   只读上游存取（发布、审定片段、公文身份）
qc/ledger.py            建账、变更批、交付确认、清单、解释、局部重算
qc/ledger_cli.py        命令行入口（qc-ledger）
tools/ledger_fixture.py 演示小样本生成（真实管道构建；只留生成方法）
tools/ledger_demo.py    可复跑验收示例（甲/乙/丙/丁全场景）
tools/ledger_scalegen.py 规模生成与更新工作量实测（产物不提交）
tests/test_ledger.py    单元测试
```

一个台账是目录：`ledger.db`（WAL）+ `.ledger.lock`。所有业务写入在单个
`BEGIN IMMEDIATE` 事务内完成并持有目录级文件锁；进程在写入中退出，
重开不会留下部分摘要、错误锁位或与清单版本不符的回执；读者只看到
已落定的台账。

## 2. 只读上游与选入

选入一篇公文必须明确给出**档号 + 已登记发布 ID**。台账按发布 ID 读取：

- `release`：发布身份、审定版本（`rev_id`/`rev_seq`）、公文集合；
- `release_fragment`：该发布下的审定片段（`seen_text` 原样文本）；
- `source_doc`：公文身份（整篇底稿的登记摘要与字节长）。

不读取、也不以当前审定稿（HEAD 状态）代替发布快照。一个档号在台账里
同时只选一个发布；可以改选（替换在用摘要）或撤出（不再列为待取用公文，
但仍参与同组计算）。清单逐篇给出**档号、发布 ID、发布中的审定版本**，
下游按原接口（`qc-ws export --release …`）取片段与引用，台账不打包
交付审定库。

非法即整批拒绝（不部分入账）：未知发布（`UNKNOWN_RELEASE`）、档号不
属于该发布（`DOC_NOT_IN_RELEASE`）、非法证据变更等；一批中只要有一条
非法，整批不入账，台账版本不前进。

## 3. 哪些公文不能分开

三类证据，任一有效即连通，关联可传递：

1. **整篇底稿摘要**：`source_doc` 登记的摘要相同即同组（即使公文没有
   任何片段）；不可豁免。
2. **合格片段摘要**：只看建账时选定的标注层；达到约定最短字节数的审定
   片段按原样 UTF-8 字节做 SHA-256（不做清洗，并与上游 `text_sha256`
   交叉核对）。建账时可配置**片段摘要豁免表**（固定落款之类套话）。
3. **人工同源证据**：运营人员独立登记/撤销，给出证据 ID、两个已知档号
   与出处。撤销保留记录，不抹掉同一对公文的其它证据。证据只在两个端点
   都属于**在用或曾交付**档号时参与计算（未选入/已撤出且未交付的端点
   使边休眠；撤出公文的摘要边仍然有效）。

共同摘要只保存「摘要 → 公文」索引（`lg_digest`），**不展开成两两记录**；
少数条款被大量公文复用时也只是一行摘要对多篇公文。证据都可定位：摘要
边给出档号、发布 ID 与发布中的片段记录（历史摘要另注明交付凭据）；
人工边给出仍有效的证据 ID 与出处。

## 4. 仓位规则

- 正常清单里每个连通组只去训练或评测。
- **交付确认**按档号永久锁定仓位，并把这次实际交付所用的整篇与合格
  片段摘要留作历史证据（`lg_delivery` + `source='delivered'` 摘要行）；
  没确认交付就不落锁。已交付档号改选发布时计算新的在用摘要，不覆盖
  已交付摘要；同仓位的档号可对另一实际交付版本再次留下摘要（多次
  交付凭据并存）。确认只能针对**当前清单版本**：旧版不能二次确认成
  新交付（`STALE_LIST_VERSION`）。
- 组内只有一种已交付仓位 → 全组服从；两种都有 → **隔离**该组全部在用
  公文（不改写交付记录来消除冲突）；没有历史仓位的组按组内 UTF-8 字节
  序最小档号的 SHA-256 首字节分仓：偶数训练、奇数评测，不做配额平衡。
- 隔离组可按任一在用成员查询：两个不同仓位的历史确认（档号、仓位、
  交付凭据），以及把查询成员与两个锁定档号连起来的证据路径（不要求
  最短路径）。

## 5. 变更批、版本与幂等

```json
// 变更批（qc-ledger apply）
{"request_id": "b1", "base_version": 3,
 "select":   [{"doc_id": "档-丁", "release_id": "release_…"}],
 "withdraw": ["档-甲"],
 "evidence_register": [{"evidence_id": "E1", "doc_a": "档-丙",
                        "doc_b": "档-乙", "provenance": "1987年移交清册第14页"}],
 "evidence_revoke": ["E0"]}

// 交付确认（qc-ledger confirm）
{"request_id": "c1", "base_version": 4, "list_version": 4,
 "doc_ids": ["档-甲", "档-乙"]}
```

- 变更与交付确认都以明确的当前台账版本为前提（`base_version`），
  过期拒绝（`STALE_BASE_VERSION`）；仓位按整批完成后的状态计算。
- 每个请求携带请求 ID：同一请求成功后即使版本前进，重放也取回原回执；
  不同内容共用同一 ID 拒绝（`REQUEST_ID_CONFLICT`）。
- 回执与版本推进在同一事务内落库；回执内含诊断信息。

## 6. 局部重算与诊断

日常只更几篇公文，库中可能有很多互不相干的组。变更时只重算受影响的
连通分量：变更档号所在分量 ∪ 共用其旧/新摘要的档号所在分量 ∪ 人工证据
邻接档号所在分量（均为索引点查），包括证据撤销后分裂的子组与新连入的
组；不重读不相干的发布明细，不遍历不相干的分量。大分量与结果清单落在
SQLite（磁盘索引）上，不把整个台账装进内存。

每次落定都记录诊断（`lg_op`，回执内也带）：读取的发布/公文、访问的
摘要/证据行数、受影响分量与重算成员数、耗时、模式（incremental/full）。
`qc-ledger rebuild` 全库重建、`qc-ledger verify` 在副本上全量重算并与
增量维护的状态逐篇比对，用于区分局部计算与全库重建。

实测（`tools/ledger_scalegen.py`，1500 个两人小组 + 1 个 2000 篇大组，
共 5001 篇）：

| 操作 | 读发布 | 读公文 | 访问摘要行 | 重算成员 | 耗时 |
| --- | --- | --- | --- | --- | --- |
| 小组内登记人工证据 | 0 | 2 | 6 | 2 | ~1 ms |
| 新公文连入大组 | 1 | 1 | 6003 | 2001 | ~82 ms |
| 撤销小组证据（分量分裂） | 0 | 0 | 6 | 2 | ~1 ms |
| 全库重建（对照） | 0 | 0 | 15003 | 5001 | ~173 ms |

## 7. 命令行

```bash
qc-ledger init    --ledger L --workspace WS --layer clause --min-bytes 16 \
                  --exempt exempt.json        # {"digests": [...], "texts": [...]}
qc-ledger apply   --ledger L --batch batch.json
qc-ledger confirm --ledger L --request confirm.json
qc-ledger lists   --ledger L --out out/       # train/eval/quarantine.jsonl + manifest
qc-ledger explain --ledger L --doc 档-丙      # 为什么分在一起/不能交付
qc-ledger path    --ledger L --from 档-丁 --to 档-甲
qc-ledger quarantine --ledger L --doc 档-丙   # 两个仓位的历史确认 + 证据路径
qc-ledger credentials --ledger L --doc 档-甲  # 历史交付凭据
qc-ledger status  --ledger L
qc-ledger verify  --ledger L
```

## 8. 可复跑示例与测试

```bash
# 验收示例：甲乙分仓交付 → 丙进入后三篇隔离 → 撤证据回仓 →
# 甲改选撤出 → 丁随甲历史仓位 → 各项对照检查
python -I tools/ledger_demo.py --root /tmp/ldemo

# 规模生成与更新工作量实测（产物不提交）
python -I tools/ledger_scalegen.py --root /tmp/lscale \
  --groups 1500 --large-group 2000

# 单元测试
python -I -m unittest discover -s tests -p "test_ledger.py" -v
```
