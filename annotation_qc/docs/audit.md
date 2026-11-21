# 发布后独立盲审（post-release blind audit）

自动质检把多家团队的意见整理成正式片段/引用，裁决工作区再把人工裁决合进
审定稿并按公文批次登记发布。档案馆在正式接收数据前，还要从**已经登记的
发布**里独立抽几篇公文做盲审。这一层提供发布后抽检，结论作为独立审计
记录留存，**不回写**质检结果、裁决工作区或发布内容。

沿用 Python、SQLite 与命令行；审计库是独立目录，已登记发布只以只读方式
打开。

## 1. 目录与模块

```
qc/audit_schema.sql   审计库结构（批次、抽样名单、片段/引用快照、盲审映射、结果）
qc/audit_common.py    连接、显式事务、跨进程文件锁、请求摘要、请求台账
qc/audit_upstream.py  只读上游：发布、发布片段/引用、公文身份
qc/audit_source.py    按批次绑定的底稿摘要核对证据坐标与原文
qc/audit.py           抽样、建立盲审批次、提交审核、只读查询与导出
qc/audit_cli.py       命令行入口（qc-audit）
tools/audit_fixture.py    可复跑小样本（真实管道；只留生成方式）
tools/audit_reference.py  不依赖产品实现的参考抽样（独立复算选中集合）
tools/audit_scalegen.py   运行时大批文档生成（产物不提交）
tools/audit_demo.py       可复跑验收示例（逐条断言）
```

## 2. 抽样：总体、单位与稳定性

```bash
python -I -c "import sys;sys.path.insert(0,'.');from qc.audit_cli import main;raise SystemExit(main())" \
  create --audit A --workspace WS --request create.json
```

```json
{"request_id": "r1", "release_id": "release_…", "sample_size": 3, "seed": 20261121}
```

- **总体**：该发布中包含至少一条**正式片段**或**正式引用**的不同公文。发布
  点名但没有正式记录的公文不进总体。
- **单位**：公文。**不放回**抽取恰好 `sample_size` 篇。
- **总体不足**：整次请求拒绝（`SAMPLE_TOO_LARGE` / `EMPTY_POPULATION`），
  不生成空壳批次，也不登记请求台账。
- **稳定性**：每篇公文的分值只由 `(种子, 档号)` 决定

  ```
  score = sha256("annotation_qc.publish_audit.sample.v1" \0
                 str(seed) \0 doc_id_utf8)
  ```

  按 `(score, doc_id UTF-8 bytes)` 升序取前 k。SQLite 行顺序、查询计划、输入
  文件排列都不参与；分值写入 SQLite 临时表排序，Python 只保留 O(k) 状态。
  `tools/audit_reference.py` 用独立实现复算同一集合。
- **同一发布 + 样本量 + 种子 → 同一批次**：批次 ID 还包含发布版本与总体/样本
  摘要，因此不会因两份发布恰好同 ID 而混淆；同参数再请求只会重开原批次。

## 3. 批次：可重开的发布绑定与快照

创建成功后，审计库在该批名下保存：

- 发布绑定：`release_id`、审定版本 `rev_id/rev_seq`、发布公文集合；
- 抽样参数与样本：样本量、种子、总体大小、总体/样本摘要、抽样名单；
- 抽中公文在该发布下的**全部**正式片段与正式引用（快照）；
- 每篇抽中公文的底稿身份：`source_doc` 登记的整篇摘要与字节长。

之后合批、改选、另发新发布都不会改变旧批次读到的内容。批次可按传入的
`release_id` 引用，也可直接用 `batch_id` 或 `blind_batch_id` 重开：

```bash
qc-audit batches --audit A
qc-audit show    --audit A --batch <batch_id 或 blind_batch_id>
```

## 4. 盲审标识与盲审导出

每批一个 32 字节随机密钥，只存审计库、永不进入导出。盲审标识：

```
blind_doc_id      = bd_ + HMAC-SHA256(secret, "document"｜正式档号)
blind_fragment_id = bf_ + HMAC-SHA256(secret, "fragment"｜fragment_uid)
blind_relation_id = br_ + HMAC-SHA256(secret, "relation"｜有序端点)
blind_batch_id    = aub_ + HMAC-SHA256(secret, "batch"｜batch_id)
```

只在本批有效，不依赖数据库自增号、明文档号或团队局部 ID，也不能跨批
互认。盲审导出逐行给出审核员需要的最小集合：

```bash
qc-audit blind --audit A --batch B --out blind.jsonl
```

```json
{"record_kind":"batch","blind_batch_id":"aub_…","sample_size":3,"population_size":4,"seed":20261121}
{"record_kind":"document","blind_doc_id":"bd_…","sample_rank":1,"byte_length":138,"source_sha256":"…"}
{"record_kind":"fragment","blind_doc_id":"bd_…","blind_fragment_id":"bf_…","layer":"clause","label":"L1","start":0,"end":8,"text_sha256":"…","text":"AAAA0000"}
{"record_kind":"relation","blind_doc_id":"bd_…","blind_relation_id":"br_…","rel_type":"ref","from":"bf_…","to":"bf_…"}
```

盲审行只含上列字段：**不含**正式档号、发布/版本、团队、包、原始投票、
自动/人工来源、裁决理由、请求 ID，也不含其它批次映射。`tools/audit_demo.py`
与单元测试逐项断言这些字段不出现。

- **精确字节范围**：`fragment` 行给出该片段的 `start/end`（半开字节区间）与
  `text`；`qc-audit source --blind-doc bd_… --start N --end M` 可按批次绑定的
  底稿摘要重新取回同一段字节并复算摘要。
- **不漏记录**：一篇公文一经抽中，其在本发布下的全部正式片段与引用都列出；
  有引用但无片段的公文也会出现在名单中，引用端点仍用本批盲审标识。

## 5. 审核提交

```bash
qc-audit submit --audit A --request submit.json --manifest M
```

```json
{"request_id": "s1", "batch_id": "aub_…", "reviewer": "auditor-1",
 "results": [
   {"subject": {"kind": "fragment", "blind_id": "bf_…"},
    "verdict": "issue", "note": "片段标签与正文不符",
    "evidence": {"blind_doc_id": "bd_…", "start": 0, "end": 8, "text": "AAAA0000"}},
   {"subject": {"kind": "document", "blind_id": "bd_…"},
    "verdict": "conforming", "note": "通读未见异常"}
 ]}
```

- **主体**用盲审标识定位，`kind` 取 `document | fragment | relation`。可选同时
  给出正式身份 `real_id`，但必须与盲审标识一致；也必须在**本批快照**中存在。
- **结论**：`conforming` 或 `issue`，均需非空 `note`。
- **问题证据**：`issue` 必须给出 `evidence`。系统按批次登记的底稿摘要实际重算
  文件摘要，校验区间非空、不越界、两端为 UTF-8 字符边界，并比对原文与证据
  文本（逐字节，不做归一化）。失败码：`BAD_EVIDENCE_RANGE`（边界/越界）、
  `EVIDENCE_TEXT_MISMATCH`（内容不符）、`EVIDENCE_DOC_NOT_IN_BATCH`。
- **整批原子**：任何一条非法（越批标识、同批重复主体、坐标非法、文本不符、
  已结案主体再次提交）整批拒绝（`SUBMISSION_REJECTED`，附逐项错误），
  **不部分写入**。
- **幂等**：成功提交用请求 ID 记录；相同请求重放返回原回执（`replayed:true`）。
  同一请求 ID 携带不同内容 `REQUEST_ID_CONFLICT`。
- **并发/中断**：创建与提交都在 `BEGIN IMMEDIATE` 单事务内完成并持文件锁。
  两个进程同时提交同一批次只会留下一次提交；进程在提交前被 SIGKILL 不留
  任何可见结果，重试可从干净状态成功。

## 6. 档案只读查询与导出

```bash
qc-audit coverage  --audit A --batch B            # 抽中范围与每篇未审数
qc-audit unsettled --audit A --batch B --out u.jsonl   # 未审记录（流式）
qc-audit results   --audit A --batch B --out r.jsonl   # 结论 + 可定位证据
qc-audit snapshot  --audit A --batch B --out s.jsonl   # 完整发布绑定快照
qc-audit resolve   --audit A --batch B --blind-id bf_…
qc-audit source    --audit A --batch B --blind-doc bd_… --start 0 --end 8
```

- `coverage`：每篇抽中公文的片段/引用总数、已审数、未审数、问题数。
- `unsettled`：尚无人审读的公文/片段/引用（给出盲审标识供档案定位）。
- `results`：结论、理由、审核员、以及原文证据（档号、字节区间、SHA-256、原文）。
- `snapshot`：发布绑定 + 抽中公文 + 全部片段与引用的正式身份与盲审标识。
- `resolve`：档案件把本批盲审标识解析回正式身份（**仅供档案，不用于盲审导出**）。

审计库独立，打开时不触碰原发布内容：`--workspace` 仅以 `mode=ro` 打开。
`results`/`snapshot` 只读；审计写入只在审计库内。

## 7. 稳定标识与确定性

| 对象 | 身份 |
| --- | --- |
| 批次 | `sha256("aub", 发布, 版本, 样本量, 种子, 总体大小, 总体摘要, 样本摘要)` |
| 请求 | 请求 ID + 内容规范化摘要（同 ID 不同内容拒绝） |
| 盲审公文 | `HMAC(本批密钥, "document", 档号)` |
| 盲审片段 | `HMAC(本批密钥, "fragment", fragment_uid)` |
| 盲审引用 | `HMAC(本批密钥, "relation", 规范化有序端点)` |
| 审核提交 | `sha256("aus", 批次, 请求 ID)` |

样本不因 SQLite 行顺序、查询计划或输入排列漂移；盲审标识不依赖自增号或
明文档号。

## 8. 可复跑例子

```bash
# 1) 生成发布/工作区小样本（真实管道；A/B 两团队 + 逆序重建）
python -I tools/audit_fixture.py --root /tmp/audit-demo/src
python -I tools/audit_fixture.py --root /tmp/audit-demo/src-rev --reverse

# 2) 完整验收（幂等、换种子、逆序稳定、盲审泄露、整批拒绝、中断恢复、
#    仅改工作区另发发布后旧批次不变；--large-docs 追加有界内存实测）
python -I tools/audit_demo.py --root /tmp/audit-demo --large-docs 20000

# 3) 参考抽样独立复算并对照批次
python -I tools/audit_reference.py --workspace /tmp/audit-demo/src/ws \
  --release <release_id> --sample-size 3 --seed 20261121 \
  --audit /tmp/audit-demo/audit --batch <batch_id>
```

测试：`python -I -m unittest discover -s tests -v`（既有 85 例 + 盲审 25 例，共 110 例）。
