# 入口说明

## 1. 输入

### 1.1 原文清单 `manifest.json`
```json
{
  "documents": [
    {"doc_id": "D1", "path": "sources/doc1.txt", "sha256": "<64 位十六进制>"}
  ]
}
```
- `path` 为相对清单所在目录的相对名，禁止绝对路径或 `..` 越界；
- 原文按字节原样读取（严格 UTF-8），构建时实际重算 SHA-256 并与清单核对。

### 1.2 策略声明 `policy.json`
```json
{
  "layers": {
    "clause": {"labels": ["L1", "L2"], "min_support": 2}
  },
  "relation_types": {
    "ref": {"from_layer": "clause", "to_layer": "annex", "min_support": 2}
  }
}
```
每层、每种关系的 `min_support` 至少为 2（二方）。

### 1.3 标注包（JSONL，可流式、可分包）
- 单文件 `*.jsonl`，或目录 bundle（其内多个 `*.jsonl`，按相对名排序）；
- 首行必须是包头，bundle 中只有第一个文件首行允许包头；
- 同 `(doc_id, team_id)` 只允许一个包。

包头：
```json
{"kind":"package","package_id":"PKG-A","doc_id":"D1","team_id":"teamA",
 "source_digest":"<可选，声明的原文摘要>"}
```

三种记录：
```json
{"kind":"review","layer":"clause","ranges":[[0,5],[5,9]]}
{"kind":"fragment","id":"f1","layer":"clause","label":"L1",
 "start":0,"end":4,"seen_text":"0123"}
{"kind":"relation","type":"ref","from_id":"f1","to_id":"a1","sign":"for"}
```
- 区间为非空半开字节区间，两端必须落在 UTF-8 字符边界；
- `seen_text` 必须与对应字节逐字节一致，不做 Unicode 归一化；
- 关系 `sign` 取 `for` / `against`；引用允许先于被引片段出现；
- 关系端点只能解析到本包实际提议的片段 ID。

## 2. 构建入口
```bash
python -m qc.cli build \
  --manifest fixtures/manifest.json \
  --packages fixtures/packages \
  --policy fixtures/policy.json \
  --out out/warehouse.db \
  --reject-report out/reject.json
```
- 成功：在 `--out` 原子发布单一 db 文件（同目录临时文件 + `os.replace`）；
- 失败：不发布、不替换既有完整结果，逐项结构错误写入拒收报告，
  错误含包、文件相对名、物理行号、包内记录序号与错误码。

隔离环境（无第三方依赖，建议用 `-I`）：
```bash
python -I -c "import sys;sys.path.insert(0,'.');from qc.cli import main;raise SystemExit(main())" build ...
```

## 3. 只读查询入口
```bash
python -m qc.cli query --db out/warehouse.db --what meta
python -m qc.cli query --db out/warehouse.db --what regions  --doc D1 --layer clause
python -m qc.cli query --db out/warehouse.db --what fragments --doc D1
python -m qc.cli query --db out/warehouse.db --what relations --rel-type ref
python -m qc.cli query --db out/warehouse.db --what reviews
python -m qc.cli query --db out/warehouse.db --what blocked
```

## 4. 逐项证据展开
```bash
# 争议区域：分母、候选票、自相矛盾团队、覆盖缺口、被排除意见、记录位置
python -m qc.cli show --db out/warehouse.db --region <region_uid>

# 关系归票组：双方票数、冲突票、每条原始记录与包证据
python -m qc.cli show-rel --db out/warehouse.db --group <rel_group_uid>

# 单条原始关系追踪：两个局部端点、各自区域、是否被采信
python -m qc.cli trace-rel --db out/warehouse.db --record <record_uid>
```

## 5. 流式导出（JSONL，服务端游标逐行）
```bash
python -m qc.cli export --db out/warehouse.db --what official_fragments --out f.jsonl
python -m qc.cli export --db out/warehouse.db --what official_relations
python -m qc.cli export --db out/warehouse.db --what review_items --doc D1
```

## 6. 库以只读方式打开
所有查询都通过 `sqlite3 ... ?mode=ro` 打开，任何写操作都会抛
`sqlite3.OperationalError`。正式库不依赖原标注包路径：片段所见原文与
摘要、审核区间、记录位置证据均已落库。

## 7. 裁决工作区（复核合批与审定发布）
在构建库之外，`qc-ws` 提供离线的裁决工作区：以构建库初始化只读底稿，
导入复核员工单、三方合批为审定版本、按公文批次登记发布。
入口与工单格式见 `docs/adjudication.md`。

```bash
qc-ws init    --workspace ws/ --base-db out/warehouse.db --policy fixtures/policy.json
qc-ws view    --workspace ws/ --rev HEAD --doc D1
qc-ws import  --workspace ws/ --ticket t.json --manifest fixtures/manifest.json
qc-ws merge   --workspace ws/ --ticket t.json --manifest fixtures/manifest.json
qc-ws release --workspace ws/ --rev HEAD --doc D1
qc-ws export  --workspace ws/ --release <id> --what relations
```

所有工作区命令都在 `python -I` 下运行：

```bash
python -I -c "import sys;sys.path.insert(0,'.');from qc.ws_cli import main;raise SystemExit(main())" view --workspace ws/ --doc D1
```

## 8. 稳定标识
- `package_uid = sha256(doc_id, team_id, package_id)`；
- `record_uid  = sha256(package_uid, 规范化记录内容, 出现序号)`；
- `region_uid  = sha256(doc_id, layer, 包络起止)`；
- `fragment_uid= sha256(doc_id, layer, 起止, 标签)`；
- 关系组 `rel_group_uid = sha256(doc_id, type, 起点片段, 终点片段)`。

业务身份不使用数据库自增号，也不使用团队局部 ID。合法记录或文件重排
只可能改变物理行号（仅作证据），不改变任何逻辑标识与业务结果。
