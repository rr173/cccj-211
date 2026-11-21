-- 离线分仓台账（partition ledger）。
--
-- 台账是独立目录：本库 + 跨进程文件锁。裁决工作区只作为只读上游，
-- 台账不复制审定库，只保存摘要、证据指针与仓位安排。
--
-- 与裁决工作区一致的约定：
--   * 所有业务写入在单个 BEGIN IMMEDIATE 事务内完成，进程中断不留半版；
--   * 业务标识为内容派生的稳定文本 ID；
--   * 隐式同组关系（共同摘要）以「摘要 → 公文」索引保存，不展开成两两记录。

PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;

-- 台账元信息：ledger_version / layer / min_segment_bytes / upstream_workspace
CREATE TABLE lg_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 建账时配置的片段摘要豁免表（只豁免片段摘要，整篇底稿摘要不可豁免）
CREATE TABLE lg_exempt (
    digest TEXT PRIMARY KEY
);

-- 请求台账：幂等依据。同一 request_id 重放返回原回执；
-- 不同内容共用同一 request_id 拒绝。
CREATE TABLE lg_request (
    request_id     TEXT PRIMARY KEY,
    request_hash   TEXT NOT NULL,
    kind           TEXT NOT NULL,        -- change | confirm
    ledger_version INTEGER NOT NULL,     -- 该请求落定后的台账版本
    receipt_json   TEXT NOT NULL
);

-- 选入状态：一个档号同时只选一个发布；撤出后行保留（仍参与同组计算）
CREATE TABLE lg_selection (
    doc_id        TEXT PRIMARY KEY,
    release_id    TEXT NOT NULL,
    rev_id        TEXT NOT NULL,         -- 发布中的审定版本
    rev_seq       INTEGER NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('selected','withdrawn')),
    whole_digest  TEXT NOT NULL,         -- 整篇底稿摘要（上游 source_doc 登记值）
    updated_version INTEGER NOT NULL
);

-- 摘要索引（隐式同组边）：digest -> 公文。
--   source = 'inuse'     当前选入发布的在用摘要
--   source = 'delivered' 交付确认时留存的历史摘要（随交付凭据永久保留）
-- kind = 'whole'（整篇底稿）| 'segment'（合格审定片段）
-- fragment_uids_json：该摘要对应发布中的审定片段记录（可定位）；整篇为 NULL
-- delivery_id：历史摘要所属交付（inuse 行为空串）
CREATE TABLE lg_digest (
    digest             TEXT NOT NULL,
    doc_id             TEXT NOT NULL,
    source             TEXT NOT NULL CHECK (source IN ('inuse','delivered')),
    kind               TEXT NOT NULL CHECK (kind IN ('whole','segment')),
    release_id         TEXT,
    fragment_uids_json TEXT,
    delivery_id        TEXT NOT NULL DEFAULT '',
    UNIQUE (doc_id, source, kind, digest, delivery_id)
);
CREATE INDEX idx_lg_digest_value ON lg_digest(digest);
CREATE INDEX idx_lg_digest_doc ON lg_digest(doc_id);

-- 人工同源证据：独立登记/撤销；撤销保留行，不抹掉同对公文的其它证据
CREATE TABLE lg_evidence (
    evidence_id     TEXT PRIMARY KEY,
    doc_a           TEXT NOT NULL,
    doc_b           TEXT NOT NULL,
    provenance      TEXT NOT NULL,       -- 出处
    status          TEXT NOT NULL CHECK (status IN ('active','revoked')),
    created_version INTEGER NOT NULL,
    revoked_version INTEGER
);
CREATE INDEX idx_lg_evidence_a ON lg_evidence(doc_a);
CREATE INDEX idx_lg_evidence_b ON lg_evidence(doc_b);

-- 交付确认：按档号永久记下仓位；同一档号可多次留下实际交付的摘要
CREATE TABLE lg_delivery (
    delivery_id     TEXT PRIMARY KEY,
    doc_id          TEXT NOT NULL,
    position        TEXT NOT NULL CHECK (position IN ('train','eval')),
    list_version    INTEGER NOT NULL,    -- 所交付的清单版本
    request_id      TEXT NOT NULL,
    created_version INTEGER NOT NULL
);
CREATE INDEX idx_lg_delivery_doc ON lg_delivery(doc_id);

-- 仓位锁：一档号一仓位，首次交付即锁定，不可改写
CREATE TABLE lg_lock (
    doc_id            TEXT PRIMARY KEY,
    position          TEXT NOT NULL CHECK (position IN ('train','eval')),
    first_delivery_id TEXT NOT NULL
);

-- 连通分量（只覆盖在账公文；component_id = 组内 UTF-8 字节序最小档号）
CREATE TABLE lg_component (
    doc_id       TEXT PRIMARY KEY,
    component_id TEXT NOT NULL
);
CREATE INDEX idx_lg_component_id ON lg_component(component_id);

-- 当前清单（只在用公文；position = train | eval | quarantine）
CREATE TABLE lg_list (
    doc_id       TEXT PRIMARY KEY,
    component_id TEXT NOT NULL,
    position     TEXT NOT NULL CHECK (position IN ('train','eval','quarantine')),
    release_id   TEXT NOT NULL,
    rev_id       TEXT NOT NULL,
    rev_seq      INTEGER NOT NULL,
    updated_version INTEGER NOT NULL
);

-- 清单历史（按版本只记发生变化的行；position 为 NULL 表示该版不在清单）
CREATE TABLE lg_list_history (
    version      INTEGER NOT NULL,
    doc_id       TEXT NOT NULL,
    component_id TEXT,
    position     TEXT,
    PRIMARY KEY (version, doc_id)
);

-- 运行诊断：每次落定请求记录读取/重算工作量
CREATE TABLE lg_op (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    version     INTEGER NOT NULL,
    kind        TEXT NOT NULL,
    request_id  TEXT,
    stats_json  TEXT NOT NULL
);
