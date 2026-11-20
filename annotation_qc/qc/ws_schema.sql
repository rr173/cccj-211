-- 离线裁决工作区（adjudication workspace）。
--
-- 一个工作区是目录：本库 + 只读的底稿构建库副本 base.db。
-- 所有业务标识仍为内容派生的稳定文本 ID。
-- 审定状态用「时间行」保存：每个版本只写发生变化的事项，闭合旧行
-- （seq_removed），不复制整份审定稿，也不遍历不相干的公文。

PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;

-- 工作区元信息与固定策略 ----------------------------------------------------
CREATE TABLE ws_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE ws_policy (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    raw_json      TEXT NOT NULL,
    policy_sha256 TEXT NOT NULL
);

CREATE TABLE ws_layer (
    layer       TEXT PRIMARY KEY,
    labels_json TEXT NOT NULL
);

CREATE TABLE ws_relation_type (
    rel_type   TEXT PRIMARY KEY,
    from_layer TEXT NOT NULL,
    to_layer   TEXT NOT NULL
);

-- 底稿构建库（只读副本） ----------------------------------------------------
CREATE TABLE base_db (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    path            TEXT NOT NULL,       -- 初始化时实际使用的构建库
    sha256          TEXT NOT NULL,       -- 原库摘要（与副本一致）
    byte_length     INTEGER NOT NULL,
    copy_name       TEXT NOT NULL,       -- 工作区内的只读副本名
    build_meta_json TEXT NOT NULL,
    imported_at     TEXT NOT NULL
);

-- 原文存取：调用者提供路径，按库中摘要核对；原库只读 ------------------------
CREATE TABLE source_doc (
    doc_id       TEXT PRIMARY KEY,
    rel_path     TEXT,
    abs_path     TEXT,
    sha256       TEXT NOT NULL,
    byte_length  INTEGER NOT NULL,
    verified_sha TEXT,
    verified     INTEGER NOT NULL DEFAULT 0
);

-- 底稿快照（初始化时一次性导入；自动计票结果原样保留） ----------------------
CREATE TABLE base_region (
    region_uid       TEXT PRIMARY KEY,
    doc_id           TEXT NOT NULL,
    layer            TEXT NOT NULL,
    start_byte       INTEGER NOT NULL,
    end_byte         INTEGER NOT NULL,
    outcome_kind     TEXT NOT NULL,     -- candidate | no_fragment | pending
    winner_candidate TEXT,
    denominator      INTEGER,
    strict_majority  INTEGER
);
CREATE INDEX idx_base_region_doc ON base_region(doc_id, layer);

CREATE TABLE base_region_candidate (
    candidate_uid   TEXT PRIMARY KEY,
    region_uid      TEXT NOT NULL,
    start_byte      INTEGER NOT NULL,
    end_byte        INTEGER NOT NULL,
    label           TEXT NOT NULL,
    is_no_fragment  INTEGER NOT NULL
);
CREATE INDEX idx_base_cand_region ON base_region_candidate(region_uid);

CREATE TABLE base_official_fragment (
    fragment_uid  TEXT PRIMARY KEY,
    doc_id        TEXT NOT NULL,
    layer         TEXT NOT NULL,
    label         TEXT NOT NULL,
    start_byte    INTEGER NOT NULL,
    end_byte      INTEGER NOT NULL,
    region_uid    TEXT NOT NULL,
    candidate_uid TEXT,
    text_sha256   TEXT NOT NULL,
    seen_text     TEXT NOT NULL
);
CREATE INDEX idx_base_frag_doc ON base_official_fragment(doc_id, layer);

CREATE TABLE base_relation_group (
    rel_group_uid   TEXT PRIMARY KEY,
    doc_id          TEXT NOT NULL,
    rel_type        TEXT NOT NULL,
    from_fragment   TEXT NOT NULL,
    to_fragment     TEXT NOT NULL,
    denominator     INTEGER NOT NULL,
    for_votes       INTEGER NOT NULL,
    against_votes   INTEGER NOT NULL,
    conflict_votes  INTEGER NOT NULL,
    strict_majority INTEGER NOT NULL,
    min_support     INTEGER NOT NULL,
    decision        TEXT NOT NULL     -- confirmed | confirmed_absent | pending
);
CREATE INDEX idx_base_relgroup_doc ON base_relation_group(doc_id, rel_type);

CREATE TABLE base_relation_group_vote (
    rel_group_uid TEXT NOT NULL,
    package_uid   TEXT NOT NULL,
    team_id       TEXT NOT NULL,
    sign          TEXT NOT NULL,
    record_uids   TEXT NOT NULL,
    PRIMARY KEY (rel_group_uid, package_uid)
);

CREATE TABLE base_review_item (
    review_uid    TEXT PRIMARY KEY,
    item_kind     TEXT NOT NULL,
    doc_id        TEXT NOT NULL,
    layer         TEXT,
    rel_type      TEXT,
    region_uid    TEXT,
    rel_group_uid TEXT,
    record_uid    TEXT,
    reason_code   TEXT NOT NULL,
    detail_json   TEXT NOT NULL
);
CREATE INDEX idx_base_review_doc ON base_review_item(doc_id, item_kind);

-- 被阻断的原始关系（底稿原样保留，用于逐条办结与回溯） ----------------------
CREATE TABLE base_blocked (
    record_uid     TEXT PRIMARY KEY,
    doc_id         TEXT NOT NULL,
    rel_type       TEXT NOT NULL,
    package_uid    TEXT NOT NULL,
    team_id        TEXT NOT NULL,
    from_local     TEXT NOT NULL,
    to_local       TEXT NOT NULL,
    sign           TEXT NOT NULL,
    from_proposal  TEXT,
    to_proposal    TEXT,
    from_region    TEXT,
    to_region      TEXT,
    blocked_reason TEXT,
    file_name      TEXT,
    line_no        INTEGER,
    record_index   INTEGER,
    payload_json   TEXT
);
CREATE INDEX idx_base_blocked_doc ON base_blocked(doc_id, rel_type);

CREATE TABLE base_package (
    package_uid  TEXT PRIMARY KEY,
    package_id   TEXT NOT NULL,
    doc_id       TEXT NOT NULL,
    team_id      TEXT NOT NULL,
    content_hash TEXT
);

CREATE TABLE base_proposal (
    proposal_uid  TEXT PRIMARY KEY,
    package_uid   TEXT NOT NULL,
    doc_id        TEXT NOT NULL,
    layer         TEXT NOT NULL,
    local_id      TEXT NOT NULL,
    label         TEXT NOT NULL,
    start_byte    INTEGER NOT NULL,
    end_byte      INTEGER NOT NULL,
    region_uid    TEXT
);
CREATE INDEX idx_base_prop_region ON base_proposal(region_uid);

-- 版本（审定稿） -------------------------------------------------------------
CREATE TABLE revision (
    rev_id        TEXT PRIMARY KEY,     -- rev_<32hex>
    seq           INTEGER NOT NULL UNIQUE,
    parent_rev    TEXT REFERENCES revision(rev_id),
    base_rev      TEXT REFERENCES revision(rev_id),   -- 工单所基于的版本
    ticket_uid    TEXT,
    created_at    TEXT NOT NULL,
    reason_json   TEXT NOT NULL         -- 本版变更摘要（不含工单正文）
);
CREATE INDEX idx_revision_seq ON revision(seq);

-- 工单：稳定身份 + 复核员 + 基版 --------------------------------------------
CREATE TABLE ticket (
    ticket_uid    TEXT PRIMARY KEY,
    reviewer_id   TEXT NOT NULL,
    base_rev      TEXT NOT NULL REFERENCES revision(rev_id),
    topic         TEXT NOT NULL,
    title         TEXT,
    content_json  TEXT NOT NULL,        -- 规范化后的工单内容（幂等依据）
    created_at    TEXT NOT NULL,
    import_seq    INTEGER NOT NULL
);
CREATE INDEX idx_ticket_base ON ticket(base_rev);

-- 工单内的新片段（提交时即固定业务身份） ------------------------------------
CREATE TABLE ticket_fragment (
    ticket_uid    TEXT NOT NULL REFERENCES ticket(ticket_uid),
    local_id      TEXT NOT NULL,        -- 工单局部名（仅工单内引用用）
    fragment_uid  TEXT NOT NULL,        -- 内容派生：doc+layer+区间+标签
    doc_id        TEXT NOT NULL,
    layer         TEXT NOT NULL,
    label         TEXT NOT NULL,
    start_byte    INTEGER NOT NULL,
    end_byte      INTEGER NOT NULL,
    text_sha256   TEXT NOT NULL,
    seen_text     TEXT NOT NULL,
    PRIMARY KEY (ticket_uid, local_id)
);

-- 工单裁决的说明与证据 -------------------------------------------------------
CREATE TABLE ticket_note (
    ticket_uid    TEXT NOT NULL REFERENCES ticket(ticket_uid),
    subject_kind  TEXT NOT NULL,        -- region | relation_decision | blocked | fragment
    subject_key   TEXT NOT NULL,
    reason        TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    PRIMARY KEY (ticket_uid, subject_kind, subject_key)
);

-- 合批批次（一次合批一个工单；结果可幂等重取） ------------------------------
CREATE TABLE merge_batch (
    batch_id       TEXT PRIMARY KEY,
    ticket_uid     TEXT NOT NULL,
    base_rev       TEXT NOT NULL,
    current_rev    TEXT NOT NULL,
    ticket_sha256  TEXT NOT NULL,
    result_rev     TEXT,
    status         TEXT NOT NULL,       -- merged | conflict
    conflict_json  TEXT,
    receipt_json   TEXT NOT NULL,
    created_at     TEXT NOT NULL
);
CREATE INDEX idx_merge_ticket ON merge_batch(ticket_uid);
CREATE INDEX idx_merge_result ON merge_batch(result_rev);

-- 审定状态（时间行）：每个版本只追加变化事项，旧行以 seq_removed 闭合 ------
-- 片段：人工确认的审定片段（含承接自动胜出的片段） --------------------------
CREATE TABLE fragment_state (
    fragment_uid  TEXT NOT NULL,
    doc_id        TEXT NOT NULL,
    layer         TEXT NOT NULL,
    label         TEXT NOT NULL,
    start_byte    INTEGER NOT NULL,
    end_byte      INTEGER NOT NULL,
    text_sha256   TEXT NOT NULL,
    seen_text     TEXT NOT NULL,
    origin        TEXT NOT NULL,        -- auto | manual
    region_uid    TEXT,                 -- 来源区域（可为 NULL：跨区域人工划出）
    seq_added     INTEGER NOT NULL,
    seq_removed   INTEGER,              -- NULL = 在本版本仍生效
    related_rev   TEXT NOT NULL,        -- 登记/最后确认它的版本
    PRIMARY KEY (fragment_uid, seq_added)
);
CREATE INDEX idx_frag_state_live ON fragment_state(doc_id, layer, seq_removed);
CREATE INDEX idx_frag_state_uid ON fragment_state(fragment_uid, seq_removed);
CREATE INDEX idx_frag_state_uid_seq ON fragment_state(fragment_uid, seq_added);

-- 区域整组判定（fragments | none | pending） ---------------------------------
CREATE TABLE region_state (
    region_uid    TEXT NOT NULL,
    doc_id        TEXT NOT NULL,
    layer         TEXT NOT NULL,
    kind          TEXT NOT NULL,        -- fragments | none | pending
    fragments_json TEXT NOT NULL,       -- 有序 JSON 列表（kind=fragments 时）
    seq_added     INTEGER NOT NULL,
    seq_removed   INTEGER,
    related_rev   TEXT NOT NULL,
    PRIMARY KEY (region_uid, seq_added)
);
CREATE INDEX idx_region_state_live ON region_state(doc_id, layer, seq_removed);
CREATE INDEX idx_region_state_doc_seq ON region_state(doc_id, seq_added, seq_removed);
CREATE INDEX idx_region_state_uid ON region_state(region_uid, seq_removed);

-- 引用判定（有/没有/退役） ---------------------------------------------------
CREATE TABLE relation_state (
    state_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id       TEXT NOT NULL,
    rel_type     TEXT NOT NULL,
    from_fragment TEXT NOT NULL,
    to_fragment  TEXT NOT NULL,
    kind         TEXT NOT NULL,         -- present | absent | retired
    origin       TEXT NOT NULL,         -- auto | manual
    seq_added    INTEGER NOT NULL,
    seq_removed  INTEGER,
    related_rev  TEXT NOT NULL
);
CREATE INDEX idx_rel_state_live ON relation_state(doc_id, rel_type, seq_removed);
CREATE INDEX idx_rel_state_key ON relation_state(doc_id, rel_type, from_fragment, to_fragment, seq_added);
CREATE INDEX idx_rel_state_to_live ON relation_state(to_fragment, seq_removed, doc_id);
CREATE INDEX idx_rel_state_doc_seq ON relation_state(doc_id, seq_added, seq_removed);
CREATE INDEX idx_rel_state_from ON relation_state(from_fragment, seq_removed);
CREATE INDEX idx_rel_state_to ON relation_state(to_fragment, seq_removed);

-- 被阻断原始关系的办结（adopted | rejected） --------------------------------
CREATE TABLE blocked_state (
    record_uid       TEXT NOT NULL,
    disposition      TEXT NOT NULL,     -- adopted | rejected
    rel_type         TEXT NOT NULL,
    from_fragment    TEXT,              -- adopted 时指向人工确认存在的引用端点
    to_fragment      TEXT,
    from_region_uid  TEXT,              -- adopted 时必须与底稿端点区域一致
    to_region_uid    TEXT,
    seq_added        INTEGER NOT NULL,
    seq_removed      INTEGER,
    related_rev      TEXT NOT NULL,
    PRIMARY KEY (record_uid, seq_added)
);
CREATE INDEX idx_block_state_live ON blocked_state(record_uid, seq_removed);
CREATE INDEX idx_block_state_seq ON blocked_state(record_uid, seq_added);


-- 状态变更证据（随版本留存；相同裁决的证据不重复追加） ----------------------
CREATE TABLE state_evidence (
    subject_kind  TEXT NOT NULL,
    subject_key   TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    source_uid    TEXT NOT NULL,        -- ticket_uid，自动承接用 'auto'
    ticket_uid    TEXT,
    reviewer_id   TEXT,
    reason        TEXT,
    evidence_json TEXT NOT NULL,
    rev_id        TEXT NOT NULL,
    PRIMARY KEY (subject_kind, subject_key, seq, source_uid)
);
CREATE INDEX idx_evidence_key ON state_evidence(subject_kind, subject_key, seq);

-- 发布登记 -------------------------------------------------------------------
CREATE TABLE release (
    release_id   TEXT PRIMARY KEY,
    rev_id       TEXT NOT NULL REFERENCES revision(rev_id),
    rev_seq      INTEGER NOT NULL,
    created_at   TEXT NOT NULL,
    doc_ids_json TEXT NOT NULL,
    request_json TEXT NOT NULL
);

CREATE TABLE release_fragment (
    release_id   TEXT NOT NULL REFERENCES release(release_id),
    fragment_uid TEXT NOT NULL,
    doc_id       TEXT NOT NULL,
    layer        TEXT NOT NULL,
    label        TEXT NOT NULL,
    start_byte   INTEGER NOT NULL,
    end_byte     INTEGER NOT NULL,
    text_sha256  TEXT NOT NULL,
    seen_text    TEXT NOT NULL,
    origin       TEXT NOT NULL,
    source_kind  TEXT NOT NULL,         -- auto | manual
    source_rev   TEXT NOT NULL,
    ticket_uid   TEXT,
    reviewer_id  TEXT,
    reason       TEXT,
    evidence_json TEXT NOT NULL,
    PRIMARY KEY (release_id, fragment_uid)
);
CREATE INDEX idx_release_frag_doc ON release_fragment(release_id, doc_id, layer);

CREATE TABLE release_relation (
    release_id    TEXT NOT NULL REFERENCES release(release_id),
    doc_id        TEXT NOT NULL,
    rel_type      TEXT NOT NULL,
    from_fragment TEXT NOT NULL,
    to_fragment   TEXT NOT NULL,
    source_kind   TEXT NOT NULL,        -- auto | manual
    source_rev    TEXT NOT NULL,        -- 令该引用成为审定引用的版本
    ticket_uid    TEXT,
    reviewer_id   TEXT,
    reason        TEXT,
    evidence_json TEXT NOT NULL,
    PRIMARY KEY (release_id, doc_id, rel_type, from_fragment, to_fragment)
);
CREATE INDEX idx_release_rel_doc ON release_relation(release_id, doc_id, rel_type);

CREATE TABLE release_blocked (
    release_id    TEXT NOT NULL REFERENCES release(release_id),
    record_uid    TEXT NOT NULL,
    disposition   TEXT NOT NULL,        -- adopted | rejected
    rel_type      TEXT NOT NULL,
    from_fragment TEXT,
    to_fragment   TEXT,
    source_kind   TEXT NOT NULL,
    source_rev    TEXT NOT NULL,
    ticket_uid    TEXT,
    reviewer_id   TEXT,
    reason        TEXT,
    evidence_json TEXT NOT NULL,
    PRIMARY KEY (release_id, record_uid)
);

