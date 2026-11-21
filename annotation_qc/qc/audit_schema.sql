-- 发布后独立盲审（post-release blind audit）。
--
-- 审计库是独立目录：本库 + 跨进程文件锁。已登记的发布只作为只读上游，
-- 审计不复制、也不回写质检库、裁决工作区或发布内容。
--
-- 一个批次在创建时把「发布绑定 + 抽样名单 + 该批全部正式片段/引用」以
-- 快照形式保存，之后可按批次 ID 重新打开；后续新的合批/发布不影响旧批次
-- 可读到的内容。盲审标识由本批随机密钥派生，只在本批有效。
--
-- 所有业务写入在单个 BEGIN IMMEDIATE 事务内完成；进程中断不留下可见的
-- 半批结果。业务标识均为内容派生的文本 ID，不使用数据库自增号。

PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;

-- 审计库元信息 --------------------------------------------------------------
CREATE TABLE aud_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 盲审批次：发布绑定 + 抽样参数 + 本批盲审密钥（密钥永不进入导出） --------
-- batch_id 由「发布 + 样本量 + 种子 + 总体/样本集合摘要」派生：相同参数
-- 得到同一批次，因此样本不会漂移。创建它的请求 ID 只作记录，不作为身份。
CREATE TABLE aud_batch (
    batch_id             TEXT PRIMARY KEY,
    created_request_id   TEXT NOT NULL,
    release_id           TEXT NOT NULL,
    rev_id               TEXT NOT NULL,
    rev_seq              INTEGER NOT NULL,
    release_doc_ids_json TEXT NOT NULL,
    release_digest       TEXT NOT NULL,   -- 抽中公文在本发布下的片段/引用摘要
    sample_size          INTEGER NOT NULL CHECK (sample_size > 0),
    seed                 INTEGER NOT NULL,
    population_size      INTEGER NOT NULL,
    population_digest    TEXT NOT NULL,   -- 排序后总体档号集合摘要
    sample_digest        TEXT NOT NULL,   -- 本批抽中档号集合摘要
    blind_secret         TEXT NOT NULL,   -- 32 字节 hex；仅存于审计库
    blind_batch_id       TEXT NOT NULL UNIQUE,
    fragment_count       INTEGER NOT NULL,
    relation_count       INTEGER NOT NULL,
    created_at           TEXT NOT NULL,
    request_json         TEXT NOT NULL
);
CREATE INDEX idx_aud_batch_release ON aud_batch(release_id);

-- 请求台账：幂等依据。同一 request_id 重放返回原回执；
-- 不同内容共用同一 request_id 拒绝。kind = create | submit。
CREATE TABLE aud_request (
    request_id   TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    kind         TEXT NOT NULL CHECK (kind IN ('create', 'submit')),
    batch_id     TEXT NOT NULL REFERENCES aud_batch(batch_id),
    receipt_json TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

-- 抽中的公文（抽样名单）。source_sha256/byte_length 来自发布绑定的公文身份，
-- 是后续核验审核证据的底稿摘要；rel_path/abs_path 仅用于按需取原文。
CREATE TABLE aud_sample_doc (
    batch_id        TEXT NOT NULL REFERENCES aud_batch(batch_id),
    doc_id          TEXT NOT NULL,
    blind_doc_id    TEXT NOT NULL,
    sample_rank     INTEGER NOT NULL,
    source_sha256   TEXT NOT NULL,
    byte_length     INTEGER NOT NULL,
    rel_path        TEXT,
    abs_path        TEXT,
    PRIMARY KEY (batch_id, doc_id),
    UNIQUE (batch_id, blind_doc_id),
    UNIQUE (batch_id, sample_rank)
);

-- 抽中公文的正式片段快照（盲审可见字段；不含团队/投票/裁决出处/理由） ------
CREATE TABLE aud_fragment (
    batch_id          TEXT NOT NULL REFERENCES aud_batch(batch_id),
    fragment_uid      TEXT NOT NULL,
    blind_fragment_id TEXT NOT NULL,
    doc_id            TEXT NOT NULL,
    blind_doc_id      TEXT NOT NULL,
    layer             TEXT NOT NULL,
    label             TEXT NOT NULL,
    start_byte        INTEGER NOT NULL,
    end_byte          INTEGER NOT NULL,
    text_sha256       TEXT NOT NULL,
    seen_text         TEXT NOT NULL,
    PRIMARY KEY (batch_id, fragment_uid),
    UNIQUE (batch_id, blind_fragment_id)
);
CREATE INDEX idx_aud_fragment_doc ON aud_fragment(batch_id, doc_id, layer,
                                                  start_byte, end_byte);

-- 抽中公文的正式引用快照（盲审可见字段；端点用本批盲审标识） --------------
CREATE TABLE aud_relation (
    batch_id          TEXT NOT NULL REFERENCES aud_batch(batch_id),
    blind_relation_id TEXT NOT NULL,
    doc_id            TEXT NOT NULL,
    blind_doc_id      TEXT NOT NULL,
    rel_type          TEXT NOT NULL,
    from_fragment     TEXT NOT NULL,
    to_fragment       TEXT NOT NULL,
    blind_from        TEXT NOT NULL,
    blind_to          TEXT NOT NULL,
    PRIMARY KEY (batch_id, blind_relation_id),
    UNIQUE (batch_id, doc_id, rel_type, from_fragment, to_fragment)
);
CREATE INDEX idx_aud_relation_doc ON aud_relation(batch_id, doc_id, rel_type);

-- 盲审标识 -> 正式记录身份映射。只供本批解析/档案查询，绝不进入盲审导出。
CREATE TABLE aud_blind_map (
    batch_id TEXT NOT NULL REFERENCES aud_batch(batch_id),
    kind     TEXT NOT NULL CHECK (kind IN ('document', 'fragment', 'relation')),
    blind_id TEXT NOT NULL,
    real_id  TEXT NOT NULL,
    doc_id   TEXT NOT NULL,
    PRIMARY KEY (batch_id, kind, blind_id),
    UNIQUE (batch_id, kind, real_id)
);
CREATE INDEX idx_aud_blind_real ON aud_blind_map(batch_id, kind, real_id);

-- 一次审核提交 --------------------------------------------------------------
CREATE TABLE aud_submission (
    submission_id TEXT PRIMARY KEY,
    batch_id      TEXT NOT NULL REFERENCES aud_batch(batch_id),
    request_id    TEXT NOT NULL UNIQUE,
    reviewer      TEXT,
    result_count  INTEGER NOT NULL,
    issue_count   INTEGER NOT NULL,
    receipt_json  TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
CREATE INDEX idx_aud_submission_batch ON aud_submission(batch_id);

-- 单条审核结果：主体按本批盲审标识，落库时解析回正式记录身份。
-- 每条正式记录在本批只允许一个结论（重复主体整批拒绝）。
CREATE TABLE aud_result (
    batch_id             TEXT NOT NULL REFERENCES aud_batch(batch_id),
    subject_kind         TEXT NOT NULL
                         CHECK (subject_kind IN ('document', 'fragment',
                                                 'relation')),
    blind_id             TEXT NOT NULL,
    real_id              TEXT NOT NULL,
    doc_id               TEXT NOT NULL,
    verdict              TEXT NOT NULL CHECK (verdict IN ('conforming',
                                                          'issue')),
    note                 TEXT NOT NULL,
    evidence_doc_id      TEXT,
    evidence_start       INTEGER,
    evidence_end         INTEGER,
    evidence_text_sha256 TEXT,
    evidence_seen_text   TEXT,
    submission_id        TEXT NOT NULL REFERENCES aud_submission(submission_id),
    created_at           TEXT NOT NULL,
    PRIMARY KEY (batch_id, subject_kind, blind_id)
);
CREATE INDEX idx_aud_result_batch ON aud_result(batch_id, verdict);
CREATE INDEX idx_aud_result_doc ON aud_result(batch_id, doc_id);
CREATE INDEX idx_aud_result_submission ON aud_result(submission_id);

-- 每条已审正式记录在本批只有一个结论；用部分索引兜底跨主体键的重复。
CREATE UNIQUE INDEX idx_aud_result_real
    ON aud_result(batch_id, subject_kind, real_id);
