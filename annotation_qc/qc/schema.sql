-- 离线质检入仓管道的正式产物库（WAL 构建，发布后以只读方式打开）。
-- 标识全部为内容派生的稳定文本 ID；不存在用自增号表达业务身份的列。

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- 构建元信息 ----------------------------------------------------------------
CREATE TABLE build_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 原文清单 ------------------------------------------------------------------
CREATE TABLE source_doc (
    doc_id        TEXT PRIMARY KEY,
    rel_path      TEXT NOT NULL,
    sha256        TEXT NOT NULL,
    byte_length   INTEGER NOT NULL,
    verified_sha  TEXT NOT NULL,           -- 构建时实算并核对
    verified      INTEGER NOT NULL         -- 1 = 摘要一致
);

-- 标注包（同 doc + team 唯一） ----------------------------------------------
CREATE TABLE package (
    package_uid   TEXT PRIMARY KEY,       -- 内容派生稳定 ID
    package_id    TEXT NOT NULL,
    doc_id        TEXT NOT NULL REFERENCES source_doc(doc_id),
    team_id       TEXT NOT NULL,
    source_digest TEXT,                   -- 包声明的原文摘要
    root_kind     TEXT NOT NULL,          -- jsonl | bundle
    content_hash  TEXT NOT NULL,          -- 物理字节证据（与逻辑标识无关）
    UNIQUE(doc_id, team_id)
);

-- 全部原始记录（重排不改变 record_uid 集合） --------------------------------
CREATE TABLE record (
    record_uid    TEXT PRIMARY KEY,       -- 包 UID + 规范化内容 + 出现序号
    package_uid   TEXT NOT NULL REFERENCES package(package_uid),
    doc_id        TEXT NOT NULL,
    team_id       TEXT NOT NULL,
    kind          TEXT NOT NULL,          -- review | fragment | relation
    file_name     TEXT NOT NULL,          -- 包内相对文件名（证据）
    line_no       INTEGER NOT NULL,       -- 物理行号（重排可变，仅证据）
    record_index  INTEGER NOT NULL,       -- 包内序号
    payload_json  TEXT NOT NULL
);
CREATE INDEX idx_record_pkg ON record(package_uid);
CREATE INDEX idx_record_doc_kind ON record(doc_id, kind);

-- 已审核字节区间（按层） ----------------------------------------------------
CREATE TABLE review_range (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,  -- 仅物理行标识
    record_uid    TEXT NOT NULL REFERENCES record(record_uid),
    package_uid   TEXT NOT NULL,
    doc_id        TEXT NOT NULL,
    team_id       TEXT NOT NULL,
    layer         TEXT NOT NULL,
    start_byte    INTEGER NOT NULL,
    end_byte      INTEGER NOT NULL
);
CREATE INDEX idx_review_doc_layer ON review_range(doc_id, layer);
CREATE INDEX idx_review_pkg_layer ON review_range(package_uid, layer);

-- 片段提议（局部 ID 仅包内唯一；proposal_uid 为全局稳定身份） ----------------
CREATE TABLE proposal (
    proposal_uid  TEXT PRIMARY KEY,
    record_uid    TEXT NOT NULL REFERENCES record(record_uid),
    package_uid   TEXT NOT NULL REFERENCES package(package_uid),
    doc_id        TEXT NOT NULL,
    team_id       TEXT NOT NULL,
    layer         TEXT NOT NULL,
    local_id      TEXT NOT NULL,
    label         TEXT NOT NULL,
    start_byte    INTEGER NOT NULL,
    end_byte      INTEGER NOT NULL,
    seen_text     TEXT NOT NULL,          -- 已与原文字节核对的所见原文
    UNIQUE(package_uid, local_id)
);
CREATE INDEX idx_proposal_doc_layer ON proposal(doc_id, layer);
CREATE INDEX idx_proposal_pkg ON proposal(package_uid);

-- 原始关系记录 --------------------------------------------------------------
CREATE TABLE raw_relation (
    record_uid    TEXT PRIMARY KEY REFERENCES record(record_uid),
    package_uid   TEXT NOT NULL,
    doc_id        TEXT NOT NULL,
    team_id       TEXT NOT NULL,
    rel_type      TEXT NOT NULL,
    from_local    TEXT NOT NULL,
    to_local      TEXT NOT NULL,
    sign          TEXT NOT NULL           -- for | against
);
CREATE INDEX idx_raw_rel_doc_type ON raw_relation(doc_id, rel_type);
CREATE INDEX idx_raw_rel_pkg ON raw_relation(package_uid);

-- 争议区域（仅由同层正长度交叠的提议归并产生；首尾相碰不合并） ---------------
CREATE TABLE region (
    region_uid    TEXT PRIMARY KEY,       -- doc+layer+包络 内容派生
    doc_id        TEXT NOT NULL,
    layer         TEXT NOT NULL,
    start_byte    INTEGER NOT NULL,
    end_byte      INTEGER NOT NULL
);
CREATE INDEX idx_region_doc_layer ON region(doc_id, layer);

-- 区域内的成员提议 ----------------------------------------------------------
CREATE TABLE region_proposal (
    region_uid    TEXT NOT NULL REFERENCES region(region_uid),
    proposal_uid  TEXT NOT NULL REFERENCES proposal(proposal_uid),
    PRIMARY KEY (region_uid, proposal_uid)
);
CREATE INDEX idx_rp_proposal ON region_proposal(proposal_uid);

-- 覆盖资格：只有完整覆盖的团队进入分母；部分覆盖团队的提议不投票 -------------
CREATE TABLE region_eligibility (
    region_uid    TEXT NOT NULL REFERENCES region(region_uid),
    package_uid   TEXT NOT NULL REFERENCES package(package_uid),
    team_id       TEXT NOT NULL,
    coverage      TEXT NOT NULL,          -- full | partial
    PRIMARY KEY (region_uid, package_uid)
);
CREATE INDEX idx_elig_pkg ON region_eligibility(package_uid);

-- 区域候选（含「无片段」）与各团队归属 --------------------------------------
CREATE TABLE region_candidate (
    candidate_uid TEXT PRIMARY KEY,       -- region + (start,end,label) 派生
    region_uid    TEXT NOT NULL REFERENCES region(region_uid),
    start_byte    INTEGER NOT NULL,
    end_byte      INTEGER NOT NULL,
    label         TEXT NOT NULL,          -- 内部 NO_FRAGMENT 标签表示无片段
    is_no_fragment INTEGER NOT NULL
);
CREATE INDEX idx_cand_region ON region_candidate(region_uid);

CREATE TABLE region_vote (
    candidate_uid TEXT REFERENCES region_candidate(candidate_uid),
    region_uid    TEXT NOT NULL REFERENCES region(region_uid),
    package_uid   TEXT NOT NULL REFERENCES package(package_uid),
    team_id       TEXT NOT NULL,
    vote_kind     TEXT NOT NULL,          -- candidate | no_fragment | self_contradiction
    PRIMARY KEY (region_uid, package_uid)
);
CREATE INDEX idx_vote_region ON region_vote(region_uid);

-- 区域判定结果 --------------------------------------------------------------
CREATE TABLE region_outcome (
    region_uid       TEXT PRIMARY KEY REFERENCES region(region_uid),
    doc_id           TEXT NOT NULL,
    layer            TEXT NOT NULL,
    denominator      INTEGER NOT NULL,    -- 完整覆盖团队数
    winner_kind      TEXT NOT NULL,       -- candidate | no_fragment | pending
    winner_candidate TEXT REFERENCES region_candidate(candidate_uid),
    min_support      INTEGER NOT NULL,
    strict_majority  INTEGER NOT NULL     -- 严格多数阈值 = denominator//2 + 1
);
CREATE INDEX idx_outcome_doc_layer ON region_outcome(doc_id, layer);

-- 正式片段（仅候选胜出时写入；身份为 doc+layer+坐标+标签 的内容派生 ID） -----
CREATE TABLE official_fragment (
    fragment_uid  TEXT PRIMARY KEY,
    doc_id        TEXT NOT NULL,
    layer         TEXT NOT NULL,
    label         TEXT NOT NULL,
    start_byte    INTEGER NOT NULL,
    end_byte      INTEGER NOT NULL,
    region_uid    TEXT NOT NULL REFERENCES region(region_uid),
    candidate_uid TEXT NOT NULL REFERENCES region_candidate(candidate_uid),
    text_sha256   TEXT NOT NULL,          -- 原文片段摘要（不依赖原包留位）
    seen_text     TEXT NOT NULL
);
CREATE INDEX idx_official_doc_layer ON official_fragment(doc_id, layer);

-- 关系端点解析结果 -----------------------------------------------------------
CREATE TABLE relation_endpoint (
    record_uid      TEXT PRIMARY KEY REFERENCES record(record_uid),
    package_uid     TEXT NOT NULL,
    from_proposal   TEXT REFERENCES proposal(proposal_uid),
    to_proposal     TEXT REFERENCES proposal(proposal_uid),
    resolved        INTEGER NOT NULL,     -- 1 = 两端都解析到本包实际提议
    blocked_reason  TEXT                  -- 端点待定/无片段/另一候选胜出
);
CREATE INDEX idx_endpoint_pkg ON relation_endpoint(package_uid);

-- 关系归票组（两端均为正式片段；反向关系天然另组） --------------------------
CREATE TABLE relation_group (
    rel_group_uid TEXT PRIMARY KEY,       -- doc+type+from_frag+to_frag 派生
    doc_id        TEXT NOT NULL,
    rel_type      TEXT NOT NULL,
    from_fragment TEXT NOT NULL REFERENCES official_fragment(fragment_uid),
    to_fragment   TEXT NOT NULL REFERENCES official_fragment(fragment_uid),
    denominator   INTEGER NOT NULL,
    for_votes     INTEGER NOT NULL,
    against_votes INTEGER NOT NULL,
    conflict_votes INTEGER NOT NULL,      -- 同团队赞成反对都出现
    strict_majority INTEGER NOT NULL,
    min_support   INTEGER NOT NULL,
    decision      TEXT NOT NULL           -- confirmed | confirmed_absent | pending
);
CREATE INDEX idx_relgroup_doc_type ON relation_group(doc_id, rel_type);

CREATE TABLE relation_group_vote (
    rel_group_uid TEXT NOT NULL REFERENCES relation_group(rel_group_uid),
    package_uid   TEXT NOT NULL REFERENCES package(package_uid),
    team_id       TEXT NOT NULL,
    sign          TEXT NOT NULL,          -- for | against | conflict
    record_uids   TEXT NOT NULL,          -- 该团队被归并的原始记录（JSON 列表）
    PRIMARY KEY (rel_group_uid, package_uid)
);

-- 正式引用（确认存在） -------------------------------------------------------
CREATE TABLE official_relation (
    rel_group_uid TEXT PRIMARY KEY REFERENCES relation_group(rel_group_uid),
    doc_id        TEXT NOT NULL,
    rel_type      TEXT NOT NULL,
    from_fragment TEXT NOT NULL REFERENCES official_fragment(fragment_uid),
    to_fragment   TEXT NOT NULL REFERENCES official_fragment(fragment_uid)
);
CREATE INDEX idx_official_rel_doc_type ON official_relation(doc_id, rel_type);

-- 复核工作库 -----------------------------------------------------------------
CREATE TABLE review_item (
    review_uid    TEXT PRIMARY KEY,       -- 内容派生
    item_kind     TEXT NOT NULL,          -- region | relation
    doc_id        TEXT NOT NULL,
    layer         TEXT,                   -- region 项有层
    rel_type      TEXT,                   -- relation 项有类型
    region_uid    TEXT REFERENCES region(region_uid),
    rel_group_uid TEXT REFERENCES relation_group(rel_group_uid),
    record_uid    TEXT REFERENCES record(record_uid),  -- 被阻断的单条关系
    reason_code   TEXT NOT NULL,
    detail_json   TEXT NOT NULL
);
CREATE INDEX idx_review_doc ON review_item(doc_id);
CREATE INDEX idx_review_kind ON review_item(item_kind, doc_id);

-- 构建期运行计数器（规模/工作量核对） ----------------------------------------
CREATE TABLE run_counter (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
