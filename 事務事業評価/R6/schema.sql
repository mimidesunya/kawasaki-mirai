
-- kawasaki_program_eval_schema_ai_fts.sql
-- Schema for storing Kawasaki City "事務事業評価シート" data
-- Optimized for AI-driven analysis and full-text search (FTS5)
-- Target: SQLite 3.x
-- Encoding: UTF-8

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

BEGIN;

-- ============================
-- Lookup (enumeration) tables
-- ============================
CREATE TABLE IF NOT EXISTS impl_mode (
  id     INTEGER PRIMARY KEY,
  code   TEXT UNIQUE NOT NULL,     -- 'direct','partial','designated_mgr','volunteer','other'
  label  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS legal_basis_type (
  id     INTEGER PRIMARY KEY,
  code   TEXT UNIQUE NOT NULL,     -- 'national_pref','national_pref_city','city_only'
  label  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS funding_source (
  id     INTEGER PRIMARY KEY,
  code   TEXT UNIQUE NOT NULL,     -- 'national_subsidy','municipal_bond','other_special_fund','general_fund'
  label  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS indicator_type (
  id     INTEGER PRIMARY KEY,
  code   TEXT UNIQUE NOT NULL,     -- 'activity','outcome','other'
  label  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS eval_category (
  id     INTEGER PRIMARY KEY,
  code   TEXT UNIQUE NOT NULL,     -- 'citizen_need','necessity','effectiveness','private_util','method_review','quality'
  label  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS action_direction (
  id     INTEGER PRIMARY KEY,
  code   TEXT UNIQUE NOT NULL,     -- 'I','II','III','IV','V','VI'
  label  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS achievement_level (
  id     INTEGER PRIMARY KEY,
  code   INTEGER UNIQUE NOT NULL CHECK (code BETWEEN 1 AND 5),
  label  TEXT NOT NULL             -- 1:大きく上回って達成 ... 5:大きく下回った
);

CREATE TABLE IF NOT EXISTS fiscal_year (
  id              INTEGER PRIMARY KEY,
  label           TEXT UNIQUE NOT NULL,  -- 'R4','R5','R6','R7' 等
  gregorian_year  INTEGER                -- 2022, 2023, 2024, 2025（任意）
);

-- ============================
-- Master / reference tables
-- ============================
CREATE TABLE IF NOT EXISTS organization (
  id        INTEGER PRIMARY KEY,
  org_code  TEXT UNIQUE,              -- 285000 等
  name      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS program (
  id                     INTEGER PRIMARY KEY,
  code                   TEXT UNIQUE NOT NULL,  -- 40301050 等
  name                   TEXT NOT NULL,
  organization_id        INTEGER REFERENCES organization(id),
  policy                 TEXT,                  -- 政策
  measure                TEXT,                  -- 施策
  direct_goal            TEXT,                  -- 直接目標
  target_population      TEXT,                  -- 事業の対象
  objective              TEXT,                  -- 事業の目的
  content                TEXT,                  -- 事業の内容
  classification1        TEXT,                  -- 分類1（例：施設の管理・運営、補助・助成金 等）
  classification2        TEXT,                  -- 分類2（内部管理、― 等）
  service_category       TEXT,                  -- 事務・サービス等の分類
  start_fiscal_year_id   INTEGER REFERENCES fiscal_year(id),
  end_fiscal_year_id     INTEGER REFERENCES fiscal_year(id),
  legal_basis_text       TEXT,                  -- 法令・要綱名
  general_plan_text      TEXT,                  -- 総合計画と連携する計画（複数はカンマ区切り等）
  sdgs_orientation       TEXT,                  -- SDGsの取組の方向性
  reform_link_text       TEXT                   -- 行財政改革第３期プログラム等の関連
);

CREATE TABLE IF NOT EXISTS program_impl_mode (
  program_id     INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  impl_mode_id   INTEGER NOT NULL REFERENCES impl_mode(id),
  PRIMARY KEY (program_id, impl_mode_id)
);

CREATE TABLE IF NOT EXISTS program_legal_basis (
  program_id            INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  legal_basis_type_id   INTEGER NOT NULL REFERENCES legal_basis_type(id),
  PRIMARY KEY (program_id, legal_basis_type_id)
);

CREATE TABLE IF NOT EXISTS sdg (
  id      INTEGER PRIMARY KEY,
  goal    INTEGER NOT NULL,     -- 8, 11 等
  target  TEXT                  -- '8.5','11.7' 等
);

CREATE TABLE IF NOT EXISTS program_sdg (
  program_id INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  sdg_id     INTEGER NOT NULL REFERENCES sdg(id),
  PRIMARY KEY (program_id, sdg_id)
);

CREATE TABLE IF NOT EXISTS linked_plan (
  id    INTEGER PRIMARY KEY,
  name  TEXT UNIQUE NOT NULL     -- 産業振興プラン 等
);

CREATE TABLE IF NOT EXISTS program_linked_plan (
  program_id     INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  linked_plan_id INTEGER NOT NULL REFERENCES linked_plan(id),
  PRIMARY KEY (program_id, linked_plan_id)
);

-- ============================
-- Finance / workload per fiscal year
-- ============================
CREATE TABLE IF NOT EXISTS program_fiscal (
  id                         INTEGER PRIMARY KEY,
  program_id                 INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  fiscal_year_id             INTEGER NOT NULL REFERENCES fiscal_year(id),
  budget_amount_a            INTEGER,   -- 予算額（事業費A, 千円）
  settlement_amount_a        INTEGER,   -- 決算額（事業費A, 千円）
  planned_project_cost_a     INTEGER,   -- 計画事業費（A, 千円）
  human_cost_b_budget        INTEGER,   -- 人件費B（予算, 千円）
  human_cost_b_settlement    INTEGER,   -- 人件費B（決算, 千円）
  total_cost_budget          INTEGER,   -- 総コスト(A+B) 予算, 千円
  total_cost_settlement      INTEGER,   -- 総コスト(A+B) 決算, 千円
  manpower_person_year       REAL,      -- 人工（人）
  UNIQUE(program_id, fiscal_year_id)
);

CREATE TABLE IF NOT EXISTS program_fiscal_funding (
  id                 INTEGER PRIMARY KEY,
  program_fiscal_id  INTEGER NOT NULL REFERENCES program_fiscal(id) ON DELETE CASCADE,
  funding_source_id  INTEGER NOT NULL REFERENCES funding_source(id),
  budget_amount      INTEGER,      -- 予算額（千円）
  settlement_amount  INTEGER       -- 決算額（千円）
);

-- ============================
-- Plan (年度取組) / Do (実績) / Indicators
-- ============================
CREATE TABLE IF NOT EXISTS planned_action (
  id               INTEGER PRIMARY KEY,
  program_id       INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  fiscal_year_id   INTEGER REFERENCES fiscal_year(id),
  item_order       INTEGER,
  text             TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS program_result (
  id                   INTEGER PRIMARY KEY,
  program_id           INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  fiscal_year_id       INTEGER REFERENCES fiscal_year(id),
  achievement_level_id INTEGER REFERENCES achievement_level(id),  -- 1～5
  result_text          TEXT                                       -- 取組内容の実績等
);

CREATE TABLE IF NOT EXISTS indicator (
  id                 INTEGER PRIMARY KEY,
  program_id         INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  name               TEXT NOT NULL,            -- 指標名
  description        TEXT,                     -- 指標の説明
  unit               TEXT,                     -- 単位（%, 人, 回, 件 等）
  indicator_type_id  INTEGER REFERENCES indicator_type(id),
  sort_order         INTEGER
);

CREATE TABLE IF NOT EXISTS indicator_value (
  id               INTEGER PRIMARY KEY,
  indicator_id     INTEGER NOT NULL REFERENCES indicator(id) ON DELETE CASCADE,
  fiscal_year_id   INTEGER NOT NULL REFERENCES fiscal_year(id),
  target_value     REAL,                       -- 目標
  actual_value     REAL                        -- 実績
);

-- ============================
-- Check (評価)
-- ============================
CREATE TABLE IF NOT EXISTS program_evaluation (
  id                   INTEGER PRIMARY KEY,
  program_id           INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  fiscal_year_id       INTEGER REFERENCES fiscal_year(id),
  environment_change   TEXT,        -- 社会環境の変化
  improvement_history  TEXT         -- 見直し・改善内容（履歴も含む）
);

CREATE TABLE IF NOT EXISTS evaluation_score (
  id                 INTEGER PRIMARY KEY,
  evaluation_id      INTEGER NOT NULL REFERENCES program_evaluation(id) ON DELETE CASCADE,
  eval_category_id   INTEGER NOT NULL REFERENCES eval_category(id),
  rating_letter      TEXT NOT NULL CHECK (rating_letter IN ('a','b','c')),
  reason             TEXT
);

CREATE TABLE IF NOT EXISTS program_contribution (
  id                 INTEGER PRIMARY KEY,
  evaluation_id      INTEGER NOT NULL REFERENCES program_evaluation(id) ON DELETE CASCADE,
  level_letter       TEXT NOT NULL CHECK (level_letter IN ('A','B','C')),
  reason             TEXT
);

-- ============================
-- Action（改善）
-- ============================
CREATE TABLE IF NOT EXISTS program_action (
  id                   INTEGER PRIMARY KEY,
  program_id           INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  fiscal_year_id       INTEGER REFERENCES fiscal_year(id),
  direction_id         INTEGER REFERENCES action_direction(id),   -- I ～ VI
  direction_text       TEXT                                       -- 方向性本文
);

CREATE TABLE IF NOT EXISTS next_year_action_item (
  id               INTEGER PRIMARY KEY,
  program_id       INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  fiscal_year_id   INTEGER REFERENCES fiscal_year(id),
  item_order       INTEGER,
  text             TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plan_change_note (
  id                       INTEGER PRIMARY KEY,
  program_id               INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  fiscal_year_id           INTEGER REFERENCES fiscal_year(id),
  change_points_text       TEXT,      -- 変更箇所
  change_reason_text       TEXT       -- 変更の理由
);

-- ============================
-- Centralized text chunks for FTS / Embedding
-- ============================
-- Each narrative field (Plan/Do/Check/Action etc.) is normalized into text_chunk.
-- This makes FTS and vector search simple and consistent.
CREATE TABLE IF NOT EXISTS text_chunk (
  id             INTEGER PRIMARY KEY,
  program_id     INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
  program_code   TEXT NOT NULL,                   -- 冗長保持（検索時に便利）
  fiscal_year_id INTEGER REFERENCES fiscal_year(id),
  year_label     TEXT,                            -- 'R4' 等（冗長保持）
  section        TEXT NOT NULL,                   -- 'PLAN','DO','CHECK','ACTION','EVAL_SCORE','INDICATOR','PROGRAM_META' 等
  content        TEXT NOT NULL,                   -- 正規化済み本文（全角/半角・改行整理などをETLで実施推奨）
  source_table   TEXT,                            -- 由来テーブル（planned_action 等）
  source_pk      INTEGER,                         -- 由来レコードのPK
  position       INTEGER,                         -- 並び順
  lang           TEXT DEFAULT 'ja',
  token_count    INTEGER,
  created_at     TEXT DEFAULT (datetime('now')),
  updated_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_text_chunk_program ON text_chunk(program_id);
CREATE INDEX IF NOT EXISTS idx_text_chunk_year ON text_chunk(fiscal_year_id);
CREATE INDEX IF NOT EXISTS idx_text_chunk_section ON text_chunk(section);

-- ============================
-- FTS5 virtual tables (external content)
-- ============================
-- NOTE: For Japanese, FTS5 'unicode61' tokenization is used.
-- If you load a tokenizer extension (e.g., MeCab/KUROMOJI), change 'tokenize' accordingly.
CREATE VIRTUAL TABLE IF NOT EXISTS text_chunk_fts USING fts5(
  content,                       -- index target
  section UNINDEXED,             -- stored for filtering/highlighting
  program_code UNINDEXED,
  year_label UNINDEXED,
  tokenize = 'unicode61 remove_diacritics 2 tokenchars ''-_.％%℃km０１２３４５６７８９一二三四五六七八九十'' ',
  content='text_chunk',
  content_rowid='id',
  prefix='2,3'
);

-- FTS maintenance triggers for external content
CREATE TRIGGER IF NOT EXISTS text_chunk_ai AFTER INSERT ON text_chunk BEGIN
  INSERT INTO text_chunk_fts(rowid, content, section, program_code, year_label)
    VALUES (new.id, new.content, new.section, new.program_code, new.year_label);
END;
CREATE TRIGGER IF NOT EXISTS text_chunk_ad AFTER DELETE ON text_chunk BEGIN
  INSERT INTO text_chunk_fts(text_chunk_fts, rowid, content, section, program_code, year_label)
    VALUES('delete', old.id, old.content, old.section, old.program_code, old.year_label);
END;
CREATE TRIGGER IF NOT EXISTS text_chunk_au AFTER UPDATE ON text_chunk BEGIN
  INSERT INTO text_chunk_fts(text_chunk_fts, rowid, content, section, program_code, year_label)
    VALUES('delete', old.id, old.content, old.section, old.program_code, old.year_label);
  INSERT INTO text_chunk_fts(rowid, content, section, program_code, year_label)
    VALUES (new.id, new.content, new.section, new.program_code, new.year_label);
END;

-- ============================
-- Program meta search (name, code, objectives, legal basis etc.)
-- ============================
CREATE TABLE IF NOT EXISTS program_search_doc (
  rowid        INTEGER PRIMARY KEY,                  -- program.id を利用
  code         TEXT,                                 -- 40301050 等
  name         TEXT,
  org_code     TEXT,
  policy       TEXT,
  measure      TEXT,
  body         TEXT                                  -- 目的、内容、直接目標、分類、実施根拠、関連計画、SDGs等をまとめた本文
);

CREATE VIRTUAL TABLE IF NOT EXISTS program_search_fts USING fts5(
  name,         -- 高ウェイト項目（クエリ時にbm25で重み付け）
  body,         -- 本文（広範な検索）
  code UNINDEXED,
  org_code UNINDEXED,
  policy UNINDEXED,
  measure UNINDEXED,
  tokenize = 'unicode61 remove_diacritics 2 tokenchars ''-_.％%℃km０１２３４５６７８９一二三四五六七八九十'' ',
  content='program_search_doc',
  content_rowid='rowid',
  prefix='2,3'
);

-- Keep program_search_doc in sync with program
CREATE TRIGGER IF NOT EXISTS program_ai AFTER INSERT ON program BEGIN
  INSERT INTO program_search_doc(rowid, code, name, org_code, policy, measure, body)
  VALUES (
    new.id,
    new.code,
    new.name,
    (SELECT org_code FROM organization WHERE id=new.organization_id),
    new.policy,
    new.measure,
    -- body concatenation
    COALESCE(new.direct_goal,'') || ' ' ||
    COALESCE(new.target_population,'') || ' ' ||
    COALESCE(new.objective,'') || ' ' ||
    COALESCE(new.content,'') || ' ' ||
    COALESCE(new.classification1,'') || ' ' ||
    COALESCE(new.classification2,'') || ' ' ||
    COALESCE(new.service_category,'') || ' ' ||
    COALESCE(new.legal_basis_text,'') || ' ' ||
    COALESCE(new.general_plan_text,'') || ' ' ||
    COALESCE(new.sdgs_orientation,'') || ' ' ||
    COALESCE(new.reform_link_text,'')
  );
END;
CREATE TRIGGER IF NOT EXISTS program_ad AFTER DELETE ON program BEGIN
  DELETE FROM program_search_doc WHERE rowid=old.id;
END;
CREATE TRIGGER IF NOT EXISTS program_au AFTER UPDATE ON program BEGIN
  UPDATE program_search_doc
    SET code=NEW.code,
        name=NEW.name,
        org_code=(SELECT org_code FROM organization WHERE id=NEW.organization_id),
        policy=NEW.policy,
        measure=NEW.measure,
        body=COALESCE(NEW.direct_goal,'') || ' ' ||
             COALESCE(NEW.target_population,'') || ' ' ||
             COALESCE(NEW.objective,'') || ' ' ||
             COALESCE(NEW.content,'') || ' ' ||
             COALESCE(NEW.classification1,'') || ' ' ||
             COALESCE(NEW.classification2,'') || ' ' ||
             COALESCE(NEW.service_category,'') || ' ' ||
             COALESCE(NEW.legal_basis_text,'') || ' ' ||
             COALESCE(NEW.general_plan_text,'') || ' ' ||
             COALESCE(NEW.sdgs_orientation,'') || ' ' ||
             COALESCE(NEW.reform_link_text,'')
   WHERE rowid=OLD.id;
END;

-- FTS triggers for program meta
CREATE TRIGGER IF NOT EXISTS program_search_doc_ai AFTER INSERT ON program_search_doc BEGIN
  INSERT INTO program_search_fts(rowid, name, body, code, org_code, policy, measure)
  VALUES (new.rowid, new.name, new.body, new.code, new.org_code, new.policy, new.measure);
END;
CREATE TRIGGER IF NOT EXISTS program_search_doc_ad AFTER DELETE ON program_search_doc BEGIN
  INSERT INTO program_search_fts(program_search_fts, rowid, name, body, code, org_code, policy, measure)
  VALUES ('delete', old.rowid, old.name, old.body, old.code, old.org_code, old.policy, old.measure);
END;
CREATE TRIGGER IF NOT EXISTS program_search_doc_au AFTER UPDATE ON program_search_doc BEGIN
  INSERT INTO program_search_fts(program_search_fts, rowid, name, body, code, org_code, policy, measure)
  VALUES ('delete', old.rowid, old.name, old.body, old.code, old.org_code, old.policy, old.measure);
  INSERT INTO program_search_fts(rowid, name, body, code, org_code, policy, measure)
  VALUES (new.rowid, new.name, new.body, new.code, new.org_code, new.policy, new.measure);
END;

-- ============================
-- Auto-chunking triggers for main narrative tables
-- ============================
-- planned_action -> section 'PLAN'
CREATE TRIGGER IF NOT EXISTS planned_action_ai AFTER INSERT ON planned_action BEGIN
  INSERT INTO text_chunk(program_id, program_code, fiscal_year_id, year_label, section, content, source_table, source_pk, position, updated_at)
  VALUES (
    NEW.program_id,
    (SELECT code FROM program WHERE id=NEW.program_id),
    NEW.fiscal_year_id,
    (SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
    'PLAN',
    NEW.text,
    'planned_action',
    NEW.id,
    NEW.item_order,
    datetime('now')
  );
END;
CREATE TRIGGER IF NOT EXISTS planned_action_ad AFTER DELETE ON planned_action BEGIN
  DELETE FROM text_chunk WHERE source_table='planned_action' AND source_pk=OLD.id;
END;
CREATE TRIGGER IF NOT EXISTS planned_action_au AFTER UPDATE ON planned_action BEGIN
  UPDATE text_chunk
     SET content=NEW.text,
         position=NEW.item_order,
         fiscal_year_id=NEW.fiscal_year_id,
         year_label=(SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
         updated_at=datetime('now')
   WHERE source_table='planned_action' AND source_pk=OLD.id;
END;

-- program_result -> section 'DO'
CREATE TRIGGER IF NOT EXISTS program_result_ai AFTER INSERT ON program_result BEGIN
  INSERT INTO text_chunk(program_id, program_code, fiscal_year_id, year_label, section, content, source_table, source_pk, position, updated_at)
  VALUES (
    NEW.program_id,
    (SELECT code FROM program WHERE id=NEW.program_id),
    NEW.fiscal_year_id,
    (SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
    'DO',
    COALESCE((SELECT label FROM achievement_level WHERE id=NEW.achievement_level_id),'') || ' ' || COALESCE(NEW.result_text,''),
    'program_result',
    NEW.id,
    NULL,
    datetime('now')
  );
END;
CREATE TRIGGER IF NOT EXISTS program_result_ad AFTER DELETE ON program_result BEGIN
  DELETE FROM text_chunk WHERE source_table='program_result' AND source_pk=OLD.id;
END;
CREATE TRIGGER IF NOT EXISTS program_result_au AFTER UPDATE ON program_result BEGIN
  UPDATE text_chunk
     SET content=COALESCE((SELECT label FROM achievement_level WHERE id=NEW.achievement_level_id),'') || ' ' || COALESCE(NEW.result_text,''),
         fiscal_year_id=NEW.fiscal_year_id,
         year_label=(SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
         updated_at=datetime('now')
   WHERE source_table='program_result' AND source_pk=OLD.id;
END;

-- program_evaluation.environment_change -> section 'CHECK_ENV'
CREATE TRIGGER IF NOT EXISTS program_eval_env_ai AFTER INSERT ON program_evaluation BEGIN
  INSERT INTO text_chunk(program_id, program_code, fiscal_year_id, year_label, section, content, source_table, source_pk, updated_at)
  VALUES (
    NEW.program_id,
    (SELECT code FROM program WHERE id=NEW.program_id),
    NEW.fiscal_year_id,
    (SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
    'CHECK_ENV',
    COALESCE(NEW.environment_change,''),
    'program_evaluation#env',
    NEW.id,
    datetime('now')
  );
END;
CREATE TRIGGER IF NOT EXISTS program_eval_env_ad AFTER DELETE ON program_evaluation BEGIN
  DELETE FROM text_chunk WHERE source_table='program_evaluation#env' AND source_pk=OLD.id;
END;
CREATE TRIGGER IF NOT EXISTS program_eval_env_au AFTER UPDATE ON program_evaluation BEGIN
  UPDATE text_chunk
     SET content=COALESCE(NEW.environment_change,''),
         fiscal_year_id=NEW.fiscal_year_id,
         year_label=(SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
         updated_at=datetime('now')
   WHERE source_table='program_evaluation#env' AND source_pk=OLD.id;
END;

-- program_evaluation.improvement_history -> section 'CHECK_IMPROVE'
CREATE TRIGGER IF NOT EXISTS program_eval_imp_ai AFTER INSERT ON program_evaluation BEGIN
  INSERT INTO text_chunk(program_id, program_code, fiscal_year_id, year_label, section, content, source_table, source_pk, updated_at)
  VALUES (
    NEW.program_id,
    (SELECT code FROM program WHERE id=NEW.program_id),
    NEW.fiscal_year_id,
    (SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
    'CHECK_IMPROVE',
    COALESCE(NEW.improvement_history,''),
    'program_evaluation#improve',
    NEW.id,
    datetime('now')
  );
END;
CREATE TRIGGER IF NOT EXISTS program_eval_imp_ad AFTER DELETE ON program_evaluation BEGIN
  DELETE FROM text_chunk WHERE source_table='program_evaluation#improve' AND source_pk=OLD.id;
END;
CREATE TRIGGER IF NOT EXISTS program_eval_imp_au AFTER UPDATE ON program_evaluation BEGIN
  UPDATE text_chunk
     SET content=COALESCE(NEW.improvement_history,''),
         fiscal_year_id=NEW.fiscal_year_id,
         year_label=(SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
         updated_at=datetime('now')
   WHERE source_table='program_evaluation#improve' AND source_pk=OLD.id;
END;

-- evaluation_score.reason -> section 'CHECK_SCORE'
CREATE TRIGGER IF NOT EXISTS eval_score_ai AFTER INSERT ON evaluation_score BEGIN
  INSERT INTO text_chunk(program_id, program_code, fiscal_year_id, year_label, section, content, source_table, source_pk, updated_at)
  VALUES (
    (SELECT program_id FROM program_evaluation WHERE id=NEW.evaluation_id),
    (SELECT code FROM program WHERE id=(SELECT program_id FROM program_evaluation WHERE id=NEW.evaluation_id)),
    (SELECT fiscal_year_id FROM program_evaluation WHERE id=NEW.evaluation_id),
    (SELECT label FROM fiscal_year WHERE id=(SELECT fiscal_year_id FROM program_evaluation WHERE id=NEW.evaluation_id)),
    'CHECK_SCORE',
    (SELECT label FROM eval_category WHERE id=NEW.eval_category_id) || ' ' || COALESCE(NEW.reason,''),
    'evaluation_score',
    NEW.id,
    datetime('now')
  );
END;
CREATE TRIGGER IF NOT EXISTS eval_score_ad AFTER DELETE ON evaluation_score BEGIN
  DELETE FROM text_chunk WHERE source_table='evaluation_score' AND source_pk=OLD.id;
END;
CREATE TRIGGER IF NOT EXISTS eval_score_au AFTER UPDATE ON evaluation_score BEGIN
  UPDATE text_chunk
     SET content=(SELECT label FROM eval_category WHERE id=NEW.eval_category_id) || ' ' || COALESCE(NEW.reason,''),
         updated_at=datetime('now')
   WHERE source_table='evaluation_score' AND source_pk=OLD.id;
END;

-- program_action.direction_text -> section 'ACTION'
CREATE TRIGGER IF NOT EXISTS program_action_ai AFTER INSERT ON program_action BEGIN
  INSERT INTO text_chunk(program_id, program_code, fiscal_year_id, year_label, section, content, source_table, source_pk, updated_at)
  VALUES (
    NEW.program_id,
    (SELECT code FROM program WHERE id=NEW.program_id),
    NEW.fiscal_year_id,
    (SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
    'ACTION',
    (SELECT label FROM action_direction WHERE id=NEW.direction_id) || ' ' || COALESCE(NEW.direction_text,''),
    'program_action',
    NEW.id,
    datetime('now')
  );
END;
CREATE TRIGGER IF NOT EXISTS program_action_ad AFTER DELETE ON program_action BEGIN
  DELETE FROM text_chunk WHERE source_table='program_action' AND source_pk=OLD.id;
END;
CREATE TRIGGER IF NOT EXISTS program_action_au AFTER UPDATE ON program_action BEGIN
  UPDATE text_chunk
     SET content=(SELECT label FROM action_direction WHERE id=NEW.direction_id) || ' ' || COALESCE(NEW.direction_text,''),
         fiscal_year_id=NEW.fiscal_year_id,
         year_label=(SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
         updated_at=datetime('now')
   WHERE source_table='program_action' AND source_pk=OLD.id;
END;

-- next_year_action_item.text -> section 'NEXT_YEAR'
CREATE TRIGGER IF NOT EXISTS next_year_action_ai AFTER INSERT ON next_year_action_item BEGIN
  INSERT INTO text_chunk(program_id, program_code, fiscal_year_id, year_label, section, content, source_table, source_pk, position, updated_at)
  VALUES (
    NEW.program_id,
    (SELECT code FROM program WHERE id=NEW.program_id),
    NEW.fiscal_year_id,
    (SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
    'NEXT_YEAR',
    NEW.text,
    'next_year_action_item',
    NEW.id,
    NEW.item_order,
    datetime('now')
  );
END;
CREATE TRIGGER IF NOT EXISTS next_year_action_ad AFTER DELETE ON next_year_action_item BEGIN
  DELETE FROM text_chunk WHERE source_table='next_year_action_item' AND source_pk=OLD.id;
END;
CREATE TRIGGER IF NOT EXISTS next_year_action_au AFTER UPDATE ON next_year_action_item BEGIN
  UPDATE text_chunk
     SET content=NEW.text,
         position=NEW.item_order,
         fiscal_year_id=NEW.fiscal_year_id,
         year_label=(SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
         updated_at=datetime('now')
   WHERE source_table='next_year_action_item' AND source_pk=OLD.id;
END;

-- plan_change_note -> section 'PLAN_CHANGE'
CREATE TRIGGER IF NOT EXISTS plan_change_note_ai AFTER INSERT ON plan_change_note BEGIN
  INSERT INTO text_chunk(program_id, program_code, fiscal_year_id, year_label, section, content, source_table, source_pk, updated_at)
  VALUES (
    NEW.program_id,
    (SELECT code FROM program WHERE id=NEW.program_id),
    NEW.fiscal_year_id,
    (SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
    'PLAN_CHANGE',
    COALESCE(NEW.change_points_text,'') || ' ' || COALESCE(NEW.change_reason_text,''),
    'plan_change_note',
    NEW.id,
    datetime('now')
  );
END;
CREATE TRIGGER IF NOT EXISTS plan_change_note_ad AFTER DELETE ON plan_change_note BEGIN
  DELETE FROM text_chunk WHERE source_table='plan_change_note' AND source_pk=OLD.id;
END;
CREATE TRIGGER IF NOT EXISTS plan_change_note_au AFTER UPDATE ON plan_change_note BEGIN
  UPDATE text_chunk
     SET content=COALESCE(NEW.change_points_text,'') || ' ' || COALESCE(NEW.change_reason_text,''),
         fiscal_year_id=NEW.fiscal_year_id,
         year_label=(SELECT label FROM fiscal_year WHERE id=NEW.fiscal_year_id),
         updated_at=datetime('now')
   WHERE source_table='plan_change_note' AND source_pk=OLD.id;
END;

-- indicator (name/description/unit) -> section 'INDICATOR'
CREATE TRIGGER IF NOT EXISTS indicator_ai AFTER INSERT ON indicator BEGIN
  INSERT INTO text_chunk(program_id, program_code, section, content, source_table, source_pk, updated_at)
  VALUES (
    NEW.program_id,
    (SELECT code FROM program WHERE id=NEW.program_id),
    'INDICATOR',
    COALESCE(NEW.name,'') || ' ' || COALESCE(NEW.description,'') || ' ' || COALESCE(NEW.unit,''),
    'indicator',
    NEW.id,
    datetime('now')
  );
END;
CREATE TRIGGER IF NOT EXISTS indicator_ad AFTER DELETE ON indicator BEGIN
  DELETE FROM text_chunk WHERE source_table='indicator' AND source_pk=OLD.id;
END;
CREATE TRIGGER IF NOT EXISTS indicator_au AFTER UPDATE ON indicator BEGIN
  UPDATE text_chunk
     SET content=COALESCE(NEW.name,'') || ' ' || COALESCE(NEW.description,'') || ' ' || COALESCE(NEW.unit,''),
         updated_at=datetime('now')
   WHERE source_table='indicator' AND source_pk=OLD.id;
END;

-- ============================
-- Optional: Embeddings for vector/hybrid search
-- ============================
CREATE TABLE IF NOT EXISTS embedding_model (
  id        INTEGER PRIMARY KEY,
  name      TEXT UNIQUE NOT NULL,            -- e.g., 'text-embedding-3-large'
  dims      INTEGER NOT NULL,                -- ベクトル次元
  vendor    TEXT,                            -- 'openai','local','other'
  version   TEXT,
  note      TEXT
);

-- Store embedding per chunk and model. Use BLOB for portability.
CREATE TABLE IF NOT EXISTS chunk_embedding (
  chunk_id   INTEGER NOT NULL REFERENCES text_chunk(id) ON DELETE CASCADE,
  model_id   INTEGER NOT NULL REFERENCES embedding_model(id) ON DELETE CASCADE,
  vector     BLOB NOT NULL,                  -- little-endian float32[] 推奨
  dims       INTEGER NOT NULL,
  created_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (chunk_id, model_id),
  CHECK (dims > 0)
);

-- ============================
-- Convenience views
-- ============================
-- Unified search view (text-only). Query with FTS5 then join back to rich metadata.
CREATE VIEW IF NOT EXISTS v_text_search AS
SELECT tc.id AS chunk_id,
       tc.program_id,
       tc.program_code,
       p.name AS program_name,
       tc.fiscal_year_id,
       tc.year_label,
       tc.section,
       tc.content,
       p.organization_id,
       o.org_code,
       o.name AS organization_name
FROM text_chunk tc
JOIN program p ON p.id = tc.program_id
LEFT JOIN organization o ON o.id = p.organization_id;

-- Program-level search view (meta)
CREATE VIEW IF NOT EXISTS v_program_search AS
SELECT psd.rowid AS program_id,
       psd.code AS program_code,
       p.name AS program_name,
       psd.name AS name_indexed,
       psd.body AS body_indexed,
       psd.org_code,
       psd.policy,
       psd.measure
FROM program_search_doc psd
JOIN program p ON p.id = psd.rowid;

-- ============================
-- Indexes
-- ============================
CREATE INDEX IF NOT EXISTS idx_program_org ON program(organization_id);
CREATE INDEX IF NOT EXISTS idx_pf_program_year ON program_fiscal(program_id, fiscal_year_id);
CREATE INDEX IF NOT EXISTS idx_ind_program ON indicator(program_id);
CREATE INDEX IF NOT EXISTS idx_ind_value_year ON indicator_value(indicator_id, fiscal_year_id);
CREATE INDEX IF NOT EXISTS idx_eval_program_year ON program_evaluation(program_id, fiscal_year_id);
CREATE INDEX IF NOT EXISTS idx_action_program_year ON program_action(program_id, fiscal_year_id);

-- ============================
-- Seed data for lookups
-- ============================
INSERT OR IGNORE INTO impl_mode (id, code, label) VALUES
  (1,'direct','市が直接実施'),
  (2,'partial','一部委託'),
  (3,'designated_mgr','全部委託・指定管理'),
  (4,'volunteer','ボランティア等との協働'),
  (5,'other','その他');

INSERT OR IGNORE INTO legal_basis_type (id, code, label) VALUES
  (1,'national_pref','国・県の制度'),
  (2,'national_pref_city','国・県の制度＋市独自の制度'),
  (3,'city_only','市独自の制度');

INSERT OR IGNORE INTO funding_source (id, code, label) VALUES
  (1,'national_subsidy','国庫支出金'),
  (2,'municipal_bond','市債'),
  (3,'other_special_fund','その他特財'),
  (4,'general_fund','一般財源');

INSERT OR IGNORE INTO indicator_type (id, code, label) VALUES
  (1,'activity','活動'),
  (2,'outcome','成果'),
  (3,'other','その他');

INSERT OR IGNORE INTO eval_category (id, code, label) VALUES
  (1,'citizen_need','市民のニーズ'),
  (2,'necessity','市が実施する必要性'),
  (3,'effectiveness','成果（有効性）'),
  (4,'private_util','民間の活用'),
  (5,'method_review','事業手法等の見直し（効率性）'),
  (6,'quality','質の向上');

INSERT OR IGNORE INTO action_direction (id, code, label) VALUES
  (1,'I','現状のまま継続'),
  (2,'II','改善しながら継続'),
  (3,'III','事業規模拡大'),
  (4,'IV','事業規模縮小'),
  (5,'V','事業廃止'),
  (6,'VI','事業終了');

INSERT OR IGNORE INTO achievement_level (id, code, label) VALUES
  (1,1,'目標を大きく上回って達成'),
  (2,2,'目標を上回って達成'),
  (3,3,'ほぼ目標どおり'),
  (4,4,'目標を下回った'),
  (5,5,'目標を大きく下回った');

-- Prototype fiscal years
INSERT OR IGNORE INTO fiscal_year (id, label, gregorian_year) VALUES
  (1,'R4',2022),
  (2,'R5',2023),
  (3,'R6',2024),
  (4,'R7',2025);

-- ============================
-- Usage hints (comments)
-- ============================
-- Full-text search example (program-level):
-- SELECT p.program_id, p.program_code, p.program_name
-- FROM v_program_search p
-- JOIN program_search_fts fts ON fts.rowid = p.program_id
-- WHERE program_search_fts MATCH 'マイスター OR 稼働率';
-- ORDER BY bm25(program_search_fts, 5.0, 1.0, 0.0, 0.0, 0.0, 0.0);

-- Full-text search example (all narrative chunks):
-- SELECT s.*
-- FROM v_text_search s
-- JOIN text_chunk_fts fts ON fts.rowid = s.chunk_id
-- WHERE text_chunk_fts MATCH '稼働率 NEAR/5 向上'
-- ORDER BY bm25(text_chunk_fts);

-- Hybrid search (FTS + metadata filter):
-- ... WHERE s.section IN ('PLAN','DO','CHECK_ENV','CHECK_IMPROVE','ACTION');

COMMIT;
