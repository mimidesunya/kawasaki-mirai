#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
import.py — Import data formatted like `schema_template.json` into a SQLite DB
whose schema is defined by `schema.sql` (Kawasaki City "事務事業評価" schema).

Usage examples:
    # Single file
    python import.py --db data.sqlite3 --json schema_template.json

    # Read from STDIN
    cat schema_template.json | python import.py --db data.sqlite3 --json -

    # Import all JSON files in a directory (non-recursive, alphabetical)
    python import.py --db data.sqlite3 --json ./json_dir

Policy:
    - Append-only by program code: if `program.code` already exists, skip (no updates).
    - Deterministic processing order for directories (alphabetical by filename).
    - If the DB has no tables yet, `schema.sql` located beside this script is
      executed automatically to initialize the schema.
    - Lookup/enumeration tables (impl_mode, legal_basis_type, etc.) and fiscal
      years are populated from the JSON if present (INSERT OR IGNORE) before
      importing programs.

Notes:
    - The input JSON should follow the structure of `schema_template.json` with
      top-level keys like `meta`, `enums`, `fiscal_years`, and `programs`.
    - Files that contain at least one skipped/errored program are listed and
      moved into a sibling 'failed' directory (same behavior as previous script).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import sqlite3
import shutil
import re
from typing import Any, Dict, List, Optional, Iterable, Tuple

# -----------------------------
# Utilities / basics
# -----------------------------

def log(msg: str) -> None:
    print(msg, file=sys.stderr)

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def maybe_init_schema(conn: sqlite3.Connection) -> None:
    """Initialize schema from schema.sql located alongside this script if needed."""
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='program'")
    if cur.fetchone() is not None:
        return
    script_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(script_dir, "schema.sql")
    if not os.path.exists(schema_path):
        log("WARNING: 'program' table not found and schema.sql not present next to script. Skipping initialization.")
        return
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()
    conn.executescript(sql)
    log(f"Initialized schema from {schema_path}")

# -----------------------------
# JSON IO helpers
# -----------------------------

def load_json(path: str) -> Dict[str, Any]:
    if path == "-":
        return json.load(sys.stdin)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def iter_json_files(root: str) -> Iterable[str]:
    """Yield JSON file paths directly under root (deterministic order, non-recursive)."""
    files = [os.path.join(root, n) for n in os.listdir(root)
             if n.lower().endswith('.json') and os.path.isfile(os.path.join(root, n))]
    for p in sorted(files):
        yield p

# -----------------------------
# Lookup resolvers / creators
# -----------------------------

# Common: SELECT id by code from a lookup table
_def_lookup_by_code_query = {
    'impl_mode': "SELECT id FROM impl_mode WHERE code=?",
    'legal_basis_type': "SELECT id FROM legal_basis_type WHERE code=?",
    'funding_source': "SELECT id FROM funding_source WHERE code=?",
    'indicator_type': "SELECT id FROM indicator_type WHERE code=?",
    'eval_category': "SELECT id FROM eval_category WHERE code=?",
    'action_direction': "SELECT id FROM action_direction WHERE code=?",
    'achievement_level': "SELECT id FROM achievement_level WHERE code=?",
}

def get_lookup_id(conn: sqlite3.Connection, table: str, code: str) -> Optional[int]:
    sql = _def_lookup_by_code_query.get(table)
    if not sql:
        raise ValueError(f"Unsupported lookup table: {table}")
    row = conn.execute(sql, (code,)).fetchone()
    return int(row[0]) if row else None

def ensure_lookup_row(conn: sqlite3.Connection, table: str, code: str, label: Optional[str]) -> int:
    """INSERT OR IGNORE a lookup row (code,label); return its id."""
    # Some tables (achievement_level.code) are INTEGER; accept both str/int inputs
    if table == 'achievement_level' and isinstance(code, int):
        conn.execute("INSERT OR IGNORE INTO achievement_level(code,label) VALUES (?,?)",
                     (code, label or str(code)))
    else:
        conn.execute(f"INSERT OR IGNORE INTO {table}(code,label) VALUES (?,?)",
                     (code, label or code))
    rid = get_lookup_id(conn, table, code)
    if rid is None:
        raise RuntimeError(f"Failed to ensure lookup {table}.{code}")
    return rid

def ensure_fiscal_year(conn: sqlite3.Connection, label: str, gregorian: Optional[int]) -> int:
    row = conn.execute("SELECT id FROM fiscal_year WHERE label=?", (label,)).fetchone()
    if row:
        return int(row[0])
    conn.execute("INSERT INTO fiscal_year(label, gregorian_year) VALUES (?,?)", (label, gregorian))
    row = conn.execute("SELECT id FROM fiscal_year WHERE label=?", (label,)).fetchone()
    return int(row[0])

def ensure_org(conn: sqlite3.Connection, org_code: Optional[str], name: str) -> int:
    row = None
    if org_code:
        row = conn.execute("SELECT id FROM organization WHERE org_code=?", (org_code,)).fetchone()
        if row:
            return int(row[0])
    # Fallback by name
    row = conn.execute("SELECT id FROM organization WHERE name=?", (name,)).fetchone()
    if row:
        # update org_code if missing
        if org_code:
            conn.execute("UPDATE organization SET org_code=COALESCE(org_code, ?) WHERE id=?", (org_code, int(row[0])))
        return int(row[0])
    conn.execute("INSERT INTO organization(org_code, name) VALUES (?,?)", (org_code, name))
    row = conn.execute("SELECT id FROM organization WHERE name=?", (name,)).fetchone()
    return int(row[0])

def ensure_sdg(conn: sqlite3.Connection, goal: int, target: Optional[str]) -> int:
    row = conn.execute("SELECT id FROM sdg WHERE goal=? AND IFNULL(target,'')=IFNULL(?, '')", (goal, target)).fetchone()
    if row:
        return int(row[0])
    conn.execute("INSERT INTO sdg(goal, target) VALUES (?,?)", (goal, target))
    row = conn.execute("SELECT id FROM sdg WHERE goal=? AND IFNULL(target,'')=IFNULL(?, '')", (goal, target)).fetchone()
    return int(row[0])

def ensure_linked_plan(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute("SELECT id FROM linked_plan WHERE name=?", (name,)).fetchone()
    if row:
        return int(row[0])
    conn.execute("INSERT INTO linked_plan(name) VALUES (?)", (name,))
    row = conn.execute("SELECT id FROM linked_plan WHERE name=?", (name,)).fetchone()
    return int(row[0])

# -----------------------------
# JSON schema loaders / variant normalizer
# -----------------------------

def _transform_table_dump_style(data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Transform a table-dump style JSON (arrays named like table names) into header+programs.

    Expected keys (optional): organization, program, program_impl_mode, program_legal_basis, linked_plan,
    program_linked_plan, sdg, program_sdg, program_fiscal, program_fiscal_funding, planned_action,
    program_result, indicator, indicator_value, program_evaluation, evaluation_score, program_contribution,
    program_action, next_year_action_item, plan_change_note, text_chunk, source.

    This builds for each program_code a composite object approximating schema_template.json's per-program structure.
    Unknown keys are ignored.
    """
    programs_src = data.get("program") or []
    if not isinstance(programs_src, list):
        raise SystemError("'program' must be a list when using table-dump style JSON")

    # Index helpers
    def idx_list(key: str, code_field: str = "program_code"):
        arr = data.get(key) or []
        m: Dict[str, List[Dict[str, Any]]] = {}
        for row in arr:
            c = row.get(code_field)
            if not c:
                continue
            m.setdefault(c, []).append(row)
        return m

    idx_impl = idx_list("program_impl_mode")
    idx_legal = idx_list("program_legal_basis")
    idx_link_plan = idx_list("program_linked_plan")
    idx_fiscal = idx_list("program_fiscal")
    idx_funding = idx_list("program_fiscal_funding")
    idx_planned = idx_list("planned_action")
    idx_result = idx_list("program_result")
    idx_indicator = idx_list("indicator")
    idx_indicator_val = idx_list("indicator_value")
    idx_eval = idx_list("program_evaluation")
    idx_eval_score = idx_list("evaluation_score")
    idx_contrib = idx_list("program_contribution")
    idx_action = idx_list("program_action")
    idx_next = idx_list("next_year_action_item")
    idx_plan_change = idx_list("plan_change_note")
    idx_chunks = idx_list("text_chunk")
    idx_source = idx_list("source")

    org_map_code_to_name = {o.get("org_code"): o.get("name") for o in (data.get("organization") or []) if o.get("org_code")}

    programs: List[Dict[str, Any]] = []
    for row in programs_src:
        code = row.get("code")
        if not code:
            continue
        # implementation
        impl_modes = [r.get("impl_mode_code") for r in idx_impl.get(code, []) if r.get("impl_mode_code")]
        legal_types = [r.get("legal_basis_type_code") for r in idx_legal.get(code, []) if r.get("legal_basis_type_code")]
        implementation = {
            "modes": impl_modes or None,
            "legal_basis_types": legal_types or None,
            "legal_basis_text": row.get("legal_basis_text"),
            "start_fiscal_year_label": row.get("start_fiscal_year_label"),
            "end_fiscal_year_label": row.get("end_fiscal_year_label"),
        }
        # policy links
        linked_plans = [r.get("plan_name") for r in idx_link_plan.get(code, []) if r.get("plan_name")]
        policy_links = {
            "policy": row.get("policy"),
            "measure": row.get("measure"),
            "linked_plans": linked_plans or None,
            "sdgs": {
                # orientation text is stored in row as sdgs_orientation
                "orientation_text": row.get("sdgs_orientation")
            } if row.get("sdgs_orientation") else None,
            "reform_program_text": row.get("reform_link_text"),
        }
        # purpose_and_content
        pac = {
            "direct_goal": row.get("direct_goal"),
            "target_population": row.get("target_population"),
            "objective": row.get("objective"),
            "content": row.get("content"),
        }
        classifications = {
            "classification1": row.get("classification1"),
            "classification2": row.get("classification2"),
            "service_category": row.get("service_category"),
        }
        # finance
        finance_list: List[Dict[str, Any]] = []
        for f in idx_fiscal.get(code, []):
            fy = f.get("fiscal_year_label")
            if not fy:
                continue
            funding_breakdown: Dict[str, Dict[str, Any]] = {}
            for fund in idx_funding.get(code, []):
                if fund.get("fiscal_year_label") != fy:
                    continue
                src = fund.get("funding_source_code")
                if not src:
                    continue
                funding_breakdown[src] = {
                    "budget": fund.get("budget_amount"),
                    "settlement": fund.get("settlement_amount"),
                }
            finance_list.append({
                "fiscal_year_label": fy,
                "a_budget": f.get("budget_amount_a"),
                "a_settlement": f.get("settlement_amount_a"),
                "a_planned": f.get("planned_project_cost_a"),
                "b_human_budget": f.get("human_cost_b_budget"),
                "b_human_settlement": f.get("human_cost_b_settlement"),
                "total_budget": f.get("total_cost_budget"),
                "total_settlement": f.get("total_cost_settlement"),
                "manpower_person_year": f.get("manpower_person_year"),
                "funding_breakdown": funding_breakdown or None,
            })
        # plan
        plan_items = sorted(idx_planned.get(code, []), key=lambda r: (r.get("item_order") or 0))
        plan_block = None
        if plan_items:
            fy_label = plan_items[0].get("fiscal_year_label")
            plan_block = {
                "applicable_year_label": fy_label,
                "action_items": [
                    {"order": it.get("item_order"), "text": it.get("text")} for it in plan_items
                ],
            }
        # results_do
        results_do = []
        for r in idx_result.get(code, []):
            results_do.append({
                "fiscal_year_label": r.get("fiscal_year_label"),
                "achievement_level": r.get("achievement_level_code"),
                "result_text": r.get("result_text"),
            })
        # indicators
        indicators = []
        for ind in idx_indicator.get(code, []):
            name = ind.get("name")
            vals = [v for v in idx_indicator_val.get(code, []) if v.get("indicator_name") == name]
            indicators.append({
                "type": ind.get("indicator_type_code"),
                "name": name,
                "description": ind.get("description"),
                "unit": ind.get("unit"),
                "values": [
                    {
                        "fiscal_year_label": v.get("fiscal_year_label"),
                        "target": v.get("target_value"),
                        "actual": v.get("actual_value"),
                    } for v in vals
                ] or None,
            })
        # evaluation
        evaluation_check = None
        ev_rows = idx_eval.get(code, [])
        if ev_rows:
            ev = ev_rows[0]
            scores = []
            for sc in idx_eval_score.get(code, []):
                scores.append({
                    "category_code": sc.get("eval_category_code"),
                    "rating": sc.get("rating_letter"),
                    "reason": sc.get("reason"),
                })
            contrib_rows = idx_contrib.get(code, [])
            contrib = contrib_rows[0] if contrib_rows else {}
            evaluation_check = {
                "fiscal_year_label": ev.get("fiscal_year_label"),
                "environment_change": ev.get("environment_change"),
                "improvements_history_text": ev.get("improvement_history"),
                "scores": scores or None,
                "contribution": {
                    "level": contrib.get("level_letter"),
                    "reason": contrib.get("reason"),
                } if contrib else None,
            }
        # action_improvement
        action_improvement = None
        act_rows = idx_action.get(code, [])
        if act_rows:
            act = act_rows[0]
            nxt_items = [
                {"order": it.get("item_order"), "text": it.get("text")} for it in idx_next.get(code, [])
            ]
            action_improvement = {
                "fiscal_year_label": act.get("fiscal_year_label"),
                "direction_code": act.get("direction_code"),
                "direction_text": act.get("direction_text"),
                "next_year_action_items": nxt_items or None,
            }
        # plan_change_note
        plan_change_note = None
        pc_rows = idx_plan_change.get(code, [])
        if pc_rows:
            pc = pc_rows[0]
            plan_change_note = {
                "fiscal_year_label": pc.get("fiscal_year_label"),
                "change_points": pc.get("change_points"),
                "change_reason": pc.get("change_reason"),
            }
        # search_chunks
        search_chunks = []
        for ch in idx_chunks.get(code, []):
            search_chunks.append({
                "section": ch.get("section"),
                "year_label": ch.get("year_label"),
                "content": ch.get("content"),
                "position": ch.get("position"),
            })
        # sources
        sources_rows = idx_source.get(code, [])
        sources = None
        if sources_rows:
            s = sources_rows[0]
            sources = {"filename": s.get("filename"), "url": s.get("url")}

        program_obj = {
            "code": code,
            "name": row.get("name"),
            "organization": {
                "org_code": row.get("organization_org_code"),
                "name": org_map_code_to_name.get(row.get("organization_org_code")) or "不明部署",
            },
            "classifications": classifications,
            "implementation": implementation,
            "policy_links": policy_links,
            "purpose_and_content": pac,
            "finance": finance_list or None,
            "plan": plan_block,
            "results_do": results_do or None,
            "indicators": indicators or None,
            "evaluation_check": evaluation_check,
            "action_improvement": action_improvement,
            "plan_change_note": plan_change_note,
            "search_chunks": search_chunks or None,
            "sources": sources,
        }
        programs.append(program_obj)

    header = {k: data.get(k) for k in ("meta","enums","fiscal_years") if k in data}
    return header, programs

def load_programs_variant(path: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Load a JSON file and return (header, programs) supporting multiple variants:
      1. schema_template style (root object with 'programs' array)
      2. single-program object (has 'code','name' etc) -> wrapped into programs list
      3. table-dump style (has 'program' array but no 'programs') -> transformed
    """
    try:
        data = load_json(path)
    except Exception as e:
        raise SystemExit(f"Failed to load JSON '{path}': {e}")
    if not isinstance(data, dict):
        raise SystemExit(f"Root JSON in '{path}' must be an object")
    if isinstance(data.get("programs"), list):
        header = {k: data.get(k) for k in ("meta","enums","fiscal_years") if k in data}
        return header, data.get("programs")
    if "program" in data and isinstance(data.get("program"), list):
        return _transform_table_dump_style(data)
    # single program object?
    if data.get("code") and data.get("name"):
        header = {k: data.get(k) for k in ("meta","enums","fiscal_years") if k in data}
        # remove header keys from program clone
        prog = {k:v for k,v in data.items() if k not in ("meta","enums","fiscal_years")}
        return header, [prog]
    raise SystemExit(f"Unrecognized JSON structure in '{path}': expected 'programs' array, 'program' table array, or single program object.")

# -----------------------------
# Importers (append-only by program.code)
# -----------------------------

def program_exists(conn: sqlite3.Connection, code: str) -> bool:
    row = conn.execute("SELECT id FROM program WHERE code=?", (code,)).fetchone()
    return bool(row)

def parse_goal_from_target(target: str) -> Optional[int]:
    m = re.match(r"^(\d+)(?:[.].*)?$", str(target).strip())
    return int(m.group(1)) if m else None

# Map schema_template keys to DB column names for program
_program_cols = [
    ("code", "code"),
    ("name", "name"),
    ("policy", "policy"),
    ("measure", "measure"),
    ("direct_goal", "direct_goal"),
    ("target_population", "target_population"),
    ("objective", "objective"),
    ("content", "content"),
    ("classification1", "classification1"),
    ("classification2", "classification2"),
    ("service_category", "service_category"),
    ("legal_basis_text", "legal_basis_text"),
    ("general_plan_text", "general_plan_text"),
    ("sdgs_orientation", "sdgs_orientation"),
    ("reform_link_text", "reform_link_text"),
]

def insert_program_core(conn: sqlite3.Connection, p: Dict[str, Any]) -> int:
    org = p.get("organization") or {}
    org_id = ensure_org(conn, org.get("org_code"), org.get("name") or "不明部署")

    impl = p.get("implementation") or {}
    start_label = impl.get("start_fiscal_year_label")
    end_label = impl.get("end_fiscal_year_label")
    start_id = None
    end_id = None
    if start_label:
        start_id = ensure_fiscal_year(conn, start_label, None)
    if end_label:
        end_id = ensure_fiscal_year(conn, end_label, None)

    # Flatten program fields from purpose_and_content + policy_links
    pac = p.get("purpose_and_content") or {}
    pl = p.get("policy_links") or {}

    prog_values: Dict[str, Any] = {
        "code": p.get("code"),
        "name": p.get("name"),
        "policy": pl.get("policy"),
        "measure": pl.get("measure"),
        "direct_goal": pac.get("direct_goal"),
        "target_population": pac.get("target_population"),
        "objective": pac.get("objective"),
        "content": pac.get("content"),
        "classification1": (p.get("classifications") or {}).get("classification1"),
        "classification2": (p.get("classifications") or {}).get("classification2"),
        "service_category": (p.get("classifications") or {}).get("service_category"),
        "legal_basis_text": impl.get("legal_basis_text"),
        "general_plan_text": ", ".join(pl.get("linked_plans") or []) if pl.get("linked_plans") else None,
        "sdgs_orientation": (pl.get("sdgs") or {}).get("orientation_text"),
        "reform_link_text": pl.get("reform_program_text"),
    }

    cols = ["code","name","organization_id","policy","measure","direct_goal","target_population","objective","content",
            "classification1","classification2","service_category","start_fiscal_year_id","end_fiscal_year_id",
            "legal_basis_text","general_plan_text","sdgs_orientation","reform_link_text"]
    vals = [
        prog_values.get("code"), prog_values.get("name"), org_id,
        prog_values.get("policy"), prog_values.get("measure"), prog_values.get("direct_goal"),
        prog_values.get("target_population"), prog_values.get("objective"), prog_values.get("content"),
        prog_values.get("classification1"), prog_values.get("classification2"), prog_values.get("service_category"),
        start_id, end_id,
        prog_values.get("legal_basis_text"), prog_values.get("general_plan_text"), prog_values.get("sdgs_orientation"),
        prog_values.get("reform_link_text"),
    ]
    placeholders = ",".join("?" for _ in cols)
    sql = f"INSERT INTO program ({','.join(cols)}) VALUES ({placeholders})"
    cur = conn.execute(sql, vals)
    return int(cur.lastrowid)

def insert_program_relationships(conn: sqlite3.Connection, program_id: int, p: Dict[str, Any]) -> None:
    impl = p.get("implementation") or {}
    # impl modes
    for code in impl.get("modes") or []:
        mid = get_lookup_id(conn, 'impl_mode', str(code)) or ensure_lookup_row(conn, 'impl_mode', str(code), None)
        conn.execute("INSERT OR IGNORE INTO program_impl_mode(program_id, impl_mode_id) VALUES (?,?)", (program_id, mid))
    # legal basis types
    for code in impl.get("legal_basis_types") or []:
        lbid = get_lookup_id(conn, 'legal_basis_type', str(code)) or ensure_lookup_row(conn, 'legal_basis_type', str(code), None)
        conn.execute("INSERT OR IGNORE INTO program_legal_basis(program_id, legal_basis_type_id) VALUES (?,?)", (program_id, lbid))

    pl = p.get("policy_links") or {}
    # linked plans
    for name in pl.get("linked_plans") or []:
        if not name:
            continue
        lpid = ensure_linked_plan(conn, name)
        conn.execute("INSERT OR IGNORE INTO program_linked_plan(program_id, linked_plan_id) VALUES (?,?)", (program_id, lpid))

    # SDGs (goals and/or targets)
    sdgs = pl.get("sdgs") or {}
    goals = sdgs.get("goals") or []
    targets = sdgs.get("targets") or []
    # target implies goal in its prefix; pair each target with its inferred goal
    for t in targets:
        g = parse_goal_from_target(t)
        if g is None:
            continue
        sid = ensure_sdg(conn, g, str(t))
        conn.execute("INSERT OR IGNORE INTO program_sdg(program_id, sdg_id) VALUES (?,?)", (program_id, sid))
    # also add goal-only rows
    for g in goals:
        try:
            gi = int(g)
        except Exception:
            continue
        sid = ensure_sdg(conn, gi, None)
        conn.execute("INSERT OR IGNORE INTO program_sdg(program_id, sdg_id) VALUES (?,?)", (program_id, sid))


def insert_finance(conn: sqlite3.Connection, program_id: int, p: Dict[str, Any]) -> None:
    for f in p.get("finance") or []:
        y = f.get("fiscal_year_label")
        if not y:
            continue
        fy_id = ensure_fiscal_year(conn, y, None)
        cols = [
            "program_id","fiscal_year_id",
            "budget_amount_a","settlement_amount_a","planned_project_cost_a",
            "human_cost_b_budget","human_cost_b_settlement",
            "total_cost_budget","total_cost_settlement","manpower_person_year",
        ]
        vals = [
            program_id, fy_id,
            f.get("a_budget"), f.get("a_settlement"), f.get("a_planned"),
            f.get("b_human_budget"), f.get("b_human_settlement"),
            f.get("total_budget"), f.get("total_settlement"), f.get("manpower_person_year"),
        ]
        placeholders = ",".join("?" for _ in cols)
        cur = conn.execute(
            f"INSERT OR REPLACE INTO program_fiscal ({','.join(cols)}) VALUES ({placeholders})",
            vals,
        )
        pf_id = int(cur.lastrowid) if cur.lastrowid else int(conn.execute(
            "SELECT id FROM program_fiscal WHERE program_id=? AND fiscal_year_id=?",
            (program_id, fy_id)
        ).fetchone()[0])
        # funding breakdown
        fb = f.get("funding_breakdown") or {}
        for src_code, pair in fb.items():
            fs_id = get_lookup_id(conn, 'funding_source', src_code) or ensure_lookup_row(conn, 'funding_source', src_code, None)
            conn.execute(
                "INSERT OR REPLACE INTO program_fiscal_funding (program_fiscal_id, funding_source_id, budget_amount, settlement_amount) VALUES (?,?,?,?)",
                (pf_id, fs_id, (pair or {}).get("budget"), (pair or {}).get("settlement")),
            )


def insert_plan(conn: sqlite3.Connection, program_id: int, p: Dict[str, Any]) -> None:
    plan = p.get("plan") or {}
    y = plan.get("applicable_year_label")
    fy_id = ensure_fiscal_year(conn, y, None) if y else None
    for item in plan.get("action_items") or []:
        conn.execute(
            "INSERT INTO planned_action(program_id, fiscal_year_id, item_order, text) VALUES (?,?,?,?)",
            (program_id, fy_id, item.get("order"), item.get("text")),
        )
    # Plan change from this 'plan' block (fallback)
    chg = plan.get("change_from_initial_plan") or {}
    if chg and (chg.get("change_points") or chg.get("change_reason")):
        conn.execute(
            "INSERT INTO plan_change_note(program_id, fiscal_year_id, change_points_text, change_reason_text) VALUES (?,?,?,?)",
            (program_id, fy_id, chg.get("change_points"), chg.get("change_reason")),
        )


def insert_results_do(conn: sqlite3.Connection, program_id: int, p: Dict[str, Any]) -> None:
    for r in p.get("results_do") or []:
        y = r.get("fiscal_year_label")
        if not y:
            continue
        fy_id = ensure_fiscal_year(conn, y, None)
        ach_code = r.get("achievement_level")
        ach_id = None
        if ach_code is not None:
            ach_id = get_lookup_id(conn, 'achievement_level', int(ach_code))
            if ach_id is None:
                ach_id = ensure_lookup_row(conn, 'achievement_level', int(ach_code), None)
        conn.execute(
            "INSERT INTO program_result(program_id, fiscal_year_id, achievement_level_id, result_text) VALUES (?,?,?,?)",
            (program_id, fy_id, ach_id, r.get("result_text")),
        )


def insert_indicators(conn: sqlite3.Connection, program_id: int, p: Dict[str, Any]) -> None:
    for ind in p.get("indicators") or []:
        tcode = ind.get("type")
        it_id = None
        if tcode:
            it_id = get_lookup_id(conn, 'indicator_type', tcode) or ensure_lookup_row(conn, 'indicator_type', tcode, None)
        cur = conn.execute(
            "INSERT INTO indicator(program_id, name, description, unit, indicator_type_id, sort_order) VALUES (?,?,?,?,?,?)",
            (program_id, ind.get("name"), ind.get("description"), ind.get("unit"), it_id, None),
        )
        indicator_id = int(cur.lastrowid)
        for v in ind.get("values") or []:
            y = v.get("fiscal_year_label")
            if not y:
                continue
            fy_id = ensure_fiscal_year(conn, y, None)
            conn.execute(
                "INSERT INTO indicator_value(indicator_id, fiscal_year_id, target_value, actual_value) VALUES (?,?,?,?)",
                (indicator_id, fy_id, v.get("target"), v.get("actual")),
            )


def insert_evaluation(conn: sqlite3.Connection, program_id: int, p: Dict[str, Any]) -> None:
    ev = p.get("evaluation_check") or {}
    if not ev:
        return
    y = ev.get("fiscal_year_label")
    fy_id = ensure_fiscal_year(conn, y, None) if y else None
    cur = conn.execute(
        "INSERT INTO program_evaluation(program_id, fiscal_year_id, environment_change, improvement_history) VALUES (?,?,?,?)",
        (program_id, fy_id, ev.get("environment_change"), ev.get("improvements_history_text")),
    )
    eval_id = int(cur.lastrowid)
    for s in ev.get("scores") or []:
        cat = s.get("category_code")
        if not cat:
            continue
        ec_id = get_lookup_id(conn, 'eval_category', cat) or ensure_lookup_row(conn, 'eval_category', cat, None)
        rating = s.get("rating")
        if rating not in ("a","b","c", None):
            log(f"WARNING: invalid rating '{rating}' for program_id={program_id} (expected 'a'|'b'|'c')")
        conn.execute(
            "INSERT INTO evaluation_score(evaluation_id, eval_category_id, rating_letter, reason) VALUES (?,?,?,?)",
            (eval_id, ec_id, rating, s.get("reason")),
        )
    contr = (ev.get("contribution") or {})
    if contr:
        conn.execute(
            "INSERT INTO program_contribution(evaluation_id, level_letter, reason) VALUES (?,?,?)",
            (eval_id, contr.get("level"), contr.get("reason")),
        )


def insert_action(conn: sqlite3.Connection, program_id: int, p: Dict[str, Any]) -> None:
    act = p.get("action_improvement") or {}
    if not act:
        return
    y = act.get("fiscal_year_label")
    fy_id = ensure_fiscal_year(conn, y, None) if y else None
    dcode = act.get("direction_code")
    d_id = None
    if dcode:
        d_id = get_lookup_id(conn, 'action_direction', dcode) or ensure_lookup_row(conn, 'action_direction', dcode, None)
    cur = conn.execute(
        "INSERT INTO program_action(program_id, fiscal_year_id, direction_id, direction_text) VALUES (?,?,?,?)",
        (program_id, fy_id, d_id, act.get("direction_text")),
    )
    # next year items
    for it in act.get("next_year_action_items") or []:
        conn.execute(
            "INSERT INTO next_year_action_item(program_id, fiscal_year_id, item_order, text) VALUES (?,?,?,?)",
            (program_id, fy_id, it.get("order"), it.get("text")),
        )


def insert_plan_change_note(conn: sqlite3.Connection, program_id: int, p: Dict[str, Any]) -> None:
    # Prefer explicit top-level block if present; else skip (plan() already inserted fallback)
    pc = p.get("plan_change_note") or {}
    if not pc:
        return
    y = pc.get("fiscal_year_label")
    fy_id = ensure_fiscal_year(conn, y, None) if y else None
    conn.execute(
        "INSERT INTO plan_change_note(program_id, fiscal_year_id, change_points_text, change_reason_text) VALUES (?,?,?,?)",
        (program_id, fy_id, pc.get("change_points"), pc.get("change_reason")),
    )


def insert_search_chunks(conn: sqlite3.Connection, program_id: int, program_code: str, p: Dict[str, Any]) -> None:
    for ch in p.get("search_chunks") or []:
        section = ch.get("section")
        if not section:
            continue
        y = ch.get("year_label")
        fy_id = ensure_fiscal_year(conn, y, None) if y else None
        conn.execute(
            """
            INSERT INTO text_chunk(program_id, program_code, fiscal_year_id, year_label, section, content, source_table, source_pk, position, lang, token_count)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                program_id, program_code, fy_id, y, section, ch.get("content"),
                'json_search_chunk', None, ch.get("position"), 'ja', None,
            ),
        )

# -----------------------------
# Top-level import unit (single program)
# -----------------------------

def import_program(conn: sqlite3.Connection, p: Dict[str, Any]) -> Optional[str]:
    code = p.get("code")
    if not code:
        raise ValueError("Each program must have a non-empty 'code'.")
    if program_exists(conn, code):
        log(f"Skip existing program code={code}")
        return None

    program_id = insert_program_core(conn, p)
    insert_program_relationships(conn, program_id, p)
    insert_finance(conn, program_id, p)
    insert_plan(conn, program_id, p)
    insert_results_do(conn, program_id, p)
    insert_indicators(conn, program_id, p)
    insert_evaluation(conn, program_id, p)
    insert_action(conn, program_id, p)
    insert_plan_change_note(conn, program_id, p)
    insert_search_chunks(conn, program_id, code, p)

    return code

# -----------------------------
# Preload: enums/fiscal years from header (INSERT OR IGNORE)
# -----------------------------

def preload_from_header(conn: sqlite3.Connection, header: Dict[str, Any]) -> None:
    enums = header.get("enums") or {}
    # impl_mode, legal_basis_type, funding_source, indicator_type, eval_category, action_direction, achievement_level
    for t in ("impl_mode","legal_basis_type","funding_source","indicator_type","eval_category","action_direction"):
        for item in enums.get(t) or []:
            ensure_lookup_row(conn, t, item.get("code"), item.get("label"))
    # achievement_level codes may be ints
    for item in enums.get("achievement_level") or []:
        code = item.get("code")
        ensure_lookup_row(conn, "achievement_level", code, item.get("label"))

    # fiscal years
    for fy in header.get("fiscal_years") or []:
        ensure_fiscal_year(conn, fy.get("label"), fy.get("gregorian"))

# -----------------------------
# Batch processors
# -----------------------------

def import_json_payload(conn: sqlite3.Connection, data: Dict[str, Any]) -> Tuple[int,int,List[str],bool]:
    """Return (total_programs, skipped_count, imported_codes, had_failures)."""
    header = {k: data.get(k) for k in ("meta","enums","fiscal_years") if k in data}
    programs = data.get("programs") or []

    preload_from_header(conn, header)

    total = len(programs)
    skipped = 0
    imported_codes: List[str] = []
    had_failures = False
    for p in programs:
        try:
            code = import_program(conn, p)
        except Exception as e:
            log(f"Error importing program code={p.get('code')}: {e}")
            code = None
            had_failures = True
        if code is None:
            skipped += 1
        else:
            imported_codes.append(code)
    return total, skipped, imported_codes, had_failures

# -----------------------------
# CLI
# -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Import schema_template.json-style data into SQLite (schema.sql)")
    ap.add_argument("--db", default="data.sqlite3", help="SQLite DB path (default: data.sqlite3)")
    ap.add_argument("--json", required=True, help="Path to JSON file, directory, or '-' for STDIN")
    args = ap.parse_args()

    json_path = args.json
    is_dir = os.path.isdir(json_path) if json_path != '-' else False
    if is_dir and json_path == '-':
        raise SystemExit("'-' (STDIN) cannot be a directory.")

    conn = connect(args.db)
    total_files = 0
    total_programs = 0
    imported_codes: List[str] = []
    skipped = 0
    failed_files: set[str] = set()
    try:
        with conn:
            maybe_init_schema(conn)
            if is_dir:
                for f in iter_json_files(json_path):
                    total_files += 1
                    try:
                        header, programs = load_programs_variant(f)
                        log(f"Processing {f} ({len(programs)} program(s)) [variant]")
                        preload_from_header(conn, header)
                        file_skipped = 0
                        for p in programs:
                            try:
                                code = import_program(conn, p)
                            except Exception as e:
                                log(f"Error importing program in {f}: {e}")
                                code = None
                            if code is None:
                                skipped += 1
                                file_skipped += 1
                            else:
                                imported_codes.append(code)
                        if file_skipped > 0:
                            failed_files.add(f)
                        total_programs += len(programs)
                    except SystemExit as se:
                        log(str(se))
                        failed_files.add(f)
            else:
                # Single file or STDIN
                data = load_json(json_path)
                total_files = 1
                try:
                    # Accept variants for single file as well
                    if isinstance(data, dict) and ("programs" in data or "program" in data or data.get("code")):
                        header, programs = load_programs_variant(json_path)  # reload path for consistency
                        preload_from_header(conn, header)
                        total = len(programs)
                        file_skipped = 0
                        codes: List[str] = []
                        had_failures = False
                        for p in programs:
                            try:
                                code = import_program(conn, p)
                            except Exception as e:
                                log(f"Error importing program: {e}")
                                code = None
                                had_failures = True
                            if code is None:
                                file_skipped += 1
                            else:
                                codes.append(code)
                    else:
                        total, file_skipped, codes, had_failures = import_json_payload(conn, data)
                    total_programs += total
                    skipped += file_skipped
                    imported_codes.extend(codes)
                    if had_failures or file_skipped > 0:
                        failed_files.add('<stdin>' if json_path=='-' else json_path)
                except SystemExit as se:
                    log(str(se))
                    failed_files.add('<stdin>' if json_path=='-' else json_path)

        log(
            f"Inserted {len(imported_codes)} new program(s), skipped {skipped} existing, "
            f"from {total_files or 1} file(s). Codes: " + (", ".join(imported_codes) if imported_codes else "<none>")
        )

        # Move failed JSON files into 'failed' subdirectory
        if failed_files:
            base_dir = None
            if is_dir:
                base_dir = os.path.abspath(json_path)
            else:
                base_dir = os.path.dirname(os.path.abspath(json_path)) if json_path != '-' else None
            moved = []
            if base_dir and json_path != '-':
                failed_dir = os.path.join(base_dir, 'failed')
                os.makedirs(failed_dir, exist_ok=True)
                for f in sorted(failed_files):
                    try:
                        abs_f = os.path.abspath(f)
                        if os.path.dirname(abs_f) == os.path.abspath(failed_dir):
                            continue
                        dest_path = os.path.join(failed_dir, os.path.basename(f))
                        if os.path.exists(dest_path):
                            stem, ext = os.path.splitext(os.path.basename(f))
                            i = 1
                            while True:
                                candidate = os.path.join(failed_dir, f"{stem}_{i}{ext}")
                                if not os.path.exists(candidate):
                                    dest_path = candidate
                                    break
                                i += 1
                        shutil.move(f, dest_path)
                        moved.append(dest_path)
                    except Exception as e:
                        log(f"Failed to move failed file {f}: {e}")
            log("Failed JSON files (one or more programs skipped/errored):")
            for f in sorted(failed_files):
                log(f"  - {f}")
            if moved:
                log("Moved failed JSON files into 'failed' directory:")
                for m in moved:
                    log(f"  -> {m}")
        else:
            log("All JSON files imported without skips/errors.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
