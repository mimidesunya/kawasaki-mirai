#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
md_to_json_v2.py
- Based on the user's original md_to_json.py (AI prompt → JSON).
- Enhancements:
  * --schema option (path override)
  * --out-format {dbjson,docjson} (default: dbjson)
  * Transforms the AI-generated domain JSON (program-centric) into
    "DB-ingest JSON" (table-like lists with logical keys), ready to insert
    into the SQLite schema provided earlier.

Assumptions:
- A `chat(system, user, model)` function is available (same as original).
- The schema template matches the domain JSON structure produced by AI.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# ==== External dependency (same as original) ====
# chat(system, user, model) should return model's text response.
sys.path.append(str(Path(__file__).parent.parent))
from chat import chat  # type: ignore

# =========================
# Prompt template (same as original, with strict rules)
# =========================
PROMPT_TEMPLATE = r"""
【役割】
あなたは厳密なデータ整形エージェントです。以下のSCHEMAに**完全準拠**し、与えられた事務事業評価シート（MDテキスト）をJSON化します。

【入出力】
- 入力1：SCHEMA（schema_template.jsonの中身。下の <<<SCHEMA を参照）
- 入力2：事務事業評価シートのテキスト（MD。下の <<<DOC を参照）
- 出力：**SCHEMAと完全一致の構造・キー・型**のJSONのみ（前後に説明や余計な文字を出さない）

【必須ルール】
1) スキーマ完全準拠：
   - キー名・配列構造・データ型（数値/文字列/null）をSCHEMAと一致させる。
   - SCHEMAに存在しないキーを追加しない。
   - 見つからない値は `null` または空文字 `""`（SCHEMA側の型・意図に合わせる）。
2) 数値正規化：
   - 金額・回数等の**数値**はカンマ・空白・単位を除去し**整数化**（例：「12,345 千円」→ 12345）。
   - 「-」「－」「—」「―」や空欄、判別不能は `null`。
3) 年度表記：
   - 年度ラベルは原文（例：`R4`/`R05`/`令和5` 等）を**年度ラベル用フィールド**にそのまま格納。
   - 必要に応じて西暦を別フィールドがある場合のみ変換して格納（なければ変換しない）。
4) テキスト整形：
   - 改行は保持。連続空白は1つに圧縮（表の読み替えで意味が変わらない範囲）。
   - 箇条書き(①②③/1)2)3)/・- 等)は配列項目に分割できる該当フィールドがSCHEMAにある場合のみ分割。なければ原文保持。
5) JSON厳格出力：
   - 有効なUTF-8 JSONのみを出力。コメント、末尾カンマ、追加の説明文を含めない。

【抽出・マッピング指針（この種の「事務事業評価シート」を想定）】
- 代表的見出しと想定マッピング（**SCHEMAにフィールドがある場合のみ**該当させる）：
  - 「事務事業コード」→ 事業コード系フィールド
  - 「事務事業名」→ 事業名
  - 「組織コード」「所属名」「担当」→ 組織/担当系
  - 「事務・サービス等の分類」「分類1/分類2」→ 分類系
  - 「実施根拠（国・県／市独自／法令・要綱）」→ 実施根拠テキスト＋区分
  - 「実施形態（市が直接／一部委託／全部委託・指定管理／協働／その他）」→ チェックの付いた項目のみ列挙
  - 「SDGsのゴール／ターゲット／方向性」→ SDGs系
  - 「政策／施策／直接目標」→ 政策・施策・直接目標
  - 「事業の対象／目的／内容」→ 計画（PLAN）節
  - 「当該年度の取組内容」→ 箇条書き配列化（SCHEMAに配列がある場合）
  - 「達成度」「取組内容の実績等」→ 実行（DO）節
  - 「指標分類／指標名／単位／目標／実績（各年度）」→ 指標配列 + 年度別値配列
  - 「予算・決算・財源内訳（各年度）」→ 年度別財務配列（予算、決算、人件費、総コスト、一般財源、特財、市債、国費 等）
  - 「社会環境の変化」→ 環境変化
  - 「評価（ニーズ/必要性/成果/民間活用/手法見直し/質の向上 等の選択肢＋理由）」→ 評価項目配列（選択肢a/b/c＋理由）
  - 「事業の見直し・改善内容（年度ごと）」→ 改善履歴（年度ラベル＋内容）
  - 「施策への貢献度（A/B/C）＋理由」→ 貢献度
  - 「今後の事業の方向性（Ⅰ〜Ⅵ）」→ 方針コード＋ラベル
  - 「次年度の取組内容／変更箇所／変更理由」→ 次年度計画関連
  - 「行財政改革第○期プログラム（改革項目・課題名）」→ 改革プログラム配列
- 表の横持ち→縦持ち整形：
  - 年度の列（R4/R5/R6…）を検出し、**年度ごと**の配列要素に分解。
- 記号・チェックボックス：
  - 「☑」「レ点」「○」「●」等は**選択済み**とみなし、対応フィールドに反映。

【バリデーション】
- 生成後にJSONを仮想的にパースし、SCHEMA必須キーの欠落や型不一致を検査。
- 欠落時は `null`/`""` を補完し、SCHEMAの配列・オブジェクト構造を維持して再出力。

【出力形式】
- **JSONのみ**（BOMなし）。余計な文字列や説明を一切付与しない。

【SCHEMA】
<<<SCHEMA
<<SCHEMA>>
SCHEMA

【入力テキスト（MD原文）】
<<<DOC
<<DOC>>
DOC
""".lstrip()


def read_text_file(path: Path) -> str:
    """Read text with gentle normalization."""
    tried = []
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis"):
        try:
            text = path.read_text(encoding=enc)
            break
        except Exception as e:
            tried.append(f"{enc}:{e}")
            continue
    else:
        raise RuntimeError(f"Failed to read: {path}\n" + "\n".join(tried))
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    for zw in ("\u200b", "\u200c", "\u200d", "\ufeff"):
        text = text.replace(zw, "")
    return text.strip("\n")


def build_prompt(schema_text: str, md_text: str) -> str:
    p = PROMPT_TEMPLATE.replace("<<SCHEMA>>", schema_text)
    p = p.replace("<<DOC>>", md_text)
    return p


def _extract_json_block(text: str) -> Optional[str]:
    start = text.find('{')
    end = text.rfind('}')
    if start == -1 or end == -1 or end <= start:
        return None
    candidate = text[start:end + 1].strip()
    candidate = re.sub(r'^```(?:json)?', '', candidate).strip()
    candidate = re.sub(r'```$', '', candidate).strip()
    return candidate


def run_ai_conversion(schema_text: str, md_text: str, *, temperature: float = 0.0, model: str = "gpt-5-mini") -> str:
    full_prompt = build_prompt(schema_text=schema_text, md_text=md_text)
    marker = "【SCHEMA】"
    if marker in full_prompt:
        idx = full_prompt.index(marker)
        system_part = full_prompt[:idx].strip()
        user_part = full_prompt[idx:].strip()
    else:
        system_part = "あなたは与えられた SCHEMA に厳密準拠して MD テキストを JSON に変換する厳格なエージェントです。JSON 以外出力禁止。"
        user_part = full_prompt

    response_text = chat(system_part, user_part, model)

    json_text = response_text.strip()
    try:
        json.loads(json_text)
        return json_text
    except Exception:
        pass
    candidate = _extract_json_block(response_text)
    if candidate:
        try:
            json.loads(candidate)
            return candidate
        except Exception:
            pass
    fenced = re.sub(r'^```[a-zA-Z]*\n?', '', response_text).strip()
    fenced = re.sub(r'```$', '', fenced).strip()
    json.loads(fenced)  # may raise
    return fenced


# =========================
# Domain JSON -> DB-ingest JSON
# =========================
def _ensure_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def domain_to_dbjson(doc: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convert domain JSON (program-centric) into DB-ingest JSON:
      Keys correspond to target tables; values are lists of rows.
      Rows use logical keys (program_code, fiscal_year_label, ...).
    """
    out = {
        "organization": [],
        "program": [],
        "program_impl_mode": [],
        "program_legal_basis": [],
        "linked_plan": [],
        "program_linked_plan": [],
        "sdg": [],
        "program_sdg": [],
        "program_fiscal": [],
        "program_fiscal_funding": [],
        "planned_action": [],
        "program_result": [],
        "indicator": [],
        "indicator_value": [],
        "program_evaluation": [],
        "evaluation_score": [],
        "program_contribution": [],
        "program_action": [],
        "next_year_action_item": [],
        "plan_change_note": [],
        "text_chunk": [],
        "source": [],
    }

    seen = {
        "organization": set(),
        "linked_plan": set(),
        "sdg": set(),  # key: (goal, target or None)
        "indicator_key": set(),  # (program_code, indicator_name)
    }

    programs = doc.get("programs") or []
    for p in programs:
        code = p.get("code")
        name = p.get("name")
        if not code:
            # skip malformed entries
            continue

        # --- organization upsert ---
        org = (p.get("organization") or {})
        org_code = org.get("org_code")
        org_name = org.get("name")
        if org_code and (org_code not in seen["organization"]):
            out["organization"].append({"org_code": org_code, "name": org_name})
            seen["organization"].add(org_code)

        # --- program row ---
        classifications = p.get("classifications") or {}
        impl = p.get("implementation") or {}
        pol = p.get("policy_links") or {}
        sdgs = (pol.get("sdgs") or {})
        purpose = p.get("purpose_and_content") or {}

        out["program"].append({
            "code": code,
            "name": name,
            "organization_org_code": org_code,
            "policy": pol.get("policy"),
            "measure": pol.get("measure"),
            "direct_goal": purpose.get("direct_goal"),
            "target_population": purpose.get("target_population"),
            "objective": purpose.get("objective"),
            "content": purpose.get("content"),
            "classification1": classifications.get("classification1"),
            "classification2": classifications.get("classification2"),
            "service_category": classifications.get("service_category"),
            "start_fiscal_year_label": impl.get("start_fiscal_year_label"),
            "end_fiscal_year_label": impl.get("end_fiscal_year_label"),
            "legal_basis_text": impl.get("legal_basis_text"),
            "general_plan_text": ", ".join([x for x in _ensure_list(pol.get("linked_plans")) if x]) or None,
            "sdgs_orientation": sdgs.get("orientation_text"),
            "reform_link_text": pol.get("reform_program_text"),
        })

        # --- impl modes / legal basis ---
        for m in _ensure_list(impl.get("modes")):
            if m:
                out["program_impl_mode"].append({"program_code": code, "impl_mode_code": m})
        for lb in _ensure_list(impl.get("legal_basis_types")):
            if lb:
                out["program_legal_basis"].append({"program_code": code, "legal_basis_type_code": lb})

        # --- linked plans ---
        for lp in _ensure_list(pol.get("linked_plans")):
            if not lp:
                continue
            if lp not in seen["linked_plan"]:
                out["linked_plan"].append({"name": lp})
                seen["linked_plan"].add(lp)
            out["program_linked_plan"].append({"program_code": code, "plan_name": lp})

        # --- SDGs ---
        # Targets like "11.7" imply goal=11, target="11.7"
        for tgt in _ensure_list(sdgs.get("targets")):
            if not tgt:
                continue
            try:
                goal_val = int(str(tgt).split(".")[0])
            except Exception:
                goal_val = None
            key = (goal_val, str(tgt))
            if key not in seen["sdg"]:
                out["sdg"].append({"goal": goal_val, "target": str(tgt)})
                seen["sdg"].add(key)
            out["program_sdg"].append({"program_code": code, "goal": goal_val, "target": str(tgt)})
        # If there are bare goals without targets, add them too
        for g in _ensure_list(sdgs.get("goals")):
            if g is None:
                continue
            key = (int(g), None)
            if key not in seen["sdg"]:
                out["sdg"].append({"goal": int(g), "target": None})
                seen["sdg"].add(key)
            out["program_sdg"].append({"program_code": code, "goal": int(g), "target": None})

        # --- finance ---
        for f in _ensure_list(p.get("finance")):
            fy = f.get("fiscal_year_label")
            out["program_fiscal"].append({
                "program_code": code,
                "fiscal_year_label": fy,
                "budget_amount_a": f.get("a_budget"),
                "settlement_amount_a": f.get("a_settlement"),
                "planned_project_cost_a": f.get("a_planned"),
                "human_cost_b_budget": f.get("b_human_budget"),
                "human_cost_b_settlement": f.get("b_human_settlement"),
                "total_cost_budget": f.get("total_budget"),
                "total_cost_settlement": f.get("total_settlement"),
                "manpower_person_year": f.get("manpower_person_year"),
            })
            fb = (f.get("funding_breakdown") or {})
            for fs_code in ("national_subsidy","municipal_bond","other_special_fund","general_fund"):
                if fs_code in fb and isinstance(fb[fs_code], dict):
                    row = {
                        "program_code": code,
                        "fiscal_year_label": fy,
                        "funding_source_code": fs_code,
                        "budget_amount": fb[fs_code].get("budget"),
                        "settlement_amount": fb[fs_code].get("settlement"),
                    }
                    out["program_fiscal_funding"].append(row)

        # --- plan (PLAN) ---
        plan = p.get("plan") or {}
        fy_plan = plan.get("applicable_year_label")
        for item in _ensure_list(plan.get("action_items")):
            out["planned_action"].append({
                "program_code": code,
                "fiscal_year_label": fy_plan,
                "item_order": item.get("order"),
                "text": item.get("text"),
            })
        ch = plan.get("change_from_initial_plan") or {}
        if ch.get("change_points") or ch.get("change_reason"):
            out["plan_change_note"].append({
                "program_code": code,
                "fiscal_year_label": fy_plan,
                "change_points_text": ch.get("change_points"),
                "change_reason_text": ch.get("change_reason"),
            })

        # --- results (DO) ---
        for r in _ensure_list(p.get("results_do")):
            out["program_result"].append({
                "program_code": code,
                "fiscal_year_label": r.get("fiscal_year_label"),
                "achievement_level_code": r.get("achievement_level"),
                "result_text": r.get("result_text"),
            })

        # --- indicators ---
        for idx, ind in enumerate(_ensure_list(p.get("indicators")), start=1):
            iname = ind.get("name")
            if not iname:
                continue
            key = (code, iname)
            if key not in seen["indicator_key"]:
                out["indicator"].append({
                    "program_code": code,
                    "name": iname,
                    "description": ind.get("description"),
                    "unit": ind.get("unit"),
                    "indicator_type_code": ind.get("type"),
                    "sort_order": idx,
                })
                seen["indicator_key"].add(key)
            for val in _ensure_list(ind.get("values")):
                out["indicator_value"].append({
                    "program_code": code,
                    "indicator_name": iname,
                    "fiscal_year_label": val.get("fiscal_year_label"),
                    "target_value": val.get("target"),
                    "actual_value": val.get("actual"),
                })

        # --- evaluation (CHECK) ---
        ev = p.get("evaluation_check") or {}
        if ev:
            out["program_evaluation"].append({
                "program_code": code,
                "fiscal_year_label": ev.get("fiscal_year_label"),
                "environment_change": ev.get("environment_change"),
                "improvement_history": ev.get("improvements_history_text"),
            })
            for s in _ensure_list(ev.get("scores")):
                out["evaluation_score"].append({
                    "program_code": code,
                    "fiscal_year_label": ev.get("fiscal_year_label"),
                    "eval_category_code": s.get("category_code"),
                    "rating_letter": s.get("rating"),
                    "reason": s.get("reason"),
                })
            contrib = ev.get("contribution") or {}
            if contrib:
                out["program_contribution"].append({
                    "program_code": code,
                    "fiscal_year_label": ev.get("fiscal_year_label"),
                    "level_letter": contrib.get("level"),
                    "reason": contrib.get("reason"),
                })

        # --- action (ACTION / NEXT_YEAR) ---
        act = p.get("action_improvement") or {}
        if act:
            out["program_action"].append({
                "program_code": code,
                "fiscal_year_label": act.get("fiscal_year_label"),
                "direction_code": act.get("direction_code"),
                "direction_text": act.get("direction_text"),
            })
            for item in _ensure_list(act.get("next_year_action_items")):
                out["next_year_action_item"].append({
                    "program_code": code,
                    "fiscal_year_label": act.get("fiscal_year_label"),
                    "item_order": item.get("order"),
                    "text": item.get("text"),
                })

        # --- prebuilt search chunks (optional; triggers will also build them at insert time) ---
        for ch in _ensure_list(p.get("search_chunks")):
            out["text_chunk"].append({
                "program_code": code,
                "section": ch.get("section"),
                "year_label": ch.get("year_label"),
                "content": ch.get("content"),
                "position": ch.get("position"),
                "lang": "ja",
            })

        # --- sources ---
        src = (p.get("sources") or {})
        out["source"].append({
            "program_code": code,
            "filename": src.get("filename"),
            "url": src.get("url"),
        })

    return out


# =========================
# Processing pipeline
# =========================
def process_single_file(md_path: Path, schema_text: str, *, model: str, out_format: str, output_path: Optional[Path]) -> int:
    md_text = read_text_file(md_path)
    prompt = build_prompt(schema_text=schema_text, md_text=md_text)
    # Call AI
    try:
        doc_json_text = run_ai_conversion(schema_text=schema_text, md_text=md_text, model=model)
    except Exception as e:
        print(f"[ERROR] AI conversion failed for {md_path.name}: {e}", file=sys.stderr)
        return 3
    try:
        doc = json.loads(doc_json_text)
    except Exception as e:
        print(f"[ERROR] JSON parse failed: {md_path.name}: {e}", file=sys.stderr)
        return 4

    # Choose output format
    if out_format == "docjson":
        out_obj = doc
    else:
        out_obj = domain_to_dbjson(doc)

    text = json.dumps(out_obj, ensure_ascii=False, indent=2)
    if output_path:
        try:
            output_path.write_text(text, encoding="utf-8")
        except Exception as e:
            print(f"[ERROR] Write failed: {output_path}: {e}", file=sys.stderr)
            return 5
    else:
        sys.stdout.write(text)
    return 0


def process_directory(md_dir: Path, out_dir: Path, schema_text: str, *, model: str, out_format: str, overwrite: bool = True) -> int:
    if not md_dir.exists() or not md_dir.is_dir():
        print(f"[ERROR] MD dir not found: {md_dir}", file=sys.stderr)
        return 10
    out_dir.mkdir(parents=True, exist_ok=True)

    md_files = sorted([p for p in md_dir.glob("*.md") if p.is_file()])
    if not md_files:
        print(f"[ERROR] No *.md in: {md_dir}", file=sys.stderr)
        return 11

    total = len(md_files)
    ok = 0
    failed: List[Path] = []
    for i, md_path in enumerate(md_files, 1):
        out_path = out_dir / f"{md_path.stem}.{out_format}.json"
        if out_path.exists() and not overwrite:
            print(f"[{i}/{total}] SKIP: {md_path.name}", file=sys.stderr)
            ok += 1
            continue
        print(f"[{i}/{total}] Convert: {md_path.name}", file=sys.stderr)
        code = process_single_file(md_path, schema_text, model=model, out_format=out_format, output_path=out_path)
        if code == 0:
            ok += 1
            print(f"[{i}/{total}] OK: {md_path.name}", file=sys.stderr)
        else:
            failed.append(md_path)
            print(f"[{i}/{total}] FAIL(code={code}): {md_path.name}", file=sys.stderr)

    print(f"Done: OK {ok}/{total}, Failed {len(failed)}", file=sys.stderr)
    return 0 if not failed else 20


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="MD→JSON (AI) with DB-ingest transformer")
    # Modes
    ap.add_argument("--md", type=Path, help="Single MD file path")
    ap.add_argument("--md-dir", type=Path, help="Directory mode: input MD dir (glob: *.md)")
    ap.add_argument("--out", type=Path, help="Single mode: output JSON file path (stdout if omitted)")
    ap.add_argument("--out-dir", type=Path, help="Directory mode: output dir")
    ap.add_argument("--no-overwrite", action="store_true", help="Skip existing outputs")
    # Schema / Model
    ap.add_argument("--schema", type=Path, default=Path(__file__).parent / "schema_template.json", help="Schema JSON path (default: ./schema_template.json)")
    ap.add_argument("--model", type=str, default="gpt-5-mini", help="Model (default: gpt-5-mini)")
    # Output flavor
    ap.add_argument("--out-format", choices=["dbjson","docjson"], default="dbjson", help="Output format (default: dbjson)")
    args = ap.parse_args(argv)

    # Resolve schema
    if not args.schema.exists():
        print(f"[WARN] schema not found at {args.schema}.", file=sys.stderr)
        return 90
    schema_text = args.schema.read_text(encoding="utf-8")

    # Directory mode
    if args.md_dir or args.out_dir:
        if not (args.md_dir and args.out_dir):
            ap.error("--md-dir and --out-dir must be specified together")
        return process_directory(args.md_dir, args.out_dir, schema_text, model=args.model, out_format=args.out_format, overwrite=not args.no_overwrite)

    # Single mode
    if not args.md:
        ap.error("--md or (--md-dir and --out-dir) required")
    return process_single_file(args.md, schema_text, model=args.model, out_format=args.out_format, output_path=args.out)


if __name__ == "__main__":
    raise SystemExit(main())
