
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
struct_pdf.py — 事務事業評価シート：連結PDF → 事務事業ごとの Markdown 複数出力
=================================================================================
依存: pdfplumber >= 0.11.0  （pip install pdfplumber）

変更点（2025-09-02）:
- 以前は全シートを 1 つの Markdown に統合して出力していたが、要求に合わせ
    「事務事業コード（またはシート番号）」ごとに別ファイルへ出力する方式に変更。
- -o/--out は "出力ディレクトリ" を指定する。存在しない場合は作成。
- ファイル名: <事務事業コード>.md。コードが取得できない場合は sheetXX_タイトル抜粋.md。
    コード重複時は _2, _3 を付番。

概要:
- PDF ページを走査し「事務事業評価シート」を含むページでシート境界を検出。
- シート単位でテキスト行を復元し、PDCA 等の見出しでセクション分割。
- 各シートを個別 Markdown として出力。

使い方例:
        python struct_pdf.py input.pdf -o out_dir

主なオプション:
        --line-tol 2.5       同一行とみなす y 距離（pt）
        --gap-tol 0.8        文字間の空白判定 x 距離（pt）
        --code-scan-pages 2  コード抽出の探索対象ページ数（シート先頭からの枚数）
'''
import argparse
import json
import re
import unicodedata
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import pdfplumber


# セクション見出し候補（必要に応じて調整）
HEADER_PATTERNS = [
    (re.compile(r"事業の概要"), "overview"),
    (re.compile(r"計\s*画.*Plan|Plan", re.IGNORECASE), "plan"),
    (re.compile(r"実施結果.*Do|Do", re.IGNORECASE), "do"),
    (re.compile(r"評\s*価.*Check|Check", re.IGNORECASE), "check"),
    (re.compile(r"改\s*善.*Action|Action", re.IGNORECASE), "action"),
]

SHEET_START_RE = re.compile(r"事務事業評価シート")
CODE_LABEL_RES = [
    re.compile(r"事務事業コード[：:\s]*([^\s　/／]+)"),
    re.compile(r"事業コード[：:\s]*([^\s　/／]+)"),
]

# 事務事業名抽出用の簡易トークン終端候補
BUSINESS_NAME_STOP_TOKENS = {"有", "無", "あり", "なし", "有り", "無し", "—", "―"}


def extract_lines(page, line_tol=2.5, gap_tol=0.8):
    '''
    pdfplumberのcharsから行を自前クラスタリング。
    y方向: topが近いものを同一行、x方向: 文字間gapが閾値以上なら空白を挿入。
    戻り値: [{"text": str, "x0": float, "y0": float, "x1": float, "y1": float}, ...]
    備考: 座標系は左上(0,0)、yは下向きに増加（pdfplumber準拠）。
    '''
    chars = sorted(page.chars, key=lambda c: (c["top"], c["x0"]))
    lines = []
    if not chars:
        return lines

    cur = []
    last_top = None

    def flush():
        nonlocal cur
        if not cur:
            return
        x0 = min(c["x0"] for c in cur)
        x1 = max(c["x1"] for c in cur)
        top = min(c["top"] for c in cur)
        bottom = max(c["bottom"] for c in cur)
        cur_sorted = sorted(cur, key=lambda c: c["x0"])
        text_parts = [cur_sorted[0]["text"]]
        for prev, now in zip(cur_sorted, cur_sorted[1:]):
            gap = now["x0"] - prev["x1"]
            if gap > gap_tol:
                text_parts.append(" ")
            text_parts.append(now["text"])
        text = "".join(text_parts)
        lines.append({"text": text.strip(), "x0": float(x0), "y0": float(top), "x1": float(x1), "y1": float(bottom)})
        cur = []

    for ch in chars:
        if not cur:
            cur = [ch]
            last_top = ch["top"]
            continue
        if abs(ch["top"] - last_top) <= line_tol:
            cur.append(ch)
            last_top = (last_top * 0.7) + (ch["top"] * 0.3)
        else:
            flush()
            cur = [ch]
            last_top = ch["top"]
    flush()
    # 行の読み順: y(昇順) → x(昇順)
    lines.sort(key=lambda ln: (ln["y0"], ln["x0"]))
    return lines


def is_sheet_start(lines: List[Dict]) -> bool:
    return any(SHEET_START_RE.search(ln["text"]) for ln in lines)


def normalize_token(s: str) -> str:
    ''' 全角→半角、空白の正規化 '''
    s = unicodedata.normalize("NFKC", s)
    return s.strip()


def safe_filename(s: str) -> str:
    '''
    ファイル名に安全な文字だけを残す。
    例: スラッシュはハイフンへ。日本語も可だが、環境依存を避けるならASCII推奨。
    '''
    s = normalize_token(s)
    s = s.replace("/", "-").replace("\\", "-")
    # 先頭末尾のドット/空白除去
    s = s.strip(" .")
    return s if s else "sheet"


def try_extract_code_from_text(text: str) -> Optional[str]:
    text_norm = normalize_token(text)
    for rx in CODE_LABEL_RES:
        m = rx.search(text_norm)
        if m:
            return normalize_token(m.group(1))
    return None


def try_extract_code_from_lines(lines: List[Dict]) -> Optional[str]:
    '''
    1) 文字列そのものに "事務事業コード: XXX" が含まれる
    2) "事務事業コード" の右隣 or 同行の次のテキストを拾う
    '''
    # 1) まとめテキストで直接拾う
    joined = "\n".join(ln["text"] for ln in lines if ln["text"])
    code = try_extract_code_from_text(joined)
    if code:
        return code

    # 2) XYを使って "右側" or "すぐ下" を拾う
    for i, ln in enumerate(lines):
        if "事務事業コード" in ln["text"] or "事業コード" in ln["text"]:
            # 同じ行（y帯域が近い）で x が右側のテキストを収集
            y0, y1 = ln["y0"], ln["y1"]
            right_candidates = [l for l in lines if (abs(((l["y0"]+l["y1"])/2.0) - ((y0+y1)/2.0)) <= 3.0) and (l["x0"] > ln["x1"] + 2.0) and l["text"]]
            right_candidates.sort(key=lambda L: (L["x0"], L["y0"]))
            for cand in right_candidates[:3]:
                c = normalize_token(cand["text"])
                # 空白含みの長文は切る（最初の空白まで）
                token = c.split()[0] if " " in c else c
                token = token.split("　")[0] if "　" in token else token
                if token:
                    return token

            # 見つからないときは直下行
            below_candidates = [l for l in lines if (l["y0"] > ln["y1"]) and (l["x0"] >= ln["x0"] - 5.0) and (l["x0"] <= ln["x1"] + 100.0)]
            below_candidates.sort(key=lambda L: (L["y0"], L["x0"]))
            for cand in below_candidates[:3]:
                token = normalize_token(cand["text"])
                if token:
                    return token
    return None


def try_extract_code_and_name_from_lines(lines: List[Dict]) -> Tuple[Optional[str], Optional[str]]:
    """コードと事務事業名を同時に抽出する試行。
    戻り値: (code, name)
    name が取得できない場合は (code, None)
    ヒューリスティック:
      - 行頭に 8 桁程度の数字(または英数字) + 空白 + 事業名 + 空白 + (有|無|...) の形式
      - トークン区切りでコード後の連続トークンを STOP_TOKENS 直前まで連結
    """
    code_only = try_extract_code_from_lines(lines)

    # 正規表現パターンで直接抽出
    re_line = re.compile(r"^(?P<code>[0-9A-Za-z]{4,})[ \t　]+(?P<name>.+?)[ \t　]+(有|無|あり|なし|有り|無し|—|―)(?:$|[ \t　])")
    for ln in lines:
        text = normalize_token(ln["text"])
        m = re_line.match(text)
        if m:
            code = normalize_token(m.group("code"))
            name = normalize_token(m.group("name"))
            return code, name

    # トークン分解方式
    for ln in lines:
        raw = normalize_token(ln["text"])
        if not raw:
            continue
        # 全角空白も分割対象に
        tokens = re.split(r"[ \t　]+", raw)
        if len(tokens) < 2:
            continue
        first = tokens[0]
        if re.fullmatch(r"[0-9A-Za-z]{4,}", first):
            # 2 つ目以降を STOP_TOKENS まで name とみなす
            name_tokens = []
            for t in tokens[1:]:
                if t in BUSINESS_NAME_STOP_TOKENS:
                    break
                name_tokens.append(t)
            if name_tokens:
                name = " ".join(name_tokens)
                return first, name

    return code_only, None


def find_sections(all_lines: List[Dict]) -> Dict[str, List[str]]:
    '''
    複数ページ連結後の行配列から、見出しに基づいてセクションを切り出し。
    戻り値: dict(section_key -> list[str])
    '''
    marks = []
    for i, ln in enumerate(all_lines):
        t = ln["text"]
        for pat, key in HEADER_PATTERNS:
            if pat.search(t):
                marks.append((key, i))
                break
    marks.sort(key=lambda x: x[1])
    sections = {}
    if not marks:
        sections["document"] = [ln["text"] for ln in all_lines if ln["text"]]
        return sections

    for idx, (key, start) in enumerate(marks):
        end = marks[idx + 1][1] if idx + 1 < len(marks) else len(all_lines)
        body = [ln["text"] for ln in all_lines[start + 1 : end] if ln["text"]]
        sections.setdefault(key, []).extend(body)
    return sections


def write_sections_md(path: Path, sections: Dict[str, List[str]]):
    md_lines = []
    order = ["overview", "plan", "do", "check", "action"]
    names = {
        "overview": "事業の概要",
        "plan": "計画（Plan）",
        "do": "実施結果（Do）",
        "check": "評価（Check）",
        "action": "改善（Action）",
        "document": "本文",
    }
    any_written = False
    for key in order:
        if key in sections and sections[key]:
            any_written = True
            md_lines.append(f"# {names[key]}\n")
            md_lines.extend([ln + "\n" for ln in sections[key]])
            md_lines.append("\n")
    # 未分類があれば最後に
    other_keys = [k for k in sections.keys() if k not in order]
    for k in other_keys:
        if sections[k]:
            any_written = True
            md_lines.append(f"# {names.get(k, k)}\n")
            md_lines.extend([ln + "\n" for ln in sections[k]])
            md_lines.append("\n")
    # どれも空なら空ファイルにしないように本文を補完
    if not any_written:
        body = sections.get("document", [])
        md_lines.append("# 本文\n")
        md_lines.extend([ln + "\n" for ln in body])
        md_lines.append("\n")
    path.write_text("".join(md_lines), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser(description="事務事業評価シート連結PDF → シートごとの Markdown 複数出力")
    ap.add_argument("pdf", help="入力PDFパス")
    ap.add_argument("-o", "--out", default="out", help="出力ディレクトリ (例: out)")
    ap.add_argument("--line-tol", type=float, default=2.5, help="同一行とみなすy距離（pt）")
    ap.add_argument("--gap-tol", type=float, default=0.8, help="文字間の空白判定x距離（pt）")
    ap.add_argument("--code-scan-pages", type=int, default=2, help="コード抽出の探索対象ページ数（シート先頭からの枚数）")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    used_names = set()

    sheets: List[Tuple[int, int]] = []  # [(start_page_idx, end_page_idx)]
    page_lines: List[List[Dict]] = []   # ページごとの行配列

    with pdfplumber.open(args.pdf) as pdf:
        # 全ページの行を抽出
        sheet_starts = []
        for pidx, page in enumerate(pdf.pages):
            lines = extract_lines(page, args.line_tol, args.gap_tol)
            for ln in lines:
                ln["page"] = pidx + 1
            page_lines.append(lines)
            if is_sheet_start(lines):
                sheet_starts.append(pidx)

        if not sheet_starts:
            sheet_starts = [0]
        for i, s in enumerate(sheet_starts):
            e = (sheet_starts[i + 1] - 1) if (i + 1 < len(sheet_starts)) else (len(pdf.pages) - 1)
            sheets.append((s, e))

    for si, (sidx, eidx) in enumerate(sheets, start=1):
        sheet_lines = []
        for p in range(sidx, eidx + 1):
            sheet_lines.extend(page_lines[p])

        # コード抽出: シート先頭から指定ページ分を対象に
        scan_end_page = min(sidx + args.code_scan_pages - 1, eidx)
        candidate_lines = []
        for p in range(sidx, scan_end_page + 1):
            candidate_lines.extend(page_lines[p])
        code, biz_name = try_extract_code_and_name_from_lines(candidate_lines)

        # 追加フォールバック: コードだけ検出された場合、先頭 ~40 行から「事業」を含む最初の適度な長さの行を名前候補に
        if code and not biz_name:
            for ln in candidate_lines[:40]:
                t = normalize_token(ln["text"])
                if not t:
                    continue
                if "コード" in t:
                    continue
                if 3 <= len(t) <= 40 and ("事業" in t or t.endswith("事業")):
                    biz_name = t
                    break

        # タイトル抽出（表示用）: コード+事務事業名 を優先
        title = None
        if code and biz_name:
            title = f"{code} {biz_name}"
        elif biz_name:
            title = biz_name
        else:
            if page_lines[sidx]:
                for ln in page_lines[sidx]:
                    if ln["text"].strip():
                        title = ln["text"].strip()
                        break
            if not title:
                title = f"事務事業評価シート {si}"

        # セクション分割
        sections = find_sections(sheet_lines)

        # ファイル名決定
        if code:
            if biz_name:
                base_name = safe_filename(f"{code}_{biz_name}")
            else:
                base_name = safe_filename(code)
        else:
            # コードなし: タイトルから名前っぽい部分を抽出する試行
            title_norm = normalize_token(title)
            base_name = safe_filename(f"sheet{si:02d}_{title_norm[:20]}")
        file_name = f"{base_name}.md"
        if file_name in used_names:
            # 重複回避
            dup_idx = 2
            while True:
                file_name = f"{base_name}_{dup_idx}.md"
                if file_name not in used_names:
                    break
                dup_idx += 1
        used_names.add(file_name)
        out_path = out_dir / file_name

        # 書き込み
        md_parts = [f"# {title}\n\n"]
        order = ["overview", "plan", "do", "check", "action"]
        names = {
            "overview": "事業の概要",
            "plan": "計画（Plan）",
            "do": "実施結果（Do）",
            "check": "評価（Check）",
            "action": "改善（Action）",
            "document": "本文",
        }
        any_written = False
        for key in order:
            if key in sections and sections[key]:
                any_written = True
                md_parts.append(f"## {names[key]}\n\n")
                md_parts.extend([ln + "\n" for ln in sections[key]])
                md_parts.append("\n")
        other_keys = [k for k in sections.keys() if k not in order]
        for k in other_keys:
            if sections[k]:
                any_written = True
                md_parts.append(f"## {names.get(k, k)}\n\n")
                md_parts.extend([ln + "\n" for ln in sections[k]])
                md_parts.append("\n")
        if not any_written:
            body = sections.get("document", [])
            md_parts.append("## 本文\n\n")
            md_parts.extend([ln + "\n" for ln in body])
            md_parts.append("\n")
        out_path.write_text("".join(md_parts), encoding="utf-8")
        print(f"[sheet {si}] -> {out_path.name}")

    print(f"[ok] {len(sheets)} markdown files written under {out_dir}")

if __name__ == "__main__":
    main()
