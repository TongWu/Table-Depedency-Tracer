#!/usr/bin/env python3
"""Sequential SAS lineage extractor (block-scoped macro evaluation).

This version fixes the SYSLAST/global-expansion bug by evaluating macros
*sequentially* and *per block* (proc-sql / data-step). For each block:
  1) Snapshot the current macro env.
  2) Parse %LET inside the block (using the snapshot to resolve nested refs).
  3) Expand the block text with the block-local env.
  4) Collect read/write tables from SQL and DATA statements.
  5) Add _INPUT/_OUTPUT/SYSLAST hints from macros in scope of the block.
  6) Merge the block's %LET into the global env for subsequent blocks.

Outputs per file:
  - Input Tables: read but never written by this script
  - Intermediate Tables: written and then read (or read and then written)
  - Output Tables: written but never read by this script

Additionally, it derives a concise "main chain" (Source -> Intermediate -> Target)
preferring udp_src.* -> udpadms.* -> ads_stg.* if present.
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


# -----------------------------------------------------------------------------
# File discovery
# -----------------------------------------------------------------------------

def iter_sas_files(root: str) -> Iterable[str]:
    for base, _, files in os.walk(root):
        for name in files:
            if name.lower().endswith(".sas"):
                yield os.path.join(base, name)


def read_text(path: str) -> str:
    for enc in ("utf-8", "latin-1"):
        try:
            with open(path, "r", encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    with open(path, "r", encoding="utf-8") as f:  # pragma: no cover
        return f.read()


# -----------------------------------------------------------------------------
# Comment / string handling
# -----------------------------------------------------------------------------

RE_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.S)
RE_LINE_COMMENT = re.compile(r"^\s*\*.*?;\s*$", re.M)

def strip_comments(text: str) -> str:
    """Remove /* ... */ and full-line '* ... ;' remarks."""
    t = RE_BLOCK_COMMENT.sub(" ", text)
    t = RE_LINE_COMMENT.sub("", t)
    return t


def strip_string_literals(text: str) -> str:
    """Replace quoted strings with spaces to avoid false matches."""
    out: List[str] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch in "'\"":
            q = ch
            j = i + 1
            while j < n:
                c = text[j]
                if c == q:
                    if j + 1 < n and text[j + 1] == q:  # doubled quote inside literal
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            else:
                j = n
            out.append(" " * (j - i))
            i = j
        else:
            out.append(ch)
            i += 1
    return "".join(out)


# -----------------------------------------------------------------------------
# Macro handling (sequential)
# -----------------------------------------------------------------------------

RE_MACRO_ASSIGN = re.compile(r"%let\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([^;]*);", re.I)
RE_MACRO_REF_DOTTED = re.compile(r"&([A-Za-z0-9_]+)\.")
RE_MACRO_REF = re.compile(r"&([A-Za-z0-9_]+)")

def _sanitize_macro_value(v: str) -> str:
    """Remove quotes and simple %func(...) wrappers."""
    val = v.strip()
    # strip surrounding quotes (repeat)
    while len(val) >= 2 and val[0] == val[-1] and val[0] in "'\"":
        val = val[1:-1].strip()
    # strip one layer of simple wrappers
    SIMPLE = ("%str", "%nrstr", "%upcase", "%quote", "%nrquote")
    low = val.lower()
    for pre in SIMPLE:
        if low.startswith(pre + "(") and val.endswith(")"):
            val = val[len(pre) + 1:-1].strip()
            break
    return val.strip()

def expand_macros_once(text: str, macros: Dict[str, str]) -> Tuple[str, bool]:
    """Single pass expansion for &name. and &name using given macros."""
    changed = False

    def repl_dot(m: re.Match[str]) -> str:
        nonlocal changed
        name = m.group(1).lower()
        if name in macros:
            changed = True
            return macros[name]
        return m.group(0)

    def repl_plain(m: re.Match[str]) -> str:
        nonlocal changed
        name = m.group(1).lower()
        if name in macros:
            changed = True
            return macros[name]
        return m.group(0)

    t = RE_MACRO_REF_DOTTED.sub(repl_dot, text)
    t = RE_MACRO_REF.sub(repl_plain, t)
    # SAS treats double dots as a single delimiter dot when a macro resolves to blank.
    t = t.replace("..", ".")
    return t, changed

def expand_macros(text: str, macros: Dict[str, str], max_iter: int = 5) -> str:
    """Iteratively expand &name/. references with the provided env."""
    cur = text
    for _ in range(max_iter):
        cur, changed = expand_macros_once(cur, macros)
        if not changed:
            break
    return cur

def eval_block_macro_assignments(block_text: str, env_in: Dict[str, str]) -> Dict[str, str]:
    """Evaluate %let inside a block using env_in; return dict of new/updated macros."""
    updates: Dict[str, str] = {}
    # We must evaluate sequentially to handle chained %let.
    pos = 0
    while True:
        m = RE_MACRO_ASSIGN.search(block_text, pos)
        if not m:
            break
        name = m.group(1).lower()
        raw_value = m.group(2)
        # Expand RHS with current env (incoming + previously updated in this block)
        merged = {**env_in, **updates}
        expanded_rhs = expand_macros(raw_value, merged)
        sanitized = _sanitize_macro_value(expanded_rhs)
        updates[name] = sanitized
        pos = m.end()
    return updates


# -----------------------------------------------------------------------------
# Block splitter
# -----------------------------------------------------------------------------

@dataclass
class Block:
    kind: str      # "sql" or "data"
    start: int     # start offset of the block header
    end: int       # end offset of the block body (exclusive)
    body: str      # text inside the block (after 'proc sql' / 'data ...' line)

RE_START_SQL = re.compile(r"^\s*proc\s+sql\b.*?;", re.I | re.M | re.S)
RE_END_SQL = re.compile(r"\bquit\s*;\s*", re.I)
RE_START_DATA = re.compile(r"^\s*data\b.*?;", re.I | re.M | re.S)
RE_END_DATA = re.compile(r"\brun\s*;\s*", re.I)

def split_blocks(text: str) -> List[Block]:
    """Greedy split of proc-sql and data-step blocks."""
    blocks: List[Block] = []
    i, n = 0, len(text)
    while i < n:
        ms = RE_START_SQL.search(text, i)
        md = RE_START_DATA.search(text, i)
        candidates = [(ms, "sql"), (md, "data")]
        candidates = [(m, k) for (m, k) in candidates if m]
        if not candidates:
            break
        m, kind = min(candidates, key=lambda t: t[0].start())
        if kind == "sql":
            endm = RE_END_SQL.search(text, m.end())
            j = endm.end() if endm else n
            body = text[m.end():j]
            blocks.append(Block(kind="sql", start=m.start(), end=j, body=body))
            i = j
        else:
            endm = RE_END_DATA.search(text, m.end())
            j = endm.end() if endm else n
            body = text[m.end():j]
            blocks.append(Block(kind="data", start=m.start(), end=j, body=body))
            i = j
    return blocks


# -----------------------------------------------------------------------------
# Identifier parsing
# -----------------------------------------------------------------------------

RE_TABLE_FQ = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$")
RE_TABLE_SIMPLE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)$")

RESERVED = {
    "a","b","by","case","connect","connection","create","data","delete","do","else",
    "end","false","format","from","group","having","if","in","index","inner","into",
    "join","label","keep","left","length","libname","missing","not","null","on",
    "options","or","order","outer","proc","put","quit","rename","right","run",
    "select","set","table","then","to","true","update","values","view","where",
    "while","with","work","hadoop","regexp_replace","eof","out","input","output",
    "name","type","noprint"
}

def normalize_identifier(token: str, macros: Dict[str, str]) -> Optional[str]:
    """Normalize to 'lib.member' (lowercased) or bare member; ignore _NULL_."""
    if not token:
        return None
    cleaned = token.strip().rstrip(';,').strip()
    # Strip dataset options / trailing parentheses.
    cleaned = cleaned.split("/", 1)[0]
    cleaned = cleaned.split("(", 1)[0]
    if not cleaned:
        return None

    expanded = expand_macros(cleaned, macros).strip()
    expanded = expanded.strip("'\"").split("(", 1)[0].split("/", 1)[0].rstrip(".")
    if not expanded or "&" in expanded:
        return None

    if expanded.upper() == "_NULL_":
        return None

    m = RE_TABLE_FQ.match(expanded)
    if m:
        return f"{m.group(1).lower()}.{m.group(2).lower()}"

    m = RE_TABLE_SIMPLE.match(expanded)
    if m:
        cand = m.group(1)
        low = cand.lower()
        if low in RESERVED or low.isdigit() or len(low) == 1:
            return None
        return low
    return None


def extract_identifiers_from_clause(clause: str, macros: Dict[str, str]) -> Iterable[str]:
    """DATA/SET clause → multiple identifiers (handles 'data a b;')."""
    stripped = clause.strip()
    if not stripped or stripped.startswith('='):
        return
    tokens: List[str] = []
    cur: List[str] = []
    depth = 0
    for ch in clause:
        if ch == '(':
            depth += 1
            continue
        if ch == ')':
            depth = max(0, depth - 1)
            continue
        if depth > 0:
            continue
        if ch in ' ,\t/;':
            if cur:
                tokens.append(''.join(cur))
                cur = []
            if ch == ';':
                break
            continue
        cur.append(ch)
    if cur:
        tokens.append(''.join(cur))

    for tok in tokens:
        low = tok.lower()
        if low in RESERVED:
            continue
        norm = normalize_identifier(tok, macros)
        if norm:
            yield norm


# -----------------------------------------------------------------------------
# Statement regexes (applied *inside a block* after expansion)
# -----------------------------------------------------------------------------

RE_CREATE_TABLE = re.compile(r"\bcreate\s+table\s+([A-Za-z0-9_.&]+)", re.I)
RE_INSERT_INTO = re.compile(r"\binsert\s+into\s+([A-Za-z0-9_.&]+)", re.I)
RE_FROM = re.compile(r"\bfrom\s+([A-Za-z0-9_.&]+)", re.I)
RE_JOIN = re.compile(r"\bjoin\s+([A-Za-z0-9_.&]+)", re.I)
RE_DATA_STMT = re.compile(r"^\s*data(?!\s*=)\s+([^;]+);", re.I | re.M)
RE_SET_STMT  = re.compile(r"^\s*set(?!\s*=)\s+([^;]+);", re.I | re.M)
RE_PROC_EXECUTE = re.compile(r"\b(?:insert\s+into|update\s+|delete\s+from)\s+([A-Za-z0-9_.]+)", re.I)
RE_OUT_OPT = re.compile(r"\bout\s*=\s*([A-Za-z0-9_.&]+)", re.I)
RE_BASE_OPT = re.compile(r"\bbase\s*=\s*([A-Za-z0-9_.&]+)", re.I)
RE_DATA_OPT = re.compile(r"\bdata\s*=\s*([A-Za-z0-9_.&]+)", re.I)


# -----------------------------------------------------------------------------
# Per-file analysis (sequential blocks)
# -----------------------------------------------------------------------------

def analyse_sas_file(path: str) -> Tuple[Set[str], Set[str], Set[str]]:
    raw = read_text(path)
    no_comments = strip_comments(raw)
    blocks = split_blocks(no_comments)

    # Evolving global macro env (lowercased keys)
    env: Dict[str, str] = {}

    read_tables: Set[str] = set()
    write_tables: Set[str] = set()

    cursor = 0
    for b in blocks:
        # Macros *before* this block (outside) — evaluate sequentially in the slice
        pre_slice = no_comments[cursor:b.start]
        pre_updates = eval_block_macro_assignments(pre_slice, env)
        if pre_updates:
            env.update(pre_updates)

        # Block-local env starts from current env
        local_env = dict(env)

        # First pass: evaluate %let inside the raw block using local_env
        block_updates = eval_block_macro_assignments(b.body, local_env)
        if block_updates:
            local_env.update(block_updates)

        # Expand block text *with block-local env* and strip strings
        block_text = expand_macros(b.body, local_env)
        block_text = strip_string_literals(block_text)

        # Collect writes
        for pat in (RE_CREATE_TABLE, RE_INSERT_INTO):
            for m in pat.finditer(block_text):
                norm = normalize_identifier(m.group(1), local_env)
                if norm:
                    write_tables.add(norm)

        # Collect reads
        for pat in (RE_FROM, RE_JOIN):
            for m in pat.finditer(block_text):
                norm = normalize_identifier(m.group(1), local_env)
                if norm:
                    read_tables.add(norm)

        # DATA step targets/inputs
        for m in RE_DATA_STMT.finditer(block_text):
            for ident in extract_identifiers_from_clause(m.group(1), local_env):
                write_tables.add(ident)
        for m in RE_SET_STMT.finditer(block_text):
            # Skip 'update' pattern immediately preceding the SET (heuristic)
            start = m.start()
            prev = block_text.rfind(';', 0, start)
            snippet = block_text[prev + 1:start].lower() if prev >= 0 else block_text[:start].lower()
            if 'update' in snippet:
                continue
            for ident in extract_identifiers_from_clause(m.group(1), local_env):
                read_tables.add(ident)

        # PROC EXECUTE (INSERT/UPDATE/DELETE)
        for m in RE_PROC_EXECUTE.finditer(block_text):
            norm = normalize_identifier(m.group(1), local_env)
            if not norm:
                continue
            head = m.group(0).strip().lower()
            if head.startswith("insert") or head.startswith("update"):
                write_tables.add(norm)
            else:
                read_tables.add(norm)

        # Option-style outputs/inputs
        for m in RE_OUT_OPT.finditer(block_text):
            norm = normalize_identifier(m.group(1), local_env)
            if norm:
                write_tables.add(norm)
        for m in RE_BASE_OPT.finditer(block_text):
            norm = normalize_identifier(m.group(1), local_env)
            if norm:
                write_tables.add(norm)
        for m in RE_DATA_OPT.finditer(block_text):
            norm = normalize_identifier(m.group(1), local_env)
            if norm:
                read_tables.add(norm)

        # Macro hints present in *this block* (and inherited)
        def is_io_name(n: str, prefix: str) -> bool:
            if not n.startswith(prefix):
                return False
            tail = n[len(prefix):]
            return (tail == "") or tail.isdigit()

        for name, val in {**env, **block_updates}.items():
            up = name.lower()
            norm = normalize_identifier(val, local_env)
            if not norm:
                continue
            if up == "syslast":
                read_tables.add(norm)
            if is_io_name(up, "_input"):
                read_tables.add(norm)
            if is_io_name(up, "_output"):
                write_tables.add(norm)

        # Merge this block's %let into global env for following blocks
        if block_updates:
            env.update(block_updates)

        cursor = b.end

    # Any trailing %let after the last block (doesn't affect read/write sets)
    # trailing_updates = eval_block_macro_assignments(no_comments[cursor:], env)
    # env.update(trailing_updates)

    intermediates = read_tables & write_tables
    inputs = read_tables - write_tables
    outputs = write_tables - read_tables
    return inputs, intermediates, outputs


# -----------------------------------------------------------------------------
# Optional: summarize the "main chain" udp_src -> udpadms -> ads_stg
# -----------------------------------------------------------------------------

def main_chain(inputs: Set[str], inters: Set[str], outputs: Set[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Pick a concise Source/Intermediate/Target by library preference."""
    def pick(cands: Iterable[str], prefer: str) -> Optional[str]:
        # exact prefix first, otherwise first sorted
        cands = sorted(cands)
        for t in cands:
            if t.lower().startswith(prefer):
                return t
        return cands[0] if cands else None

    src = pick(inputs, "udp_src.")
    mid = pick(inters, "udpadms.")
    tgt = pick(outputs or inters, "ads_stg.")  # target may also be in inters if read back

    # If outputs is empty but an ads_stg table exists in inters, prefer that as target.
    if not (outputs and tgt in outputs) and any(t.startswith("ads_stg.") for t in inters):
        tgt = pick({t for t in inters if t.startswith("ads_stg.")}, "ads_stg.") or tgt

    return src, mid, tgt


# -----------------------------------------------------------------------------
# Formatting
# -----------------------------------------------------------------------------

def format_table_list(title: str, tables: Sequence[str], indent: str = "    ") -> List[str]:
    lines = [f"{title}:"]
    if tables:
        for t in tables:
            lines.append(f"{indent}- {t}")
    else:
        lines.append(f"{indent}(none)")
    return lines


def analyse_folder(root: str) -> List[str]:
    summaries: List[str] = []
    any_file = False
    for path in sorted(iter_sas_files(root)):
        any_file = True
        inputs, inters, outputs = analyse_sas_file(path)
        rel = os.path.relpath(path, root)
        summaries.append(f"=== {rel} ===")
        summaries.extend(format_table_list("Input Tables", sorted(inputs)))
        summaries.extend(format_table_list("Intermediate Tables", sorted(inters)))
        summaries.extend(format_table_list("Output Tables", sorted(outputs)))
        # Main chain summary (helpful for DI jobs)
        s, m, t = main_chain(inputs, inters, outputs)
        summaries.append("Main Chain (best effort):")
        summaries.append(f"    Source      : {s or '(unknown)'}")
        summaries.append(f"    Intermediate: {m or '(unknown)'}")
        summaries.append(f"    Target      : {t or '(unknown)'}")
        summaries.append("")
    if not any_file:
        summaries.append("No SAS files found.")
    return summaries


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Trace table lineage in SAS DI scripts (sequential, block-scoped).")
    parser.add_argument("root", help="Root folder containing .sas files")
    args = parser.parse_args(argv)

    out = analyse_folder(os.path.abspath(args.root))
    print("\n".join(out))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
