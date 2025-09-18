#!/usr/bin/env python3
"""Simple SAS lineage extractor.

This utility scans ``.sas`` files under a provided directory and attempts to
infer the tables that are **read**, **written**, and consequently the ones that
behave like *intermediate* assets in every script.

The rules implemented here follow the quick heuristics described in the user
instructions:

* Macro variables such as ``_INPUT``/``_OUTPUT`` (and their numbered variants)
  hint at upstream/downstream tables.
* Standard ``proc sql`` DDL/DML statements (``create table``, ``insert into``)
  and data-step ``data ...; set ...;`` blocks expose table usage.
* A few lightweight macro expansions are supported so that references such as
  ``&SYSLAST`` or ``&lib..table`` resolve to the underlying table name.

The script is *best effort* – it does not try to evaluate the full SAS macro
language – but it already covers common SAS DI generated jobs.
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


# ---------------------------------------------------------------------------
# Regex helpers
# ---------------------------------------------------------------------------

RE_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", flags=re.S)
RE_LINE_COMMENT = re.compile(r"^\s*\*.*?;\s*$", flags=re.M)
RE_MACRO_ASSIGN = re.compile(r"%let\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([^;]*);", re.IGNORECASE)

# Patterns that discover write/read intents. We only capture the dataset/macro
# token; aliases or options are handled separately.
TOKEN = r"(&[A-Za-z0-9_]+|[A-Za-z0-9_]+\.[A-Za-z0-9_]+)"
RE_CREATE_TABLE = re.compile(rf"\bcreate\s+table\s+{TOKEN}", re.IGNORECASE)
RE_DATA_STEP = re.compile(rf"\bdata\s+{TOKEN}", re.IGNORECASE)
RE_INSERT_INTO = re.compile(rf"\binsert\s+into\s+{TOKEN}", re.IGNORECASE)
RE_TRUNCATE_TABLE = re.compile(rf"\btruncate\s+table\s+{TOKEN}", re.IGNORECASE)
RE_UPDATE_TABLE = re.compile(rf"\bupdate\s+{TOKEN}\b", re.IGNORECASE)

RE_FROM = re.compile(rf"\bfrom\s+{TOKEN}", re.IGNORECASE)
RE_JOIN = re.compile(rf"\bjoin\s+{TOKEN}", re.IGNORECASE)
RE_SET = re.compile(rf"\bset\s+{TOKEN}", re.IGNORECASE)
RE_MERGE = re.compile(rf"\bmerge\s+{TOKEN}", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def strip_comments(text: str) -> str:
    """Remove block (/* ... */) and one-line (* ...;) comments."""

    without_block = RE_BLOCK_COMMENT.sub(" ", text)
    # Remove standalone "* comment;" lines – SAS uses these for remarks.
    return RE_LINE_COMMENT.sub("", without_block)


def sanitize_macro_value(value: str) -> str:
    """Normalize a macro assignment RHS to make dataset detection easier."""

    v = value.strip()
    # Remove surrounding quotes.
    if (v.startswith("'") and v.endswith("'")) or (v.startswith('"') and v.endswith('"')):
        v = v[1:-1]

    # Remove simple %str()/ %nrstr()/ %upcase() wrappers.
    SIMPLE_WRAPPERS = ("%str", "%nrstr", "%upcase", "%quote", "%nrquote")
    for prefix in SIMPLE_WRAPPERS:
        lower = v.lower()
        if lower.startswith(prefix + "(") and v.endswith(")"):
            v = v[len(prefix) + 1 : -1]
            v = v.strip()
            break

    return v.strip()


def expand_macros_once(text: str, macros: Dict[str, str]) -> Tuple[str, bool]:
    """Expand ``&macro`` references in ``text`` once.

    Returns the expanded text and a flag indicating whether any replacement
    happened. The macro names are treated case-insensitively.
    """

    changed = False

    def repl_with_dot(match: re.Match[str]) -> str:
        nonlocal changed
        name = match.group(1).lower()
        value = macros.get(name)
        if value is None:
            return match.group(0)
        changed = True
        return value

    def repl_plain(match: re.Match[str]) -> str:
        nonlocal changed
        name = match.group(1).lower()
        value = macros.get(name)
        if value is None:
            return match.group(0)
        changed = True
        return value

    # Handle the delimiter form (&macro.) first so that the trailing dot is
    # discarded (SAS treats it purely as a delimiter).
    text_after_dot = re.sub(r"&([A-Za-z0-9_]+)\.", repl_with_dot, text)
    text_after_plain = re.sub(r"&([A-Za-z0-9_]+)", repl_plain, text_after_dot)
    return text_after_plain, changed


def expand_macros(text: str, macros: Dict[str, str], iterations: int = 5) -> str:
    """Expand simple ``&macro`` references using ``macros`` up to ``iterations``."""

    current = text
    for _ in range(iterations):
        current, changed = expand_macros_once(current, macros)
        if not changed:
            break
    return current


def normalize_dataset(token: str) -> Optional[str]:
    """Return a lower-cased ``libref.table`` if ``token`` looks like one."""

    cleaned = token.strip().strip("'\"")
    # Remove trailing statement punctuation/parentheses/options.
    cleaned = cleaned.rstrip(";,)")
    cleaned = cleaned.lstrip("(")
    cleaned = cleaned.strip()

    match = re.match(r"^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", cleaned)
    if match:
        return f"{match.group(1).lower()}.{match.group(2).lower()}"
    return None


def resolve_token(token: str, macros: Dict[str, str]) -> Optional[str]:
    """Resolve a dataset/macro token into ``lib.table`` when possible."""

    stripped = token.strip()
    if stripped.lower() == "_null_":
        return None

    if stripped.startswith("&"):
        name = stripped[1:].rstrip(".").lower()
        value = macros.get(name)
        if value is None:
            return None
        return normalize_dataset(value)

    return normalize_dataset(stripped)


def find_tables(pattern: re.Pattern[str], text: str, macros: Dict[str, str]) -> Set[str]:
    results: Set[str] = set()
    for match in pattern.finditer(text):
        token = match.group(1)
        dataset = resolve_token(token, macros)
        if dataset:
            results.add(dataset)
    return results


@dataclass
class SASLineage:
    path: Path
    inputs: Set[str]
    intermediates: Set[str]
    outputs: Set[str]


def extract_macros(text: str) -> Dict[str, str]:
    """Return macro assignments found in ``text`` (case-insensitive)."""

    macros: Dict[str, str] = {}
    for match in RE_MACRO_ASSIGN.finditer(text):
        name = match.group(1).lower()
        raw_value = match.group(2)
        expanded = expand_macros(raw_value, macros)
        sanitized = sanitize_macro_value(expanded)
        macros[name] = sanitized
    return macros


def analyze_file(path: Path) -> SASLineage:
    raw_text = path.read_text(encoding="utf-8", errors="ignore")
    without_comments = strip_comments(raw_text)
    macros = extract_macros(without_comments)

    # Expand macros inside the code so we capture constructs such as
    # &lib..table or &SYSLAST.
    expanded_text = expand_macros(without_comments, macros)

    write_tables: Set[str] = set()
    read_tables: Set[str] = set()

    # Macro hints ( _INPUT / _OUTPUT )
    for name, value in macros.items():
        dataset = normalize_dataset(value)
        if not dataset:
            continue
        if name.startswith("_input"):
            read_tables.add(dataset)
        elif name.startswith("_output"):
            write_tables.add(dataset)

    # Explicit SQL/data step reads & writes.
    for pattern in (RE_CREATE_TABLE, RE_DATA_STEP, RE_INSERT_INTO, RE_TRUNCATE_TABLE, RE_UPDATE_TABLE):
        write_tables.update(find_tables(pattern, expanded_text, macros))

    for pattern in (RE_FROM, RE_JOIN, RE_SET, RE_MERGE):
        read_tables.update(find_tables(pattern, expanded_text, macros))

    intermediates = read_tables & write_tables
    inputs = read_tables - write_tables
    outputs = write_tables - read_tables

    return SASLineage(path=path, inputs=inputs, intermediates=intermediates, outputs=outputs)


def iter_sas_files(root: Path) -> Iterable[Path]:
    for directory, _, files in os.walk(root):
        for filename in files:
            if filename.lower().endswith(".sas"):
                yield Path(directory) / filename


def format_table_list(tables: Sequence[str]) -> str:
    if not tables:
        return "(none)"
    return ", ".join(tables)


def main() -> None:
    parser = argparse.ArgumentParser(description="Trace SAS table lineage for every .sas script under a folder.")
    parser.add_argument("folder", type=Path, help="Root folder containing SAS scripts")
    args = parser.parse_args()

    if not args.folder.exists():
        raise SystemExit(f"Folder not found: {args.folder}")

    sas_files = sorted(iter_sas_files(args.folder))
    if not sas_files:
        print(f"No SAS files found under {args.folder}")
        return

    for path in sas_files:
        lineage = analyze_file(path)
        rel_path = path.relative_to(args.folder)
        print(f"File: {rel_path}")
        print(f"  Inputs       : {format_table_list(sorted(lineage.inputs))}")
        print(f"  Intermediates: {format_table_list(sorted(lineage.intermediates))}")
        print(f"  Outputs      : {format_table_list(sorted(lineage.outputs))}")
        print()


if __name__ == "__main__":
    main()
