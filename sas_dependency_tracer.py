#!/usr/bin/env python3
"""Utility to derive table lineage from SAS Data Integration scripts.

Given a folder, this script walks all ``.sas`` files and attempts to extract
the input, intermediate, and output tables referenced within each script.  The
approach is intentionally heuristic but is guided by common SAS DI patterns:

* Macro variables such as ``_INPUT`` / ``_OUTPUT`` and ``SYSLAST``.
* SQL statements (``CREATE TABLE``, ``INSERT INTO``, ``FROM`` / ``JOIN``).
* DATA step moves (``data``/``set`` statements).

For every SAS file, the script prints three lists:

``Input Tables``
    Tables read by the script but not written within the same script.

``Intermediate Tables``
    Tables that are both produced and subsequently consumed in the script.

``Output Tables``
    Tables written by the script but never read afterwards.

The lists use lowercase table names and include both fully-qualified
``libref.member`` names and bare dataset names (e.g. temporary WORK tables)
when the libref cannot be determined.
"""

from __future__ import annotations

import argparse
import os
import re
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


# ---------------------------------------------------------------------------
# File discovery helpers
# ---------------------------------------------------------------------------

def iter_sas_files(root: str) -> Iterable[str]:
    """Yield absolute paths to all ``.sas`` files under ``root``."""

    for base, _, files in os.walk(root):
        for name in files:
            if name.lower().endswith(".sas"):
                yield os.path.join(base, name)


def read_text(path: str) -> str:
    """Read a text file using UTF-8 with a latin-1 fallback."""

    for encoding in ("utf-8", "latin-1"):
        try:
            with open(path, "r", encoding=encoding) as handle:
                return handle.read()
        except UnicodeDecodeError:
            continue
    # Fall back to the default behaviour (will raise the original exception).
    with open(path, "r", encoding="utf-8") as handle:  # pragma: no cover
        return handle.read()


# ---------------------------------------------------------------------------
# Comment & macro handling utilities
# ---------------------------------------------------------------------------

RE_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
RE_LINE_COMMENT = re.compile(r"^\s*\*.*?;\s*$", re.MULTILINE)
def strip_comments(text: str) -> str:
    """Remove SAS block comments (/* */) and full-line ``* comment;`` statements."""

    without_block = RE_BLOCK_COMMENT.sub(" ", text)
    without_line = RE_LINE_COMMENT.sub("", without_block)
    return without_line


def strip_string_literals(text: str) -> str:
    """Replace quoted string literals with spaces to avoid false regex matches."""

    result: List[str] = []
    i = 0
    length = len(text)

    while i < length:
        ch = text[i]
        if ch in "'\"":
            quote = ch
            end = i + 1
            while end < length:
                candidate = text[end]
                if candidate == quote:
                    # Doubled quotes represent escaped characters inside the literal.
                    if end + 1 < length and text[end + 1] == quote:
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
            else:
                # Unterminated literal: consume the remainder of the file.
                end = length
            result.append(" " * (end - i))
            i = end
            continue

        result.append(ch)
        i += 1

    return "".join(result)


RE_MACRO_ASSIGN = re.compile(r"%let\s+([A-Za-z0-9_]+)\s*=\s*([^;]+);", re.IGNORECASE)
RE_MACRO_REF = re.compile(r"&([A-Za-z0-9_]+)")
RE_WRAPPER_FUNC = re.compile(r"^%[A-Za-z0-9_]+\((.*)\)$", re.IGNORECASE)


def clean_macro_value(raw: str) -> str:
    """Strip quoting/wrappers around a macro assignment value."""

    value = raw.strip()
    # Remove surrounding quotes repeatedly.
    while len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1].strip()
    # Remove a single layer of %macro(...) style wrappers repeatedly.
    # Nested wrappers are handled iteratively.
    while True:
        match = RE_WRAPPER_FUNC.match(value)
        if not match:
            break
        candidate = match.group(1).strip()
        if not candidate:
            break
        value = candidate
    return value


def parse_macro_assignments(text: str) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """Return macro expansion map and a history of assigned values."""

    macros: Dict[str, str] = {}
    history: Dict[str, List[str]] = defaultdict(list)
    for match in RE_MACRO_ASSIGN.finditer(text):
        name = match.group(1).upper()
        value = clean_macro_value(match.group(2))
        macros[name] = value
        history[name].append(value)
    return macros, history


def expand_macros(token: str, macros: Dict[str, str], max_depth: int = 10) -> str:
    """Recursively expand ``&MACRO`` references using ``macros``."""

    result = token
    for _ in range(max_depth):
        replaced = False

        def repl(match: re.Match[str]) -> str:
            nonlocal replaced
            macro_name = match.group(1).upper()
            if macro_name in macros:
                replaced = True
                return macros[macro_name]
            return match.group(0)

        result = RE_MACRO_REF.sub(repl, result)
        # SAS double dots (e.g. &LIB..TABLE) resolve to a single literal dot.
        result = result.replace("..", ".")
        if not replaced:
            break
    return result


# ---------------------------------------------------------------------------
# Identifier parsing helpers
# ---------------------------------------------------------------------------

RE_TABLE_FQ = re.compile(r"^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)$")
RE_TABLE_SIMPLE = re.compile(r"^([A-Za-z0-9_]+)$")
RESERVED_WORDS = {
    "a",
    "b",
    "by",
    "case",
    "connect",
    "connection",
    "create",
    "data",
    "delete",
    "do",
    "else",
    "end",
    "false",
    "format",
    "from",
    "group",
    "having",
    "if",
    "in",
    "index",
    "inner",
    "into",
    "join",
    "label",
    "keep",
    "left",
    "length",
    "libname",
    "missing",
    "not",
    "null",
    "on",
    "options",
    "or",
    "order",
    "outer",
    "proc",
    "put",
    "quit",
    "rename",
    "right",
    "run",
    "select",
    "set",
    "table",
    "then",
    "to",
    "true",
    "update",
    "values",
    "view",
    "where",
    "while",
    "with",
    "work",
    "hadoop",
    "regexp_replace",
    "eof",
    "end",
    "out",
    "input",
    "output",
    "name",
    "type",
    "noprint",
}


def normalize_identifier(token: str, macros: Dict[str, str]) -> Optional[str]:
    """Convert an identifier or macro reference to a normalized table name."""

    if not token:
        return None

    cleaned = token.strip()
    # Remove trailing punctuation commonly attached to identifiers.
    cleaned = cleaned.rstrip(';,')
    # Remove dataset option sections such as ``(drop=...)`` or ``/ view=...``.
    cleaned = cleaned.split("/", 1)[0].strip()
    cleaned = cleaned.split("(", 1)[0].strip()

    if not cleaned:
        return None

    expanded = expand_macros(cleaned, macros)
    if '(' in expanded:
        inner_section = expanded.split('(', 1)[1].split(')', 1)[0]
        if '=' not in inner_section:
            return None
    expanded = expanded.strip().strip("'\"")
    expanded = expanded.split("(", 1)[0].strip()
    expanded = expanded.split("/", 1)[0].strip()
    expanded = expanded.rstrip('.')

    if not expanded or '&' in expanded:
        return None

    match_fq = RE_TABLE_FQ.match(expanded)
    if match_fq:
        libref, member = match_fq.groups()
        return f"{libref.lower()}.{member.lower()}"

    match_simple = RE_TABLE_SIMPLE.match(expanded)
    if match_simple:
        candidate = match_simple.group(1)
        candidate_lower = candidate.lower()
        if candidate.upper() == "_NULL_":
            return None
        if candidate_lower.isdigit():
            return None
        if candidate_lower in RESERVED_WORDS or len(candidate_lower) == 1:
            return None
        return candidate_lower

    return None


def extract_identifiers_from_clause(clause: str, macros: Dict[str, str]) -> Iterable[str]:
    """Yield table identifiers found in a DATA/SET clause."""

    stripped = clause.strip()
    if not stripped or stripped.startswith('='):
        return

    tokens: List[str] = []
    current: List[str] = []
    depth = 0
    i = 0
    length = len(clause)

    while i < length:
        ch = clause[i]
        if ch == '(':
            if depth == 0 and current:
                token = ''.join(current).strip()
                if token:
                    tokens.append(token)
                current = []
            depth += 1
            i += 1
            continue
        if ch == ')':
            depth = max(0, depth - 1)
            i += 1
            continue
        if depth > 0:
            i += 1
            continue
        if ch in '/;':
            if current:
                token = ''.join(current).strip()
                if token:
                    tokens.append(token)
                current = []
            break
        if ch == '=':
            if current:
                token = ''.join(current).strip()
                if token:
                    tokens.append(token)
                current = []
            break
        if ch.isspace():
            if current:
                token = ''.join(current).strip()
                if token:
                    tokens.append(token)
                current = []
            i += 1
            continue
        current.append(ch)
        i += 1

    if current:
        token = ''.join(current).strip()
        if token:
            tokens.append(token)

    for token in tokens:
        lower = token.lower()
        if lower in RESERVED_WORDS:
            continue
        normalized = normalize_identifier(token, macros)
        if normalized:
            yield normalized


# ---------------------------------------------------------------------------
# Statement regexes
# ---------------------------------------------------------------------------

RE_CREATE_TABLE = re.compile(r"\bcreate\s+table\s+([&A-Za-z0-9_.]+)", re.IGNORECASE)
RE_INSERT_INTO = re.compile(r"\binsert\s+into\s+([&A-Za-z0-9_.]+)", re.IGNORECASE)
RE_FROM = re.compile(r"\bfrom\s+([&A-Za-z0-9_.]+)", re.IGNORECASE)
RE_JOIN = re.compile(r"\bjoin\s+([&A-Za-z0-9_.]+)", re.IGNORECASE)
RE_DATA_STMT = re.compile(r"^\s*data(?!\s*=)\s+([^;]+);", re.IGNORECASE | re.MULTILINE)
RE_SET_STMT = re.compile(r"^\s*set(?!\s*=)\s+([^;]+);", re.IGNORECASE | re.MULTILINE)
RE_PROC_EXECUTE = re.compile(r"\b(?:insert\s+into|update\s+|delete\s+from)\s+([A-Za-z0-9_.]+)", re.IGNORECASE)
RE_OUT_OPTION = re.compile(r"\bout\s*=\s*([&A-Za-z0-9_.]+)", re.IGNORECASE)
RE_DATA_OPTION = re.compile(r"\bdata\s*=\s*([&A-Za-z0-9_.]+)", re.IGNORECASE)
RE_BASE_OPTION = re.compile(r"\bbase\s*=\s*([&A-Za-z0-9_.]+)", re.IGNORECASE)


def collect_table_references(text: str, macros: Dict[str, str]) -> Tuple[Set[str], Set[str]]:
    """Return ``(inputs, outputs)`` inferred from the SAS source text."""

    inputs: Set[str] = set()
    outputs: Set[str] = set()

    for pattern, target_set in (
        (RE_CREATE_TABLE, outputs),
        (RE_INSERT_INTO, outputs),
    ):
        for match in pattern.finditer(text):
            normalized = normalize_identifier(match.group(1), macros)
            if normalized:
                target_set.add(normalized)

    for pattern in (RE_FROM, RE_JOIN):
        for match in pattern.finditer(text):
            normalized = normalize_identifier(match.group(1), macros)
            if normalized:
                inputs.add(normalized)

    # DATA step: capture targets and sources via DATA/SET statements.
    for match in RE_DATA_STMT.finditer(text):
        clause = match.group(1)
        for identifier in extract_identifiers_from_clause(clause, macros):
            outputs.add(identifier)

    for match in RE_SET_STMT.finditer(text):
        start = match.start()
        prev_semicolon = text.rfind(';', 0, start)
        snippet = text[prev_semicolon + 1:start].lower() if prev_semicolon >= 0 else text[:start].lower()
        if 'update' in snippet:
            continue
        clause = match.group(1)
        for identifier in extract_identifiers_from_clause(clause, macros):
            inputs.add(identifier)

    # SQL execute (connect to ... execute (...)).
    for match in RE_PROC_EXECUTE.finditer(text):
        normalized = normalize_identifier(match.group(1), macros)
        if normalized:
            # INSERT INTO and UPDATE write, DELETE reads.
            snippet = match.group(0).lower()
            if snippet.startswith("insert") or snippet.startswith("update"):
                outputs.add(normalized)
            else:
                inputs.add(normalized)

    for match in RE_OUT_OPTION.finditer(text):
        normalized = normalize_identifier(match.group(1), macros)
        if normalized:
            outputs.add(normalized)

    for match in RE_BASE_OPTION.finditer(text):
        normalized = normalize_identifier(match.group(1), macros)
        if normalized:
            outputs.add(normalized)

    for match in RE_DATA_OPTION.finditer(text):
        normalized = normalize_identifier(match.group(1), macros)
        if normalized:
            inputs.add(normalized)

    return inputs, outputs


def enrich_with_macro_io(
    macro_history: Dict[str, List[str]],
    macros: Dict[str, str],
    inputs: Set[str],
    outputs: Set[str],
) -> None:
    """Augment input/output sets with _INPUT/_OUTPUT macro metadata."""

    def is_data_macro(name: str, prefix: str) -> bool:
        if not name.startswith(prefix):
            return False
        suffix = name[len(prefix):]
        return suffix == "" or suffix.isdigit()

    for name, values in macro_history.items():
        upper = name.upper()
        for value in values:
            normalized = normalize_identifier(value, macros)
            if not normalized:
                continue
            if upper == "SYSLAST":
                inputs.add(normalized)
            if is_data_macro(upper, "_INPUT"):
                inputs.add(normalized)
            if is_data_macro(upper, "_OUTPUT"):
                outputs.add(normalized)


# ---------------------------------------------------------------------------
# High-level analysis
# ---------------------------------------------------------------------------

def analyse_sas_file(path: str) -> Tuple[Set[str], Set[str], Set[str]]:
    """Return ``(inputs, intermediates, outputs)`` for the SAS script."""

    raw_text = read_text(path)
    text = strip_comments(raw_text)
    macros, macro_history = parse_macro_assignments(text)
    scan_text = strip_string_literals(text)
    inputs, outputs = collect_table_references(scan_text, macros)
    enrich_with_macro_io(macro_history, macros, inputs, outputs)

    intermediates = inputs & outputs
    pure_inputs = inputs - outputs
    pure_outputs = outputs - inputs

    return pure_inputs, intermediates, pure_outputs


def format_table_list(title: str, tables: Sequence[str], indent: str = "    ") -> List[str]:
    """Format a titled block for console output."""

    lines = [f"{title}:"]
    if tables:
        for table in tables:
            lines.append(f"{indent}- {table}")
    else:
        lines.append(f"{indent}(none)")
    return lines


def analyse_folder(root: str) -> List[str]:
    """Produce a multi-line textual summary for all SAS files under ``root``."""

    summaries: List[str] = []
    for path in sorted(iter_sas_files(root)):
        inputs, intermediates, outputs = analyse_sas_file(path)
        relative_path = os.path.relpath(path, root)
        summaries.append(f"=== {relative_path} ===")
        summaries.extend(
            format_table_list("Input Tables", sorted(inputs))
        )
        summaries.extend(
            format_table_list("Intermediate Tables", sorted(intermediates))
        )
        summaries.extend(
            format_table_list("Output Tables", sorted(outputs))
        )
        summaries.append("")
    if not summaries:
        summaries.append("No SAS files found.")
    return summaries


# ---------------------------------------------------------------------------
# Command-line interface
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Trace table lineage in SAS scripts.")
    parser.add_argument("root", help="Root folder containing SAS scripts")
    args = parser.parse_args(argv)

    summaries = analyse_folder(os.path.abspath(args.root))
    print("\n".join(summaries))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
