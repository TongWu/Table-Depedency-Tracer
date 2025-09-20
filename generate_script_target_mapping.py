#!/usr/bin/env python3
"""Generate a CSV mapping between scripts and their target tables/views.

This utility scans Python and SQL files under a provided root directory and
emits a CSV with two columns: ``script name`` and ``target table``. Python
files are analysed with the same detection logic that powers
``TableDependencyTracer`` (AST analysis, header parsing, and ``insertInto``
heuristics). SQL files contribute targets only when they define ``CREATE
VIEW`` statements.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import warnings
from typing import List, Optional, Sequence, Set, Tuple

from TableDependencyTracer import (
    extract_output_tables_from_python,
    list_code_files,
    parse_insertinto_targets,
    parse_output_tables_from_header,
    read_text,
)

CREATE_VIEW_PATTERN = re.compile(
    r"\bcreate\s+(?:or\s+replace\s+)?view\s+([a-z0-9_]+(?:\.[a-z0-9_]+)?)\b"
)


def _extract_python_targets(text: str) -> Set[str]:
    """Return fully-qualified target tables referenced in a Python script."""

    targets: Set[str] = set()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", SyntaxWarning)
        targets |= extract_output_tables_from_python(text)
    targets |= parse_output_tables_from_header(text)
    targets |= parse_insertinto_targets(text)
    return targets


def _extract_sql_targets(text: str) -> Set[str]:
    """Return views created inside the SQL script."""

    lowered = text.lower()
    return {match.group(1) for match in CREATE_VIEW_PATTERN.finditer(lowered)}


def _collect_targets_for_file(path: str, root: str) -> List[Tuple[str, str]]:
    """Collect ``(script, target)`` pairs for the provided file."""

    text = read_text(path)
    if text is None:
        logging.warning("Skipping unreadable file: %s", path)
        return []

    ext = os.path.splitext(path)[1].lower()
    if ext == ".py":
        targets = sorted(_extract_python_targets(text))
    elif ext == ".sql":
        targets = sorted(_extract_sql_targets(text))
    else:
        return []

    if not targets:
        logging.debug("No targets detected in %s", path)
        return []

    rel_path = os.path.relpath(path, root)
    return [(rel_path, target) for target in targets]


def build_script_target_mapping(root: str) -> List[Tuple[str, str]]:
    """Scan the root directory and build the script/target mapping."""

    root_path = os.path.abspath(root)
    files = list_code_files(root_path)
    logging.info("Scanning %d candidate files under %s", len(files), root_path)

    pairs: List[Tuple[str, str]] = []
    for file_path in files:
        pairs.extend(_collect_targets_for_file(file_path, root_path))

    # Sort for deterministic output (by script name then target table)
    pairs.sort(key=lambda item: (item[0], item[1]))
    logging.info("Detected %d script→target mappings", len(pairs))
    return pairs


def write_mapping_csv(pairs: Sequence[Tuple[str, str]], out_path: str) -> None:
    """Write the mapping pairs to a CSV file."""

    if not pairs:
        logging.warning("No mappings to write. CSV will only contain the header.")

    directory = os.path.dirname(os.path.abspath(out_path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["script name", "target table"])
        for script_name, target_table in pairs:
            writer.writerow([script_name, target_table])

    logging.info("Wrote mapping CSV with %d rows to %s", len(pairs), out_path)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a script-to-target-table mapping CSV"
    )
    parser.add_argument("--root", required=True, help="Root folder to scan")
    parser.add_argument(
        "--out",
        default="script_target_mapping.csv",
        help="Destination CSV path (default: script_target_mapping.csv)",
    )
    parser.add_argument(
        "--log",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    mapping = build_script_target_mapping(args.root)
    write_mapping_csv(mapping, args.out)


if __name__ == "__main__":
    main()
