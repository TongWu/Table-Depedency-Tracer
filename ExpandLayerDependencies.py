#!/usr/bin/env python3
"""Expand table dependency CSV by promoting layer tables to targets.

The :mod:`TableDependencyTracer` script emits a CSV where each row tracks a
lineage path from a target table down to one of its source tables::

    Target Table, Layer 1, Layer 2, ..., Source Table

This helper promotes every intermediate ``Layer N`` table to the ``Target
Table`` column so that the dependency chain for each layer becomes explicit in
its own row.  Existing target rows are preserved.  For every non-empty layer in
a row that does *not* already exist as a target table elsewhere in the input,
a new row is appended with that layer as the target and the remaining tail of
the dependency chain copied over.  ``Source Table`` entries are not promoted.

Example
-------

Given an input row::

    Target Table = ads.tgt
    Layer 1      = ads.mid
    Layer 2      = ads.stg
    Source Table = ads.src

The script will emit the original row plus::

    Target Table = ads.mid
    Layer 1      = ads.stg
    Source Table = ads.src

    Target Table = ads.stg
    Source Table = ads.src

Usage
-----

``python ExpandLayerDependencies.py --input lineage.csv --output expanded.csv``

The script preserves the original column order in the output CSV.  If the input
contains no ``Layer`` columns or is empty, the output is identical to the
input.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
from typing import Dict, Iterable, List, Sequence, Tuple

LOGGER = logging.getLogger(__name__)

TARGET_COLUMN = "Target Table"
SOURCE_COLUMN = "Source Table"
_LAYER_PATTERN = re.compile(r"^Layer\s+(\d+)$")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Return parsed CLI arguments."""

    parser = argparse.ArgumentParser(
        description="Promote intermediate Layer columns to target table rows",
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to the TableDependencyTracer CSV",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Destination CSV that will include exploded layer rows",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO)",
    )
    return parser.parse_args(argv)


def _read_csv(path: str) -> tuple[List[Dict[str, str]], List[str]]:
    """Read ``path`` and return (rows, fieldnames)."""

    if not os.path.exists(path):
        raise FileNotFoundError(f"Input CSV not found: {path}")

    with open(path, "r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames[:] if reader.fieldnames else []
        rows: List[Dict[str, str]] = [dict(row) for row in reader]
    return rows, fieldnames


def _write_csv(path: str, rows: Iterable[Dict[str, str]], fieldnames: Sequence[str]) -> None:
    """Write ``rows`` to ``path`` preserving ``fieldnames`` order."""

    os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _normalise_value(value: str | None) -> str:
    """Return a stripped string (``""`` for ``None``)."""

    if value is None:
        return ""
    return value.strip()


def _extract_layer_columns(fieldnames: Sequence[str]) -> List[str]:
    """Return layer columns sorted numerically (Layer 1, Layer 2, ...)."""

    layers = []
    for name in fieldnames:
        match = _LAYER_PATTERN.match(name)
        if match:
            layers.append((int(match.group(1)), name))
    layers.sort(key=lambda item: item[0])
    return [name for _, name in layers]


def expand_rows(rows: Sequence[Dict[str, str]], layer_columns: Sequence[str]) -> List[Dict[str, str]]:
    """Return rows plus new rows for each intermediate layer table, grouped by target table."""

    if not rows:
        LOGGER.info("Input CSV is empty. Nothing to expand.")
        return []

    if TARGET_COLUMN not in rows[0]:
        raise KeyError(f"Missing required column: {TARGET_COLUMN}")
    if SOURCE_COLUMN not in rows[0]:
        raise KeyError(f"Missing required column: {SOURCE_COLUMN}")

    base_targets = {
        _normalise_value(row.get(TARGET_COLUMN))
        for row in rows
        if _normalise_value(row.get(TARGET_COLUMN))
    }

    grouped_rows = {}

    for original in rows:
        target_value = _normalise_value(original.get(TARGET_COLUMN))
        if not target_value:
            continue
        grouped_rows.setdefault(target_value, []).append(dict(original))

        for idx, layer in enumerate(layer_columns):
            layer_value = _normalise_value(original.get(layer))
            if not layer_value:
                continue
            if layer_value in base_targets:
                LOGGER.debug("Skip layer '%s' because it already exists as a target", layer_value)
                continue

            new_row: Dict[str, str] = {TARGET_COLUMN: layer_value}
            shift_position = 1
            for tail_idx in range(idx + 1, len(layer_columns)):
                tail_value = _normalise_value(original.get(layer_columns[tail_idx]))
                if not tail_value:
                    continue
                new_row[f"Layer {shift_position}"] = tail_value
                shift_position += 1

            source_value = _normalise_value(original.get(SOURCE_COLUMN))
            if source_value:
                new_row[SOURCE_COLUMN] = source_value

            grouped_rows.setdefault(layer_value, []).append(new_row)
            LOGGER.debug("Added exploded row for layer '%s'", layer_value)

    ordered_targets = []
    for original in rows:
        target_value = _normalise_value(original.get(TARGET_COLUMN))
        if target_value and target_value not in ordered_targets:
            ordered_targets.append(target_value)
    for target in grouped_rows:
        if target not in ordered_targets:
            ordered_targets.append(target)

    expanded = []
    for target in ordered_targets:
        expanded.extend(grouped_rows[target])

    return expanded


def _deduplicate_rows(
    rows: Sequence[Dict[str, str]], fieldnames: Sequence[str]
) -> List[Dict[str, str]]:
    """Return ``rows`` with duplicate entries removed while preserving order."""

    seen_signatures = set()
    deduplicated: List[Dict[str, str]] = []

    for row in rows:
        signature: Tuple[str, ...] = tuple(row.get(field, "") or "" for field in fieldnames)
        if signature in seen_signatures:
            LOGGER.debug("Skipping duplicate row for signature: %s", signature)
            continue
        seen_signatures.add(signature)
        deduplicated.append(row)

    return deduplicated


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s: %(message)s")

    rows, original_fieldnames = _read_csv(args.input)
    if not rows:
        LOGGER.warning("Input CSV had no data rows. Writing empty output.")
        # still emit header so use discovered fieldnames
        fieldnames = original_fieldnames or [TARGET_COLUMN, SOURCE_COLUMN]
        _write_csv(args.output, [], fieldnames)
        return

    layer_columns = _extract_layer_columns(original_fieldnames)
    if not layer_columns:
        LOGGER.info("Input has no Layer columns. Copying rows as-is.")
        _write_csv(args.output, rows, original_fieldnames)
        return

    expanded_rows = expand_rows(rows, layer_columns)
    fieldnames = original_fieldnames or [TARGET_COLUMN, *layer_columns, SOURCE_COLUMN]
    deduplicated_rows = _deduplicate_rows(expanded_rows, fieldnames)
    if len(deduplicated_rows) != len(expanded_rows):
        LOGGER.info(
            "Removed %d duplicate rows after expansion",
            len(expanded_rows) - len(deduplicated_rows),
        )
    expanded_rows = deduplicated_rows
    LOGGER.info(
        "Expanded %d original rows into %d rows (added %d exploded rows)",
        len(rows),
        len(expanded_rows),
        len(expanded_rows) - len(rows),
    )
    _write_csv(args.output, expanded_rows, fieldnames)


if __name__ == "__main__":
    main()
