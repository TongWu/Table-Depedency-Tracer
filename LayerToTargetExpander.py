#!/usr/bin/env python3
"""Expand lineage CSV by treating intermediate layers as targets."""

from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path
from typing import Dict, List, Sequence, Set, Tuple


def _extract_layer_index(column: str) -> int:
    """Return the numeric index from a ``Layer N`` column name."""

    try:
        return int(column.split()[1])
    except (IndexError, ValueError):
        raise ValueError(f"Invalid layer column name: {column}") from None


def _normalise_value(value: str | None) -> str:
    """Trim surrounding whitespace and fallback to empty string."""

    return value.strip() if value else ""


def _prepare_template(fieldnames: Sequence[str]) -> Dict[str, str]:
    """Return a dict initialised with empty strings for all known columns."""

    return {name: "" for name in fieldnames}


def expand_lineage_rows(
    rows: Sequence[Dict[str, str]],
    fieldnames: Sequence[str],
    *,
    target_column: str = "Target Table",
    source_column: str = "Source Table",
) -> List[Dict[str, str]]:
    """
    Expand lineage rows by promoting layer tables into the target column.

    Each layer table inherits the downstream dependency chain from the source
    row.  Newly created rows are appended to the original list while ensuring
    that tables already listed as targets are not duplicated.
    """

    if target_column not in fieldnames:
        raise KeyError(f"Input is missing required column: '{target_column}'")
    if source_column not in fieldnames:
        raise KeyError(f"Input is missing required column: '{source_column}'")

    layer_columns: List[str] = [
        name
        for name in fieldnames
        if name.startswith("Layer ")
    ]
    layer_columns.sort(key=_extract_layer_index)

    if not layer_columns:
        logging.info("No layer columns found. Nothing to expand.")
        return [dict(row) for row in rows]

    logging.debug("Detected layer columns: %s", ", ".join(layer_columns))

    expanded_rows: List[Dict[str, str]] = [dict(row) for row in rows]

    seen_targets: Set[str] = {
        _normalise_value(row.get(target_column)).lower()
        for row in rows
        if _normalise_value(row.get(target_column))
    }

    for row in rows:
        target_value = _normalise_value(row.get(target_column))
        if not target_value:
            logging.debug("Skipping row without target: %s", row)
            continue

        for idx, layer_column in enumerate(layer_columns):
            layer_value = _normalise_value(row.get(layer_column))
            if not layer_value:
                continue

            layer_key = layer_value.lower()
            if layer_key in seen_targets:
                logging.debug(
                    "Layer '%s' already present as target. Skipping expansion.",
                    layer_value,
                )
                continue

            downstream_chain: List[str] = []
            for next_col in layer_columns[idx + 1 :]:
                next_value = _normalise_value(row.get(next_col))
                if not next_value:
                    break
                downstream_chain.append(next_value)

            source_value = _normalise_value(row.get(source_column))

            new_row = _prepare_template(fieldnames)
            new_row[target_column] = layer_value
            for new_idx, chain_value in enumerate(downstream_chain, start=1):
                col_name = f"Layer {new_idx}"
                if col_name not in new_row:
                    logging.debug(
                        "Encountered dependency depth beyond known columns: %s",
                        col_name,
                    )
                    continue
                new_row[col_name] = chain_value
            new_row[source_column] = source_value

            logging.debug(
                "Promoted layer '%s' to target with %d downstream layer(s).",
                layer_value,
                len(downstream_chain),
            )

            expanded_rows.append(new_row)
            seen_targets.add(layer_key)

    return expanded_rows


def write_rows(path: Path, rows: Sequence[Dict[str, str]], fieldnames: Sequence[str]) -> None:
    """Write lineage rows back to disk."""

    if not fieldnames:
        raise ValueError("No column headers available for CSV output.")

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def load_rows(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    """Load rows from a lineage CSV file."""

    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError("Input CSV is missing a header row.")
        fieldnames = list(reader.fieldnames)
        rows = [dict(row) for row in reader]
    return fieldnames, rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Expand lineage output so that each intermediate layer is treated as a "
            "target table when it is not already present."
        )
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the lineage CSV produced by the dependency tracer.",
    )
    parser.add_argument(
        "--output",
        required=False,
        help="Destination path for the expanded lineage CSV.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging verbosity (DEBUG, INFO, WARNING, ERROR).",
    )

    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(levelname)s %(message)s")

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path.with_name(
        f"{input_path.stem}_expanded{input_path.suffix or '.csv'}"
    )

    fieldnames, rows = load_rows(input_path)
    logging.info("Loaded %d row(s) from %s", len(rows), input_path)

    expanded_rows = expand_lineage_rows(rows, fieldnames)
    logging.info("Writing %d row(s) to %s", len(expanded_rows), output_path)
    write_rows(output_path, expanded_rows, fieldnames)


if __name__ == "__main__":
    main()
