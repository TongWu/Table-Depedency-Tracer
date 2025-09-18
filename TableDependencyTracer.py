#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dependency tracer for Spark/SQL pipelines with recursive lineage expansion.

- Strict case-sensitive match on table names.
- Crawl .py and .sql files under a given root (including subfolders).
- Determine writers:
  * Spark ETL: parse header "Output table(s):" to find fully-qualified output tables.
  * VIEW SQL: parse "CREATE VIEW <db>.<view>" or "CREATE VIEW <view>" to map view -> file.
- Determine upstreams:
  * Spark ETL: regex on spark.table('db.tbl') → upstream table names (use table name only).
  * VIEW SQL: regex on FROM/JOIN db.tbl → upstream tables (use table name only).
- Build lineage paths: For each target, enumerate all paths to sources.
  * "Layer i" columns contain tables that still have upstreams.
  * "Source Table" is a leaf table that has no upstreams.
- Emit a flat table with columns: ["Target Table", "Layer 1", ..., "Source Table"].

Author: Wu Tong
"""

import argparse
import logging
import os
import re
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple, Optional

# ------------------------------
# Regexes (case-insensitive for SQL verbs, case-sensitive for identifiers)
# ------------------------------

# --- replace the previous parse_output_tables_from_header with this strict version ---

import re
from typing import Set, Tuple

# FQTN patterns (db.tbl) in code
RE_CREATE_VIEW_L = re.compile(r"\bcreate\s+view\s+([a-z0-9_]+(?:\.[a-z0-9_]+)?)\b")
RE_SPARK_TABLE_L = re.compile(r"spark\.table\(\s*['\"]([a-z0-9_]+)\.([a-z0-9_]+)['\"]\s*\)")
RE_FROM_JOIN_L = re.compile(r"\b(?:from|join)\s+([a-z0-9_]+)\.([a-z0-9_]+)\b")
# Optional: keep insertInto fallback as a real writer signal
RE_INSERT_INTO_L = re.compile(r"\.insertinto\(\s*['\"]([a-z0-9_]+)\.([a-z0-9_]+)['\"]\s*,")


# Safer per-line alternative if big block fails: lines starting with '#   schema.table'
RE_OUTPUT_LINE = re.compile(r"^#\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s*$", re.MULTILINE)

# FROM/JOIN sources in SQL (db.tbl). Keep case-sensitive match for identifiers by post-check.
RE_FROM_JOIN = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\b",
    re.IGNORECASE
)

# spark.table('db.tbl') in Py files (keep quotes single or double)
RE_SPARK_TABLE = re.compile(
    r"spark\.table\(\s*['\"]([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)['\"]\s*\)"
)

# Also allow DataFrameWriter insertInto('db.tbl') patterns as a fallback (not required, but helpful)
RE_INSERT_INTO = re.compile(
    r"\.insertInto\(\s*['\"]([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)['\"]\s*,",
)

# Word boundary strict search for file prefilter (table name only)
def word_boundary_pattern(name: str) -> re.Pattern:
    """Build a strict word-boundary regex for a *lower-cased* table name."""
    name_l = re.escape(name.lower())
    return re.compile(rf"\b{name_l}\b")

def normalize_table_only(name: str) -> str:
    """Return table-only part in LOWER CASE (drop db if present)."""
    base = name.split('.', 1)[1] if '.' in name else name
    return base.lower()



@dataclass
class WriterInfo:
    """Represents a script/view that writes a specific output table."""
    file_path: str
    kind: str  # 'spark' or 'view'

def to_fqtn(db: str, tbl: str) -> str:
    """Build lower-cased fully-qualified table name 'db.tbl'."""
    return f"{db.lower()}.{tbl.lower()}"

def normalize_fqtn(name: str) -> Optional[str]:
    """Return lower-cased fully-qualified 'db.tbl', or None if input not qualified."""
    n = name.strip().lower()
    return n if "." in n else None

def word_boundary_pattern_fqtn(fqtn: str) -> re.Pattern:
    """Strict word-boundary regex for a lower-cased FQTN, with escaped dot."""
    pat = re.escape(fqtn)  # escapes the dot
    return re.compile(rf"\b{pat}\b")


# ------------------------------
# File scanning & indexing
# ------------------------------

def list_code_files(root: str) -> List[str]:
    """List .py and .sql files under root recursively."""
    targets = []
    for base, _, files in os.walk(root):
        for f in files:
            ext = os.path.splitext(f)[1].lower()
            if ext in ('.py', '.sql'):
                targets.append(os.path.join(base, f))
    return targets


def read_text(path: str) -> Optional[str]:
    """Read file as UTF-8 (fallback to latin-1)."""
    try:
        with open(path, 'r', encoding='utf-8') as fh:
            return fh.read()
    except UnicodeDecodeError:
        try:
            with open(path, 'r', encoding='latin-1') as fh:
                return fh.read()
        except Exception as e:
            logging.warning("Failed to read %s: %s", path, e)
            return None
    except Exception as e:
        logging.warning("Failed to read %s: %s", path, e)
        return None


# BEFORE
# RE_FQTN_INLINE_L = re.compile(r"\b([a-z0-9_]+)\.([a-z0-9_]+)\b")

# AFTER — demand a strict right boundary: space/comma/semicolon/)/#/dash or EOL
RE_FQTN_INLINE_L = re.compile(
    r"\b([a-z0-9_]+)\.([a-z0-9_]+)(?=[\s,;)\]#\-]|$)"
)


def _is_comment_or_blank(raw_line: str) -> bool:
    """Return True if the raw line is a comment or blank. Used to bound header sections."""
    if raw_line.strip() == "":
        return True
    # Starts with typical comment markers: '#', '/*', '*', '--' (SQL), or banner lines
    return bool(re.match(r"^\s*(#|//|/\*|\*|--)", raw_line)) or bool(re.match(r"^\s*#{5,}\s*$", raw_line))

def _normalize_line(s: str) -> str:
    """Lower-case, strip BOM/Unicode spaces, and collapse leading comment clutter."""
    s = s.replace("\ufeff", "")           # BOM
    s = s.replace("\u00a0", " ")          # NBSP
    s = s.replace("\u200b", "")           # zero-width space
    s = s.lower().rstrip()
    # Remove leading comment and decoration characters but keep the raw line for comment detection
    s = re.sub(r"^[\s#/\*\-\|>]+", "", s)
    return s

def _is_output_header(norm_line: str) -> bool:
    """Detect 'output table(s)' header variants."""
    return (
        re.match(r"^output\s+table(?:s)?(\s*[:：\-\u2013\u2014]\s*)?$", norm_line) is not None
        or re.match(r"^output\s+table(?:s)?\b", norm_line) is not None
    )

def _is_section_break(norm_line: str) -> bool:
    """Other headers that should end the current section."""
    if re.match(r"^\s*#{5,}\s*$", norm_line):   # banner like '#####'
        return True
    if re.match(r"^(input|job|jobs|user|used|usage|purpose|revision|revisions|history|company|author|date|data|datastage|sas|view)\b", norm_line):
        return True
    if re.match(r"^[a-z][a-z0-9 _/\-\(\)]*\s*[:：\-\u2013\u2014]\s*$", norm_line):
        return True
    return False

def parse_output_tables_from_header(text: str) -> Set[str]:
    """
    Parse ALL 'Output table(s)' sections in file.

    Key fix:
    - Only consume lines within the contiguous comment block after the header.
    - Stop the section when hitting a non-comment line OR another header-like line.
    """
    out: Set[str] = set()
    raw_lines = text.splitlines()
    norm_lines = [_normalize_line(ln) for ln in raw_lines]

    i = 0
    sections = 0
    while i < len(norm_lines):
        if _is_output_header(norm_lines[i]):
            sections += 1
            logging.debug("OUTPUT header @ line %d: %r", i + 1, raw_lines[i])
            i += 1
            # consume this section inside the comment block only
            while i < len(norm_lines):
                norm = norm_lines[i]
                raw  = raw_lines[i]

                # Hard stop if we left the comment block (entered real code)
                if not _is_comment_or_blank(raw):
                    logging.debug("Code encountered -> end section @ line %d: %r", i + 1, raw)
                    break

                # Stop if we see another header-like label (e.g., 'Input table(s):')
                if _is_section_break(norm) and not _is_output_header(norm):
                    logging.debug("Section break @ line %d: %r", i + 1, raw)
                    break

                # extract all db.tbl tokens from this comment line
                for db, tbl in RE_FQTN_INLINE_L.findall(norm):
                    fq = f"{db}.{tbl}"
                    out.add(fq)
                    logging.debug("  FQTN @ line %d: %s   (raw: %r)", i + 1, fq, raw)
                i += 1
            continue
        i += 1

    logging.debug("parse_output_tables_from_header: sections=%d, tables=%d -> %s",
                  sections, len(out), ", ".join(sorted(out)))
    return out


def parse_insertinto_targets(text: str) -> Set[str]:
    t = text.lower()
    return {to_fqtn(db, tbl) for db, tbl in RE_INSERT_INTO_L.findall(t)}

def parse_view_name(text: str) -> Optional[str]:
    """
    Parse CREATE VIEW target and return lower-cased name.
    Prefer fully-qualified; if unqualified, return the bare name (caller may ignore).
    """
    t = text.lower()
    m = RE_CREATE_VIEW_L.search(t)
    if not m:
        return None
    raw = m.group(1)
    return raw  # could be 'db.view' or just 'view'


def index_writers(files: List[str]) -> Dict[str, List[WriterInfo]]:
    index: Dict[str, List[WriterInfo]] = defaultdict(list)
    scanned = 0
    for p in files:
        text = read_text(p)
        if text is None:
            continue
        scanned += 1
        ext = os.path.splitext(p)[1].lower()

        outs = parse_output_tables_from_header(text)
        ins_into = parse_insertinto_targets(text)
        if ins_into:
            logging.debug("insertInto hits in %s: %s", p, ", ".join(sorted(ins_into)))
        outs |= ins_into

        if outs:
            logging.info("Output-section hits in %s: %s", p, ", ".join(sorted(outs)))
        for fqtn in outs:
            if "." in fqtn:
                index[fqtn].append(WriterInfo(file_path=p, kind='spark'))
                #logging.info("Indexed Spark writer: %s -> %s", fqtn, p)

        if ext == '.sql':
            v = parse_view_name(text)  # lower-cased; may be bare
            if v and "." in v:
                index[v].append(WriterInfo(file_path=p, kind='view'))
                #logging.info("Indexed View writer: %s -> %s", v, p)

    logging.info("Indexing done. Scanned %d files. Indexed %d distinct output FQTN(s).",
                 scanned, len(index))
    return index

def debug_scan_output_headers(root: str):
    files = list_code_files(root)
    logging.info("[DEBUG SCAN] searching potential output headers in %d files", len(files))
    for p in files:
        text = read_text(p)
        if text is None:
            continue
        for idx, ln in enumerate(text.splitlines(), start=1):
            low = ln.lower()
            #if "output" in low and "table" in low:
                #logging.info("[CAND] %s:%d  %r", p, idx, ln)


# ------------------------------
# Upstream extraction
# ------------------------------

def extract_upstreams_from_spark(text: str) -> Set[str]:
    t = text.lower()
    return {to_fqtn(db, tbl) for db, tbl in RE_SPARK_TABLE_L.findall(t)}

def extract_upstreams_from_view(text: str) -> Set[str]:
    t = text.lower()
    return {to_fqtn(db, tbl) for db, tbl in RE_FROM_JOIN_L.findall(t)}

def get_upstreams_for_writer(writer: WriterInfo) -> Set[str]:
    """Read file and dispatch to appropriate extractor."""
    text = read_text(writer.file_path)
    if text is None:
        return set()
    if writer.kind == 'spark':
        return extract_upstreams_from_spark(text)
    elif writer.kind == 'view':
        return extract_upstreams_from_view(text)
    else:
        return set()


# ------------------------------
# Candidate filtering by strict name hit
# ------------------------------

def filter_candidate_files_by_name(files: List[str], fqtn: str) -> List[str]:
    """
    Prefilter files by LOWER-CASED FQTN occurrence.
    """
    pat = word_boundary_pattern_fqtn(fqtn)
    out = []
    for p in files:
        text = read_text(p)
        if text is None:
            continue
        if pat.search(text.lower()):
            out.append(p)
    return out


def find_writers_for_table(
    fqtn: str,
    all_files: List[str],
    global_index: Dict[str, List[WriterInfo]]
) -> List[WriterInfo]:
    """
    Resolve writers for a fully-qualified table name (lower-cased).
    """
    fqtn = fqtn.lower()
    candidates = filter_candidate_files_by_name(all_files, fqtn)
    logging.info("FQTN '%s': %d candidate files by strict FQTN search", fqtn, len(candidates))

    writers: List[WriterInfo] = []

    if fqtn in global_index:
        for w in global_index[fqtn]:
            if w.file_path in candidates:
                writers.append(w)

    if not writers:
        # direct parse as fallback (should rarely be needed if index is built)
        for p in candidates:
            text = read_text(p)
            if text is None:
                continue
            outs = parse_output_tables_from_header(text) | parse_insertinto_targets(text)
            if fqtn in outs:
                writers.append(WriterInfo(file_path=p, kind='spark'))
                continue
            v = parse_view_name(text)
            if v == fqtn:
                writers.append(WriterInfo(file_path=p, kind='view'))

    uniq = {(w.file_path, w.kind): w for w in writers}
    deduped = list(uniq.values())
    logging.info("FQTN '%s': %d confirmed writer(s)", fqtn, len(deduped))
    for w in deduped:
        logging.debug("Writer for %s -> kind=%s, file=%s", fqtn, w.kind, w.file_path)
    return deduped


# ------------------------------
# Lineage expansion
# ------------------------------

def dfs_lineage_paths(
    start_fqtn: str,
    all_files: List[str],
    writers_index: Dict[str, List[WriterInfo]],
    visited_stack: Optional[List[str]] = None,
    cache_upstreams: Optional[Dict[str, Set[str]]] = None
) -> List[List[str]]:
    """
    Depth-first enumerate all lineage paths from a fully-qualified table (FQTN: db.tbl) to sources.
    Each returned path is [target_fqtn, layer1_fqtn, layer2_fqtn, ..., source_fqtn].

    - A node has upstreams if there exists >=1 writer and those writer files reference upstream FQTN(s).
    - If no writers or upstream set is empty, the node is treated as a source.
    - Cycle-safe via 'visited_stack' (FQTN-based).
    """
    if visited_stack is None:
        visited_stack = []
    if cache_upstreams is None:
        cache_upstreams = {}

    # Normalize to lower-case FQTN just in case upstream callers didn't
    start_fqtn = start_fqtn.lower()

    # Cycle guard
    if start_fqtn in visited_stack:
        logging.warning("Cycle detected at %s. Cutting branch.", start_fqtn)
        return [[start_fqtn]]  # cut the branch at the cycle

    # Find writers for this FQTN
    writers = find_writers_for_table(start_fqtn, all_files, writers_index)

    # Collect upstreams (union across writers)
    upstream_union: Set[str] = set()

    # Optional memoization by FQTN to avoid re-parsing the same files repeatedly
    if start_fqtn in cache_upstreams:
        upstream_union = cache_upstreams[start_fqtn]
    else:
        for w in writers:
            ups = get_upstreams_for_writer(w)  # returns a set of FQTN strings like 'db.tbl'
            upstream_union |= ups
        cache_upstreams[start_fqtn] = upstream_union

    # No upstreams ⇒ treat as source
    if not writers or len(upstream_union) == 0:
        logging.info("Table '%s' has no upstreams. Treat as source.", start_fqtn)
        return [[start_fqtn]]

    # Branch on each upstream FQTN
    all_paths: List[List[str]] = []
    for up_fqtn in sorted(upstream_union):
        subpaths = dfs_lineage_paths(
            up_fqtn, all_files, writers_index,
            visited_stack=visited_stack + [start_fqtn],
            cache_upstreams=cache_upstreams
        )
        for sp in subpaths:
            all_paths.append([start_fqtn] + sp)
    return all_paths


# ------------------------------
# Tabular output shaping
# ------------------------------

def shape_paths_to_rows(target: str, paths: List[List[str]]) -> List[Dict[str, str]]:
    """
    Convert paths like [target, a, b, src] into row dicts:
      {"Target Table": target, "Layer 1": a, "Layer 2": b, ..., "Source Table": src}
    Only include intermediate nodes that still have upstreams. In our path semantics:
      - All nodes except the last are "have-upstreams" along that path.
      - The last node is the leaf → "Source Table".
    """
    rows: List[Dict[str, str]] = []
    for p in paths:
        if len(p) == 1:
            # Path is just [source] == also the target has no upstream → 'Source Table' equals target
            row: Dict[str, str] = {"Target Table": target, "Source Table": p[0]}
            rows.append(row)
            continue
        # p[0] is the target itself (table-only). We'll set "Target Table" to the target (as passed-in, table-only).
        inter = p[1:-1]  # layers that still have upstreams
        leaf = p[-1]
        row = {"Target Table": target}
        for i, name in enumerate(inter, start=1):
            row[f"Layer {i}"] = name
        row["Source Table"] = leaf
        rows.append(row)
    return rows


# ------------------------------
# CSV writer
# ------------------------------

def write_csv(rows: List[Dict[str, str]], out_path: str) -> None:
    """Write rows to CSV with dynamic columns."""
    if not rows:
        logging.warning("No rows to write.")
        # still write header?
    # Determine columns
    cols: List[str] = ["Target Table"]
    max_layer = 0
    for r in rows:
        for k in r.keys():
            if k.startswith("Layer "):
                try:
                    n = int(k.split()[1])
                    max_layer = max(max_layer, n)
                except Exception:
                    pass
    for i in range(1, max_layer + 1):
        cols.append(f"Layer {i}")
    cols.append("Source Table")

    import csv
    os.makedirs(os.path.dirname(out_path), exist_ok=True) if os.path.dirname(out_path) else None
    with open(out_path, 'w', newline='', encoding='utf-8') as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction='ignore')
        w.writeheader()
        for r in rows:
            w.writerow(r)
    logging.info("Wrote %d rows to %s", len(rows), out_path)


# ------------------------------
# Main
# ------------------------------

def tracer():
    parser = argparse.ArgumentParser(description="Recursive lineage tracer for Spark/SQL codebase (FQTN-aware).")
    parser.add_argument("--root", required=True, help="Root folder to scan (.py/.sql recursively).")
    parser.add_argument(
        "--targets",
        required=False,
        help="Comma-separated list of targets. Prefer fully-qualified 'db.tbl'. "
             "If bare table names are given, they will be expanded using writers index."
    )
    parser.add_argument("--out", default="lineage.csv", help="Output CSV path.")
    parser.add_argument("--log", default="INFO", help="Log level (DEBUG, INFO, WARNING).")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s"
    )

    # 1) Scan files and build writers index FIRST (keys are lower-cased FQTN)
    logging.info("Scanning files under: %s", args.root)
    debug_scan_output_headers(args.root)
    files = list_code_files(args.root)
    logging.info("Found %d candidate files (.py/.sql).", len(files))

    logging.info("Indexing writers from headers / CREATE VIEW...")
    writers_index = index_writers(files)
    logging.info("Indexed %d distinct output FQTN(s).", len(writers_index))

    # Helper: normalize a target to lower-cased FQTN if already qualified, else None
    def _normalize_fqtn_or_none(t: str) -> Optional[str]:
        t = t.strip()
        t_low = t.lower()
        return t_low if "." in t_low else None

    # Helper: expand a bare table into candidate FQTNs using writers_index keys
    def _expand_bare_table(tbl_bare: str) -> List[str]:
        """Return all FQTNs from writers_index keys that end with '.tbl_bare'."""
        bare = tbl_bare.strip().lower()
        suffix = f".{bare}"
        cands = [k for k in writers_index.keys() if k.endswith(suffix)]
        if not cands:
            logging.warning("No FQTN candidates found for bare table '%s' in writers index.", tbl_bare)
        else:
            logging.info("Expanded bare table '%s' to %d FQTN candidate(s).", tbl_bare, len(cands))
        return sorted(set(cands))

    # 2) Resolve targets
    if args.targets:
        raw_targets = [x for x in (t.strip() for t in args.targets.split(",")) if x]
    else:
        # If you keep a default list, they may be bare names; we'll expand below.
        # Part 1
        raw_targets = ["CODEDECODEACTIVITY","CODEDECODEADDRESSTYPE","CODEDECODEAUDITREASON","CODEDECODEAUDITSOURCE","CODEDECODEBUSINESSTYPE","CODEDECODECLASS","CODEDECODECOMPANYSTATUS","CODEDECODECOMPANYTYPE","CODEDECODEDATAISOLATIONUSER","CODEDECODEDELIVERYMODE","CODEDECODEDIGITALSERVICEDOCUMENTSUBTYPE","CODEDECODEDOCUMENTTYPE","CODEDECODEENTITYRELATIONSHIPTYPE","CODEDECODEENTITYTYPE","CODEDECODEEXTERNALSYSTEMERRORMESSAGE","CODEDECODEFILINGFREQUENCY","CODEDECODEFILINGMODE","CODEDECODEFORMTYPE","CODEDECODEGIRSUBMISSIONSTATUS","CODEDECODEGIRSUBMISSIONTYPE","CODEDECODEISOCOUNTRY","CODEDECODEISOCURRENCY","CODEDECODEMAILCLASS","CODEDECODEMICROSERVICEERRORMESSAGE","CODEDECODEPRINTMODE","CODEDECODERETURNPACKAGE","CODEDECODERETURNSTATUS","CODEDECODETAXENTITYIDTYPE","CODEDECODETAXENTITYIDTYPEUEN","CODEDECODETAXTYPE","CODEDECODETAXTYPESTATUS","CODEDECODETRANSACTIONSTATUS","CODESTABLE FOR IRIN3 PILLAR 2","COGNOS_IRIN_CP_M094A","COGNOS_IRIN_CP_M094B","CONTROLPILLAR2FORMSTATUS","CONTROLPILLAR2VIEWFILINGSTATUS","CORPORATE BASE ","ENFD_INPUT_FILING_COMPLIANCE","ENFD_INPUT_RECLASS_ARREARS","ENFD_INPUT_TAX_ARREARS","ENFD_INPUT_WRITEOFF_TALL","ENFD_INT_DM_EFFECTIVENESS_CIT","ENFD_INT_DM_EFFECTIVENESS_GST","ENFD_INT_DM_EFFECTIVENESS_IIT","ENFD_INT_FILING_COMPLIANCE_CIT","ENFD_INT_FILING_COMPLIANCE_GST","ENFD_INT_FILING_COMPLIANCE_IIT","ENFD_INT_FILING_COMPLIANCE_SD","ENFD_INT_PYMT_COMPLIANCE_CIT_DENO","ENFD_INT_PYMT_COMPLIANCE_CIT_NUME","ENFD_INT_PYMT_COMPLIANCE_GST_DENO","ENFD_INT_PYMT_COMPLIANCE_GST_NUME","ENFD_INT_PYMT_COMPLIANCE_IIT_DENO","ENFD_INT_PYMT_COMPLIANCE_IIT_NUME","ENFD_INT_RECLASS_ARREARS_CIT","ENFD_INT_RECLASS_ARREARS_GST","ENFD_INT_RECLASS_ARREARS_IIT","ENFD_INT_RECLASS_ARREARS_OTHERS","ENFD_INT_RECLASS_ARREARS_PT","ENFD_RPT_DM_EFFECTIVENESS","ENFD_RPT_FILING_COMPLIANCE","ENFD_RPT_MSHL_CASE_BASE","ENFD_RPT_MSHL_PREMIUM_COLLECTION","ENFD_RPT_NF_EFFECTIVENESS","ENFD_RPT_PYMT_COMPLIANCE","ENFD_RPT_RECLASS_ARREARS","ENFD_RPT_REOFFENDING_RATE","ENFD_RPT_S45_FILING","ENFD_RPT_TAX_ARREARS","ENFD_RPT_WRITEOFF_TALL"]
        # Part 2
        # raw_targets = ["ENTITY DETAILS","GIR STATUS MESSAGE","GIR TRANSMISSION","GIRCONSOLIDATION","GIRFILENOTIFICATIONMATCHING","GIRFILEXMLSECTION","GIRJURISDITIONRECIPENT","GSTD_RPT_GSTD_KPI","GSTCASEASSESSMENT","H_GDS_EMPLOYEE_QUALIFYING","H_GDS_YRLY_GROSS_WAGES","IRIN_EWF_D024_DLY","PILLAR2ACCOUNTINGPERIOD","PILLAR2FORMDETAIL","PILLAR2FORMHEADER","PILLAR2RETURNOBLIGE","PILLAR2TAXTYPE","RETRIEVE GIR","RPT_ASSESSMENT_CLUBS_ASSOCIATION","RPT_ASSESSMENT_CLUBS_ASSOCIATION_AGGR","RPT_ASSESSMENT_TRUST_UNIT_TRUST","RPT_ASSESSMENT_TRUST_UNIT_TRUST_AGGR","RPT_CIT_ASMT_NTA_ENTITY_MTH","RPT_COMPLIANCE_PROGRAM_CMS","RPT_CORPORATE_TAX_ASSESSMENT","RPT_CORPORATE_TAX_ASSESSMENT_CM","RPT_DIM_TARGET_GRP","RPT_FCT_CIT_RTN_SUM","RPT_NON_FINANCIALS","RPT_RETURNS_CA_TUT_CM","RPT_VOLUNTARY_DISCLOSURE_CM","SFA RECORDS ","TDW_AEOI_PAYLOAD_IB","TDW_AEOI_PAYLOAD_OB","TDW_AEOI_PAYLOAD_XML","TDW_ASSET_CO_SHARES","TDW_CBCTR_ADD_INFO","TDW_CBCTR_CBC_RPT","TDW_CBCTR_CONSTITUENT_ENTITY","TDW_CBCTR_REPORT","TDW_CBCTR_RPT_ENTITY","TDW_CBCTR_TRACK","TDW_CG_ASSESSMENT","TDW_CG_FINANCIAL_CATEGORY","TDW_CG_FINANCIAL_TRANS","TDW_CIT_ASSESSMENT","TDW_CIT_NTA_REJECT_LOG","TDW_CIT_RETURN_OBLIGE","TDW_CIT_WAIVER","TDW_EIS_CP_ANL","TDW_EIS_CP_FORM","TDW_EIS_EDA_FORM"]
        # Part 3
        # raw_targets = ["TDW_EIS_PROFILE","TDW_EIS_PROFILE_USER","TDW_EMPLOYEE_CONTRIBUTION","TDW_EMPLOYER_CONTRIBUTION","TDW_EXEMPTION","TDW_FORM_C","TDW_FORM_C_APP_A","TDW_FORM_C_UR","TDW_FORM_ECI","TDW_GR_A_CLAIMANT","TDW_GR_A_HEADER","TDW_GR_B_HEADER","TDW_GR_B_TRANSFEROR","TDW_GST_ASMT_DTL","TDW_GST_ASMT_DTL_DLY","TDW_GST_ASMT_DTL_MTH_TRADE","TDW_GST_ASSESSMENT","TDW_GST_CED_MTH","TDW_GST_CRITERIA_HIT","TDW_GST_EFILER_DETAIL","TDW_GST_LVG_MTH","TDW_GST_REFUND","TDW_GST_REFUND_DLY","TDW_GST_REGN","TDW_GST_RETURN_OBLIGE","TDW_GST_TRADER","TDW_IP_TRANSACTION_LOG","TDW_IR37","TDW_LVG_GST","TDW_LVG_GST_ACCESS_PROFILE","TDW_LVG_GST_AM_PROFILE","TDW_LVG_GST_TN_PROFILE","TDW_MSHL_CASE","TDW_MSHL_CASE_ACTIVITY","TDW_MSHL_CASE_ENTITY_REL","TDW_MSHL_CASE_ITEM","TDW_MSHL_PAYMENT_DTL","TDW_MSHL_TRANSACTION","TDW_NR_DETAIL","TDW_NR_TRANS","TDW_RIC_ADJ","TDW_RIC_CE","TDW_RIC_LOA","TDW_RIC_LOC","TDW_RIC_NOM_DETAIL","TDW_RIC_NOM_HEADER","TDW_RIC_SOA","TDW_RIC_TRANS","TDW_S45_FINANCIAL_TRANS","TDW_S45_NR_STG","TDW_S45_PAYER_NR_STG","TDW_S45_RETURN_OBLIGE","TDW_SCHEME","TDW_SCHEME_DTL"]
        # Part 4
        # raw_targets = ["T_ADM_ASMT_CT_LF","T_ADM_ASMT_GST_LF","T_ADM_CMS_GSTD_CASE_DTL","T_ADM_ENTITY_RISK_PROFILE","T_ADM_ESP_GST_LOG","T_ADM_ESP_GST_PRA_ENRICHED","T_ADM_ESP_GST_SNA_ENRICHED","T_ADM_ESP_GST_SNA_TXN_ENRICHED","T_ADM_FI_CT","T_ADM_FI_GST","T_ADM_LCP_GST","T_ADM_OTH_CNTC_H_ENQUIRY","T_ADM_OTH_CNTC_T_ENQ_MSG","T_ADM_PF_COY","T_ADM_PR_PYMT_CT_YA","T_ADM_PR_PYMT_GST_PD","T_ADM_SNA_REF_RISK","T_ADM_SNA_REG_RISK","T_ADM_SNA_SCORE_BAND","T_ADM_SNA_SCORE_LINK","T_ADM_SNA_SCORE_RULE","T_ADM_XBRL_BASIC","T_ADM_XBRL_ELEM_LOOKUP","T_ADM_XBRL_ST_BAL_SHEET","T_ADM_XBRL_ST_NOTES_BAL","T_ENTITY_MAPPING","T_REFUND_RVW_RPT","V_ADM_ESP_CONTACT","V_ADM_ESP_DT_PARAM","V_ADM_ESP_GST_ASMT_DTL","V_ADM_ESP_GST_BM","V_ADM_ESP_GST_CED_MTH","V_ADM_ESP_GST_PF","V_ADM_ESP_REL","V_ADM_ESP_SNA_ENT_SCORE","V_ADM_ESP_SNA_PARAM","V_ADM_ESP_SNA_SCORECARD","V_ADM_ESP_SNA_TXN_RULE"]
        # Part 5
        # raw_targets = ["NEW REPORTING TABLE AUDITCASEANDASSESSMENT","NEW REPORTING TABLE AUDITCASEANDISSUE","NEW REPORTING TABLE AUDITCASEANDLATESTACTION","NEW REPORTING TABLE AUDITCASEANDRECOVERY","NEW REPORTING TABLE COMPLTR","NEW REPORTING TABLE COMPOSITIONANDSURCHARGE","NEW REPORTING TABLE OBJECTION","NEW REPORTING TABLE OPENYA","NEW REPORTING TABLE OPENYA MP","NEW REPORTING TABLE TIER1","RPT_CMPLNC_PRGRM_ACTUAL_NOTIONAL_RECVRY_CM","RPT_CMPLNC_PRGRM_AUDIT_CASES_CM","RPT_CMPLNC_PRGRM_HIT_RATE_RECVRY_BLP_CM","RPT_CMPLNC_PRGRM_HIT_RATE_RECVRY_TCP_CM","RPT_CMPLNC_PRGRM_SUMMARY","RPT_RETURNS_CORPORATE_TAX_AGGR","RPT_RETURNS_CORPORATE_TAX_TRANS","RPT_RETURNS_TUT_TRANS","RPT_SERVICE_STANDARD_CMS","RPT_SERVICE_STANDARD_CORRESPONDENCE_AGGR","RPT_SERVICE_STANDARD_CORRESPONDENCE_CM","RPT_SERVICE_STANDARD_EMAIL_AGGR","RPT_SERVICE_STANDARD_EMAIL_CM","SMALLVOLUMEFORMDETAIL","SMALLVOLUMEFORMHEADER","SMALLVOLUMERETURNOBLIGE","SMALLVOLUMETAXTYPE","TDW_CA_230","TDW_CA_RETURN_OBLIGE","TDW_CBCTR_ERROR","TDW_CBCTR_FILER","TDW_CBCTR_MAP","TDW_CBCTR_STATUS_MESSAGE","TDW_CBCTR_STATUS_MESSAGE_DTL","TDW_CBCTR_TRANSMISSION","TRUSTALLOCATIONBENEFICIARY","TRUSTALLOCATIONFORMHEADER","TRUSTALLOCATIONOTHERS","TRUSTALLOCATIONTOTAL","T_ADM_EN_CASE_INFO","T_ADM_XBRL_CONTEXT_REF","T_ADM_XBRL_LABEL","COGNOS_IRIN_CP_D090","COGNOS_IRIN_ESV_A505"]
        # Part 6
        # raw_targets = ["GSTD_RPT_CALENDAR","GSTD_RPT_CMS_GEN_OPS_MTH","GSTD_RPT_CMS_GEN_OPS_WK","GSTD_RPT_CMS_OVERALL_MTH","GSTD_RPT_CMS_OVERALL_WK","GSTD_RPT_CODES_TABLES","GSTD_RPT_COGNOS_EWF_D024","GSTD_RPT_CONSOLIDATED_KPI","GSTD_RPT_CONSOLIDATED_KPI_VW","GSTD_RPT_GEN_KPI","GSTD_RPT_GST_RETURNS_N_APPLICATION","GSTD_RPT_HEADCOUNT_ALLOCATION","GSTD_RPT_ISO_SERVICE_STANDARD","GSTD_RPT_LB_KPI","GSTD_RPT_PTR_KPI","GSTD_RPT_REFUND_REVIEW","GSTD_RPT_SERVICE_STANDARDS","GSTD_RPT_TOP_SIX_RECOVERY","GSTD_RPT_TRADER_BASE","GSTD_RPT_WT_KPI","INT_PURE_PAY_ON_TIME_CIT","INT_PURE_PAY_ON_TIME_GST","INT_PURE_PAY_ON_TIME_IIT","INT_PURE_PAY_ON_TIME_PT","IRIN_CP_D090_DLY","RPT_PURE_PAY_ON_TIME","TDW_EIS_CP_INNOPROJ","TDW_EIS_CP_REGIP","TDW_EIS_CP_RND","TDW_EIS_CP_TRAINING","TDW_EIS_EDA_ANL","TDW_EIS_EDA_INNOPROJ","TDW_EIS_EDA_REGIP","TDW_EIS_EDA_RND","TDW_EIS_EDA_TRAINING","TDW_EIS_PROFILE_ANL","TDW_EIS_PROFILE_INNOPROJ","TDW_EIS_PROFILE_REGIP","TDW_EIS_PROFILE_RND","TDW_EIS_PROFILE_TRAINING","TDW_GST_REFUND_INTEREST","TDW_PROGRESS_STS_HDR","TDW_XREF_INVOICE_DETAIL","T_ADM_ASMT_CT_TP","T_ADM_BM_CT_LF_TRD"]
        # Part 7
        # raw_targets = ["T_ADM_BM_CT_TP_IND","T_ADM_BM_GST_LF_TRD","T_ADM_LCP_CT","T_ADM_PR_RFD_CT","T_ADM_PR_RFD_GST","T_ADM_SNA_CMS_MAPPING","T_ADM_XBRL_OTHER","T_ADM_XBRL_OTHER_TXTBLOCK","T_ADM_XBRL_OTHER_TXTBLOCK_FINAL","T_ADM_XBRL_ST_BAL_SHEET_TEXT","T_ADM_XBRL_ST_CASH_FLOWS","T_ADM_XBRL_ST_CASH_FLOWS_TEXT","T_ADM_XBRL_ST_CHG_EQUITY","T_ADM_XBRL_ST_CHG_EQUITY_TEXT","T_ADM_XBRL_ST_INC_EXP","T_ADM_XBRL_ST_INC_EXP_TEXT","T_ADM_XBRL_ST_NOTES_COY","T_ADM_XBRL_ST_NOTES_INC","T_ADM_XBRL_ST_PROFILE","T_ADM_XBRL_UN_NOTES_BAL","T_ADM_XBRL_UN_NOTES_BAL_FINAL","T_ADM_XBRL_UN_NOTES_COY","T_ADM_XBRL_UN_NOTES_COY_FINAL","T_ADM_XBRL_UN_NOTES_INC","T_ADM_XBRL_UN_NOTES_INC_FINAL"]

    # 3) Normalize/expand into FQTN set (all lower-case)
    fqtn_targets: List[str] = []
    for t in raw_targets:
        fq = _normalize_fqtn_or_none(t)
        if fq:
            fqtn_targets.append(fq)
        else:
            # bare name → expand via writers_index keys
            fqtn_targets.extend(_expand_bare_table(t))

    fqtn_targets = sorted(set(fqtn_targets))
    if not fqtn_targets:
        logging.error("No valid targets to process. Provide fully-qualified names like 'schema.table', "
                      "or ensure bare names exist in writers index.")
        sys.exit(2)

    # 4) Run DFS for each target FQTN and write rows
    all_rows: List[Dict[str, str]] = []
    for tgt_fqtn in fqtn_targets:
        logging.info("=== Start lineage for target: %s ===", tgt_fqtn)
        paths = dfs_lineage_paths(tgt_fqtn, files, writers_index)
        rows = shape_paths_to_rows(tgt_fqtn, paths)  # paths contain FQTN at each hop
        all_rows.extend(rows)
        logging.info("=== Done lineage for target: %s (paths=%d) ===", tgt_fqtn, len(paths))

    write_csv(all_rows, args.out)



if __name__ == "__main__":
    tracer()
