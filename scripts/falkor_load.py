#!/usr/bin/env python3
"""
SF Emergency KG — FalkorDB Loader (v2 — fixed batch strategy)
==============================================================
Loads RDF/TTL files into FalkorDB using streaming (line-by-line).
Only loads records from 2020 onwards.

Fix vs v1: FalkorDB does not allow reusing variable names (n, a, b)
across multiple MERGE statements in a single query. Each statement
is now sent individually. Batching is used only for progress tracking.

Usage:
    # Test (5000 blocks per file):
    python3 scripts/falkor_load.py --rdf data/rdf/ --test

    # Full load (2020+):
    python3 scripts/falkor_load.py --rdf data/rdf/

    # Resume a specific file:
    python3 scripts/falkor_load.py --rdf data/rdf/ --file police_incidents.ttl
"""

import re
import sys
import time
import argparse
from pathlib import Path

try:
    from falkordb import FalkorDB
except ImportError:
    print("ERROR: falkordb package not installed.")
    print("Run: pip3 install falkordb")
    sys.exit(1)


# =============================================================================
# CONFIGURATION
# =============================================================================

BATCH_SIZE = 1000
MIN_YEAR   = 2020
GRAPH_NAME = "SF_KG"

LOAD_ORDER = [
    ("police_districts.ttl",    "districts"),
    ("call_types.ttl",          "call_types_fire"),
    ("police_call_types.ttl",   "call_types_police"),
    ("neighborhoods.ttl",       "neighborhoods"),
    ("stations.ttl",            "stations"),
    ("units.ttl",               "units"),
    ("time_windows.ttl",        "time_windows_fire"),
    ("police_time_windows.ttl", "time_windows_police"),
    ("locations.ttl",           "locations_fire"),
    ("police_locations.ttl",    "locations_police"),
    ("incidents.ttl",           "incidents_fire"),
    ("police_incidents.ttl",    "incidents_police"),
    ("responses.ttl",           "responses_fire"),
    ("police_responses.ttl",    "responses_police"),
]


# =============================================================================
# TTL PARSER
# =============================================================================

def parse_ttl_blocks(filepath):
    current_lines = []
    current_subject = None

    with open(filepath, 'r', encoding='utf-8') as f:
        for raw_line in f:
            line = raw_line.strip()

            if not line or line.startswith('#') or line.startswith('@prefix'):
                if current_subject and current_lines:
                    block = _parse_block(current_subject, current_lines)
                    if block:
                        yield block
                    current_lines = []
                    current_subject = None
                continue

            if line.startswith('ems:') and not raw_line.startswith(' ') and not raw_line.startswith('\t'):
                if current_subject and current_lines:
                    block = _parse_block(current_subject, current_lines)
                    if block:
                        yield block
                    current_lines = []
                current_subject = line.rstrip(' ;.')
                continue

            if current_subject:
                current_lines.append(line)

    if current_subject and current_lines:
        block = _parse_block(current_subject, current_lines)
        if block:
            yield block


def _parse_block(subject_raw, lines):
    uri = subject_raw.replace('ems:', '').strip()
    node_type = None
    props = {}
    rels = {}

    for line in lines:
        line = line.rstrip(' ;.')
        if not line:
            continue

        type_match = re.match(r'a\s+ems:(\w+)', line)
        if type_match:
            if node_type is None:
                node_type = type_match.group(1)
            continue

        pred_obj = re.match(r'ems:(\w+)\s+(.*)', line)
        if not pred_obj:
            continue

        pred = pred_obj.group(1)
        obj  = pred_obj.group(2).strip()

        str_match = re.match(r'"(.*?)"(?:\^\^xsd:\w+)?$', obj)
        if str_match:
            props[pred] = str_match.group(1)
            continue

        num_match = re.match(r'^-?[\d.]+$', obj)
        if num_match:
            try:
                props[pred] = float(obj) if '.' in obj else int(obj)
            except ValueError:
                props[pred] = obj
            continue

        if obj in ('true', 'false'):
            props[pred] = obj == 'true'
            continue

        if 'ems:' in obj:
            targets = [t.replace('ems:', '').strip() for t in obj.split(',')]
            rels[pred] = targets
            continue

    return {
        'uri':  uri,
        'type': node_type or 'Node',
        'props': props,
        'rels':  rels,
    }


def should_load(block, min_year=MIN_YEAR):
    time_sensitive = {'Incident', 'Response', 'PoliceResponse'}
    if block['type'] not in time_sensitive:
        return True

    for key, val in block['props'].items():
        if 'Timestamp' in key or 'timestamp' in key:
            try:
                year = int(str(val)[:4])
                return year >= min_year
            except (ValueError, TypeError):
                continue

    for rel_targets in block['rels'].values():
        for target in rel_targets:
            tw_match = re.search(r'TimeWindow_(\d{4})_', target)
            if tw_match:
                year = int(tw_match.group(1))
                return year >= min_year

    return True


# =============================================================================
# CYPHER GENERATOR
# =============================================================================

def block_to_statements(block):
    """
    One Cypher statement per operation.
    FalkorDB does not allow reusing variable names within a single query,
    so we never concatenate multiple MERGE clauses into one query string.
    """
    uri   = block['uri']
    ntype = block['type']
    props = block['props']
    rels  = block['rels']

    statements = []

    # Node upsert
    set_parts = [f'n.uri = "{_esc(uri)}"']
    for k, v in props.items():
        if isinstance(v, str):
            set_parts.append(f'n.{k} = "{_esc(v)}"')
        elif isinstance(v, bool):
            set_parts.append(f'n.{k} = {"true" if v else "false"}')
        else:
            set_parts.append(f'n.{k} = {v}')

    statements.append(
        f'MERGE (n:{ntype} {{uri: "{_esc(uri)}"}}) SET {", ".join(set_parts)}'
    )

    # One statement per relationship edge
    for pred, targets in rels.items():
        rel_type = _pred_to_rel(pred)
        for target in targets:
            target_type = _infer_type(target)
            statements.append(
                f'MERGE (src:{ntype} {{uri: "{_esc(uri)}"}}) '
                f'MERGE (tgt:{target_type} {{uri: "{_esc(target)}"}}) '
                f'MERGE (src)-[:{rel_type}]->(tgt)'
            )

    return statements


def _esc(s):
    return str(s).replace('\\', '\\\\').replace('"', '\\"')


def _infer_type(uri):
    prefixes = {
        'Incident_':           'Incident',
        'Response_':           'Response',
        'PoliceResponse_':     'PoliceResponse',
        'CallType_':           'CallType',
        'Concept_':            'IncidentConcept',
        'Location_':           'Location',
        'Neighborhood_':       'Neighborhood',
        'PoliceDistrict_':     'PoliceDistrict',
        'SupervisorDistrict_': 'SupervisorDistrict',
        'TimeWindow_':         'TimeWindow',
        'Unit_':               'Unit',
        'Agency_':             'Agency',
        'StationArea_':        'StationArea',
        'Battalion_':          'Battalion',
    }
    for prefix, ntype in prefixes.items():
        if uri.startswith(prefix):
            return ntype
    return 'Node'


def _pred_to_rel(pred):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', pred).upper()


# =============================================================================
# LOADER
# =============================================================================

def create_indexes(graph):
    print("  Creating indexes...")
    specs = [
        ("Incident",         "uri"),
        ("Response",         "uri"),
        ("PoliceResponse",   "uri"),
        ("CallType",         "uri"),
        ("IncidentConcept",  "uri"),
        ("Location",         "uri"),
        ("Neighborhood",     "uri"),
        ("PoliceDistrict",   "uri"),
        ("TimeWindow",       "uri"),
        ("Unit",             "uri"),
        ("Agency",           "uri"),
    ]
    for label, prop in specs:
        try:
            graph.query(f"CREATE INDEX FOR (n:{label}) ON (n.{prop})")
        except Exception:
            pass
    print("  Indexes ready.")


def load_file(graph, filepath, label, min_year, batch_size, test_mode=False):
    filepath = Path(filepath)
    if not filepath.exists():
        print(f"  SKIP (not found): {filepath.name}")
        return 0, 0

    file_size = filepath.stat().st_size / (1024 * 1024)
    print(f"\n  Loading: {filepath.name} ({file_size:.0f} MB)")

    nodes_loaded  = 0
    nodes_skipped = 0
    start         = time.time()
    max_blocks    = 5000 if test_mode else None

    for i, block in enumerate(parse_ttl_blocks(filepath)):
        if max_blocks and i >= max_blocks:
            break

        if not should_load(block, min_year):
            nodes_skipped += 1
            continue

        for stmt in block_to_statements(block):
            try:
                graph.query(stmt)
            except Exception:
                pass

        nodes_loaded += 1

        if nodes_loaded % batch_size == 0:
            elapsed = time.time() - start
            rate = nodes_loaded / elapsed if elapsed > 0 else 0
            print(f"    {nodes_loaded:>8,} nodes | {nodes_skipped:>6,} skipped | "
                  f"{rate:>5.0f} nodes/s", end='\r')

    elapsed = time.time() - start
    rate = nodes_loaded / elapsed if elapsed > 0 else 0
    print(f"    {nodes_loaded:>8,} nodes loaded | {nodes_skipped:>6,} skipped | "
          f"{elapsed:.1f}s ({rate:.0f} nodes/s)    ")

    return nodes_loaded, nodes_skipped


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='SF Emergency KG — FalkorDB Loader')
    parser.add_argument('--rdf',   '-r', default='data/rdf/')
    parser.add_argument('--graph', '-g', default=GRAPH_NAME)
    parser.add_argument('--host',        default='localhost')
    parser.add_argument('--port',  '-p', type=int, default=6379)
    parser.add_argument('--batch', '-b', type=int, default=BATCH_SIZE)
    parser.add_argument('--year',  '-y', type=int, default=MIN_YEAR)
    parser.add_argument('--test',  '-t', action='store_true',
                        help='Test mode: 5000 blocks per file')
    parser.add_argument('--file',  '-f', default=None,
                        help='Load only this TTL file (for resuming)')
    args = parser.parse_args()

    rdf_dir = Path(args.rdf)

    print(f"\n{'='*60}")
    print(f"  SF EMERGENCY KG — FALKORDB LOADER v2")
    print(f"{'='*60}")
    print(f"  Host:        {args.host}:{args.port}")
    print(f"  Graph:       {args.graph}")
    print(f"  RDF dir:     {rdf_dir}")
    print(f"  Year filter: >= {args.year}")
    print(f"  Test mode:   {'YES (5000 blocks/file)' if args.test else 'NO'}")
    print(f"{'='*60}\n")

    print("Connecting to FalkorDB...")
    try:
        db    = FalkorDB(host=args.host, port=args.port)
        graph = db.select_graph(args.graph)
        print("  Connected.\n")
    except Exception as e:
        print(f"  ERROR: {e}")
        print(f"  Make sure FalkorDB is running: docker-compose up -d")
        sys.exit(1)

    create_indexes(graph)

    files_to_load = [(args.file, args.file)] if args.file else LOAD_ORDER

    total_start   = time.time()
    total_loaded  = 0
    total_skipped = 0

    for filename, label in files_to_load:
        loaded, skipped = load_file(
            graph, rdf_dir / filename, label,
            min_year  = args.year,
            batch_size= args.batch,
            test_mode = args.test,
        )
        total_loaded  += loaded
        total_skipped += skipped

    elapsed = time.time() - total_start

    print(f"\n{'='*60}")
    print(f"  LOAD COMPLETE")
    print(f"{'='*60}")
    print(f"  Total nodes loaded:  {total_loaded:>10,}")
    print(f"  Total nodes skipped: {total_skipped:>10,}")
    print(f"  Total time:          {elapsed/60:.1f} min")
    print(f"{'='*60}")
    print(f"\n  FalkorDB browser: http://localhost:3000")
    print(f"  Graph name: {args.graph}")
    print(f"\n  Sanity check:")
    print(f"    python3 scripts/falkor_queries.py --graph {args.graph}")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()