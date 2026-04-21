#!/usr/bin/env python3
"""
SF Emergency KG — Smart Query Router (v1 — rule-based classifier)
==================================================================
Interactive CLI that receives a natural language question, classifies
it as GRAPH or FLAT, executes the optimal query, and explains why.

Classification logic:
  GRAPH  — questions involving relationships, traversals, patterns,
            neighborhoods, districts, unit connections, concept mapping
  FLAT   — simple aggregations, counts, averages on a single dimension
            that don't require traversing relationships

The router can be extended with an LLM classifier (Claude API) later —
the classify() function is designed to be swappable.

Usage:
    python3 scripts/smart_router.py

    # With explicit CSV path if needed:
    python3 scripts/smart_router.py --csv data/raw/fire_calls_for_service.csv
"""

import sys
import re
import time
import argparse
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERROR: pip3 install pandas")
    sys.exit(1)

try:
    from falkordb import FalkorDB
except ImportError:
    print("ERROR: pip3 install falkordb")
    sys.exit(1)


# =============================================================================
# CONFIGURATION
# =============================================================================

GRAPH_NAME  = "SF_KG"
FALKOR_HOST = "localhost"
FALKOR_PORT = 6379
TIMEOUT     = 120000

# Default CSV — fire calls (the flat baseline)
DEFAULT_CSV = "data/raw/fire_calls_for_service.csv"

BANNER = """
╔══════════════════════════════════════════════════════════════╗
║         SF EMERGENCY KG — SMART QUERY ROUTER v1             ║
║         Rule-based classifier: GRAPH vs FLAT                 ║
╚══════════════════════════════════════════════════════════════╝
Type a question in plain English. The router will:
  1. Classify it as GRAPH (FalkorDB/Cypher) or FLAT (pandas/CSV)
  2. Execute the optimal query
  3. Show results + explain the routing decision

Type 'help' for example questions, 'exit' to quit.
"""


# =============================================================================
# RULE-BASED CLASSIFIER
# =============================================================================

# Signal words that suggest graph traversal is needed
GRAPH_SIGNALS = [
    # Relationship/traversal
    r'\b(connect|linked|related|path|traverse|hop|neighbor)\w*\b',
    r'\b(responded together|co.respond|same incident)\b',
    r'\b(through|via|across)\b',
    # Graph entities requiring multi-hop
    r'\b(neighborhood|barrio|district|distrito)\w*\b',
    r'\b(unit type|tipo de unidad)\w*\b',
    r'\b(call type|tipo de llamada|incident type|concept)\w*\b',
    r'\b(agency|agencia)\w*\b',
    r'\b(response time|tiempo de respuesta)\w*\b',
    # Comparative / pattern
    r'\b(most common|most frequent|pattern|trend over)\w*\b',
    r'\b(which (neighborhood|district|unit|agency))\b',
    r'\b(top \d+ (neighborhood|district|unit|barrio))\b',
    r'\b(per (neighborhood|district|unit|barrio|agency|distrito))\b',
    r'\b(by (neighborhood|district|barrio|distrito))\b',
    # Multi-agency
    r'\b(police|policia|fire|bomberos|ems|both agencies)\w*\b',
    # Temporal graph patterns
    r'\b(busiest|quietest|peak|off.peak)\w*\b',
    r'\b(day of week|shift|turno|dia de la semana)\w*\b',
    r'\b(year.over.year|trend|tendencia|evolucion)\w*\b',
]

# Signal words that suggest flat aggregation is sufficient
FLAT_SIGNALS = [
    r'\b(how many|cu[aá]ntos?|cu[aá]ntas?)\b',
    r'\b(total|sum|suma)\b',
    r'\b(average|mean|median|promedio|media)\b',
    r'\b(count|contar|conteo)\b',
    r'\b(list all|lista de todos)\b',
    r'\b(what is the (number|total|count))\b',
    r'\b(percentage|porcentaje|ratio)\b',
]

# Questions that look flat but are actually graph (override flat signals)
GRAPH_OVERRIDE = [
    r'\bneighborhood\b',
    r'\bdistrict\b',
    r'\bbarrio\b',
    r'\bdistrito\b',
    r'\bunit type\b',
    r'\bcall type\b',
    r'\bresponse time\b',
    r'\bpolice\b',
    r'\bpolicia\b',
]


def classify(question: str) -> dict:
    """
    Classify a question as GRAPH or FLAT.
    Returns dict with: route, confidence, graph_signals, flat_signals, reasoning
    """
    q = question.lower()

    graph_hits = []
    for pattern in GRAPH_SIGNALS:
        match = re.search(pattern, q, re.IGNORECASE)
        if match:
            graph_hits.append(match.group(0))

    flat_hits = []
    for pattern in FLAT_SIGNALS:
        match = re.search(pattern, q, re.IGNORECASE)
        if match:
            flat_hits.append(match.group(0))

    override_hits = []
    for pattern in GRAPH_OVERRIDE:
        match = re.search(pattern, q, re.IGNORECASE)
        if match:
            override_hits.append(match.group(0))

    graph_score = len(graph_hits) * 2 + len(override_hits) * 3
    flat_score  = max(0, len(flat_hits) * 2 - len(override_hits) * 3)

    if graph_score > flat_score:
        route      = "GRAPH"
        confidence = min(99, 50 + (graph_score - flat_score) * 10)
        reasoning  = (
            f"Detected relational signals: {', '.join(set(graph_hits + override_hits))}. "
            f"This question requires traversing relationships between nodes "
            f"— optimal for a graph database."
        )
    elif flat_score > graph_score:
        route      = "FLAT"
        confidence = min(99, 50 + (flat_score - graph_score) * 10)
        reasoning  = (
            f"Detected aggregation signals: {', '.join(set(flat_hits))}. "
            f"No relationship traversal needed "
            f"— a simple pandas aggregation is faster and sufficient."
        )
    else:
        # Tie → default to GRAPH (richer answer)
        route      = "GRAPH"
        confidence = 50
        reasoning  = (
            "No clear signal either way. Defaulting to GRAPH "
            "for a richer, relationship-aware answer."
        )

    return {
        "route":        route,
        "confidence":   confidence,
        "graph_signals": list(set(graph_hits + override_hits)),
        "flat_signals":  list(set(flat_hits)),
        "reasoning":    reasoning,
    }


# =============================================================================
# QUERY CATALOG
# Predefined Cypher and pandas queries mapped to question patterns
# =============================================================================

CYPHER_CATALOG = [
    {
        "patterns": [r"top.*neighborhood", r"barrio.*m[aá]s", r"neighborhood.*incident",
                     r"busiest.*neighborhood", r"most.*incident.*neighborhood"],
        "description": "Top neighborhoods by incident volume",
        "cypher": """
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
RETURN n.uri AS neighborhood, count(i) AS incidents
ORDER BY incidents DESC LIMIT 10
""",
    },
    {
        "patterns": [r"response time.*district", r"district.*response time",
                     r"tiempo.*distrito", r"distrito.*tiempo"],
        "description": "Average police response time by district",
        "cypher": """
MATCH (r:PoliceResponse)-[:RESPONSE_TO]->(i:Incident)-[:IN_POLICE_DISTRICT]->(d:PoliceDistrict)
WHERE r.responseTimeMinutes > 0 AND r.responseTimeMinutes < 120
RETURN d.uri AS district,
       round(avg(r.responseTimeMinutes) * 10) / 10 AS avg_min,
       count(r) AS responses
ORDER BY avg_min ASC
""",
    },
    {
        "patterns": [r"medical.*neighborhood", r"neighborhood.*medical",
                     r"emergencia.*m[eé]dica", r"m[eé]dica.*barrio"],
        "description": "Medical emergencies by neighborhood",
        "cypher": """
MATCH (i:Incident)-[:HAS_CALL_TYPE]->(ct:CallType)-[:REALIZATION_OF]->(c:IncidentConcept)
MATCH (i)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
WHERE c.uri = 'Concept_MEDICAL_EMERGENCY'
RETURN n.uri AS neighborhood, count(i) AS medical_incidents
ORDER BY medical_incidents DESC LIMIT 10
""",
    },
    {
        "patterns": [r"busiest.*time", r"peak.*hour", r"shift", r"turno",
                     r"day of week", r"d[ií]a.*semana", r"when.*most"],
        "description": "Busiest time windows",
        "cypher": """
MATCH (i:Incident)-[:IN_TIME_WINDOW]->(tw:TimeWindow)
RETURN tw.shift AS shift, tw.dayOfWeek AS day_of_week, count(i) AS incidents
ORDER BY incidents DESC LIMIT 10
""",
    },
    {
        "patterns": [r"year.*trend", r"tendencia.*a[nñ]o", r"year.over.year",
                     r"evolution", r"evoluci[oó]n", r"2020.*2026", r"annual"],
        "description": "Year-over-year incident trend",
        "cypher": """
MATCH (i:Incident)-[:IN_TIME_WINDOW]->(tw:TimeWindow)
WHERE tw.year >= '2020'
RETURN tw.year AS year, count(i) AS incidents
ORDER BY year
""",
    },
    {
        "patterns": [r"unit.*response time", r"response time.*unit",
                     r"unidad.*tiempo", r"slowest.*unit", r"fastest.*unit"],
        "description": "Fire units by average response time",
        "cypher": """
MATCH (u:Unit)
WITH u
MATCH (r:Response)-[:HAS_UNIT]->(u)
WHERE r.responseTimeMinutes > 0 AND r.responseTimeMinutes < 60
WITH u, avg(r.responseTimeMinutes) AS avg_min, count(r) AS deployments
WHERE deployments > 100
RETURN u.uri AS unit, u.unitType AS type,
       round(avg_min * 10) / 10 AS avg_min, deployments
ORDER BY avg_min DESC LIMIT 10
""",
    },
    {
        "patterns": [r"most common.*call type", r"top.*call type",
                     r"frequent.*call type", r"call type.*common",
                     r"tipo.*llamada.*com[uú]n", r"llamada.*frecuente"],
        "description": "Most common call types across all incidents",
        "cypher": """
MATCH (i:Incident)-[:HAS_CALL_TYPE]->(ct:CallType)
RETURN ct.uri AS call_type, count(i) AS incidents
ORDER BY incidents DESC LIMIT 15
""",
    },
    {
        "patterns": [r"call type.*concept", r"traffic.*collision.*map",
                     r"unif.*vocabular", r"same concept", r"realization"],
        "description": "Call types mapped to shared IncidentConcept",
        "cypher": """
MATCH (ct:CallType)-[:REALIZATION_OF]->(c:IncidentConcept)
RETURN c.uri AS concept, collect(ct.uri) AS call_types, count(ct) AS count
ORDER BY count DESC
""",
    },
    {
        "patterns": [r"what.*graph", r"how many node", r"graph.*summary",
                     r"schema", r"resumen.*grafo", r"cu[aá]ntos.*nodo"],
        "description": "Graph schema summary",
        "cypher": """
MATCH (n) RETURN labels(n)[0] AS node_type, count(n) AS count ORDER BY count DESC
""",
    },
]

PANDAS_CATALOG = [
    {
        "patterns": [r"how many.*incident", r"total.*incident",
                     r"cu[aá]ntos.*incidente", r"total.*llamada"],
        "description": "Total number of incidents in fire dataset",
        "pandas": lambda df: pd.DataFrame({
            "metric": ["Total incidents"],
            "value":  [len(df)]
        }),
    },
    {
        "patterns": [r"average.*resolution", r"mean.*resolution",
                     r"promedio.*resoluci[oó]n", r"average.*time"],
        "description": "Average resolution time across all incidents",
        "pandas": lambda df: df[["Call Number"]].assign(
            avg_resolution=df.get("Available DtTm", pd.Series(dtype=str))
        ).head(0).assign(
            note=["Resolution time requires timestamp parsing — use graph Q for this"]
        ),
    },
    {
        "patterns": [r"most common.*call type", r"top.*call type",
                     r"tipo.*llamada.*m[aá]s.*com[uú]n", r"frecuente.*tipo"],
        "description": "Most common call types in fire dataset",
        "pandas": lambda df: (
            df["Call Type"].value_counts().head(10)
            .reset_index().rename(columns={"index": "call_type", "Call Type": "count"})
        ),
    },
    {
        "patterns": [r"how many.*unit", r"total.*unit", r"cu[aá]ntas.*unidad"],
        "description": "Number of unique units in fire dataset",
        "pandas": lambda df: pd.DataFrame({
            "metric": ["Unique units"],
            "value":  [df["Unit ID"].nunique()]
        }),
    },
    {
        "patterns": [r"priority.*distribution", r"distribuci[oó]n.*prioridad",
                     r"how many.*priority", r"breakdown.*priority"],
        "description": "Incident distribution by priority",
        "pandas": lambda df: (
            df["Final Priority"].value_counts()
            .reset_index().rename(columns={"index": "priority", "Final Priority": "count"})
        ),
    },
]


# =============================================================================
# QUERY EXECUTOR
# =============================================================================

def find_query(question: str, catalog: list):
    """Find the best matching query in a catalog."""
    q = question.lower()
    for entry in catalog:
        for pattern in entry["patterns"]:
            if re.search(pattern, q, re.IGNORECASE):
                return entry
    return None


def execute_graph(question: str, graph, df=None) -> None:
    """Find and execute the best Cypher query. Falls back to flat if no match."""
    entry = find_query(question, CYPHER_CATALOG)

    if entry is None:
        if df is not None:
            flat_entry = find_query(question, PANDAS_CATALOG)
            if flat_entry is not None:
                print("\n  ↩️  No Cypher match — falling back to FLAT (pandas)")
                execute_flat(question, df)
                return
        print("\n  ⚠️  No predefined query matches this question.")
        print("  Try one of these:")
        for e in CYPHER_CATALOG:
            print(f"    • {e['description']}")
        return

    print(f"\n  📊 Query: {entry['description']}")
    print(f"  Cypher:")
    for line in entry['cypher'].strip().split('\n'):
        print(f"    {line}")

    start = time.time()
    try:
        result = graph.query(entry['cypher'], timeout=TIMEOUT)
        elapsed = time.time() - start
        rows = result.result_set

        if not rows:
            print("\n  (no results)")
            return

        print(f"\n  Results ({len(rows)} rows in {elapsed:.2f}s):")
        header = result.header
        col_widths = [max(len(str(h)), 10) for h in header]
        for row in rows[:15]:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)))

        print('  ' + '  '.join(str(h).ljust(col_widths[i]) for i, h in enumerate(header)))
        print('  ' + '─' * (sum(col_widths) + 2 * len(col_widths)))
        for row in rows[:15]:
            print('  ' + '  '.join(str(v).ljust(col_widths[i]) for i, v in enumerate(row)))
        if len(rows) > 15:
            print(f"  ... and {len(rows) - 15} more rows")

    except Exception as e:
        print(f"\n  ERROR: {e}")


def execute_flat(question: str, df: pd.DataFrame) -> None:
    """Find and execute the best pandas query for the question."""
    entry = find_query(question, PANDAS_CATALOG)

    if entry is None:
        print("\n  ⚠️  No predefined pandas query matches this question.")
        print("  Try one of these flat questions:")
        for e in PANDAS_CATALOG:
            print(f"    • {e['description']}")
        return

    print(f"\n  📊 Query: {entry['description']}")
    print(f"  Method: pandas aggregation on fire_calls CSV ({len(df):,} rows)")

    start = time.time()
    try:
        result_df = entry['pandas'](df)
        elapsed = time.time() - start

        print(f"\n  Results ({len(result_df)} rows in {elapsed:.3f}s):")
        print(result_df.to_string(index=False))

    except Exception as e:
        print(f"\n  ERROR: {e}")


# =============================================================================
# HELP TEXT
# =============================================================================

HELP_TEXT = """
  EXAMPLE QUESTIONS — GRAPH route (FalkorDB/Cypher):
  ─────────────────────────────────────────────────
  • Which neighborhoods have the most incidents?
  • What is the average response time per police district?
  • Show medical emergencies by neighborhood
  • When is SF busiest? (shift and day of week)
  • Year-over-year incident trend from 2020 to 2026
  • Which fire units have the slowest response time?
  • How do call types map to shared concepts?
  • What's in the graph? (schema summary)

  EXAMPLE QUESTIONS — FLAT route (pandas/CSV):
  ─────────────────────────────────────────────
  • How many incidents are in the fire dataset?
  • What are the most common call types?
  • How many unique units are there?
  • What is the priority distribution?

  COMMANDS:
  • help    — show this help
  • exit    — quit
  • why     — explain last routing decision in detail
"""


# =============================================================================
# MAIN CLI LOOP
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='SF Emergency KG — Smart Query Router')
    parser.add_argument('--csv',   default=DEFAULT_CSV, help='Path to fire calls CSV')
    parser.add_argument('--graph', default=GRAPH_NAME,  help='FalkorDB graph name')
    parser.add_argument('--host',  default=FALKOR_HOST)
    parser.add_argument('--port',  type=int, default=FALKOR_PORT)
    args = parser.parse_args()

    print(BANNER)

    # Connect to FalkorDB
    print("  Connecting to FalkorDB...", end=' ')
    try:
        db    = FalkorDB(host=args.host, port=args.port)
        graph = db.select_graph(args.graph)
        print("✓")
    except Exception as e:
        print(f"✗\n  ERROR: {e}")
        print("  Make sure FalkorDB is running: docker-compose up -d")
        sys.exit(1)

    # Load CSV for flat queries
    csv_path = Path(args.csv)
    df = None
    if csv_path.exists():
        print(f"  Loading CSV ({csv_path.name})...", end=' ')
        try:
            df = pd.read_csv(csv_path, low_memory=False)
            print(f"✓ ({len(df):,} rows)")
        except Exception as e:
            print(f"✗ ({e}) — flat queries will be unavailable")
    else:
        print(f"  CSV not found at {csv_path} — flat queries unavailable")

    print()
    last_classification = None

    while True:
        try:
            question = input("  ❯ ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Bye!")
            break

        if not question:
            continue

        if question.lower() == 'exit':
            print("  Bye!")
            break

        if question.lower() == 'help':
            print(HELP_TEXT)
            continue

        if question.lower() == 'why' and last_classification:
            c = last_classification
            print(f"\n  Last routing decision:")
            print(f"  Route:      {c['route']} (confidence: {c['confidence']}%)")
            print(f"  Reasoning:  {c['reasoning']}")
            if c['graph_signals']:
                print(f"  Graph signals detected: {c['graph_signals']}")
            if c['flat_signals']:
                print(f"  Flat signals detected:  {c['flat_signals']}")
            print()
            continue

        # Classify
        classification = classify(question)
        last_classification = classification

        route      = classification['route']
        confidence = classification['confidence']

        print(f"\n  ┌─ ROUTER DECISION ──────────────────────────────────┐")
        print(f"  │  Route:      {route:<10}  Confidence: {confidence}%          │")
        print(f"  │  Reasoning:  {classification['reasoning'][:52]}  │")
        print(f"  └────────────────────────────────────────────────────┘")

        if route == "GRAPH":
            execute_graph(question, graph, df)
        else:
            if df is None:
                print("\n  CSV not available — routing to GRAPH instead.")
                execute_graph(question, graph, df)
            else:
                execute_flat(question, df)

        print()


if __name__ == '__main__':
    main()