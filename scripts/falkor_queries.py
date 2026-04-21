#!/usr/bin/env python3
"""
SF Emergency KG — FalkorDB Query Demonstrations (v2)
=====================================================
Fixed: correct graph traversal paths + timeout=120000 on all queries.

Key path corrections:
  - Neighborhood: Incident → HAS_LOCATION → Location → IN_NEIGHBORHOOD → Neighborhood
  - PoliceDistrict: Incident → IN_POLICE_DISTRICT → PoliceDistrict (direct)
  - Concept: Incident → HAS_CALL_TYPE → CallType → REALIZATION_OF → IncidentConcept

Usage:
    python3 scripts/falkor_queries.py
    python3 scripts/falkor_queries.py --query 4
"""

import sys
import time
import argparse

try:
    from falkordb import FalkorDB
except ImportError:
    print("ERROR: pip3 install falkordb")
    sys.exit(1)

TIMEOUT = 120000  # 2 minutes per query

QUERIES = [

    {
        "id": 1,
        "title": "Top 10 neighborhoods by incident volume (fire + police unified)",
        "description": "Both agencies share the same Neighborhood nodes via Location.",
        "cypher": """
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
RETURN n.uri AS neighborhood, count(i) AS total_incidents
ORDER BY total_incidents DESC
LIMIT 10
""",
        "why_graph": "One query covers both SFFD and SFPD incidents — they share Location and Neighborhood nodes."
    },

    {
        "id": 2,
        "title": "Shared IncidentConcept: call types mapping to TRAFFIC_COLLISION",
        "description": "Fire and police use different names for the same concept.",
        "cypher": """
MATCH (ct:CallType)-[:REALIZATION_OF]->(c:IncidentConcept {uri: 'Concept_TRAFFIC_COLLISION'})
RETURN ct.uri AS call_type, ct.hasAgency AS agency
ORDER BY agency
""",
        "why_graph": "Semantic unification across agency vocabularies — impossible cleanly without an ontology layer."
    },

    {
        "id": 3,
        "title": "Top 5 police districts by average response time",
        "description": "Three hops: PoliceResponse → Incident → PoliceDistrict.",
        "cypher": """
MATCH (r:PoliceResponse)-[:RESPONSE_TO]->(i:Incident)-[:IN_POLICE_DISTRICT]->(d:PoliceDistrict)
WHERE r.responseTimeMinutes IS NOT NULL
  AND r.responseTimeMinutes > 0
  AND r.responseTimeMinutes < 120
RETURN d.uri AS district,
       round(avg(r.responseTimeMinutes) * 10) / 10 AS avg_response_min,
       count(r) AS responses
ORDER BY avg_response_min ASC
LIMIT 5
""",
        "why_graph": "Three-hop traversal in one query. SQL needs two explicit JOINs on multi-million row tables."
    },

    {
        "id": 4,
        "title": "Top 10 neighborhoods by MEDICAL_EMERGENCY incidents",
        "description": "Traverses Incident → Location → Neighborhood filtered by IncidentConcept.",
        "cypher": """
MATCH (i:Incident)-[:HAS_CALL_TYPE]->(ct:CallType)-[:REALIZATION_OF]->(c:IncidentConcept)
MATCH (i)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
WHERE c.uri = 'Concept_MEDICAL_EMERGENCY'
RETURN n.uri AS neighborhood, count(i) AS medical_incidents
ORDER BY medical_incidents DESC
LIMIT 10
""",
        "why_graph": "Works identically for fire and police incidents — both link to the shared IncidentConcept."
    },

    {
        "id": 5,
        "title": "Busiest time windows — shift and day of week",
        "description": "When does SF have the most emergencies?",
        "cypher": """
MATCH (i:Incident)-[:IN_TIME_WINDOW]->(tw:TimeWindow)
RETURN tw.shift AS shift, tw.dayOfWeek AS day_of_week, count(i) AS incidents
ORDER BY incidents DESC
LIMIT 10
""",
        "why_graph": "TimeWindow is a shared reference node — no GROUP BY on raw timestamps needed."
    },

    {
        "id": 6,
        "title": "Police call type diversity per district",
        "description": "How many distinct IncidentConcepts appear in each police district?",
        "cypher": """
MATCH (i:Incident)-[:IN_POLICE_DISTRICT]->(d:PoliceDistrict)
MATCH (i)-[:HAS_CALL_TYPE]->(ct:CallType)-[:REALIZATION_OF]->(c:IncidentConcept)
RETURN d.uri AS district,
       count(DISTINCT c.uri) AS unique_concepts,
       count(i) AS total_incidents
ORDER BY unique_concepts DESC
""",
        "why_graph": "Measures semantic diversity per district — not just syntactic call type count."
    },

    {
        "id": 7,
        "title": "Fire units with highest average response time (>100 deployments)",
        "description": "Unit performance analysis across all incidents they responded to.",
        "cypher": """
MATCH (r:Response)-[:HAS_UNIT]->(u:Unit)
WHERE r.responseTimeMinutes IS NOT NULL
  AND r.responseTimeMinutes > 0
  AND r.responseTimeMinutes < 60
WITH u, avg(r.responseTimeMinutes) AS avg_min, count(r) AS deployments
WHERE deployments > 100
RETURN u.uri AS unit,
       u.unitType AS type,
       round(avg_min * 10) / 10 AS avg_response_min,
       deployments
ORDER BY avg_response_min DESC
LIMIT 10
""",
        "why_graph": "Unit nodes aggregate performance across thousands of responses naturally."
    },

    {
        "id": 8,
        "title": "Locations with both fire AND police incidents",
        "description": "Intersections that appear in both datasets — multi-agency hotspots.",
        "cypher": """
MATCH (i1:Incident)-[:HAS_LOCATION]->(loc:Location)<-[:HAS_LOCATION]-(i2:Incident)
WHERE i1.cadNumber STARTS WITH '1'
  AND i2.cadNumber STARTS WITH '2'
WITH loc, count(DISTINCT i1) AS fire_incidents, count(DISTINCT i2) AS police_incidents
WHERE fire_incidents > 5 AND police_incidents > 5
RETURN loc.uri AS location,
       fire_incidents,
       police_incidents,
       fire_incidents + police_incidents AS total
ORDER BY total DESC
LIMIT 10
""",
        "why_graph": "Location is a shared reference node — both agencies link to the same node naturally."
    },

    {
        "id": 9,
        "title": "Year-over-year incident trend (2020-2026)",
        "description": "Temporal analysis using TimeWindow nodes.",
        "cypher": """
MATCH (i:Incident)-[:IN_TIME_WINDOW]->(tw:TimeWindow)
WHERE tw.year >= '2020'
RETURN tw.year AS year, count(i) AS incidents
ORDER BY year
""",
        "why_graph": "TimeWindow nodes make temporal aggregation a traversal, not a timestamp GROUP BY."
    },

    {
        "id": 10,
        "title": "Graph schema — node counts by type",
        "description": "Full inventory of what's in the graph.",
        "cypher": """
MATCH (n)
RETURN labels(n)[0] AS node_type, count(n) AS count
ORDER BY count DESC
""",
        "why_graph": "One query replaces SELECT COUNT(*) FROM every table."
    },
]


def run_query(graph, q, verbose=True):
    print(f"\n{'─'*60}")
    print(f"  Q{q['id']}: {q['title']}")
    if verbose:
        print(f"  {q['description']}")
    print(f"{'─'*60}")

    start = time.time()
    try:
        result = graph.query(q['cypher'], timeout=TIMEOUT)
        elapsed = time.time() - start
        rows = result.result_set

        if not rows:
            print("  (no results)")
            return

        header = result.header
        col_widths = [max(len(str(h)), 12) for h in header]
        for row in rows[:20]:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)))

        print('  ' + '  '.join(str(h).ljust(col_widths[i]) for i, h in enumerate(header)))
        print('  ' + '-' * (sum(col_widths) + 2 * len(col_widths)))
        for row in rows[:20]:
            print('  ' + '  '.join(str(v).ljust(col_widths[i]) for i, v in enumerate(row)))

        if len(rows) > 20:
            print(f"  ... and {len(rows) - 20} more rows")

        print(f"\n  ✓ {len(rows)} rows in {elapsed:.2f}s")
        if verbose:
            print(f"  WHY GRAPH: {q['why_graph']}")

    except Exception as e:
        print(f"  ERROR: {e}")


def main():
    parser = argparse.ArgumentParser(description='SF Emergency KG — Query Demonstrations')
    parser.add_argument('--graph', '-g', default='SF_KG')
    parser.add_argument('--host',        default='localhost')
    parser.add_argument('--port',  '-p', type=int, default=6379)
    parser.add_argument('--query', '-q', type=int, default=None)
    parser.add_argument('--quiet',       action='store_true')
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  SF EMERGENCY KG — QUERY DEMONSTRATIONS v2")
    print(f"{'='*60}")
    print(f"  Graph: {args.graph} @ {args.host}:{args.port}\n")

    try:
        db    = FalkorDB(host=args.host, port=args.port)
        graph = db.select_graph(args.graph)
    except Exception as e:
        print(f"  ERROR: {e}")
        sys.exit(1)

    queries_to_run = QUERIES if args.query is None else [q for q in QUERIES if q['id'] == args.query]

    for q in queries_to_run:
        run_query(graph, q, verbose=not args.quiet)

    print(f"\n{'='*60}")
    print(f"  Done. Browser: http://localhost:3000  (graph: {args.graph})")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()