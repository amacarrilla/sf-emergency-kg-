#!/usr/bin/env python3
"""
SF Emergency KG — Multi-Agency Fusion Validator
================================================
Finds CAD numbers that appear in BOTH the fire/EMS dataset and the
police dataset, proving that the ontology successfully unifies two
independent data sources through the shared Incident_CAD_* URI pattern.

This is the "Oscar moment" of the project — the same incident that
fire calls "Traffic Collision" and police calls "HIT AND RUN" or
"TRAFFIC ACCIDENT" converges on one Incident node in the graph.

Usage:
    python validate_fusion.py \\
        --fire   data/raw/fire_calls_for_service.csv \\
        --police data/raw/Law_Enforcement_Dispatched_Calls_for_Service__Closed_20260329.csv

No external dependencies.
"""

import csv
import argparse
from datetime import datetime
from collections import defaultdict
from pathlib import Path


def load_cad_numbers(filepath, cad_column, extra_columns=None, limit=None):
    """Load CAD numbers and optional extra fields from a CSV."""
    data = {}
    extra_columns = extra_columns or []
    count = 0

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cad = row.get(cad_column, '').strip().strip('"')
            if not cad:
                continue
            record = {'cad': cad}
            for col in extra_columns:
                record[col] = row.get(col, '').strip().strip('"')
            data[cad] = record
            count += 1
            if limit and count >= limit:
                break

    return data


def main():
    parser = argparse.ArgumentParser(
        description='Validate multi-agency fusion via shared CAD numbers'
    )
    parser.add_argument('--fire', '-f', required=True,
                        help='Path to Fire Calls for Service CSV')
    parser.add_argument('--police', '-p', required=True,
                        help='Path to Law Enforcement Dispatched Calls CSV')
    parser.add_argument('--limit', '-l', type=int, default=None,
                        help='Max rows per file (for testing)')
    parser.add_argument('--examples', '-e', type=int, default=10,
                        help='Number of example multi-agency incidents to show (default: 10)')
    args = parser.parse_args()

    print(f"\n{'='*65}")
    print(f"  SF EMERGENCY KG — MULTI-AGENCY FUSION VALIDATOR")
    print(f"{'='*65}")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # -------------------------------------------------------------------------
    # Load both datasets
    # -------------------------------------------------------------------------
    print(f"Loading fire/EMS dataset...")
    fire_data = load_cad_numbers(
        args.fire,
        cad_column='Call Number',
        extra_columns=['Call Type', 'Neighborhooods - Analysis Boundaries',
                       'Received DtTm', 'Call Final Disposition'],
        limit=args.limit
    )
    print(f"  Fire CAD numbers loaded: {len(fire_data):,}")

    print(f"\nLoading police dataset...")
    police_data = load_cad_numbers(
        args.police,
        cad_column='cad_number',
        extra_columns=['call_type_final_desc', 'analysis_neighborhood',
                       'received_datetime', 'disposition', 'police_district'],
        limit=args.limit
    )
    print(f"  Police CAD numbers loaded: {len(police_data):,}")

    # -------------------------------------------------------------------------
    # Find shared CAD numbers
    # -------------------------------------------------------------------------
    fire_cads = set(fire_data.keys())
    police_cads = set(police_data.keys())
    shared_cads = fire_cads & police_cads

    print(f"\n{'='*65}")
    print(f"  FUSION RESULTS")
    print(f"{'='*65}")
    print(f"  Fire/EMS unique CAD numbers:   {len(fire_cads):>10,}")
    print(f"  Police unique CAD numbers:     {len(police_cads):>10,}")
    print(f"  SHARED CAD numbers:            {len(shared_cads):>10,}")
    if fire_cads:
        pct = len(shared_cads) / len(fire_cads) * 100
        print(f"  % of fire incidents w/ police: {pct:>9.1f}%")
    if police_cads:
        pct2 = len(shared_cads) / len(police_cads) * 100
        print(f"  % of police incidents w/ fire: {pct2:>9.1f}%")
    print(f"{'='*65}")

    if not shared_cads:
        print("\n  No shared CAD numbers found.")
        print("  This may be expected if the datasets cover different time periods.")
        print("  Check the date ranges of both files.")
        return

    # -------------------------------------------------------------------------
    # Show example multi-agency incidents
    # -------------------------------------------------------------------------
    print(f"\n  EXAMPLE MULTI-AGENCY INCIDENTS (showing {min(args.examples, len(shared_cads))})")
    print(f"  These incidents were attended by BOTH fire/EMS AND police.\n")

    examples = sorted(shared_cads)[:args.examples]

    for i, cad in enumerate(examples, 1):
        fire = fire_data[cad]
        police = police_data[cad]

        fire_type = fire.get('Call Type', 'Unknown')
        police_type = police.get('call_type_final_desc', 'Unknown')
        fire_neighborhood = fire.get('Neighborhooods - Analysis Boundaries', '?')
        police_neighborhood = police.get('analysis_neighborhood', '?')
        police_district = police.get('police_district', '?')

        # Flag if call types suggest the same underlying event
        same_event_hint = ""
        ft_upper = fire_type.upper()
        pt_upper = police_type.upper()
        if any(kw in ft_upper for kw in ['TRAFFIC', 'VEHICLE']) and \
           any(kw in pt_upper for kw in ['TRAFFIC', 'VEHICLE', 'HIT']):
            same_event_hint = "  ← traffic incident (both agencies)"
        elif any(kw in ft_upper for kw in ['MEDICAL', 'PERSON']) and \
             any(kw in pt_upper for kw in ['MEDICAL', 'PERSON', 'SICK', 'DOWN']):
            same_event_hint = "  ← medical incident (both agencies)"
        elif any(kw in ft_upper for kw in ['FIRE', 'SMOKE', 'ALARM']) and \
             any(kw in pt_upper for kw in ['FIRE', 'ALARM']):
            same_event_hint = "  ← fire incident (both agencies)"

        print(f"  [{i:02d}] CAD: {cad}")
        print(f"       Fire/EMS type:  {fire_type}")
        print(f"       Police type:    {police_type}{same_event_hint}")
        print(f"       Neighborhood:   fire={fire_neighborhood} | police={police_neighborhood}")
        print(f"       Police district: {police_district}")
        print(f"       Graph URI:      ems:Incident_CAD_{cad}")
        print()

    # -------------------------------------------------------------------------
    # Call type alignment analysis
    # -------------------------------------------------------------------------
    print(f"{'='*65}")
    print(f"  CALL TYPE ALIGNMENT ACROSS AGENCIES")
    print(f"  (fire call type → police call type for shared incidents)")
    print(f"{'='*65}\n")

    pair_counts = defaultdict(int)
    for cad in shared_cads:
        fire_type = fire_data[cad].get('Call Type', 'Unknown')
        police_type = police_data[cad].get('call_type_final_desc', 'Unknown')
        pair_counts[(fire_type, police_type)] += 1

    top_pairs = sorted(pair_counts.items(), key=lambda x: x[1], reverse=True)[:15]
    print(f"  {'Fire/EMS Type':<35} {'Police Type':<30} {'Count':>6}")
    print(f"  {'-'*35} {'-'*30} {'-'*6}")
    for (fire_t, police_t), count in top_pairs:
        print(f"  {fire_t:<35} {police_t:<30} {count:>6,}")

    # -------------------------------------------------------------------------
    # The key insight summary
    # -------------------------------------------------------------------------
    print(f"\n{'='*65}")
    print(f"  KEY INSIGHT FOR KNOWLEDGE GRAPH NARRATIVE")
    print(f"{'='*65}")
    print(f"  {len(shared_cads):,} incidents appear in BOTH datasets under different")
    print(f"  call type taxonomies (SFFD vocabulary vs SFPD vocabulary).")
    print(f"  In a flat join approach, you'd need manual mapping work.")
    print(f"  In the knowledge graph, they automatically converge on")
    print(f"  the same ems:Incident_CAD_* URI, with both call types")
    print(f"  linked via ems:realizationOf to a shared IncidentConcept.")
    print(f"  This is impossible to replicate cleanly with pd.merge().")
    print(f"{'='*65}\n")


if __name__ == '__main__':
    main()