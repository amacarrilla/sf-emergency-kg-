#!/usr/bin/env python3
"""
SF Emergency KG — Export incidents to JSON for map visualization
=================================================================
Reads the Layer 1 + Layer 2 TTL files and exports a lightweight JSON
with only the fields needed for the interactive map.

Usage:
    python3 scripts/export_map_data.py --rdf-dir data/rdf/ --inferred-dir data/rdf/inferred/ --output docs/map_data.json
    python3 scripts/export_map_data.py --rdf-dir data/rdf/ --inferred-dir data/rdf/inferred/ --output docs/map_data.json --limit 100000

Output JSON format (one record per incident):
    {
      "incidents": [
        {
          "id": "231580001",
          "lat": 37.7837,
          "lng": -122.407,
          "ts": "2023-04-15T14:23:00",
          "ct": "Medical Incident",
          "pr": 3,
          "rc": 3,
          "sev": "MODERATE",
          "ma": true,
          "rm": 47.5,
          "addr": "MARKET ST/5TH ST",
          "nb": "South of Market",
          "disp": "Code 2 Transport"
        }, ...
      ],
      "stats": { ... }
    }
"""

import re
import json
import sys
from datetime import datetime
from collections import defaultdict, Counter
from pathlib import Path
import argparse


def parse_and_export(rdf_dir, inferred_dir, output_path, limit=None):
    rdf_dir = Path(rdf_dir)
    inferred_dir = Path(inferred_dir)

    print("Exporting map data from knowledge graph...")

    # --- Step 1: Parse locations (address → lat/lng/neighborhood) ---
    print("  Parsing locations...")
    locations = {}  # location_uri_suffix → {lat, lng, addr, neighborhood}
    current_key = None
    current = {}

    with open(rdf_dir / 'locations.ttl', 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('@'):
                continue

            if line.startswith('ems:Location_'):
                if current_key and current.get('lat'):
                    locations[current_key] = current
                current_key = line.rstrip(' ;.').replace('ems:Location_', '')
                current = {}
                continue

            if not current_key:
                continue

            if 'ems:latitude' in line:
                m = re.search(r'latitude\s+([-\d.]+)', line)
                if m:
                    try:
                        current['lat'] = round(float(m.group(1)), 5)
                    except ValueError:
                        pass

            elif 'ems:longitude' in line:
                m = re.search(r'longitude\s+([-\d.]+)', line)
                if m:
                    try:
                        current['lng'] = round(float(m.group(1)), 5)
                    except ValueError:
                        pass

            elif 'ems:address' in line:
                m = re.search(r'address\s+"([^"]*)"', line)
                if m: current['addr'] = m.group(1)

            elif 'ems:inNeighborhood' in line:
                m = re.search(r'Neighborhood_(\S+)', line)
                if m: current['nb'] = m.group(1).rstrip(' ;.').replace('_', ' ')

    if current_key and current.get('lat'):
        locations[current_key] = current

    print(f"    {len(locations):,} locations with coordinates")

    # --- Step 2: Parse call types (uri → label) ---
    print("  Parsing call types...")
    call_type_labels = {}

    with open(rdf_dir / 'call_types.ttl', 'r', encoding='utf-8') as f:
        current_key = None
        for line in f:
            line = line.strip()
            if line.startswith('ems:CallType_SFFD_'):
                current_key = line.rstrip(' ;.').replace('ems:CallType_SFFD_', '')
            elif current_key and 'rdfs:label' in line:
                m = re.search(r'label\s+"([^"]*)"', line)
                if m:
                    call_type_labels[current_key] = m.group(1)
                    current_key = None

    print(f"    {len(call_type_labels)} call type labels")

    # --- Step 3: Parse incidents ---
    print("  Parsing incidents...")
    incidents_raw = {}
    current_uri = None
    current = {}
    count = 0

    with open(rdf_dir / 'incidents.ttl', 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('@'):
                continue

            if line.startswith('ems:Incident_CAD_') and 'a ems:' not in line:
                if current_uri and current:
                    incidents_raw[current_uri] = current
                    count += 1
                    if limit and count >= limit:
                        break
                m = re.search(r'Incident_CAD_(\S+)', line)
                current_uri = m.group(1).rstrip(' ;.') if m else None
                current = {}
                continue

            if not current_uri:
                continue

            if 'ems:cadNumber' in line:
                m = re.search(r'cadNumber\s+"([^"]*)"', line)
                if m: current['id'] = m.group(1)

            elif 'ems:receivedTimestamp' in line:
                m = re.search(r'"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"', line)
                if m: current['ts'] = m.group(1)

            elif 'ems:hasCallType' in line:
                m = re.search(r'CallType_SFFD_(\S+)', line)
                if m: current['ct_key'] = m.group(1).rstrip(' ;.')

            elif 'ems:priorityCode' in line:
                m = re.search(r'priorityCode\s+(\d+)', line)
                if m: current['pr'] = int(m.group(1))

            elif 'ems:responseCount' in line:
                m = re.search(r'responseCount\s+(\d+)', line)
                if m: current['rc'] = int(m.group(1))

            elif 'ems:totalResolutionMinutes' in line:
                m = re.search(r'totalResolutionMinutes\s+([\d.]+)', line)
                if m: current['rm'] = round(float(m.group(1)), 1)

            elif 'ems:finalDisposition' in line:
                m = re.search(r'finalDisposition\s+"([^"]*)"', line)
                if m: current['disp'] = m.group(1)

            elif 'ems:hasLocation' in line:
                m = re.search(r'Location_(\S+)', line)
                if m: current['loc_key'] = m.group(1).rstrip(' ;.')

    # Last one
    if current_uri and current:
        incidents_raw[current_uri] = current

    print(f"    {len(incidents_raw):,} incidents parsed")

    # --- Step 4: Parse inferred properties ---
    print("  Parsing inferred properties...")
    inferred_file = inferred_dir / 'inferred_incident_properties.ttl'
    inferred = {}

    if inferred_file.exists():
        current_key = None
        current_inf = {}

        with open(inferred_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or line.startswith('@'):
                    continue

                if line.startswith('ems:Incident_CAD_'):
                    if current_key and current_inf:
                        inferred[current_key] = current_inf
                    m = re.search(r'Incident_CAD_(\S+)', line)
                    current_key = m.group(1).rstrip(' ;.') if m else None
                    current_inf = {}
                    continue

                if not current_key:
                    continue

                if 'ems:isMultiAgency' in line:
                    current_inf['ma'] = 'true' in line

                elif 'ems:hasSeverity' in line:
                    m = re.search(r'Severity_(\w+)', line)
                    if m: current_inf['sev'] = m.group(1)

        if current_key and current_inf:
            inferred[current_key] = current_inf

        print(f"    {len(inferred):,} inferred records")

    # --- Step 5: Join everything into output records ---
    print("  Joining data...")
    output_incidents = []
    skipped_no_loc = 0
    skipped_no_coords = 0

    for uri_key, inc in incidents_raw.items():
        loc_key = inc.get('loc_key', '')
        loc = locations.get(loc_key)

        if not loc:
            skipped_no_loc += 1
            continue

        if not loc.get('lat') or not loc.get('lng'):
            skipped_no_coords += 1
            continue

        # Get call type label
        ct_key = inc.get('ct_key', '')
        ct_label = call_type_labels.get(ct_key, ct_key.replace('_', ' '))

        # Get inferred props
        inf = inferred.get(uri_key, {})

        record = {
            'id': inc.get('id', ''),
            'lat': loc['lat'],
            'lng': loc['lng'],
            'ts': inc.get('ts', ''),
            'ct': ct_label,
            'pr': inc.get('pr', 2),
            'rc': inc.get('rc', 1),
            'sev': inf.get('sev', ''),
            'ma': inf.get('ma', False),
            'addr': loc.get('addr', ''),
            'nb': loc.get('nb', ''),
        }

        # Only include optional fields if they have values
        if 'rm' in inc:
            record['rm'] = inc['rm']
        if 'disp' in inc:
            record['disp'] = inc['disp']

        output_incidents.append(record)

    print(f"    Exported: {len(output_incidents):,}")
    print(f"    Skipped (no location): {skipped_no_loc:,}")
    print(f"    Skipped (no coords): {skipped_no_coords:,}")

    # --- Step 6: Compute summary stats ---
    severity_counts = Counter(r['sev'] for r in output_incidents if r['sev'])
    call_type_counts = Counter(r['ct'] for r in output_incidents)
    neighborhood_counts = Counter(r['nb'] for r in output_incidents if r['nb'])
    multi_agency_count = sum(1 for r in output_incidents if r['ma'])

    # Date range
    dates = [r['ts'][:10] for r in output_incidents if r['ts']]
    min_date = min(dates) if dates else ''
    max_date = max(dates) if dates else ''

    stats = {
        'total': len(output_incidents),
        'multi_agency': multi_agency_count,
        'date_min': min_date,
        'date_max': max_date,
        'severity': dict(severity_counts.most_common()),
        'call_types': dict(call_type_counts.most_common(20)),
        'neighborhoods': dict(neighborhood_counts.most_common()),
    }

    # --- Step 7: Write JSON ---
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    output = {
        'generated': datetime.now().isoformat(),
        'stats': stats,
        'incidents': output_incidents,
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, separators=(',', ':'))

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\n  Output: {output_path} ({size_mb:.1f} MB)")
    print(f"  Incidents: {len(output_incidents):,}")
    print(f"  Date range: {min_date} → {max_date}")
    print(f"  Top call types: {', '.join(list(call_type_counts.keys())[:5])}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Export KG incidents to JSON for map')
    parser.add_argument('--rdf-dir', default='data/rdf/', help='Layer 1 RDF directory')
    parser.add_argument('--inferred-dir', default='data/rdf/inferred/', help='Layer 2 directory')
    parser.add_argument('--output', '-o', default='docs/map_data.json', help='Output JSON path')
    parser.add_argument('--limit', '-l', type=int, default=None, help='Max incidents to export')
    args = parser.parse_args()

    parse_and_export(args.rdf_dir, args.inferred_dir, args.output, args.limit)