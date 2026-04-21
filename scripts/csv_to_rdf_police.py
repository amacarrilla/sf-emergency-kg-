#!/usr/bin/env python3
"""
SF Emergency Services Knowledge Graph — Police Layer Pipeline
=============================================================
Converts the SFPD Law Enforcement Dispatched Calls for Service CSV
into RDF/Turtle aligned with the existing EMS ontology.

Key design principle: FUSION via CAD number
  - If a police incident shares a cad_number with a fire/EMS incident,
    the new triples attach to the SAME Incident_CAD_* URI.
  - Zero ontology changes needed — PoliceResponse, PoliceCallType, and
    PoliceDistrict are new subclasses; everything else reuses v1 patterns.

Usage:
    # Full run
    python csv_to_rdf_police.py \\
        --input  data/raw/Law_Enforcement_Dispatched_Calls_for_Service__Closed_20260329.csv \\
        --output data/rdf/

    # Quick test (first 50k rows)
    python csv_to_rdf_police.py \\
        --input  data/raw/Law_Enforcement_Dispatched_Calls_for_Service__Closed_20260329.csv \\
        --output data/rdf/ \\
        --limit 50000

Output files (all written to --output directory):
    police_incidents.ttl      — Incident nodes (new + already existing via CAD)
    police_responses.ttl      — PoliceResponse nodes
    police_call_types.ttl     — PoliceCallType nodes mapped to IncidentConcept
    police_locations.ttl      — Location nodes (intersection-based)
    police_districts.ttl      — PoliceDistrict + SupervisorDistrict nodes
    police_time_windows.ttl   — TimeWindow nodes (same pattern as v1)

No external dependencies — pure Python 3.8+ standard library.
"""

import csv
import hashlib
import re
import sys
import os
from datetime import datetime
from collections import defaultdict
from pathlib import Path
import argparse


# =============================================================================
# CONFIGURATION
# =============================================================================

NAMESPACE = "http://example.org/ems#"
PREFIX = "ems"

# -----------------------------------------------------------------------------
# Police call type → shared IncidentConcept mapping
# This is THE key alignment table — same concepts as v1, different surface forms
# -----------------------------------------------------------------------------
POLICE_CALL_TYPE_CONCEPT_MAP = {
    # Traffic
    "TRAFFIC ACCIDENT": "TRAFFIC_COLLISION",
    "TRAFFIC ACCIDENT INVESTIGATION": "TRAFFIC_COLLISION",
    "TRAFFIC ACCIDENT INJURY": "TRAFFIC_COLLISION",
    "TRAFFIC ACCIDENT MAJOR INJURY": "TRAFFIC_COLLISION",
    "TRAFFIC ACCIDENT FATAL": "TRAFFIC_COLLISION",
    "HIT AND RUN": "TRAFFIC_COLLISION",
    "PEDESTRIAN ACCIDENT": "TRAFFIC_COLLISION",
    "BICYCLE ACCIDENT": "TRAFFIC_COLLISION",
    # Medical / Welfare
    "WELL BEING CHECK": "WELFARE_CHECK",
    "PERSON DOWN": "MEDICAL_EMERGENCY",
    "UNCONSCIOUS PERSON": "MEDICAL_EMERGENCY",
    "SICK": "MEDICAL_EMERGENCY",
    "MEDICAL AID": "MEDICAL_EMERGENCY",
    "OVERDOSE": "MEDICAL_EMERGENCY",
    # Fire
    "FIRE": "STRUCTURE_FIRE",
    "STRUCTURE FIRE": "STRUCTURE_FIRE",
    "VEHICLE FIRE": "VEHICLE_FIRE",
    "EXPLOSION": "STRUCTURE_FIRE",
    # Hazmat
    "HAZMAT": "HAZMAT",
    "GAS LEAK": "HAZMAT",
    "FUEL SPILL": "HAZMAT",
    # Rescue
    "RESCUE": "RESCUE",
    "TRAPPED PERSON": "RESCUE",
    # Alarm
    "ALARM": "ALARM",
    "BURGLAR ALARM": "ALARM",
    "FIRE ALARM": "ALARM",
    # DV-related (map to WELFARE_CHECK as closest concept)
    "THREATS DV": "WELFARE_CHECK",
    "DOMESTIC VIOLENCE": "WELFARE_CHECK",
    "FAMILY DISPUTE": "WELFARE_CHECK",
    # Default
    "OTHER": "OTHER",
}

# Priority letter codes used by SFPD
PRIORITY_MAP = {
    "A": 1,   # Immediate
    "B": 2,   # Priority
    "C": 3,   # Routine
    "E": 4,   # Non-emergency
}


# =============================================================================
# HELPER FUNCTIONS (same contracts as csv_to_rdf.py)
# =============================================================================

def safe_uri(text):
    """Convert a string into a safe URI fragment."""
    if not text:
        return "UNKNOWN"
    safe = re.sub(r'[^a-zA-Z0-9_]', '_', str(text).strip())
    safe = re.sub(r'_+', '_', safe).strip('_')
    return safe or "UNKNOWN"


def short_hash(text, length=8):
    """Generate a short hash for uniqueness."""
    return hashlib.md5(str(text).encode()).hexdigest()[:length]


def parse_datetime(dt_str):
    """
    Parse datetime strings from SFPD dataset.
    Extends v1 parser with the SFPD format: '2026/01/13 09:37:06 PM'
    """
    if not dt_str or dt_str.strip() == '':
        return None
    s = dt_str.strip().strip('"')
    formats = [
        "%Y/%m/%d %I:%M:%S %p",    # 2026/01/13 09:37:06 PM  ← SFPD format
        "%Y %b %d %I:%M:%S %p",    # 2016 Apr 03 11:15:12 PM (SFFD)
        "%Y-%m-%dT%H:%M:%S",       # ISO
        "%m/%d/%Y %I:%M:%S %p",    # 04/15/2023 02:23:00 PM
        "%m/%d/%Y %H:%M:%S",       # 04/15/2023 14:23:00
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def to_xsd_datetime(dt):
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def minutes_between(dt1, dt2):
    if dt1 is None or dt2 is None:
        return None
    diff = (dt2 - dt1).total_seconds() / 60.0
    return round(diff, 2) if diff >= 0 else None


def get_shift(hour):
    if 6 <= hour < 14:
        return "MORNING"
    elif 14 <= hour < 22:
        return "AFTERNOON"
    else:
        return "NIGHT"


def escape_turtle_string(s):
    if not s:
        return ""
    return s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '')


def get_concept(call_type_desc):
    """Map a police call type description to a shared IncidentConcept."""
    if not call_type_desc:
        return "OTHER"
    upper = call_type_desc.strip().upper()
    # Exact match first
    if upper in POLICE_CALL_TYPE_CONCEPT_MAP:
        return POLICE_CALL_TYPE_CONCEPT_MAP[upper]
    # Partial match (e.g. "650DV" → "THREATS DV" not in map, fallback)
    for key, concept in POLICE_CALL_TYPE_CONCEPT_MAP.items():
        if key in upper or upper in key:
            return concept
    return "OTHER"


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def process_police_calls(input_path, output_dir, limit=None):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Reading: {input_path}")

    # -------------------------------------------------------------------------
    # Phase 1: Read CSV — one row = one police response (unlike fire where
    # multiple rows share a CAD number, here each row is already one dispatch)
    # -------------------------------------------------------------------------
    rows_by_cad = defaultdict(list)
    row_count = 0
    skipped = 0

    with open(input_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cad = row.get('cad_number', '').strip().strip('"')
            if not cad:
                skipped += 1
                continue
            rows_by_cad[cad].append(row)
            row_count += 1
            if limit and row_count >= limit:
                break

    print(f"  Rows read:        {row_count:>8,}")
    print(f"  Rows skipped:     {skipped:>8,}")
    print(f"  Unique CAD #s:    {len(rows_by_cad):>8,}")

    # -------------------------------------------------------------------------
    # Phase 2: Track unique entities
    # -------------------------------------------------------------------------
    police_districts_seen = set()
    supervisor_districts_seen = set()
    neighborhoods_seen = set()
    call_types_seen = {}     # code → desc
    time_windows_seen = {}
    locations_seen = {}      # intersection_name → {lat, lng, neighborhood, pd, sd}

    incident_triples = []
    response_triples = []
    triple_count = 0

    # For the multi-agency validation report
    multi_agency_cads = []

    # -------------------------------------------------------------------------
    # Phase 3: Generate RDF
    # -------------------------------------------------------------------------
    for cad, rows in rows_by_cad.items():
        first = rows[0]

        # Parse primary timestamp
        received_dt = parse_datetime(first.get('received_datetime', ''))
        if received_dt is None:
            # Some rows only have entry_datetime
            received_dt = parse_datetime(first.get('entry_datetime', ''))
        if received_dt is None:
            skipped += 1
            continue

        # --- Shared URI (same pattern as v1 — FUSION point) ---
        incident_uri = f"{PREFIX}:Incident_CAD_{safe_uri(cad)}"

        # --- Call type ---
        call_type_code = first.get('call_type_final', first.get('call_type_original', '')).strip().strip('"')
        call_type_desc = first.get('call_type_final_desc', first.get('call_type_original_desc', '')).strip().strip('"')
        call_type_safe = safe_uri(call_type_code)
        if call_type_code:
            call_types_seen[call_type_code] = call_type_desc

        # --- Geography ---
        intersection = first.get('intersection_name', '').strip().strip('"')
        neighborhood = first.get('analysis_neighborhood', '').strip().strip('"')
        police_district = first.get('police_district', '').strip().strip('"')
        supervisor_district = first.get('supervisor_district', '').strip().strip('"')

        if police_district:
            police_districts_seen.add(police_district)
        if supervisor_district:
            supervisor_districts_seen.add(supervisor_district)
        if neighborhood:
            neighborhoods_seen.add(neighborhood)

        # --- Coordinates from intersection_point ---
        lat, lng = '', ''
        pt = first.get('intersection_point', '').strip().strip('"')
        if pt:
            point_match = re.search(r'POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)', pt)
            if point_match:
                lng, lat = point_match.group(1), point_match.group(2)

        if intersection:
            locations_seen[intersection] = {
                'lat': lat, 'lng': lng,
                'neighborhood': neighborhood,
                'police_district': police_district,
                'supervisor_district': supervisor_district,
            }

        # --- Priority ---
        priority_code = first.get('priority_final', first.get('priority_original', '')).strip().strip('"')
        priority_num = PRIORITY_MAP.get(priority_code.upper(), None)

        # --- Disposition ---
        disposition = first.get('disposition', '').strip().strip('"')

        # --- Sensitive call flag ---
        sensitive = first.get('sensitive_call', '').strip().strip('"').lower() == 'true'

        # --- TimeWindow ---
        year = received_dt.year
        month = received_dt.month
        dow = received_dt.isoweekday()
        shift = get_shift(received_dt.hour)
        tw_key = f"{year}_{month:02d}_{dow}_{shift}"
        time_windows_seen[tw_key] = {'year': year, 'month': month, 'dow': dow, 'shift': shift}

        # --- Resolution time ---
        close_dt = parse_datetime(first.get('close_datetime', ''))
        resolution_min = minutes_between(received_dt, close_dt)

        # --- Build Incident triples ---
        # Note: we ADD police properties to the existing Incident node.
        # In Turtle, multiple .ttl files can assert triples about the same URI —
        # when loaded together into a triplestore, they merge automatically.
        lines = []
        lines.append(f'{incident_uri}')
        lines.append(f'    a {PREFIX}:Incident ;')
        date_str = received_dt.strftime('%Y-%m-%d')
        label = f'{call_type_desc or call_type_code} — {intersection or "Unknown location"} — {date_str}'
        lines.append(f'    rdfs:label "{escape_turtle_string(label)}" ;')
        lines.append(f'    {PREFIX}:cadNumber "{escape_turtle_string(cad)}" ;')
        lines.append(f'    {PREFIX}:policeReceivedTimestamp "{to_xsd_datetime(received_dt)}"^^xsd:dateTime ;')

        entry_dt = parse_datetime(first.get('entry_datetime', ''))
        if entry_dt:
            lines.append(f'    {PREFIX}:policeEntryTimestamp "{to_xsd_datetime(entry_dt)}"^^xsd:dateTime ;')

        if call_type_code:
            lines.append(f'    {PREFIX}:hasCallType {PREFIX}:CallType_SFPD_{call_type_safe} ;')

        if intersection:
            lines.append(f'    {PREFIX}:hasLocation {PREFIX}:Location_SFPD_{safe_uri(intersection)}_{short_hash(intersection)} ;')

        if police_district:
            lines.append(f'    {PREFIX}:inPoliceDistrict {PREFIX}:PoliceDistrict_{safe_uri(police_district)} ;')

        if supervisor_district:
            lines.append(f'    {PREFIX}:inSupervisorDistrict {PREFIX}:SupervisorDistrict_{safe_uri(supervisor_district)} ;')

        if neighborhood:
            lines.append(f'    {PREFIX}:inNeighborhood {PREFIX}:Neighborhood_{safe_uri(neighborhood)} ;')

        lines.append(f'    {PREFIX}:inTimeWindow {PREFIX}:TimeWindow_{tw_key} ;')

        if priority_num:
            lines.append(f'    {PREFIX}:priorityCode {priority_num} ;')
            lines.append(f'    {PREFIX}:policePriorityLetter "{escape_turtle_string(priority_code)}" ;')

        if disposition:
            lines.append(f'    {PREFIX}:policeDisposition "{escape_turtle_string(disposition)}" ;')

        if sensitive:
            lines.append(f'    {PREFIX}:sensitiveCall true ;')

        if resolution_min is not None and resolution_min > 0:
            lines.append(f'    {PREFIX}:policeResolutionMinutes {resolution_min} ;')

        # Police response references
        resp_uris = []
        for i, r in enumerate(rows):
            resp_uri = f"{PREFIX}:PoliceResponse_{safe_uri(cad)}_{i:03d}"
            resp_uris.append(resp_uri)

        if resp_uris:
            resp_str = ' ,\n        '.join(resp_uris)
            lines.append(f'    {PREFIX}:hasResponse {resp_str} ;')

        last = lines[-1]
        lines[-1] = last[:-1] + '.'
        lines.append('')
        incident_triples.append('\n'.join(lines))
        triple_count += len(lines) - 1

        # --- Build PoliceResponse triples ---
        for i, r in enumerate(rows):
            resp_uri = f"{PREFIX}:PoliceResponse_{safe_uri(cad)}_{i:03d}"
            rlines = []
            rlines.append(f'{resp_uri}')
            rlines.append(f'    a {PREFIX}:PoliceResponse, {PREFIX}:Response ;')
            rlines.append(f'    rdfs:label "{escape_turtle_string(f"Police response to CAD {cad}")}" ;')
            rlines.append(f'    {PREFIX}:responseTo {incident_uri} ;')
            rlines.append(f'    {PREFIX}:respondingAgency {PREFIX}:Agency_SFPD ;')

            dispatch_dt = parse_datetime(r.get('dispatch_datetime', ''))
            enroute_dt = parse_datetime(r.get('enroute_datetime', ''))
            onscene_dt = parse_datetime(r.get('onscene_datetime', ''))
            close_dt_r = parse_datetime(r.get('close_datetime', ''))

            if dispatch_dt:
                rlines.append(f'    {PREFIX}:dispatchTimestamp "{to_xsd_datetime(dispatch_dt)}"^^xsd:dateTime ;')
            if enroute_dt:
                rlines.append(f'    {PREFIX}:enRouteTimestamp "{to_xsd_datetime(enroute_dt)}"^^xsd:dateTime ;')
            if onscene_dt:
                rlines.append(f'    {PREFIX}:onSceneTimestamp "{to_xsd_datetime(onscene_dt)}"^^xsd:dateTime ;')

            resp_time = minutes_between(dispatch_dt, onscene_dt)
            if resp_time is not None and resp_time >= 0:
                rlines.append(f'    {PREFIX}:responseTimeMinutes {resp_time} ;')

            if close_dt_r:
                rlines.append(f'    {PREFIX}:closeTimestamp "{to_xsd_datetime(close_dt_r)}"^^xsd:dateTime ;')

            pd_incident = r.get('pd_incident_report', '').strip().strip('"')
            if pd_incident:
                rlines.append(f'    {PREFIX}:pdIncidentReport "{escape_turtle_string(pd_incident)}" ;')

            last_r = rlines[-1]
            rlines[-1] = last_r[:-1] + '.'
            rlines.append('')
            response_triples.append('\n'.join(rlines))
            triple_count += len(rlines) - 1

    # -------------------------------------------------------------------------
    # Phase 4: Write output files
    # -------------------------------------------------------------------------
    prefixes = f"""@prefix {PREFIX}:   <{NAMESPACE}> .
@prefix owl:   <http://www.w3.org/2002/07/owl#> .
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
@prefix skos:  <http://www.w3.org/2004/02/skos/core#> .

"""

    # --- police_incidents.ttl ---
    with open(output_dir / 'police_incidents.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Police Incidents\n")
        f.write(f"# Generated: {datetime.now().isoformat()}\n")
        f.write(f"# Source: Law Enforcement Dispatched Calls for Service (SFPD)\n")
        f.write(f"# Unique CAD numbers: {len(rows_by_cad)}\n")
        f.write(f"#\n")
        f.write(f"# FUSION NOTE: Incident URIs use the same ems:Incident_CAD_* pattern\n")
        f.write(f"# as incidents.ttl. When loaded together in a triplestore, triples from\n")
        f.write(f"# both files merge automatically on shared CAD numbers.\n\n")
        f.write('\n'.join(incident_triples))

    # --- police_responses.ttl ---
    with open(output_dir / 'police_responses.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Police Responses\n")
        f.write(f"# Generated: {datetime.now().isoformat()}\n")
        f.write(f"# Total responses: {len(response_triples)}\n\n")
        f.write('\n'.join(response_triples))

    # --- police_call_types.ttl ---
    with open(output_dir / 'police_call_types.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Police Call Types\n")
        f.write(f"# Each PoliceCallType maps to a shared IncidentConcept\n\n")

        concepts_used = set()
        for code, desc in call_types_seen.items():
            concepts_used.add(get_concept(desc))

        # Emit only new concepts (ones not already in call_types.ttl)
        # We mark them with a comment so it's clear which are police-specific
        f.write("# --- Shared IncidentConcept nodes (may already exist from fire data) ---\n\n")
        for concept in sorted(concepts_used):
            f.write(f'{PREFIX}:Concept_{concept}\n')
            f.write(f'    a {PREFIX}:IncidentConcept ;\n')
            f.write(f'    rdfs:label "{concept.replace("_", " ").title()}" .\n\n')

        f.write("# --- Police-specific CallType nodes ---\n\n")
        for code, desc in sorted(call_types_seen.items()):
            concept = get_concept(desc)
            f.write(f'{PREFIX}:CallType_SFPD_{safe_uri(code)}\n')
            f.write(f'    a {PREFIX}:PoliceCallType, {PREFIX}:CallType ;\n')
            f.write(f'    rdfs:label "{escape_turtle_string(desc or code)}" ;\n')
            f.write(f'    {PREFIX}:callTypeCode "{escape_turtle_string(code)}" ;\n')
            f.write(f'    {PREFIX}:hasAgency {PREFIX}:Agency_SFPD ;\n')
            f.write(f'    {PREFIX}:realizationOf {PREFIX}:Concept_{concept} .\n\n')

    # --- police_locations.ttl ---
    with open(output_dir / 'police_locations.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Police Locations (intersection-based)\n")
        f.write(f"# Unique intersections: {len(locations_seen)}\n\n")
        for intersection, info in sorted(locations_seen.items()):
            loc_uri = f'{PREFIX}:Location_SFPD_{safe_uri(intersection)}_{short_hash(intersection)}'
            f.write(f'{loc_uri}\n')
            f.write(f'    a {PREFIX}:Location ;\n')
            f.write(f'    rdfs:label "{escape_turtle_string(intersection)}" ;\n')
            f.write(f'    {PREFIX}:address "{escape_turtle_string(intersection)}" ;\n')
            if info['lat']:
                f.write(f'    {PREFIX}:latitude {info["lat"]} ;\n')
            if info['lng']:
                f.write(f'    {PREFIX}:longitude {info["lng"]} ;\n')
            if info['neighborhood']:
                f.write(f'    {PREFIX}:inNeighborhood {PREFIX}:Neighborhood_{safe_uri(info["neighborhood"])} ;\n')
            if info['police_district']:
                f.write(f'    {PREFIX}:inPoliceDistrict {PREFIX}:PoliceDistrict_{safe_uri(info["police_district"])} ;\n')
            f.write(f'    .\n\n')

    # --- police_districts.ttl ---
    with open(output_dir / 'police_districts.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Police Districts & Supervisor Districts\n\n")

        f.write("# Agency node (referenced from responses)\n")
        f.write(f'{PREFIX}:Agency_SFPD\n')
        f.write(f'    a {PREFIX}:Agency ;\n')
        f.write(f'    rdfs:label "San Francisco Police Department" ;\n')
        f.write(f'    {PREFIX}:agencyCode "SFPD" .\n\n')

        f.write("# Police Districts\n\n")
        for pd in sorted(police_districts_seen):
            f.write(f'{PREFIX}:PoliceDistrict_{safe_uri(pd)}\n')
            f.write(f'    a {PREFIX}:PoliceDistrict ;\n')
            f.write(f'    rdfs:label "{escape_turtle_string(pd)}" .\n\n')

        f.write("# Supervisor Districts\n\n")
        for sd in sorted(supervisor_districts_seen):
            if sd:
                f.write(f'{PREFIX}:SupervisorDistrict_{safe_uri(sd)}\n')
                f.write(f'    a {PREFIX}:SupervisorDistrict ;\n')
                f.write(f'    rdfs:label "Supervisor District {escape_turtle_string(sd)}" .\n\n')

    # --- police_time_windows.ttl ---
    with open(output_dir / 'police_time_windows.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Time Windows (police)\n")
        f.write(f"# Uses identical URI pattern to time_windows.ttl — merges automatically\n\n")
        dow_names = {1:'Mon',2:'Tue',3:'Wed',4:'Thu',5:'Fri',6:'Sat',7:'Sun'}
        for key, info in sorted(time_windows_seen.items()):
            f.write(f'{PREFIX}:TimeWindow_{key}\n')
            f.write(f'    a {PREFIX}:TimeWindow ;\n')
            f.write(f'    rdfs:label "{info["year"]} {info["month"]:02d} {dow_names[info["dow"]]} {info["shift"]}" ;\n')
            f.write(f'    {PREFIX}:year "{info["year"]}"^^xsd:gYear ;\n')
            f.write(f'    {PREFIX}:month {info["month"]} ;\n')
            f.write(f'    {PREFIX}:dayOfWeek {info["dow"]} ;\n')
            f.write(f'    {PREFIX}:shift "{info["shift"]}" .\n\n')

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"  POLICE LAYER PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"  Unique CAD numbers: {len(rows_by_cad):>8,}")
    print(f"  Police Responses:   {len(response_triples):>8,}")
    print(f"  Call Types:         {len(call_types_seen):>8,}")
    print(f"  Locations:          {len(locations_seen):>8,}")
    print(f"  Police Districts:   {len(police_districts_seen):>8,}")
    print(f"  Supervisor Dist.:   {len(supervisor_districts_seen):>8,}")
    print(f"  Neighborhoods:      {len(neighborhoods_seen):>8,}")
    print(f"  Time Windows:       {len(time_windows_seen):>8,}")
    print(f"  ~Triples:           {triple_count:>8,}")
    print(f"{'='*60}")
    print(f"  Output files in: {output_dir}/")
    print(f"    police_incidents.ttl")
    print(f"    police_responses.ttl")
    print(f"    police_call_types.ttl")
    print(f"    police_locations.ttl")
    print(f"    police_districts.ttl")
    print(f"    police_time_windows.ttl")
    print(f"{'='*60}")
    print(f"\n  MULTI-AGENCY FUSION:")
    print(f"  Any CAD number that appears in BOTH incidents.ttl and")
    print(f"  police_incidents.ttl will automatically merge into one")
    print(f"  Incident node when loaded into a triplestore.")
    print(f"  Run validate_fusion.py to count shared CAD numbers.")
    print(f"{'='*60}")

    return {
        'cad_numbers': len(rows_by_cad),
        'responses': len(response_triples),
        'call_types': len(call_types_seen),
        'locations': len(locations_seen),
        'triples': triple_count,
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='SF Emergency KG — Police CSV to RDF Pipeline'
    )
    parser.add_argument('--input', '-i', required=True,
                        help='Path to Law Enforcement Dispatched Calls CSV')
    parser.add_argument('--output', '-o', default='data/rdf/',
                        help='Output directory for TTL files')
    parser.add_argument('--limit', '-l', type=int, default=None,
                        help='Max rows to process (for testing)')
    args = parser.parse_args()

    process_police_calls(args.input, args.output, args.limit)