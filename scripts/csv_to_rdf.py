#!/usr/bin/env python3
"""
SF Emergency Services Knowledge Graph — Layer 1 Pipeline
=========================================================
Converts SF Open Data CSVs into RDF/Turtle aligned with the EMS ontology.

Usage:
    python csv_to_rdf.py --input data/raw/fire_calls.csv --output data/rdf/ --limit 10000

The pipeline:
    1. Reads the Fire Dept Calls for Service CSV
    2. Groups rows by Call Number (CAD#) → one Incident per group
    3. Each row within a group becomes a Response
    4. Extracts unique Units, Locations, Neighborhoods, StationAreas, Battalions
    5. Maps Call Types to IncidentConcept via alignment table
    6. Derives TimeWindow from timestamps
    7. Outputs everything as Turtle (.ttl) files

This script has NO external dependencies beyond Python 3.8+ standard library.
No rdflib needed — we generate Turtle strings directly for maximum portability.
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

# Call Type → IncidentConcept mapping
# Extend this as you encounter new call types in the data
CALL_TYPE_CONCEPT_MAP = {
    # Fire-related
    "Structure Fire": "STRUCTURE_FIRE",
    "Outside Fire": "STRUCTURE_FIRE",
    "Vehicle Fire": "VEHICLE_FIRE",
    "Alarms": "ALARM",
    "Smoke Investigation (Outside)": "ALARM",
    "HazMat": "HAZMAT",
    # Medical
    "Medical Incident": "MEDICAL_EMERGENCY",
    "Citizen Assist / Service Call": "WELFARE_CHECK",
    # Traffic
    "Traffic Collision": "TRAFFIC_COLLISION",
    # Rescue
    "Water Rescue": "RESCUE",
    "Elevator / Escalator Rescue": "RESCUE",
    "Extrication / Entrapped (Machinery, Vehicle)": "RESCUE",
    "Confined Space / Structure Collapse": "RESCUE",
    # Other
    "Other": "OTHER",
    "Electrical Hazard": "HAZMAT",
    "Fuel Spill": "HAZMAT",
    "Gas Leak (Natural and LP Gases)": "HAZMAT",
    "Odor (Strange / Unknown)": "ALARM",
    "Explosion": "STRUCTURE_FIRE",
    "Industrial Accidents": "RESCUE",
    "Train / Rail Incident": "TRAFFIC_COLLISION",
    "Aircraft Emergency": "RESCUE",
    "Administrative": "OTHER",
    "Mutual Aid / Assist Outside Agency": "OTHER",
}

# Unit ID prefix → Unit Type mapping
UNIT_TYPE_MAP = {
    "E": "ENGINE",
    "T": "TRUCK",
    "M": "MEDIC",
    "BC": "CHIEF",
    "RS": "RESCUE_SQUAD",
    "HM": "HAZMAT",
    "B": "ENGINE",      # backup engine
    "AM": "AMBULANCE",
    "RA": "RESCUE_AMBULANCE",
    "RC": "RESCUE_CAPTAIN",
    "SF": "SUPPORT",
    "FB": "FIREBOAT",
    "D": "DIVISION_CHIEF",
    "C": "CHIEF",
    "XV": "EXTRA_VEHICLE",
}

# Agency assignment based on unit type
AGENCY_MAP = {
    "ENGINE": "SFFD",
    "TRUCK": "SFFD",
    "CHIEF": "SFFD",
    "RESCUE_SQUAD": "SFFD",
    "HAZMAT": "SFFD",
    "SUPPORT": "SFFD",
    "FIREBOAT": "SFFD",
    "DIVISION_CHIEF": "SFFD",
    "EXTRA_VEHICLE": "SFFD",
    "MEDIC": "EMS",
    "AMBULANCE": "EMS",
    "RESCUE_AMBULANCE": "EMS",
    "RESCUE_CAPTAIN": "EMS",
    "PRIVATE": "EMS",  # private ambulance services
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def safe_uri(text):
    """Convert a string into a safe URI fragment."""
    if not text:
        return "UNKNOWN"
    # Replace problematic characters
    safe = re.sub(r'[^a-zA-Z0-9_]', '_', str(text).strip())
    safe = re.sub(r'_+', '_', safe).strip('_')
    return safe or "UNKNOWN"


def short_hash(text, length=8):
    """Generate a short hash for uniqueness."""
    return hashlib.md5(str(text).encode()).hexdigest()[:length]


def parse_datetime(dt_str):
    """Parse SF Open Data datetime format. Tries multiple formats."""
    if not dt_str or dt_str.strip() == '':
        return None
    s = dt_str.strip().strip('"')
    formats = [
        "%Y %b %d %I:%M:%S %p",   # 2016 Apr 03 11:15:12 PM (real data)
        "%Y-%m-%dT%H:%M:%S",       # ISO format
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
    """Convert datetime to XSD format."""
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def minutes_between(dt1, dt2):
    """Calculate minutes between two datetimes."""
    if dt1 is None or dt2 is None:
        return None
    diff = (dt2 - dt1).total_seconds() / 60.0
    return round(diff, 2) if diff >= 0 else None


def get_shift(hour):
    """Determine shift from hour: MORNING (06-14), AFTERNOON (14-22), NIGHT (22-06)."""
    if 6 <= hour < 14:
        return "MORNING"
    elif 14 <= hour < 22:
        return "AFTERNOON"
    else:
        return "NIGHT"


def get_unit_type(unit_id):
    """Determine unit type from unit ID prefix."""
    if not unit_id:
        return "UNKNOWN"
    uid = unit_id.strip().upper()

    # Try longest prefixes first
    for prefix in sorted(UNIT_TYPE_MAP.keys(), key=len, reverse=True):
        if uid.startswith(prefix):
            return UNIT_TYPE_MAP[prefix]

    return "UNKNOWN"


def get_agency(unit_type):
    """Determine agency from unit type."""
    return AGENCY_MAP.get(unit_type, "SFFD")


def escape_turtle_string(s):
    """Escape a string for use in Turtle literals."""
    if not s:
        return ""
    return s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '')


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def process_fire_calls(input_path, output_dir, limit=None):
    """
    Main pipeline: reads Fire Calls CSV, groups by incident, outputs RDF.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Reading: {input_path}")

    # --- Phase 1: Read and group by Call Number ---
    incidents = defaultdict(list)
    row_count = 0

    with open(input_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            call_num = row.get('Call Number', '').strip()
            if not call_num:
                continue
            incidents[call_num].append(row)
            row_count += 1
            if limit and row_count >= limit:
                break

    print(f"  Rows read: {row_count}")
    print(f"  Unique incidents: {len(incidents)}")

    # --- Phase 2: Extract unique entities ---
    units_seen = {}         # unit_id → {type, agency}
    locations_seen = {}     # address → {lat, lng, zip, neighborhood, station, battalion}
    neighborhoods_seen = set()
    stations_seen = {}      # station → battalion
    battalions_seen = set()
    call_types_seen = set()
    time_windows_seen = {}  # key → {year, month, dow, shift}

    # --- Phase 3: Generate RDF ---
    incident_triples = []
    response_triples = []
    triple_count = 0

    for call_num, rows in incidents.items():
        first = rows[0]  # Use first row for incident-level data

        # Parse timestamps
        received_dt = parse_datetime(first.get('Received DtTm', ''))
        if received_dt is None:
            continue

        # --- Incident ---
        incident_uri = f"{PREFIX}:Incident_CAD_{safe_uri(call_num)}"
        call_type_raw = first.get('Call Type', '').strip()
        call_type_safe = safe_uri(call_type_raw)
        address = first.get('Address', '').strip()
        address_safe = safe_uri(address)
        zipcode = first.get('Zipcode of Incident', '').strip()
        priority = first.get('Final Priority', first.get('Priority', '')).strip()
        disposition = first.get('Call Final Disposition', '').strip()
        neighborhood = first.get('Neighborhooods - Analysis Boundaries', '').strip()
        if not neighborhood:
            neighborhood = first.get('Neighborhood  District', '').strip()
        if not neighborhood:
            neighborhood = first.get('Neighborhood District', '').strip()
        station = first.get('Station Area', '').strip()
        battalion = first.get('Battalion', '').strip()
        num_alarms = first.get('Number of Alarms', '1').strip()

        # Location
        lat, lng = '', ''
        location_str = first.get('case_location', '')
        if not location_str:
            location_str = first.get('Location', '')
        if location_str:
            match = re.search(r'[-]?[\d.]+', location_str)
            # Try POINT (-122.407 37.7837) format
            point_match = re.search(r'POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)', location_str)
            if point_match:
                lng, lat = point_match.group(1), point_match.group(2)
            else:
                # Try (37.7837,-122.407) format
                coord_match = re.search(r'\(?([-\d.]+),\s*([-\d.]+)\)?', location_str)
                if coord_match:
                    lat, lng = coord_match.group(1), coord_match.group(2)

        # TimeWindow
        year = received_dt.year
        month = received_dt.month
        dow = received_dt.isoweekday()  # 1=Mon, 7=Sun
        shift = get_shift(received_dt.hour)
        tw_key = f"{year}_{month:02d}_{dow}_{shift}"
        time_windows_seen[tw_key] = {'year': year, 'month': month, 'dow': dow, 'shift': shift}

        # Calculate total resolution time
        last_available = None
        for r in rows:
            avail = parse_datetime(r.get('Available DtTm', ''))
            if avail and (last_available is None or avail > last_available):
                last_available = avail
        resolution_min = minutes_between(received_dt, last_available)

        # Track entities
        call_types_seen.add(call_type_raw)
        if neighborhood:
            neighborhoods_seen.add(neighborhood)
        if station:
            stations_seen[station] = battalion
        if battalion:
            battalions_seen.add(battalion)
        if address:
            locations_seen[address] = {
                'lat': lat, 'lng': lng, 'zip': zipcode,
                'neighborhood': neighborhood, 'station': station
            }

        # Build incident triples
        lines = []
        lines.append(f'{incident_uri}')
        lines.append(f'    a {PREFIX}:Incident ;')
        date_str = received_dt.strftime('%Y-%m-%d')
        label = f'{call_type_raw} — {address} — {date_str}'
        lines.append(f'    rdfs:label "{escape_turtle_string(label)}" ;')
        lines.append(f'    {PREFIX}:cadNumber "{escape_turtle_string(call_num)}" ;')
        lines.append(f'    {PREFIX}:receivedTimestamp "{to_xsd_datetime(received_dt)}"^^xsd:dateTime ;')

        entry_dt = parse_datetime(first.get('Entry DtTm', ''))
        if entry_dt:
            lines.append(f'    {PREFIX}:entryTimestamp "{to_xsd_datetime(entry_dt)}"^^xsd:dateTime ;')

        if priority and priority.isdigit():
            lines.append(f'    {PREFIX}:priorityCode {priority} ;')

        if call_type_raw:
            lines.append(f'    {PREFIX}:hasCallType {PREFIX}:CallType_SFFD_{call_type_safe} ;')

        if address:
            lines.append(f'    {PREFIX}:hasLocation {PREFIX}:Location_{safe_uri(address)}_{short_hash(address)} ;')

        lines.append(f'    {PREFIX}:inTimeWindow {PREFIX}:TimeWindow_{tw_key} ;')

        if disposition:
            lines.append(f'    {PREFIX}:finalDisposition "{escape_turtle_string(disposition)}" ;')

        lines.append(f'    {PREFIX}:responseCount {len(rows)} ;')

        if resolution_min is not None and resolution_min > 0:
            lines.append(f'    {PREFIX}:totalResolutionMinutes {resolution_min} ;')

        if num_alarms and num_alarms.isdigit() and int(num_alarms) > 1:
            lines.append(f'    {PREFIX}:numberOfAlarms {num_alarms} ;')

        # Add response references
        response_uris = []
        for r in rows:
            uid = r.get('Unit ID', '').strip()
            if uid:
                resp_uri = f"{PREFIX}:Response_{safe_uri(call_num)}_{safe_uri(uid)}"
                response_uris.append(resp_uri)

        if response_uris:
            resp_str = ' ,\n        '.join(response_uris)
            lines.append(f'    {PREFIX}:hasResponse {resp_str} ;')

        # Close with period (replace last semicolon)
        last_line = lines[-1]
        lines[-1] = last_line[:-1] + '.'
        lines.append('')

        incident_triples.append('\n'.join(lines))
        triple_count += len(lines) - 1

        # --- Responses ---
        for r in rows:
            uid = r.get('Unit ID', '').strip()
            if not uid:
                continue

            resp_uri = f"{PREFIX}:Response_{safe_uri(call_num)}_{safe_uri(uid)}"
            unit_type = get_unit_type(uid)
            agency = get_agency(unit_type)

            # Track unit
            units_seen[uid] = {'type': unit_type, 'agency': agency}

            rlines = []
            rlines.append(f'{resp_uri}')
            rlines.append(f'    a {PREFIX}:Response ;')
            rlines.append(f'    rdfs:label "{escape_turtle_string(f"{uid} response to CAD {call_num}")}" ;')
            rlines.append(f'    {PREFIX}:responseTo {incident_uri} ;')
            rlines.append(f'    {PREFIX}:hasUnit {PREFIX}:Unit_{safe_uri(uid)} ;')

            dispatch_dt = parse_datetime(r.get('Dispatch DtTm', ''))
            enroute_dt = parse_datetime(r.get('Response DtTm', ''))
            onscene_dt = parse_datetime(r.get('On Scene DtTm', ''))
            transport_dt = parse_datetime(r.get('Transport DtTm', ''))
            avail_dt = parse_datetime(r.get('Available DtTm', ''))

            if dispatch_dt:
                rlines.append(f'    {PREFIX}:dispatchTimestamp "{to_xsd_datetime(dispatch_dt)}"^^xsd:dateTime ;')
            if enroute_dt:
                rlines.append(f'    {PREFIX}:enRouteTimestamp "{to_xsd_datetime(enroute_dt)}"^^xsd:dateTime ;')
            if onscene_dt:
                rlines.append(f'    {PREFIX}:onSceneTimestamp "{to_xsd_datetime(onscene_dt)}"^^xsd:dateTime ;')
            if transport_dt:
                rlines.append(f'    {PREFIX}:transportTimestamp "{to_xsd_datetime(transport_dt)}"^^xsd:dateTime ;')
            if avail_dt:
                rlines.append(f'    {PREFIX}:availableTimestamp "{to_xsd_datetime(avail_dt)}"^^xsd:dateTime ;')

            resp_time = minutes_between(dispatch_dt, onscene_dt)
            if resp_time is not None and resp_time >= 0:
                rlines.append(f'    {PREFIX}:responseTimeMinutes {resp_time} ;')

            last_line = rlines[-1]
            rlines[-1] = last_line[:-1] + '.'
            rlines.append('')

            response_triples.append('\n'.join(rlines))
            triple_count += len(rlines) - 1

    # --- Phase 4: Write output files ---
    prefixes = f"""@prefix {PREFIX}:   <{NAMESPACE}> .
@prefix owl:   <http://www.w3.org/2002/07/owl#> .
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
@prefix skos:  <http://www.w3.org/2004/02/skos/core#> .

"""

    # --- File 1: Incidents ---
    with open(output_dir / 'incidents.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Incidents\n")
        f.write(f"# Generated: {datetime.now().isoformat()}\n")
        f.write(f"# Source: Fire Dept Calls for Service\n")
        f.write(f"# Incidents: {len(incidents)}\n\n")
        f.write('\n'.join(incident_triples))

    # --- File 2: Responses ---
    with open(output_dir / 'responses.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Responses\n")
        f.write(f"# Generated: {datetime.now().isoformat()}\n")
        f.write(f"# Responses: {len(response_triples)}\n\n")
        f.write('\n'.join(response_triples))

    # --- File 3: Units ---
    with open(output_dir / 'units.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Units\n")
        f.write(f"# Unique units: {len(units_seen)}\n\n")
        for uid, info in sorted(units_seen.items()):
            f.write(f'{PREFIX}:Unit_{safe_uri(uid)}\n')
            f.write(f'    a {PREFIX}:Unit ;\n')
            f.write(f'    rdfs:label "{escape_turtle_string(uid)}" ;\n')
            f.write(f'    {PREFIX}:unitId "{escape_turtle_string(uid)}" ;\n')
            f.write(f'    {PREFIX}:unitType "{info["type"]}" ;\n')
            f.write(f'    {PREFIX}:belongsToAgency {PREFIX}:Agency_{info["agency"]} .\n\n')

    # --- File 4: Locations ---
    with open(output_dir / 'locations.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Locations\n")
        f.write(f"# Unique locations: {len(locations_seen)}\n\n")
        for addr, info in sorted(locations_seen.items()):
            loc_uri = f'{PREFIX}:Location_{safe_uri(addr)}_{short_hash(addr)}'
            f.write(f'{loc_uri}\n')
            f.write(f'    a {PREFIX}:Location ;\n')
            f.write(f'    rdfs:label "{escape_turtle_string(addr)}" ;\n')
            f.write(f'    {PREFIX}:address "{escape_turtle_string(addr)}" ;\n')
            if info['lat']:
                f.write(f'    {PREFIX}:latitude {info["lat"]} ;\n')
            if info['lng']:
                f.write(f'    {PREFIX}:longitude {info["lng"]} ;\n')
            if info['zip']:
                f.write(f'    {PREFIX}:zipCode "{info["zip"]}" ;\n')
            if info['neighborhood']:
                f.write(f'    {PREFIX}:inNeighborhood {PREFIX}:Neighborhood_{safe_uri(info["neighborhood"])} ;\n')
            if info['station']:
                f.write(f'    {PREFIX}:inStationArea {PREFIX}:StationArea_{safe_uri(info["station"])} ;\n')
            f.write(f'    .\n\n')  # handle trailing semicolon

    # --- File 5: Neighborhoods ---
    with open(output_dir / 'neighborhoods.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Neighborhoods\n\n")
        for n in sorted(neighborhoods_seen):
            f.write(f'{PREFIX}:Neighborhood_{safe_uri(n)}\n')
            f.write(f'    a {PREFIX}:Neighborhood ;\n')
            f.write(f'    rdfs:label "{escape_turtle_string(n)}" .\n\n')

    # --- File 6: Station Areas + Battalions ---
    with open(output_dir / 'stations.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Station Areas & Battalions\n\n")
        for battalion in sorted(battalions_seen):
            f.write(f'{PREFIX}:Battalion_{safe_uri(battalion)}\n')
            f.write(f'    a {PREFIX}:Battalion ;\n')
            f.write(f'    rdfs:label "{escape_turtle_string(battalion)}" .\n\n')
        for station, battalion in sorted(stations_seen.items()):
            f.write(f'{PREFIX}:StationArea_{safe_uri(station)}\n')
            f.write(f'    a {PREFIX}:StationArea ;\n')
            f.write(f'    rdfs:label "Station {escape_turtle_string(station)}" ;\n')
            if battalion:
                f.write(f'    {PREFIX}:inBattalion {PREFIX}:Battalion_{safe_uri(battalion)} ;\n')
            f.write(f'    .\n\n')

    # --- File 7: Call Types + Concept mappings ---
    with open(output_dir / 'call_types.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Call Types & Incident Concepts\n\n")

        # Incident Concepts
        concepts_used = set()
        for ct in sorted(call_types_seen):
            concept = CALL_TYPE_CONCEPT_MAP.get(ct, "OTHER")
            concepts_used.add(concept)

        for concept in sorted(concepts_used):
            f.write(f'{PREFIX}:Concept_{concept}\n')
            f.write(f'    a {PREFIX}:IncidentConcept ;\n')
            f.write(f'    rdfs:label "{concept.replace("_", " ").title()}" .\n\n')

        # Call Types with concept mappings
        for ct in sorted(call_types_seen):
            concept = CALL_TYPE_CONCEPT_MAP.get(ct, "OTHER")
            f.write(f'{PREFIX}:CallType_SFFD_{safe_uri(ct)}\n')
            f.write(f'    a {PREFIX}:CallType ;\n')
            f.write(f'    rdfs:label "{escape_turtle_string(ct)}" ;\n')
            f.write(f'    {PREFIX}:hasAgency {PREFIX}:Agency_SFFD ;\n')
            f.write(f'    {PREFIX}:realizationOf {PREFIX}:Concept_{concept} .\n\n')

    # --- File 8: Time Windows ---
    with open(output_dir / 'time_windows.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Time Windows\n\n")
        dow_names = {1:'Mon',2:'Tue',3:'Wed',4:'Thu',5:'Fri',6:'Sat',7:'Sun'}
        for key, info in sorted(time_windows_seen.items()):
            f.write(f'{PREFIX}:TimeWindow_{key}\n')
            f.write(f'    a {PREFIX}:TimeWindow ;\n')
            f.write(f'    rdfs:label "{info["year"]} {info["month"]:02d} {dow_names[info["dow"]]} {info["shift"]}" ;\n')
            f.write(f'    {PREFIX}:year "{info["year"]}"^^xsd:gYear ;\n')
            f.write(f'    {PREFIX}:month {info["month"]} ;\n')
            f.write(f'    {PREFIX}:dayOfWeek {info["dow"]} ;\n')
            f.write(f'    {PREFIX}:shift "{info["shift"]}" .\n\n')

    # --- Summary ---
    print(f"\n{'='*60}")
    print(f"  LAYER 1 PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"  Incidents:      {len(incidents):>8,}")
    print(f"  Responses:      {len(response_triples):>8,}")
    print(f"  Units:          {len(units_seen):>8,}")
    print(f"  Locations:      {len(locations_seen):>8,}")
    print(f"  Neighborhoods:  {len(neighborhoods_seen):>8,}")
    print(f"  Station Areas:  {len(stations_seen):>8,}")
    print(f"  Battalions:     {len(battalions_seen):>8,}")
    print(f"  Call Types:     {len(call_types_seen):>8,}")
    print(f"  Time Windows:   {len(time_windows_seen):>8,}")
    print(f"  ~Triples:       {triple_count:>8,}")
    print(f"{'='*60}")
    print(f"  Output: {output_dir}/")
    print(f"    incidents.ttl")
    print(f"    responses.ttl")
    print(f"    units.ttl")
    print(f"    locations.ttl")
    print(f"    neighborhoods.ttl")
    print(f"    stations.ttl")
    print(f"    call_types.ttl")
    print(f"    time_windows.ttl")
    print(f"{'='*60}")

    return {
        'incidents': len(incidents),
        'responses': len(response_triples),
        'units': len(units_seen),
        'locations': len(locations_seen),
        'triples': triple_count,
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SF Emergency KG — CSV to RDF Pipeline')
    parser.add_argument('--input', '-i', required=True, help='Path to Fire Calls CSV')
    parser.add_argument('--output', '-o', default='data/rdf/', help='Output directory for TTL files')
    parser.add_argument('--limit', '-l', type=int, default=None, help='Max rows to process (for testing)')
    args = parser.parse_args()

    process_fire_calls(args.input, args.output, args.limit)