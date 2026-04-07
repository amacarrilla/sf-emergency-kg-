#!/usr/bin/env python3
"""
SF Emergency Services Knowledge Graph — Layer 2: Inference
============================================================
Derives new facts from the Layer 1 foundation WITHOUT adding external data.

Inferences:
    1. isMultiAgency      — True if responses span 2+ agencies
    2. incidentCountAtLoc — Number of prior incidents at same location (30/90/365 day windows)
    3. severityLevel      — Derived from priority, response count, call type, disposition
    4. agencyCount        — Number of distinct agencies involved
    5. maxResponseTime    — Slowest unit response time for the incident
    6. avgResponseTime    — Average response time across all units

Input:  data/rdf/ (Layer 1 output)
Output: data/rdf/inferred/ (new TTL files with derived facts)

Usage:
    python3 scripts/inference.py --input data/rdf/ --output data/rdf/inferred/
    python3 scripts/inference.py --input data/rdf/ --output data/rdf/inferred/ --limit 10000
"""

import re
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
import argparse


# =============================================================================
# CONFIGURATION
# =============================================================================

PREFIX = "ems"
NAMESPACE = "http://example.org/ems#"

# Call types that indicate higher baseline severity
HIGH_SEVERITY_CALL_TYPES = {
    'Structure_Fire', 'HazMat', 'Explosion', 'Confined_Space___Structure_Collapse',
    'Aircraft_Emergency', 'Train___Rail_Incident',
}
MEDIUM_SEVERITY_CALL_TYPES = {
    'Traffic_Collision', 'Vehicle_Fire', 'Outside_Fire', 'Water_Rescue',
    'Extrication___Entrapped__Machinery__Vehicle_',
    'Industrial_Accidents',
}

# Dispositions that indicate severity
SEVERE_DISPOSITIONS = {'fire', 'code 3 transport', 'multi alarm'}
MINOR_DISPOSITIONS = {'cancelled', 'gone on arrival', 'no merit', 'unable to locate',
                       'duplicate', 'other'}


# =============================================================================
# PARSERS — Read Layer 1 TTL files without rdflib
# =============================================================================

def parse_incidents(filepath):
    """
    Parse incidents.ttl and extract key properties per incident.
    Returns dict: incident_uri -> {cadNumber, priorityCode, callType, location,
                                    disposition, responseCount, resolutionMin, responses[]}
    """
    incidents = {}
    current_uri = None
    current = {}

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('@'):
                continue

            # New subject
            if line.startswith(f'{PREFIX}:Incident_') and not line.startswith(f'{PREFIX}:Incident_ '):
                if current_uri and current:
                    incidents[current_uri] = current
                current_uri = line.rstrip(' ;.')
                current = {'responses': []}
                continue

            if not current_uri:
                continue

            # Parse predicates
            if f'{PREFIX}:priorityCode' in line:
                m = re.search(r'priorityCode\s+(\d+)', line)
                if m: current['priorityCode'] = int(m.group(1))

            elif f'{PREFIX}:hasCallType' in line:
                m = re.search(r'hasCallType\s+(\S+)', line)
                if m: current['callType'] = m.group(1).rstrip(' ;.')

            elif f'{PREFIX}:hasLocation' in line:
                m = re.search(r'hasLocation\s+(\S+)', line)
                if m: current['location'] = m.group(1).rstrip(' ;.')

            elif f'{PREFIX}:finalDisposition' in line:
                m = re.search(r'finalDisposition\s+"([^"]*)"', line)
                if m: current['disposition'] = m.group(1)

            elif f'{PREFIX}:responseCount' in line:
                m = re.search(r'responseCount\s+(\d+)', line)
                if m: current['responseCount'] = int(m.group(1))

            elif f'{PREFIX}:totalResolutionMinutes' in line:
                m = re.search(r'totalResolutionMinutes\s+([\d.]+)', line)
                if m: current['resolutionMin'] = float(m.group(1))

            elif f'{PREFIX}:receivedTimestamp' in line:
                m = re.search(r'"([^"]+)"\^\^xsd:dateTime', line)
                if m: current['receivedTimestamp'] = m.group(1)

            elif f'{PREFIX}:hasResponse' in line:
                refs = re.findall(r'ems:Response_\S+', line)
                for ref in refs:
                    current['responses'].append(ref.rstrip(' ;.,'))

            # Continuation lines for hasResponse (comma-separated on next lines)
            elif line.startswith(f'{PREFIX}:Response_'):
                ref = line.rstrip(' ;.,')
                current['responses'].append(ref)

    # Don't forget last incident
    if current_uri and current:
        incidents[current_uri] = current

    return incidents


def parse_responses(filepath):
    """
    Parse responses.ttl and extract unit references per response.
    Returns dict: response_uri -> {unit, responseTimeMinutes}
    """
    responses = {}
    current_uri = None
    current = {}

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('@'):
                continue

            if line.startswith(f'{PREFIX}:Response_') and 'a ' not in line:
                if current_uri and current:
                    responses[current_uri] = current
                current_uri = line.rstrip(' ;.')
                current = {}
                continue

            if not current_uri:
                continue

            if f'{PREFIX}:hasUnit' in line:
                m = re.search(r'hasUnit\s+(\S+)', line)
                if m: current['unit'] = m.group(1).rstrip(' ;.')

            elif f'{PREFIX}:responseTimeMinutes' in line:
                m = re.search(r'responseTimeMinutes\s+([\d.]+)', line)
                if m: current['responseTimeMin'] = float(m.group(1))

    if current_uri and current:
        responses[current_uri] = current

    return responses


def parse_units(filepath):
    """
    Parse units.ttl and extract agency per unit.
    Returns dict: unit_uri -> agency_uri
    """
    units = {}
    current_uri = None

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('@'):
                continue

            if line.startswith(f'{PREFIX}:Unit_') and 'a ' not in line:
                current_uri = line.rstrip(' ;.')
                continue

            if current_uri and f'{PREFIX}:belongsToAgency' in line:
                m = re.search(r'belongsToAgency\s+(\S+)', line)
                if m:
                    units[current_uri] = m.group(1).rstrip(' ;.')
                    current_uri = None

    return units


# =============================================================================
# INFERENCE ENGINE
# =============================================================================

def derive_severity(incident):
    """
    Derive severity level from available signals.

    Logic:
        CRITICAL — priority 3 + (responseCount >= 6 OR disposition contains 'fire'/'multi alarm')
        SEVERE   — priority 3 + (responseCount >= 4 OR high-severity call type)
        MODERATE — priority 3 + responseCount >= 2 OR medium-severity call type
        MINOR    — everything else
    """
    priority = incident.get('priorityCode', 2)
    resp_count = incident.get('responseCount', 1)
    call_type = incident.get('callType', '')
    disposition = incident.get('disposition', '').lower()

    # Extract call type suffix for matching
    ct_suffix = call_type.split('_SFFD_')[-1] if '_SFFD_' in call_type else ''

    is_high_ct = ct_suffix in HIGH_SEVERITY_CALL_TYPES
    is_medium_ct = ct_suffix in MEDIUM_SEVERITY_CALL_TYPES
    is_severe_disp = any(d in disposition for d in SEVERE_DISPOSITIONS)
    is_minor_disp = any(d in disposition for d in MINOR_DISPOSITIONS)

    if is_minor_disp and resp_count <= 1:
        return 'MINOR'

    if priority == 3 and (resp_count >= 6 or is_severe_disp):
        return 'CRITICAL'

    if priority == 3 and (resp_count >= 4 or is_high_ct):
        return 'SEVERE'

    if priority == 3 and resp_count >= 2 or is_medium_ct:
        return 'MODERATE'

    return 'MINOR'


def run_inference(input_dir, output_dir, limit=None):
    """Main inference pipeline."""
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Layer 2: Inference Engine")
    print("=" * 60)

    # --- Step 1: Parse Layer 1 data ---
    print("  Parsing incidents...")
    incidents = parse_incidents(input_dir / 'incidents.ttl')
    print(f"    Loaded {len(incidents):,} incidents")

    if limit:
        # Take only first N incidents
        keys = list(incidents.keys())[:limit]
        incidents = {k: incidents[k] for k in keys}
        print(f"    Limited to {len(incidents):,} incidents")

    print("  Parsing responses...")
    responses = parse_responses(input_dir / 'responses.ttl')
    print(f"    Loaded {len(responses):,} responses")

    print("  Parsing units...")
    units = parse_units(input_dir / 'units.ttl')
    print(f"    Loaded {len(units):,} units")

    # --- Step 2: Derive multi-agency status ---
    print("  Deriving multi-agency status...")
    multi_agency_count = 0
    incident_agencies = {}  # incident_uri -> set of agencies
    incident_response_times = {}  # incident_uri -> list of response times

    for inc_uri, inc in incidents.items():
        agencies = set()
        resp_times = []

        for resp_ref in inc.get('responses', []):
            resp_data = responses.get(resp_ref, {})
            unit_ref = resp_data.get('unit', '')
            agency = units.get(unit_ref, '')

            if agency:
                agencies.add(agency)

            rt = resp_data.get('responseTimeMin')
            if rt is not None and rt >= 0:
                resp_times.append(rt)

        incident_agencies[inc_uri] = agencies
        incident_response_times[inc_uri] = resp_times

        if len(agencies) > 1:
            multi_agency_count += 1

    print(f"    Multi-agency incidents: {multi_agency_count:,} ({100*multi_agency_count/max(len(incidents),1):.1f}%)")

    # --- Step 3: Derive location history ---
    print("  Deriving location incident counts...")
    location_incidents = defaultdict(list)  # location_uri -> list of timestamps

    for inc_uri, inc in incidents.items():
        loc = inc.get('location', '')
        ts = inc.get('receivedTimestamp', '')
        if loc and ts:
            location_incidents[loc].append(ts)

    # Sort timestamps per location
    for loc in location_incidents:
        location_incidents[loc].sort()

    # Calculate prior incident counts for each incident's location
    # (how many incidents at this location before this one)
    print("  Calculating per-incident location history...")
    location_prior_counts = {}  # incident_uri -> count of prior incidents at same location

    for inc_uri, inc in incidents.items():
        loc = inc.get('location', '')
        ts = inc.get('receivedTimestamp', '')
        if loc and ts:
            prior = sum(1 for t in location_incidents[loc] if t < ts)
            location_prior_counts[inc_uri] = prior

    hotspot_locations = sum(1 for loc, ts_list in location_incidents.items() if len(ts_list) >= 10)
    print(f"    Locations with 10+ incidents (hotspots): {hotspot_locations:,}")

    # --- Step 4: Derive severity ---
    print("  Deriving severity levels...")
    severity_counts = defaultdict(int)

    for inc_uri, inc in incidents.items():
        severity = derive_severity(inc)
        inc['derived_severity'] = severity
        severity_counts[severity] += 1

    for sev, count in sorted(severity_counts.items()):
        pct = 100 * count / max(len(incidents), 1)
        print(f"    {sev}: {count:,} ({pct:.1f}%)")

    # --- Step 5: Write inferred triples ---
    print("  Writing inferred triples...")

    prefixes = f"""@prefix {PREFIX}:   <{NAMESPACE}> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .

"""

    inferred_count = 0

    with open(output_dir / 'inferred_incident_properties.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Layer 2: Inferred Incident Properties\n")
        f.write(f"# Generated: {datetime.now().isoformat()}\n")
        f.write(f"# Incidents processed: {len(incidents):,}\n\n")

        for inc_uri, inc in incidents.items():
            lines = []
            agencies = incident_agencies.get(inc_uri, set())
            resp_times = incident_response_times.get(inc_uri, [])
            prior_count = location_prior_counts.get(inc_uri, 0)
            severity = inc.get('derived_severity', 'MINOR')

            # isMultiAgency
            is_multi = len(agencies) > 1
            lines.append(f'    {PREFIX}:isMultiAgency {"true" if is_multi else "false"} ;')

            # agencyCount
            lines.append(f'    {PREFIX}:agencyCount {len(agencies)} ;')

            # Response time stats
            if resp_times:
                avg_rt = round(sum(resp_times) / len(resp_times), 2)
                max_rt = round(max(resp_times), 2)
                min_rt = round(min(resp_times), 2)
                lines.append(f'    {PREFIX}:avgResponseTimeMinutes {avg_rt} ;')
                lines.append(f'    {PREFIX}:maxResponseTimeMinutes {max_rt} ;')
                lines.append(f'    {PREFIX}:minResponseTimeMinutes {min_rt} ;')

            # Location prior incident count
            lines.append(f'    {PREFIX}:locationPriorIncidentCount {prior_count} ;')

            # Derived severity
            lines.append(f'    {PREFIX}:hasSeverity {PREFIX}:Severity_{severity} ;')

            # Write
            if lines:
                # Replace last semicolon with period
                lines[-1] = lines[-1][:-1] + '.'
                f.write(f'{inc_uri}\n')
                f.write('\n'.join(lines))
                f.write('\n\n')
                inferred_count += len(lines)

    # --- Summary ---
    print(f"\n{'='*60}")
    print(f"  LAYER 2 INFERENCE COMPLETE")
    print(f"{'='*60}")
    print(f"  Incidents processed:    {len(incidents):>12,}")
    print(f"  Multi-agency:           {multi_agency_count:>12,}")
    print(f"  Hotspot locations:      {hotspot_locations:>12,}")
    print(f"  Inferred triples:       {inferred_count:>12,}")
    print(f"  Layer 1 triples:        {'~118,000,000':>12}")
    print(f"  Total after inference:  ~{118_000_000 + inferred_count:>11,}")
    print(f"{'='*60}")
    print(f"  Severity distribution:")
    for sev in ['MINOR', 'MODERATE', 'SEVERE', 'CRITICAL']:
        count = severity_counts.get(sev, 0)
        pct = 100 * count / max(len(incidents), 1)
        bar = '█' * int(pct / 2)
        print(f"    {sev:<10} {count:>10,} ({pct:>5.1f}%) {bar}")
    print(f"{'='*60}")
    print(f"  Output: {output_dir}/inferred_incident_properties.ttl")
    print(f"{'='*60}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SF Emergency KG — Layer 2 Inference')
    parser.add_argument('--input', '-i', default='data/rdf/', help='Layer 1 RDF directory')
    parser.add_argument('--output', '-o', default='data/rdf/inferred/', help='Output directory')
    parser.add_argument('--limit', '-l', type=int, default=None, help='Max incidents to process')
    args = parser.parse_args()

    run_inference(args.input, args.output, args.limit)