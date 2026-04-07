#!/usr/bin/env python3
"""
SF Emergency Services Knowledge Graph — Layers 3 & 4
======================================================
Layer 3 (Light): Derive enrichment features from existing data (no external APIs)
Layer 4: Train predictive models and compare KG approach vs flat tabular baseline

Three prediction targets:
    1. Severity classification (MINOR / MODERATE / SEVERE / CRITICAL)
    2. Multi-agency escalation (binary: True / False)
    3. Resolution time estimation (regression: minutes)

The key comparison:
    - KG model:  uses graph-derived features (location history, agency chain, cross-type alignment)
    - Flat model: uses only features available in the raw CSV (no graph traversal)

Usage:
    python3 scripts/predict.py --rdf-dir data/rdf/ --inferred-dir data/rdf/inferred/ --limit 50000
    python3 scripts/predict.py --rdf-dir data/rdf/ --inferred-dir data/rdf/inferred/
"""

import re
import csv
import json
import sys
import os
from datetime import datetime
from collections import defaultdict, Counter
from pathlib import Path
import argparse
import math
import random

random.seed(42)


# =============================================================================
# STEP 1: PARSE ALL LAYERS INTO A UNIFIED FEATURE TABLE
# =============================================================================

def parse_incidents_full(rdf_dir, inferred_dir, limit=None):
    """
    Parse Layer 1 + Layer 2 TTL files and build a unified feature table.
    Each row = one incident with all available features.
    """
    rdf_dir = Path(rdf_dir)
    inferred_dir = Path(inferred_dir)

    print("Parsing Layer 1: incidents...")
    incidents = {}
    current_uri = None
    current = {}

    with open(rdf_dir / 'incidents.ttl', 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('@'):
                continue

            if line.startswith('ems:Incident_CAD_') and 'a ems:' not in line and ':' in line:
                if current_uri and current:
                    incidents[current_uri] = current
                current_uri = line.rstrip(' ;.')
                current = {'response_refs': []}
                continue

            if not current_uri:
                continue

            if 'ems:priorityCode' in line:
                m = re.search(r'priorityCode\s+(\d+)', line)
                if m: current['priority'] = int(m.group(1))

            elif 'ems:hasCallType' in line:
                m = re.search(r'CallType_SFFD_(\S+)', line)
                if m: current['call_type'] = m.group(1).rstrip(' ;.')

            elif 'ems:hasLocation' in line:
                m = re.search(r'Location_(\S+)', line)
                if m: current['location'] = m.group(1).rstrip(' ;.')

            elif 'ems:responseCount' in line:
                m = re.search(r'responseCount\s+(\d+)', line)
                if m: current['response_count'] = int(m.group(1))

            elif 'ems:totalResolutionMinutes' in line:
                m = re.search(r'totalResolutionMinutes\s+([\d.]+)', line)
                if m: current['resolution_min'] = float(m.group(1))

            elif 'ems:receivedTimestamp' in line:
                m = re.search(r'"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"', line)
                if m: current['timestamp'] = m.group(1)

            elif 'ems:finalDisposition' in line:
                m = re.search(r'finalDisposition\s+"([^"]*)"', line)
                if m: current['disposition'] = m.group(1)

            elif 'ems:inTimeWindow' in line:
                m = re.search(r'TimeWindow_(\S+)', line)
                if m: current['time_window'] = m.group(1).rstrip(' ;.')

            elif 'ems:hasResponse' in line:
                refs = re.findall(r'ems:Response_\S+', line)
                for ref in refs:
                    current['response_refs'].append(ref.rstrip(' ;.,'))

            elif line.startswith('ems:Response_'):
                current['response_refs'].append(line.rstrip(' ;.,'))

    if current_uri and current:
        incidents[current_uri] = current

    print(f"  Loaded {len(incidents):,} incidents")

    if limit and len(incidents) > limit:
        keys = list(incidents.keys())[:limit]
        incidents = {k: incidents[k] for k in keys}
        print(f"  Limited to {len(incidents):,}")

    # --- Parse Layer 2 inferred properties ---
    print("Parsing Layer 2: inferred properties...")
    inferred_file = inferred_dir / 'inferred_incident_properties.ttl'
    inferred_count = 0

    if inferred_file.exists():
        current_uri = None
        with open(inferred_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or line.startswith('@'):
                    continue

                if line.startswith('ems:Incident_CAD_'):
                    current_uri = line.rstrip(' ;.')
                    continue

                if current_uri and current_uri in incidents:
                    inc = incidents[current_uri]

                    if 'ems:isMultiAgency' in line:
                        inc['is_multi_agency'] = 'true' in line

                    elif 'ems:agencyCount' in line:
                        m = re.search(r'agencyCount\s+(\d+)', line)
                        if m: inc['agency_count'] = int(m.group(1))

                    elif 'ems:avgResponseTimeMinutes' in line:
                        m = re.search(r'avgResponseTimeMinutes\s+([\d.]+)', line)
                        if m: inc['avg_response_time'] = float(m.group(1))

                    elif 'ems:maxResponseTimeMinutes' in line:
                        m = re.search(r'maxResponseTimeMinutes\s+([\d.]+)', line)
                        if m: inc['max_response_time'] = float(m.group(1))

                    elif 'ems:locationPriorIncidentCount' in line:
                        m = re.search(r'locationPriorIncidentCount\s+(\d+)', line)
                        if m: inc['location_prior_count'] = int(m.group(1))

                    elif 'ems:hasSeverity' in line:
                        m = re.search(r'Severity_(\w+)', line)
                        if m:
                            inc['severity'] = m.group(1)
                            inferred_count += 1

        print(f"  Matched inferred properties for {inferred_count:,} incidents")
    else:
        print(f"  WARNING: {inferred_file} not found. Run inference.py first.")

    return incidents


# =============================================================================
# STEP 2: LAYER 3 LIGHT ENRICHMENT (temporal features from timestamp)
# =============================================================================

def enrich_temporal(incidents):
    """Add temporal features derived from the timestamp — no external APIs needed."""
    print("Layer 3: Temporal enrichment...")
    enriched = 0

    for uri, inc in incidents.items():
        ts = inc.get('timestamp', '')
        if not ts:
            continue

        try:
            dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue

        inc['hour'] = dt.hour
        inc['day_of_week'] = dt.isoweekday()  # 1=Mon, 7=Sun
        inc['month'] = dt.month
        inc['year'] = dt.year
        inc['is_weekend'] = 1 if dt.isoweekday() >= 6 else 0
        inc['is_night'] = 1 if dt.hour < 6 or dt.hour >= 22 else 0
        inc['is_rush_hour'] = 1 if (7 <= dt.hour <= 9) or (16 <= dt.hour <= 19) else 0

        # Season
        if dt.month in (12, 1, 2):
            inc['season'] = 0  # winter
        elif dt.month in (3, 4, 5):
            inc['season'] = 1  # spring
        elif dt.month in (6, 7, 8):
            inc['season'] = 2  # summer
        else:
            inc['season'] = 3  # fall

        enriched += 1

    print(f"  Enriched {enriched:,} incidents with temporal features")
    return incidents


# =============================================================================
# STEP 3: BUILD FEATURE MATRICES
# =============================================================================

# Call type to numeric encoding
CALL_TYPE_ENCODING = {}

def encode_call_type(ct):
    if ct not in CALL_TYPE_ENCODING:
        CALL_TYPE_ENCODING[ct] = len(CALL_TYPE_ENCODING)
    return CALL_TYPE_ENCODING[ct]


def build_feature_matrices(incidents):
    """
    Build two feature sets:
        - KG features: includes graph-derived features (multi-agency, location history, etc.)
        - Flat features: only raw CSV-level features (no graph traversal)

    Returns: (X_kg, X_flat, y_severity, y_multi, y_resolution, valid_uris)
    """
    print("Building feature matrices...")

    X_kg = []
    X_flat = []
    y_severity = []
    y_multi_agency = []
    y_resolution = []
    valid_uris = []

    severity_map = {'MINOR': 0, 'MODERATE': 1, 'SEVERE': 2, 'CRITICAL': 3}

    skipped = 0
    for uri, inc in incidents.items():
        severity = inc.get('severity', '')
        if severity not in severity_map:
            skipped += 1
            continue

        priority = inc.get('priority', 2)
        resp_count = inc.get('response_count', 1)
        call_type_num = encode_call_type(inc.get('call_type', 'UNKNOWN'))
        hour = inc.get('hour', 12)
        dow = inc.get('day_of_week', 1)
        month = inc.get('month', 1)
        is_weekend = inc.get('is_weekend', 0)
        is_night = inc.get('is_night', 0)
        is_rush = inc.get('is_rush_hour', 0)
        season = inc.get('season', 0)

        # === FLAT features (available from raw CSV only) ===
        flat_row = [
            priority,
            resp_count,
            call_type_num,
            hour,
            dow,
            month,
            is_weekend,
            is_night,
            is_rush,
            season,
        ]

        # === KG features (adds graph-derived signals) ===
        agency_count = inc.get('agency_count', 1)
        avg_rt = inc.get('avg_response_time', 0)
        max_rt = inc.get('max_response_time', 0)
        loc_prior = inc.get('location_prior_count', 0)

        kg_row = flat_row + [
            agency_count,           # from graph: Response → Unit → Agency chain
            avg_rt,                 # from graph: aggregated across responses
            max_rt,                 # from graph: aggregated across responses
            loc_prior,              # from graph: location inverse → incident history
            1 if agency_count > 1 else 0,  # is_multi (as feature, not target)
        ]

        X_flat.append(flat_row)
        X_kg.append(kg_row)
        y_severity.append(severity_map[severity])
        y_multi_agency.append(1 if inc.get('is_multi_agency', False) else 0)
        y_resolution.append(inc.get('resolution_min', 0))
        valid_uris.append(uri)

    print(f"  Valid samples: {len(X_kg):,} (skipped {skipped:,})")
    print(f"  Flat features: {len(X_flat[0])} columns")
    print(f"  KG features:   {len(X_kg[0])} columns (+{len(X_kg[0])-len(X_flat[0])} graph-derived)")

    return X_kg, X_flat, y_severity, y_multi_agency, y_resolution, valid_uris


# =============================================================================
# STEP 4: SIMPLE ML MODELS (no sklearn dependency)
# =============================================================================

def train_test_split_manual(X, y, test_ratio=0.2):
    """Split data into train/test sets."""
    n = len(X)
    indices = list(range(n))
    random.shuffle(indices)
    split = int(n * (1 - test_ratio))
    train_idx = indices[:split]
    test_idx = indices[split:]

    X_train = [X[i] for i in train_idx]
    X_test = [X[i] for i in test_idx]
    y_train = [y[i] for i in train_idx]
    y_test = [y[i] for i in test_idx]

    return X_train, X_test, y_train, y_test, test_idx


def normalize(X_train, X_test):
    """Min-max normalization."""
    n_features = len(X_train[0])
    mins = [min(row[j] for row in X_train) for j in range(n_features)]
    maxs = [max(row[j] for row in X_train) for j in range(n_features)]

    def norm_row(row):
        return [(row[j] - mins[j]) / (maxs[j] - mins[j] + 1e-10) for j in range(n_features)]

    return [norm_row(r) for r in X_train], [norm_row(r) for r in X_test]


class KNNClassifier:
    """Simple k-NN classifier for classification tasks."""
    def __init__(self, k=7):
        self.k = k

    def fit(self, X, y):
        self.X = X
        self.y = y

    def predict(self, X_test):
        predictions = []
        for test_row in X_test:
            # Calculate distances to all training points
            dists = []
            for i, train_row in enumerate(self.X):
                d = sum((a - b) ** 2 for a, b in zip(test_row, train_row))
                dists.append((d, self.y[i]))

            # Sort by distance, take k nearest
            dists.sort(key=lambda x: x[0])
            k_nearest = [d[1] for d in dists[:self.k]]

            # Majority vote
            counts = Counter(k_nearest)
            predictions.append(counts.most_common(1)[0][0])

        return predictions


class KNNRegressor:
    """Simple k-NN regressor for resolution time."""
    def __init__(self, k=7):
        self.k = k

    def fit(self, X, y):
        self.X = X
        self.y = y

    def predict(self, X_test):
        predictions = []
        for test_row in X_test:
            dists = []
            for i, train_row in enumerate(self.X):
                d = sum((a - b) ** 2 for a, b in zip(test_row, train_row))
                dists.append((d, self.y[i]))

            dists.sort(key=lambda x: x[0])
            k_nearest = [d[1] for d in dists[:self.k]]
            predictions.append(sum(k_nearest) / len(k_nearest))

        return predictions


# =============================================================================
# STEP 5: EVALUATION METRICS
# =============================================================================

def accuracy(y_true, y_pred):
    correct = sum(1 for a, b in zip(y_true, y_pred) if a == b)
    return correct / len(y_true) if y_true else 0


def f1_per_class(y_true, y_pred, num_classes):
    """Calculate F1 per class and macro F1."""
    results = {}
    for c in range(num_classes):
        tp = sum(1 for t, p in zip(y_true, y_pred) if t == c and p == c)
        fp = sum(1 for t, p in zip(y_true, y_pred) if t != c and p == c)
        fn = sum(1 for t, p in zip(y_true, y_pred) if t == c and p != c)
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        results[c] = {'precision': precision, 'recall': recall, 'f1': f1}

    macro_f1 = sum(r['f1'] for r in results.values()) / len(results)
    return results, macro_f1


def mae(y_true, y_pred):
    return sum(abs(a - b) for a, b in zip(y_true, y_pred)) / len(y_true) if y_true else 0


def rmse(y_true, y_pred):
    mse = sum((a - b) ** 2 for a, b in zip(y_true, y_pred)) / len(y_true) if y_true else 0
    return math.sqrt(mse)


# =============================================================================
# STEP 6: RUN EXPERIMENTS
# =============================================================================

def run_experiment(X_kg, X_flat, y, task_name, task_type='classification', num_classes=4):
    """Run KG vs Flat comparison for one prediction target."""
    print(f"\n{'─'*60}")
    print(f"  {task_name}")
    print(f"{'─'*60}")

    # Use a smaller test sample for k-NN (it's O(n²))
    max_train = min(8000, int(len(X_kg) * 0.8))
    max_test = min(2000, int(len(X_kg) * 0.2))

    # Split
    indices = list(range(len(X_kg)))
    random.shuffle(indices)
    train_idx = indices[:max_train]
    test_idx = indices[max_train:max_train + max_test]

    X_kg_train = [X_kg[i] for i in train_idx]
    X_kg_test = [X_kg[i] for i in test_idx]
    X_flat_train = [X_flat[i] for i in train_idx]
    X_flat_test = [X_flat[i] for i in test_idx]
    y_train = [y[i] for i in train_idx]
    y_test = [y[i] for i in test_idx]

    # Normalize
    X_kg_train_n, X_kg_test_n = normalize(X_kg_train, X_kg_test)
    X_flat_train_n, X_flat_test_n = normalize(X_flat_train, X_flat_test)

    print(f"  Train: {len(y_train):,}  Test: {len(y_test):,}")

    if task_type == 'classification':
        # --- KG model ---
        print(f"  Training KG model (k-NN, k=7)...")
        kg_model = KNNClassifier(k=7)
        kg_model.fit(X_kg_train_n, y_train)
        kg_pred = kg_model.predict(X_kg_test_n)

        kg_acc = accuracy(y_test, kg_pred)
        kg_classes, kg_macro_f1 = f1_per_class(y_test, kg_pred, num_classes)

        # --- Flat model ---
        print(f"  Training Flat model (k-NN, k=7)...")
        flat_model = KNNClassifier(k=7)
        flat_model.fit(X_flat_train_n, y_train)
        flat_pred = flat_model.predict(X_flat_test_n)

        flat_acc = accuracy(y_test, flat_pred)
        flat_classes, flat_macro_f1 = f1_per_class(y_test, flat_pred, num_classes)

        # --- Results ---
        improvement_acc = ((kg_acc - flat_acc) / max(flat_acc, 0.001)) * 100
        improvement_f1 = ((kg_macro_f1 - flat_macro_f1) / max(flat_macro_f1, 0.001)) * 100

        print(f"\n  {'Metric':<20} {'Flat (CSV only)':<18} {'KG (graph)':<18} {'Δ':>8}")
        print(f"  {'─'*64}")
        print(f"  {'Accuracy':<20} {flat_acc:<18.4f} {kg_acc:<18.4f} {improvement_acc:>+7.1f}%")
        print(f"  {'Macro F1':<20} {flat_macro_f1:<18.4f} {kg_macro_f1:<18.4f} {improvement_f1:>+7.1f}%")

        return {
            'task': task_name,
            'kg_accuracy': round(kg_acc, 4),
            'flat_accuracy': round(flat_acc, 4),
            'kg_macro_f1': round(kg_macro_f1, 4),
            'flat_macro_f1': round(flat_macro_f1, 4),
            'improvement_acc': round(improvement_acc, 1),
            'improvement_f1': round(improvement_f1, 1),
        }

    else:  # regression
        # --- KG model ---
        print(f"  Training KG model (k-NN regressor, k=7)...")
        kg_model = KNNRegressor(k=7)
        kg_model.fit(X_kg_train_n, y_train)
        kg_pred = kg_model.predict(X_kg_test_n)

        kg_mae = mae(y_test, kg_pred)
        kg_rmse = rmse(y_test, kg_pred)

        # --- Flat model ---
        print(f"  Training Flat model (k-NN regressor, k=7)...")
        flat_model = KNNRegressor(k=7)
        flat_model.fit(X_flat_train_n, y_train)
        flat_pred = flat_model.predict(X_flat_test_n)

        flat_mae = mae(y_test, flat_pred)
        flat_rmse = rmse(y_test, flat_pred)

        improvement_mae = ((flat_mae - kg_mae) / max(flat_mae, 0.001)) * 100
        improvement_rmse = ((flat_rmse - kg_rmse) / max(flat_rmse, 0.001)) * 100

        print(f"\n  {'Metric':<20} {'Flat (CSV only)':<18} {'KG (graph)':<18} {'Δ':>8}")
        print(f"  {'─'*64}")
        print(f"  {'MAE (minutes)':<20} {flat_mae:<18.2f} {kg_mae:<18.2f} {improvement_mae:>+7.1f}%")
        print(f"  {'RMSE (minutes)':<20} {flat_rmse:<18.2f} {kg_rmse:<18.2f} {improvement_rmse:>+7.1f}%")

        return {
            'task': task_name,
            'kg_mae': round(kg_mae, 2),
            'flat_mae': round(flat_mae, 2),
            'kg_rmse': round(kg_rmse, 2),
            'flat_rmse': round(flat_rmse, 2),
            'improvement_mae': round(improvement_mae, 1),
            'improvement_rmse': round(improvement_rmse, 1),
        }


# =============================================================================
# STEP 7: WRITE PREDICTIONS BACK TO GRAPH (Layer 4 output)
# =============================================================================

def write_predictions(output_dir, task_name, results):
    """Write prediction metadata as a ForecastSet in the graph."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    prefixes = """@prefix ems:   <http://example.org/ems#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .

"""

    safe_name = re.sub(r'[^a-zA-Z0-9]', '_', task_name)
    with open(output_dir / f'predictions_{safe_name}.ttl', 'w', encoding='utf-8') as f:
        f.write(prefixes)
        f.write(f"# SF Emergency KG — Layer 4: Prediction Results\n")
        f.write(f"# Task: {task_name}\n")
        f.write(f"# Generated: {datetime.now().isoformat()}\n\n")

        f.write(f'ems:PredictionSet_{safe_name}_{datetime.now().strftime("%Y%m%d")}\n')
        f.write(f'    a ems:PredictionSet ;\n')
        f.write(f'    rdfs:label "{task_name}" ;\n')
        f.write(f'    ems:modelName "KNN_k7_vs_flat_comparison" ;\n')
        f.write(f'    ems:predictionTarget "{safe_name}" ;\n')
        f.write(f'    ems:dateGenerated "{datetime.now().isoformat()}"^^xsd:dateTime ;\n')
        f.write(f'    ems:featureSet "{json.dumps(results)}" .\n')

    print(f"  Saved: predictions_{safe_name}.ttl")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='SF Emergency KG — Layers 3 & 4')
    parser.add_argument('--rdf-dir', default='data/rdf/', help='Layer 1 RDF directory')
    parser.add_argument('--inferred-dir', default='data/rdf/inferred/', help='Layer 2 inferred directory')
    parser.add_argument('--output', default='data/rdf/predictions/', help='Output directory')
    parser.add_argument('--limit', '-l', type=int, default=None, help='Max incidents to process')
    args = parser.parse_args()

    print("=" * 60)
    print("  SF EMERGENCY KG — LAYERS 3 & 4")
    print("  Knowledge Graph vs Flat Table Comparison")
    print("=" * 60)

    # --- Parse all data ---
    incidents = parse_incidents_full(args.rdf_dir, args.inferred_dir, args.limit)

    # --- Layer 3: Light enrichment ---
    incidents = enrich_temporal(incidents)

    # --- Build feature matrices ---
    X_kg, X_flat, y_severity, y_multi, y_resolution, valid_uris = build_feature_matrices(incidents)

    if len(X_kg) < 100:
        print("ERROR: Not enough valid samples. Make sure inference.py has been run.")
        sys.exit(1)

    # --- Feature names for reference ---
    flat_features = [
        'priority', 'response_count', 'call_type', 'hour', 'day_of_week',
        'month', 'is_weekend', 'is_night', 'is_rush_hour', 'season'
    ]
    kg_extra_features = [
        'agency_count', 'avg_response_time', 'max_response_time',
        'location_prior_count', 'is_multi_agency_feature'
    ]

    print(f"\n  Feature comparison:")
    print(f"  Flat model:  {flat_features}")
    print(f"  KG adds:     {kg_extra_features}")

    # --- Run experiments ---
    all_results = []

    # 1. Severity prediction
    r1 = run_experiment(X_kg, X_flat, y_severity,
                        "Severity Classification", 'classification', num_classes=4)
    all_results.append(r1)
    write_predictions(args.output, "severity_classification", r1)

    # 2. Multi-agency prediction
    r2 = run_experiment(X_kg, X_flat, y_multi,
                        "Multi-Agency Escalation", 'classification', num_classes=2)
    all_results.append(r2)
    write_predictions(args.output, "multi_agency_escalation", r2)

    # 3. Resolution time prediction (filter out zeros)
    valid_res = [(kg, fl, y) for kg, fl, y in zip(X_kg, X_flat, y_resolution) if y > 0]
    if len(valid_res) > 100:
        X_kg_res = [v[0] for v in valid_res]
        X_flat_res = [v[1] for v in valid_res]
        y_res = [v[2] for v in valid_res]
        r3 = run_experiment(X_kg_res, X_flat_res, y_res,
                            "Resolution Time (minutes)", 'regression')
        all_results.append(r3)
        write_predictions(args.output, "resolution_time", r3)

    # --- Final summary ---
    print(f"\n{'='*60}")
    print(f"  FINAL RESULTS — KG vs FLAT TABLE")
    print(f"{'='*60}")
    for r in all_results:
        task = r['task']
        if 'kg_accuracy' in r:
            delta = r['improvement_acc']
            symbol = '✓' if delta > 0 else '✗'
            print(f"  {symbol} {task}")
            print(f"    Accuracy:  Flat {r['flat_accuracy']:.4f}  →  KG {r['kg_accuracy']:.4f}  ({delta:+.1f}%)")
            print(f"    Macro F1:  Flat {r['flat_macro_f1']:.4f}  →  KG {r['kg_macro_f1']:.4f}  ({r['improvement_f1']:+.1f}%)")
        else:
            delta = r['improvement_mae']
            symbol = '✓' if delta > 0 else '✗'
            print(f"  {symbol} {task}")
            print(f"    MAE:   Flat {r['flat_mae']:.2f} min  →  KG {r['kg_mae']:.2f} min  ({delta:+.1f}%)")
            print(f"    RMSE:  Flat {r['flat_rmse']:.2f} min  →  KG {r['kg_rmse']:.2f} min  ({r['improvement_rmse']:+.1f}%)")
    print(f"{'='*60}")
    print(f"  Graph-derived features: {kg_extra_features}")
    print(f"  These features require traversing the knowledge graph")
    print(f"  and are NOT available from a flat CSV export.")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()