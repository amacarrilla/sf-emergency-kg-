# SF Emergency Services Knowledge Graph

**A multi-agency knowledge graph of San Francisco emergency services — from raw dispatch data to a live analytical dashboard.**

Built on two public SF Open Data datasets (11M+ records), converted to 344M RDF triples, loaded into FalkorDB, and surfaced through an analytical dashboard with forecasting and a smart query router.

[Interactive ontology viewer](https://amacarrilla.github.io/sf-emergency-kg-/) · [Incident map](https://amacarrilla.github.io/sf-emergency-kg-/map.html) · [Results](RESULTS.md) · [Design document](PROJECT_SUMMARY.md)

---

## What is this?

A proof of concept that answers a practical question: **when does a knowledge graph actually add value over a flat table?**

The project integrates two independent datasets — SFFD fire/EMS calls and SFPD police dispatches — through a shared ontology. The key insight: the same intersection that fire calls "Traffic Collision" and police calls "TRAFFIC ACCIDENT" maps to a single `IncidentConcept` node in the graph, with no manual join required.

---

## Architecture — 5 phases

```
Phase 1 · RDF Generation ─────── CSV → 344M triples via shared EMS ontology
Phase 2 · Graph Database ─────── FalkorDB (10.5M nodes, Cypher queries)
Phase 3 · Smart Router ───────── Rule-based GRAPH vs FLAT query classifier
Phase 4 · Forecasting ────────── Ridge model with graph-derived features
Phase 5 · Dashboard ──────────── Flask + Chart.js, Paradigma branding
```

### Key results

| Task | Baseline | Graph-enriched | Delta |
|---|---|---|---|
| Multi-agency escalation (v1, accuracy) | 83.9% | **100.0%** | **+19.2%** |
| Resolution time (v1, MAE) | 24.75 min | **23.94 min** | **+3.3%** |
| Forecast MAE reduction (v2) | — | **21%** vs RandomForest baseline | — |

**Why does the graph help?** Multi-agency status requires traversing `Incident → Response → Unit → Agency` — a relationship that doesn't exist in flat CSV. For forecasting, graph features like `medical_ratio` and `total_incidents` per neighborhood require multi-hop traversals that would need pre-built pipelines in SQL.

---

## Quick start

### Prerequisites

```
Python 3.8+
Docker Desktop (for FalkorDB)
pip packages: falkordb, flask, flask-cors, pandas, numpy, scikit-learn, matplotlib
```

Install Python dependencies:

```bash
pip3 install falkordb flask flask-cors pandas numpy scikit-learn matplotlib
```

### 1. Download the datasets

All datasets are free, public, and require no registration. Download as CSV and save to `data/raw/`.

| Dataset | URL | Save as |
|---|---|---|
| Fire Dept Calls for Service (~1.5 GB) | [nuek-vuh3](https://data.sfgov.org/d/nuek-vuh3) | `fire_calls_for_service.csv` |
| Fire Incidents | [wr8u-xric](https://data.sfgov.org/d/wr8u-xric) | `Fire_Incidents.csv` |
| Police Dispatched Calls — Closed (~2 GB) | [2zdj-bwza](https://data.sfgov.org/d/2zdj-bwza) | `Law_Enforcement_Dispatched_Calls_for_Service__Closed_YYYYMMDD.csv` |

Direct CSV download URLs:
```
https://data.sfgov.org/api/views/nuek-vuh3/rows.csv?accessType=DOWNLOAD
https://data.sfgov.org/api/views/wr8u-xric/rows.csv?accessType=DOWNLOAD
https://data.sfgov.org/api/views/2zdj-bwza/rows.csv?accessType=DOWNLOAD
```

### 2. Generate RDF triples (Phase 1)

```bash
# Fire/EMS dataset → ~141M triples
python3 scripts/csv_to_rdf.py \
  --input data/raw/fire_calls_for_service.csv \
  --output data/rdf/

# Police dataset → ~203M triples
python3 scripts/csv_to_rdf_police.py \
  --input "data/raw/Law_Enforcement_Dispatched_Calls_for_Service__Closed_*.csv" \
  --output data/rdf/

# Optional: validate cross-agency fusion
python3 scripts/validate_fusion.py \
  --fire   data/raw/fire_calls_for_service.csv \
  --police "data/raw/Law_Enforcement_Dispatched_Calls_for_Service__Closed_*.csv"
```

> **Note:** Use `--limit 50000` on both scripts for a quick test run before processing the full datasets.

### 3. Start FalkorDB (Phase 2)

```bash
docker-compose up -d

# Verify it's running
docker logs sf_kg | tail -3
# Expected: "✓ Ready in 0ms"
```

### 4. Load the graph

```bash
# Full load — filters to 2020+ records (~22 hours on M1/M2 Mac, ~8h on x86)
python3 scripts/falkor_load.py --rdf data/rdf/

# Quick test (5000 blocks per file, ~2 minutes)
python3 scripts/falkor_load.py --rdf data/rdf/ --test
```

The loader filters records to 2020+ by default, loading ~10.5M nodes. FalkorDB data persists in a named Docker volume (`sf_emergency_kg_falkordb_data`) across container restarts.

### 5. Run the dashboard

```bash
python3 scripts/dashboard.py
# Open: http://localhost:5001
```

### 6. Optional: run individual components

```bash
# Run Cypher query demonstrations
python3 scripts/falkor_queries.py --graph SF_KG

# Interactive smart query router (GRAPH vs FLAT)
python3 scripts/smart_router.py

# Time series forecasting (Phase 4)
python3 scripts/time_series_forecast.py --neighborhoods 6

# Export data for the incident map
python3 scripts/export_map_data.py \
  --rdf-dir data/rdf/ \
  --inferred-dir data/rdf/inferred/ \
  --output docs/map_data.json
```

---

## Project structure

```
sf_emergency_kg/
│
├── docs/                               ← Static site (GitHub Pages)
│   ├── index.html                      ← Ontology graph + v1 results
│   ├── map.html                        ← Interactive incident map (Leaflet)
│   ├── dashboard.html                  ← Analytical dashboard frontend
│   ├── map_data.json                   ← Generated by export_map_data.py (gitignored)
│   ├── forecast_comparison.png         ← Phase 4 output chart
│   └── feature_importance.png          ← Phase 4 output chart
│
├── ontology/
│   ├── ems_ontology.ttl                ← OWL ontology: 15 classes, 19 object props
│   └── sample_instance.ttl             ← Example incident in RDF
│
├── scripts/
│   ├── csv_to_rdf.py                   ← Phase 1: Fire/EMS CSV → RDF
│   ├── csv_to_rdf_police.py            ← Phase 1: Police CSV → RDF
│   ├── validate_fusion.py              ← Cross-agency fusion analysis
│   ├── inference.py                    ← v1: Derive inferred facts
│   ├── predict.py                      ← v1: Train + compare KG vs flat
│   ├── export_map_data.py              ← Export JSON for incident map
│   ├── falkor_load.py                  ← Phase 2: Load TTL → FalkorDB
│   ├── falkor_queries.py               ← Phase 2: Cypher query demos
│   ├── smart_router.py                 ← Phase 3: GRAPH vs FLAT router
│   ├── time_series_forecast.py         ← Phase 4: Forecasting
│   └── dashboard.py                    ← Phase 5: Flask backend
│
├── data/
│   ├── raw/                            ← Source CSVs — NOT in repo (see Download above)
│   ├── rdf/                            ← Generated TTL files — NOT in repo
│   └── falkordb/                       ← FalkorDB data dir — NOT in repo
│
├── docker-compose.yml                  ← FalkorDB container with persistent volume
├── PROJECT_SUMMARY.md                  ← Full design document
├── RESULTS.md                          ← Results and interpretation
├── requirements.txt                    ← Python dependencies
└── .gitignore
```

---

## Ontology overview

The central entity is `Incident`, identified by its CAD number. The `IncidentConcept` class enables cross-agency semantic alignment.

```
Incident ──hasResponse──► Response ──hasUnit──► Unit ──belongsToAgency──► Agency
    │
    ├── hasCallType ──► CallType ──realizationOf──► IncidentConcept
    ├── hasLocation ──► Location ──inNeighborhood──► Neighborhood
    ├── inTimeWindow ──► TimeWindow
    └── inPoliceDistrict ──► PoliceDistrict
```

### Cross-agency concept alignment

| IncidentConcept | SFFD label | SFPD label |
|---|---|---|
| TRAFFIC_COLLISION | Traffic Collision | TRAFFIC ACCIDENT |
| MEDICAL_EMERGENCY | Medical Incident | PERSON DOWN / SICK |
| STRUCTURE_FIRE | Structure Fire | FIRE |
| WELFARE_CHECK | Citizen Assist | WELL BEING CHECK |
| HAZMAT | HazMat / Gas Leak | GAS LEAK / HAZMAT |

---

## Graph features vs flat features

| Feature | Flat CSV | Graph | How |
|---|---|---|---|
| priority, call_type, timestamps | ✓ | ✓ | Raw fields |
| agency_count | — | ✓ | `Incident → Response → Unit → Agency` |
| avg_response_time | — | ✓ | Aggregated across Response nodes |
| location_incident_history | — | ✓ | `Location ← hasLocation ← Incident` |
| medical_ratio per neighborhood | — | ✓ | `Incident → CallType → IncidentConcept` |
| police_response_time per area | — | ✓ | `PoliceResponse → Incident → Location → Neighborhood` |

---

## Tech stack

| Component | Technology |
|---|---|
| Ontology | OWL 2 / Turtle (.ttl) |
| RDF pipeline | Python 3.9 (stdlib only for Phases 1–2) |
| Graph database | FalkorDB (Docker) |
| Graph queries | Cypher |
| ML / forecasting | scikit-learn, pandas, numpy |
| API backend | Flask, flask-cors |
| Frontend | Chart.js, Leaflet.js, HTML/CSS/JS |
| Fonts | Inter + JetBrains Mono |

---

## Data volumes

| Source | Raw rows | RDF triples | Graph nodes (2020+) |
|---|---|---|---|
| Fire/EMS calls | 3.4M | ~141M | ~3.1M incidents + responses |
| Police calls | 7.6M | ~203M | ~7.3M incidents + responses |
| **Total** | **11M** | **~344M** | **~10.5M** |

---

## Known limitations

- **No shared CAD number between agencies:** SFFD and SFPD use independent CAD systems with no common incident identifier. Cross-agency fusion is semantic (via `IncidentConcept`) but not instance-level. In production this would require a master incident ID from a unified CAD system.
- **FalkorDB load time:** ~22 hours on Apple M1 (16GB RAM) due to single-statement Cypher inserts. Can be reduced 10x with bulk import on x86 hardware.
- **Forecast accuracy:** With 216 monthly data points across 6 neighborhoods, the Ridge model achieves R²=0.16 with graph features vs R²=0.44 for the temporal baseline. The graph features add structural context but don't overcome the limited training data.

---

## References

- SF Open Data portal: [data.sfgov.org](https://data.sfgov.org)
- FalkorDB documentation: [docs.falkordb.com](https://docs.falkordb.com)

---

## License

MIT — see [LICENSE](LICENSE)