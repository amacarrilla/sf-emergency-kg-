# SF Emergency Services Knowledge Graph — Project Summary

## From dispatch data to live analytical dashboard: a 5-phase knowledge graph architecture

**Author:** Andrés Macarrilla · Paradigma Digital  
**Date:** April 2026  
**Version:** 2.0  
**Status:** Complete — all 5 phases implemented and operational

---

## 1. Origin and motivation

Our goal: apply the same pattern to **San Francisco's multi-agency emergency response system**, extend it with a live graph database, and answer a practical question:

> **When does a knowledge graph actually add value over a flat table?**

The honest answer, reached through the project: **the graph adds value when the prediction depends on relationships between entities that don't exist in the raw data**. For tasks where the signal is already in the flat fields, the graph adds dimensionality without proportional information gain.

---

## 2. Architecture — 5 phases

```
┌──────────────────────────────────────────────────────────────────┐
│  5  ANALYTICAL DASHBOARD                                         │
│  Flask backend · Chart.js · Paradigma branding · 5 tabs          │
│  Period filter · Live Cypher queries · Forecast · Map            │
├──────────────────────────────────────────────────────────────────┤
│  4  TIME SERIES FORECASTING                                      │
│  Ridge model (graph-enriched) vs baseline (temporal only)        │
│  Features: medical_ratio, police_rt, concept_diversity           │
│  21% MAE improvement vs RandomForest baseline                    │
├──────────────────────────────────────────────────────────────────┤
│  3  SMART QUERY ROUTER                                           │
│  Rule-based classifier: GRAPH (Cypher/FalkorDB) vs FLAT (pandas) │
│  Confidence scoring · Automatic fallback · why explanation       │
├──────────────────────────────────────────────────────────────────┤
│  2  GRAPH DATABASE (FalkorDB)                                    │
│  10.5M nodes · 8 Cypher query types · Docker · persistent volume │
│  Year filter · Sub-4s response on 4.7M Incident nodes            │
├──────────────────────────────────────────────────────────────────┤
│  1  RDF GENERATION + ONTOLOGY                                    │
│  344M triples · 2 datasets · shared IncidentConcept alignment    │
│  SFFD (141M) + SFPD (203M) unified through EMS ontology          │
└──────────────────────────────────────────────────────────────────┘
```

### Analogy with Hedden's Oscar model

| Oscars (Hedden) | Emergency Services (this project) |
|---|---|
| Nomination | Incident (central entity, identified by CAD #) |
| AwardSystem (Oscars, BAFTA, SAG…) | Agency (SFFD, SFPD, EMS) |
| AwardConcept | IncidentConcept (aligns call types across agencies) |
| Film | Location (intersection / address) |
| Person | Unit (Engine 7, Medic 33, SFPD patrol) |
| AwardCategory | CallType (per-agency incident label) |
| AwardCeremony | TimeWindow (year, month, shift) |

---

## 3. Ontology

### 3.1 Classes

| Class | Role | Key properties |
|---|---|---|
| **Incident** | Central entity (pivot) | cadNumber, receivedTimestamp, priorityCode, finalDisposition, responseCount, totalResolutionMinutes |
| **Response** | Fire/EMS unit dispatch | dispatchTimestamp, enRouteTimestamp, onSceneTimestamp, responseTimeMinutes |
| **PoliceResponse** | Police dispatch (subclass of Response) | dispatchTimestamp, onSceneTimestamp, closeTimestamp, pdIncidentReport |
| **Unit** | Specific vehicle/team | unitId, unitType (ENGINE, TRUCK, MEDIC, CHIEF…) |
| **Agency** | Reporting system | agencyCode (SFFD, SFPD, EMS) |
| **CallType** | Per-agency incident label | linked to Agency and IncidentConcept |
| **PoliceCallType** | Police-specific label (subclass of CallType) | callTypeCode |
| **IncidentConcept** | Cross-agency semantic alignment | abstract type unifying equivalent call types |
| **Location** | Geographic point | address, latitude, longitude, zipCode |
| **Neighborhood** | SF neighborhood | label |
| **PoliceDistrict** | SFPD patrol district | label |
| **SupervisorDistrict** | SF supervisor district | label |
| **StationArea** | Fire station zone | linked to Battalion |
| **Battalion** | Fire command grouping | label |
| **TimeWindow** | Temporal context | year, month, dayOfWeek, shift |

### 3.2 Key relationships

```
Incident ──hasResponse──────► Response ──hasUnit──► Unit ──belongsToAgency──► Agency
    │
    ├── hasCallType ──► CallType ──realizationOf──► IncidentConcept
    ├── hasLocation ──► Location ──inNeighborhood──► Neighborhood
    │                       └──── inPoliceDistrict ──► PoliceDistrict
    └── inTimeWindow ──► TimeWindow
```

### 3.3 Cross-agency concept alignment

This is the core value of the ontology layer — two agencies with independent taxonomies map to shared concepts:

| IncidentConcept | SFFD label | SFPD label |
|---|---|---|
| TRAFFIC_COLLISION | Traffic Collision | TRAFFIC ACCIDENT / HIT AND RUN |
| MEDICAL_EMERGENCY | Medical Incident | PERSON DOWN / SICK / OVERDOSE |
| STRUCTURE_FIRE | Structure Fire | FIRE |
| WELFARE_CHECK | Citizen Assist | WELL BEING CHECK |
| HAZMAT | HazMat / Gas Leak | GAS LEAK / HAZMAT |
| ALARM | Alarms | BURGLAR ALARM / FIRE ALARM |
| RESCUE | Water Rescue / Extrication | TRAPPED PERSON |

### 3.4 Why no shared CAD number?

SFFD and SFPD use independent CAD (Computer Aided Dispatch) systems with separate numbering schemes. Fire incidents are numbered `16XXXXXXX`, police incidents `26XXXXXXX`. There is no master incident ID linking the two.

This is a real-world constraint, not a data quality issue. In production, a unified CAD system would provide a cross-agency incident identifier. In this project, fusion is semantic (via `IncidentConcept`) rather than instance-level. This is documented honestly as a limitation rather than presented as a solved problem.

---

## 4. Data sources

### 4.1 Fire Dept Calls for Service (primary)

- **URL:** `https://data.sfgov.org/d/nuek-vuh3`
- **Download:** `https://data.sfgov.org/api/views/nuek-vuh3/rows.csv?accessType=DOWNLOAD`
- **Volume:** ~3.4M rows (2003–2026), ~1.5 GB
- **Granularity:** One row per unit dispatched per incident
- **Key fields:** Call Number (CAD #), Unit ID, Call Type, all timestamps, Address, Battalion, Station Area, Neighborhood, Priority, Disposition, lat/lng
- **Save as:** `data/raw/fire_calls_for_service.csv`

### 4.2 Police Dispatched Calls — Closed

- **URL:** `https://data.sfgov.org/d/2zdj-bwza`
- **Download:** `https://data.sfgov.org/api/views/2zdj-bwza/rows.csv?accessType=DOWNLOAD`
- **Volume:** ~7.6M rows (2014–2026), ~2 GB
- **Key fields:** cad_number, call_type_final, call_type_final_desc, received_datetime, dispatch_datetime, onscene_datetime, close_datetime, disposition, police_district, analysis_neighborhood, intersection_point
- **Note:** Replaces deprecated dataset `hz9m-tj6z`
- **Save as:** `data/raw/Law_Enforcement_Dispatched_Calls_for_Service__Closed_YYYYMMDD.csv`

### 4.3 Fire Incidents (complementary)

- **URL:** `https://data.sfgov.org/d/wr8u-xric`
- **Download:** `https://data.sfgov.org/api/views/wr8u-xric/rows.csv?accessType=DOWNLOAD`
- **Volume:** ~660K rows
- **Use:** Property loss, field observations, ground truth for severity
- **Save as:** `data/raw/Fire_Incidents.csv`

---

## 5. Pipeline — phase by phase

### Phase 1 — RDF generation

Two scripts convert the raw CSVs to RDF/Turtle files aligned with the EMS ontology. No external Python dependencies — pure stdlib.

```bash
# Fire/EMS dataset
python3 scripts/csv_to_rdf.py \
  --input data/raw/fire_calls_for_service.csv \
  --output data/rdf/ \
  [--limit 50000]  # for testing

# Police dataset
python3 scripts/csv_to_rdf_police.py \
  --input "data/raw/Law_Enforcement_Dispatched_Calls_for_Service__Closed_*.csv" \
  --output data/rdf/ \
  [--limit 50000]
```

**Output (full run):**

| File | Size | Content |
|---|---|---|
| incidents.ttl | 2.2 GB | 1.03M fire/EMS incidents (2020+) |
| responses.ttl | 3.1 GB | 2.1M fire/EMS responses |
| police_incidents.ttl | 5.9 GB | 3.7M police incidents (2020+) |
| police_responses.ttl | 3.6 GB | 3.7M police responses |
| locations.ttl | 3.9 MB | 11,567 fire locations |
| police_locations.ttl | 3.0 MB | 6,652 police intersections |
| call_types.ttl | 8 KB | 41 SFFD call types |
| police_call_types.ttl | 40 KB | 179 SFPD call types |
| neighborhoods.ttl | 8 KB | 42 SF neighborhoods |
| time_windows.ttl | 1.2 MB | 6,552 time windows |
| **Total** | **~20 GB** | **~344M triples** |

### Phase 2 — FalkorDB graph database

FalkorDB runs in Docker with a persistent named volume. The loader streams TTL files line-by-line (never loading multi-GB files into memory) and filters to 2020+ records.

```bash
# Start FalkorDB
docker-compose up -d

# Load graph (full run: ~22h on M1 Mac, ~8h on x86)
python3 scripts/falkor_load.py --rdf data/rdf/

# Test run (5000 blocks/file, ~2 min)
python3 scripts/falkor_load.py --rdf data/rdf/ --test
```

**Graph inventory (2020+ filter):**

| Node type | Count |
|---|---|
| Incident | 4,698,100 |
| PoliceResponse | 3,668,114 |
| Response | 2,100,178 |
| Location | 18,219 |
| TimeWindow | 6,552 |
| Unit | 1,109 |
| CallType + PoliceCallType | 379 |
| Neighborhood | 42 |
| PoliceDistrict | 10 |
| **Total** | **~10.5M** |

**Query performance (M1 Mac, 16GB RAM):**

| Query | Time |
|---|---|
| Top 10 neighborhoods by volume | 1.5s |
| Police response time by district | 4.0s |
| Year-over-year trend | 1.1s |
| Medical emergencies by neighborhood | 1.1s |
| Graph schema count | ~60s (full scan) |

### Phase 3 — Smart query router

A CLI that classifies natural language questions as GRAPH (→ Cypher/FalkorDB) or FLAT (→ pandas/CSV) using rule-based signal detection, executes the optimal query, and explains the routing decision.

```bash
python3 scripts/smart_router.py
```

**Routing examples:**

| Question | Route | Confidence |
|---|---|---|
| "Which neighborhoods have the most incidents?" | GRAPH | 99% |
| "Average response time per police district?" | GRAPH | 99% |
| "How many unique units are there?" | FLAT | 70% |
| "Total incidents in fire dataset" | FLAT (fallback) | 50% |

The classifier is swappable — a Claude API version can replace the rule-based one by implementing the same `classify()` interface.

### Phase 4 — Time series forecasting

Monthly incident forecasting per neighborhood using two models:

- **Baseline:** Ridge regression with temporal features only (lags, month, year)
- **Graph-enriched:** Baseline + graph-derived features (medical_ratio, avg_police_response_min, concept_diversity, total_incidents)

```bash
python3 scripts/time_series_forecast.py --neighborhoods 6 --forecast-weeks 4
```

**Results (6 neighborhoods, 216 training samples, TimeSeriesSplit CV):**

| Model | MAE | RMSE | R² |
|---|---|---|---|
| Baseline — Ridge | 376.6 | 475.0 | **0.438** |
| Baseline — RandomForest | 549.7 | 676.8 | −0.018 |
| Graph — Ridge | 483.7 | 594.8 | 0.160 |
| Graph — RandomForest | 541.0 | 667.3 | 0.009 |

**Honest interpretation:** The temporal baseline Ridge wins on R². The graph features (`total_incidents` contributes 0.027 importance in RandomForest) add structural context per neighborhood but don't overcome the dominance of recent lags (lag_1 = 0.63 importance) with this dataset size. The value of the graph is in the ease of feature extraction — multi-hop traversals that would require pre-built SQL pipelines are available as natural Cypher queries.

### Phase 5 — Analytical dashboard

Flask backend + single-page HTML frontend with Paradigma Digital branding.

```bash
python3 scripts/dashboard.py
# Open: http://localhost:5001
```

**Tabs:**
- **Overview:** 4 auto-loading charts (neighborhoods, yearly trend, time windows, police response time)
- **Explore:** 8 interactive Cypher queries with live FalkorDB execution
- **Forecast:** Configurable neighborhood/month selector, historical + forecast chart, data table
- **Map:** Embedded incident map (Leaflet.js)
- **About:** Architecture, stack, Phase 4 charts

**Period filter:** Global year selector (2026 / 2025 / 2024 / 2023 / 2020+) applied to all Overview and Explore queries. Default: 2025 for fast load times.

---

## 6. v1 results (4-layer architecture)

The original v1 used only the SFFD dataset, parsed TTL files with regex, and used k-NN models without scikit-learn.

| Task | Flat (CSV) | KG (graph) | Delta |
|---|---|---|---|
| Multi-agency escalation (accuracy) | 83.9% | **100.0%** | **+19.2%** |
| Resolution time (MAE) | 24.75 min | **23.94 min** | **+3.3%** |
| Severity classification (accuracy) | **86.9%** | 85.1% | −2.2% |

**Interpretation:** The KG achieves 100% on multi-agency because that label is **defined** by the graph structure (Incident → Response → Unit → Agency). It's not a prediction — it's a derivation. For severity, the raw fields (priority, response count) already carry the signal, so the graph adds noise rather than value.

---

## 7. Known limitations and honest assessments

**No cross-agency CAD fusion:** SFFD and SFPD use independent systems. Semantic alignment via `IncidentConcept` is real but instance-level fusion (same physical incident in both datasets) is not achievable without a shared ID.

**FalkorDB load time:** ~22h on M1 Mac due to single-statement Cypher inserts required by FalkorDB's variable redeclaration constraint. Bulk import APIs would reduce this 10x.

**Memory-intensive queries:** Queries using `DISTINCT` over 4.7M Incident nodes crash FalkorDB on 16GB RAM. Workaround: use bounded queries starting from small reference nodes (PoliceDistrict has 10 nodes, Neighborhood has 42).

**Forecast with limited data:** 216 monthly samples is at the lower limit for meaningful time series CV. Adding more neighborhoods or switching to weekly granularity would improve model stability.

**Smart router is rule-based:** The classifier uses keyword patterns. It misroutes ambiguous questions and defaults to GRAPH on ties. A Claude API integration would improve routing accuracy significantly.

---

## 8. Development environment

### VS Code extensions

- **Stardog RDF Grammars** — `.ttl` syntax highlighting
- **Python** (Microsoft) — linting and debugging
- **Docker** — container management

### Tools for RDF exploration

- **Online:** [issemantic.net/rdf-visualizer](https://issemantic.net/rdf-visualizer), [semantechs.co.uk/turtle-editor-viewer](https://semantechs.co.uk/turtle-editor-viewer)
- **Desktop:** [Protégé](https://protege.stanford.edu) — OWL ontology editor and reasoner
- **CLI:** Apache Jena (`riot`, `sparql`) for TTL validation and SPARQL queries

### Apache Jena (optional validation)

```bash
# Validate TTL syntax
riot --validate data/rdf/incidents.ttl

# Count triples
riot --count data/rdf/*.ttl
```

---

## 9. Future work

- [ ] **Claude API router:** Replace rule-based classifier with Claude-powered query classification and natural language to Cypher translation
- [ ] **FalkorDB Text-to-Cypher:** Integrate FalkorDB's native NL→Cypher for the Explore tab
- [ ] **Bulk import:** Replace single-statement inserts with FalkorDB bulk CSV import for 10x load speed
- [ ] **Real-time updates:** Stream new SF Open Data records into the graph via the Socrata API
- [ ] **Unified CAD simulation:** Cross-reference by address + timestamp window to approximate instance-level multi-agency fusion
- [ ] **Forecasting improvements:** Weekly granularity, more neighborhoods, SARIMA baseline comparison
- [ ] **SPARQL endpoint:** Load into Apache Jena Fuseki for standards-compliant graph queries

---

## 10. References

- SF Open Data. [data.sfgov.org](https://data.sfgov.org)
- FalkorDB documentation. [docs.falkordb.com](https://docs.falkordb.com)
- SF Emergency Services Data Guide. [sf.gov](https://www.sf.gov/understanding-san-franciscos-emergency-services-data)