# SF Emergency Services Knowledge Graph — Design Document

## Proof of Concept: Knowledge Graph vs Traditional ML for Emergency Prediction

**Author:** Andrés
**Date:** April 2026
**Version:** 1.0
**Status:** Complete — all 4 layers implemented and tested with real data

---

## 1. Origin and motivation

This project originated from the analysis of Steve Hedden's article *"Using a Knowledge Graph to Generate Predictive Models for the Oscars"* (Towards AI, March 2026), which proposes a 4-layer architecture for building a reusable semantic data foundation.

Hedden's key insight: the article is not about a single predictive model — it's about constructing an **infrastructure** that lets anyone (human or agent) produce forecasts in a reproducible way. The Oscars are simply the case study.

Our goal: extrapolate the same architecture to **San Francisco's multi-agency emergency response system**, demonstrating its value with three prediction targets and a direct comparison against traditional flat-table ML.

**Prediction targets:**

1. **Severity classification** (MINOR / MODERATE / SEVERE / CRITICAL)
2. **Multi-agency escalation** (binary: does the incident involve 2+ agencies?)
3. **Resolution time estimation** (regression: minutes from call receipt to last unit available)

---

## 2. Architecture — 4 layers

```
┌─────────────────────────────────────────────────────────────┐
│  4  PREDICTIVE MODELS                          [user-defined]│
│  Severity | Multi-agency | Resolution time                   │
│  k-NN classifier/regressor, KG features vs flat baseline.    │
├─────────────────────────────────────────────────────────────┤
│  3  ENRICHMENT (light)                         [user-defined]│
│  Temporal: hour, day_of_week, month, season, is_weekend,     │
│  is_night, is_rush_hour. Future: Weather, Census, Zoning.    │
├─────────────────────────────────────────────────────────────┤
│  2  INFERENCE                                                │
│  isMultiAgency | agencyCount | avgResponseTime |             │
│  maxResponseTime | locationPriorIncidentCount | severity      │
│  Derived from graph traversal, no external data added.       │
├─────────────────────────────────────────────────────────────┤
│  1  FOUNDATION                                   [published] │
│  ONTOLOGY: 15 classes, 19 object props, 44 data props        │
│  INSTANCE DATA: 7.2M rows → ~118M triples                   │
│  Source: SF Fire Dept Calls for Service (nuek-vuh3)          │
└─────────────────────────────────────────────────────────────┘
```

### Analogy with Hedden's Oscar model

| Oscars (Hedden) | Emergency Services (ours) |
|---|---|
| Nomination | Incident (central entity, identified by CAD #) |
| AwardSystem (Oscars, BAFTA, SAG...) | Agency (SFFD, SFPD, EMS) |
| AwardConcept (aligns categories across systems) | IncidentConcept (aligns call types across agencies) |
| Film | Location (intersection / call box) |
| Person | Unit (Engine 7, Medic 33, SFPD 3B) |
| AwardCategory | CallType (per-agency incident label) |
| AwardCeremony | TimeWindow (year, month, shift) |
| ForecastSet / Forecast | PredictionSet / Prediction |
| Precursor awards → predict Oscar | Dispatch signals → predict severity |

---

## 3. Ontology

### 3.1 Classes (15)

| Class | Role | Key properties |
|---|---|---|
| **Incident** | Central entity (pivot) | cadNumber, receivedTimestamp, priorityCode, finalDisposition, responseCount, totalResolutionMinutes |
| **Response** | Unit dispatch lifecycle | dispatchTimestamp, enRouteTimestamp, onSceneTimestamp, transportTimestamp, availableTimestamp, responseTimeMinutes |
| **Unit** | Specific vehicle/team | unitId, unitType (ENGINE, TRUCK, MEDIC, CHIEF, PATROL...) |
| **Agency** | Reporting system | title (SFFD, SFPD, EMS) |
| **CallType** | Per-agency label | linked to Agency and IncidentConcept |
| **IncidentConcept** | Cross-agency alignment | abstract type unifying equivalent call types |
| **Location** | Geographic point | address, latitude, longitude, zipCode |
| **Neighborhood** | SF neighborhood | label |
| **StationArea** | Fire station zone | linked to Battalion |
| **Battalion** | Command grouping | label |
| **TimeWindow** | Temporal context | year, month, dayOfWeek, shift |
| **SeverityLevel** | Outcome classification | MINOR, MODERATE, SEVERE, CRITICAL |
| **PredictionSet** | Model run metadata | modelName, featureSet, dateGenerated, accuracy |
| **Prediction** | Per-incident forecast | predictedClass, predictedProbability, predictedValue |
| **EnrichmentSource** | Data provenance | NOAA, Census ACS, SF Planning, Events Calendar |

### 3.2 Relationships

```
Incident ──hasResponse──► Response ──hasUnit──► Unit ──belongsToAgency──► Agency
    │
    ├── hasCallType ──► CallType ──realizationOf──► IncidentConcept
    │                       └──── hasAgency ──► Agency
    │
    ├── hasLocation ──► Location ──inNeighborhood──► Neighborhood
    │                       └──── inStationArea ──► StationArea ──inBattalion──► Battalion
    │
    ├── inTimeWindow ──► TimeWindow
    ├── hasSeverity ──► SeverityLevel
    └── enrichedBy ──► EnrichmentSource

PredictionSet ──hasPrediction──► Prediction ──predictsIncident──► Incident
```

### 3.3 Inferred relationships (Layer 2)

- `responseTo` — inverse of hasResponse
- `hasIncidentHistory` — inverse of hasLocation
- `isMultiAgency` — derived: true if responses span 2+ distinct agencies
- `nominatedFor` — derived: direct link Unit → Incident via Response chain

### 3.4 Cross-agency concept alignment

| IncidentConcept | SFFD CallType | SFPD CallType | EMS CallType |
|---|---|---|---|
| STRUCTURE_FIRE | Structure Fire | — | — |
| TRAFFIC_COLLISION | Traffic Collision | Vehicle Accident | — |
| MEDICAL_EMERGENCY | Medical Incident | — | Medical Emergency |
| ASSAULT | — | Assault | Trauma / Injury |
| HAZMAT | HazMat | — | — |
| ALARM | Alarms | — | — |
| WELFARE_CHECK | — | Well Being Check | — |

---

## 4. Data sources

### 4.1 Primary: Fire Dept Calls for Service

- **URL:** https://data.sfgov.org/d/nuek-vuh3
- **Volume:** ~7.2M rows (~1.5 GB)
- **Granularity:** One row per unit dispatched (not per incident)
- **Key fields:** Call Number (CAD #), Unit ID, Call Type, all timestamps (Received, Dispatch, Response, On Scene, Available), Address, Battalion, Station Area, Neighborhood, Priority, Disposition, lat/lng
- **Update frequency:** Daily

### 4.2 Complementary: Fire Incidents

- **URL:** https://data.sfgov.org/d/wr8u-xric
- **Volume:** ~660K rows
- **Use:** Ground truth — has property loss, field observations (prime situation), actions taken

### 4.3 Police: Law Enforcement Dispatched Calls — Closed

- **URL:** https://data.sfgov.org/d/2zdj-bwza
- **Volume:** ~5M rows
- **Use:** Cross-agency join via shared CAD Number
- **Note:** Replaces deprecated dataset `hz9m-tj6z`

### 4.4 Download instructions

1. Open the dataset URL
2. Click "Export" → CSV
3. Save to `data/raw/`

No registration required. All datasets are public.

---

## 5. Pipeline — step by step

### Step 1: Layer 1 — Foundation (csv_to_rdf.py)

Reads the Fire Calls CSV, groups rows by Call Number (one Incident per group), creates Response entities for each row, extracts unique Units/Locations/Neighborhoods/Stations, maps CallTypes to IncidentConcepts, and outputs 8 Turtle files.

```bash
python3 scripts/csv_to_rdf.py \
  --input data/raw/fire_calls_for_service.csv \
  --output data/rdf/
```

Output: ~118M triples across 8 files (incidents, responses, units, locations, neighborhoods, stations, call_types, time_windows).

### Step 2: Layer 2 — Inference (inference.py)

Parses Layer 1 output and derives: isMultiAgency (by traversing Response → Unit → Agency), locationPriorIncidentCount, severity classification, response time aggregates (avg, max, min).

```bash
python3 scripts/inference.py \
  --input data/rdf/ \
  --output data/rdf/inferred/
```

Output: ~23M additional inferred triples. Total graph: ~141M triples.

### Step 3: Layers 3+4 — Enrichment + Prediction (predict.py)

Adds temporal features (hour, day, month, season, weekend, night, rush hour), builds KG and flat feature matrices, trains k-NN models for all three targets, and compares results.

```bash
python3 scripts/predict.py \
  --rdf-dir data/rdf/ \
  --inferred-dir data/rdf/inferred/ \
  --limit 20000
```

Use `--limit` because k-NN is O(n²). 20K incidents is sufficient for statistically valid comparison.

### Step 4: Map export (export_map_data.py)

Exports incident data from the graph to a lightweight JSON for the interactive map.

```bash
python3 scripts/export_map_data.py \
  --rdf-dir data/rdf/ \
  --inferred-dir data/rdf/inferred/ \
  --output docs/map_data.json \
  --limit 200000
```

### Step 5: View visualizations

```bash
cd docs/
python3 -m http.server 8080
# Open http://localhost:8080 (ontology) or http://localhost:8080/map.html (map)
```

---

## 6. Results

See [RESULTS.md](RESULTS.md) for the full analysis. Summary:

| Task | Flat (CSV only) | KG (graph) | Delta |
|---|---|---|---|
| Multi-agency escalation (accuracy) | 83.9% | **100.0%** | **+19.2%** |
| Resolution time (MAE) | 24.75 min | **23.94 min** | **+3.3%** |
| Severity classification (accuracy) | **86.9%** | 85.1% | −2.2% |

**Key insight:** The KG excels when the prediction depends on relationships between entities (multi-agency requires traversing Incident → Response → Unit → Agency). It adds less value when the signal already exists in raw fields (severity depends on priority and response count).

---

## 7. Development environment

### VS Code setup

Install **Stardog RDF Grammars** extension for `.ttl` syntax highlighting:
`Ctrl+Shift+X` → search "Stardog RDF Grammars" → Install

### RDF visualization tools

- **Web (no install):** issemantic.net/rdf-visualizer, rdfvisualizer.com, semantechs.co.uk/turtle-editor-viewer
- **Desktop:** Protégé (protege.stanford.edu) for ontology editing and reasoning

### Tech stack

| Component | Technology |
|---|---|
| Ontology | OWL 2 / Turtle (.ttl) |
| Instance pipeline | Python 3.8+ (stdlib only, no dependencies) |
| ML models | k-NN (stdlib implementation, no sklearn) |
| Visualization | Leaflet.js (map), Canvas 2D (graph), HTML/CSS/JS |
| Hosting | GitHub Pages (static) |

---

## 8. Future work

- [ ] **Full Layer 3 enrichment:** NOAA weather API, Census ACS demographics, SF Planning zoning data
- [ ] **Police dataset integration:** Join police calls via shared CAD number for true multi-agency coverage
- [ ] **Better ML models:** Replace k-NN with scikit-learn (random forest, gradient boosting, constrained logistic regression as in Hedden's approach)
- [ ] **Natural language queries:** Chat interface using LLM + pre-aggregated data for ad-hoc questions
- [ ] **SPARQL endpoint:** Load into Apache Jena Fuseki or GraphDB for native graph queries
- [ ] **Fire Incidents integration:** Use property loss and prime situation as ground truth for severity

---

## 9. References

- Hedden, S. (2026). *Using a Knowledge Graph to Generate Predictive Models for the Oscars*. [Towards AI](https://pub.towardsai.net/using-a-knowledge-graph-to-build-a-predictive-model-for-the-oscars-8203bc11d906) · [GitHub](https://github.com/SteveHedden/fckg) · [Interactive viewer](https://stevehedden.github.io/fckg/)
- SF Open Data. [data.sfgov.org](https://data.sfgov.org)
- SF Emergency Services Data Guide. [sf.gov](https://www.sf.gov/understanding-san-franciscos-emergency-services-data)