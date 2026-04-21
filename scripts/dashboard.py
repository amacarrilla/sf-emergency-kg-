#!/usr/bin/env python3
"""
SF Emergency KG — Phase 5: Analytical Dashboard v2
===================================================
Flask backend with year filter support on all queries.

Usage:
    python3 scripts/dashboard.py
    Open: http://localhost:5001
"""

import sys
import time
from pathlib import Path

try:
    from flask import Flask, jsonify, request, send_from_directory
    from flask_cors import CORS
except ImportError:
    print("ERROR: pip3 install flask flask-cors"); sys.exit(1)

try:
    from falkordb import FalkorDB
except ImportError:
    print("ERROR: pip3 install falkordb"); sys.exit(1)

try:
    import numpy as np
    import pandas as pd
    from sklearn.linear_model import Ridge
    from sklearn.preprocessing import LabelEncoder
    import warnings; warnings.filterwarnings('ignore')
except ImportError:
    print("ERROR: pip3 install scikit-learn pandas numpy"); sys.exit(1)

GRAPH_NAME  = "SF_KG"
FALKOR_HOST = "localhost"
FALKOR_PORT = 6379
TIMEOUT     = 120000
DOCS_DIR    = Path(__file__).parent.parent / "docs"
PORT        = 5001

app = Flask(__name__, static_folder=str(DOCS_DIR))
CORS(app)
graph = None

def get_graph():
    global graph
    if graph is None:
        db = FalkorDB(host=FALKOR_HOST, port=FALKOR_PORT)
        graph = db.select_graph(GRAPH_NAME)
    return graph

# =============================================================================
# QUERY CATALOG — all support {year_from} placeholder
# =============================================================================

QUERIES = {
    "neighborhoods_by_volume": {
        "title": "Top neighborhoods by incident volume",
        "cypher": """
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
MATCH (i)-[:IN_TIME_WINDOW]->(tw:TimeWindow)
WHERE tw.year >= '{year_from}'
RETURN n.uri AS neighborhood, count(i) AS incidents
ORDER BY incidents DESC LIMIT 10
""",
    },
    "police_response_by_district": {
        "title": "Average police response time by district",
        "cypher": """
MATCH (r:PoliceResponse)-[:RESPONSE_TO]->(i:Incident)-[:IN_POLICE_DISTRICT]->(d:PoliceDistrict)
MATCH (i)-[:IN_TIME_WINDOW]->(tw:TimeWindow)
WHERE r.responseTimeMinutes > 0 AND r.responseTimeMinutes < 120
AND tw.year >= '{year_from}'
RETURN d.uri AS district,
       round(avg(r.responseTimeMinutes) * 10) / 10 AS avg_min,
       count(r) AS responses
ORDER BY avg_min ASC
""",
    },
    "medical_by_neighborhood": {
        "title": "Medical emergencies by neighborhood",
        "cypher": """
MATCH (i:Incident)-[:HAS_CALL_TYPE]->(ct:CallType)-[:REALIZATION_OF]->(c:IncidentConcept)
MATCH (i)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
MATCH (i)-[:IN_TIME_WINDOW]->(tw:TimeWindow)
WHERE c.uri = 'Concept_MEDICAL_EMERGENCY' AND tw.year >= '{year_from}'
RETURN n.uri AS neighborhood, count(i) AS medical_incidents
ORDER BY medical_incidents DESC LIMIT 10
""",
    },
    "busiest_time_windows": {
        "title": "Busiest time windows",
        "cypher": """
MATCH (i:Incident)-[:IN_TIME_WINDOW]->(tw:TimeWindow)
WHERE tw.year >= '{year_from}'
RETURN tw.shift AS shift, tw.dayOfWeek AS day_of_week, count(i) AS incidents
ORDER BY incidents DESC LIMIT 10
""",
    },
    "yearly_trend": {
        "title": "Year-over-year incident trend",
        "cypher": """
MATCH (i:Incident)-[:IN_TIME_WINDOW]->(tw:TimeWindow)
WHERE tw.year >= '2020'
RETURN tw.year AS year, count(i) AS incidents
ORDER BY year
""",
    },
    "call_types": {
        "title": "Most common call types",
        "cypher": """
MATCH (i:Incident)-[:HAS_CALL_TYPE]->(ct:CallType)
MATCH (i)-[:IN_TIME_WINDOW]->(tw:TimeWindow)
WHERE tw.year >= '{year_from}'
RETURN ct.uri AS call_type, count(i) AS incidents
ORDER BY incidents DESC LIMIT 15
""",
    },
    "units_response_time": {
        "title": "Fire units by average response time",
        "cypher": """
MATCH (u:Unit)
WITH u
MATCH (r:Response)-[:HAS_UNIT]->(u)
WHERE r.responseTimeMinutes > 0 AND r.responseTimeMinutes < 60
WITH u, avg(r.responseTimeMinutes) AS avg_min, count(r) AS deployments
WHERE deployments > 100
RETURN u.uri AS unit, u.unitType AS type,
       round(avg_min * 10) / 10 AS avg_min, deployments
ORDER BY avg_min DESC LIMIT 10
""",
    },
    "concept_mapping": {
        "title": "Call types mapped to shared IncidentConcept",
        "cypher": """
MATCH (ct:CallType)-[:REALIZATION_OF]->(c:IncidentConcept)
RETURN c.uri AS concept, count(ct) AS call_types
ORDER BY call_types DESC
""",
    },
}


# =============================================================================
# ROUTES
# =============================================================================

@app.route('/')
def index():
    return send_from_directory(str(DOCS_DIR), 'dashboard.html')

@app.route('/map')
def map_view():
    return send_from_directory(str(DOCS_DIR), 'map.html')

@app.route('/docs/<path:filename>')
def docs_static(filename):
    return send_from_directory(str(DOCS_DIR), filename)

@app.route('/map_data.json')
def map_data_json():
    return send_from_directory(str(DOCS_DIR), 'map_data.json')

@app.route('/map_data.json_small')
def map_data_json_small():
    return send_from_directory(str(DOCS_DIR), 'map_data.json_small')

@app.route('/api/stats')
def api_stats():
    try:
        g = get_graph()
        r = g.query(
            'MATCH (n) RETURN labels(n)[0] AS type, count(n) AS count ORDER BY count DESC',
            timeout=TIMEOUT
        )
        stats = [{"type": row[0], "count": row[1]} for row in r.result_set]
        return jsonify({"stats": stats, "total": sum(s["count"] for s in stats)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/queries')
def api_queries():
    return jsonify([{"id": k, "title": v["title"]} for k, v in QUERIES.items()])

@app.route('/api/query', methods=['POST'])
def api_query():
    data      = request.get_json()
    query_id  = data.get("query_id")
    year_from = str(data.get("year_from", "2025"))

    if query_id not in QUERIES:
        return jsonify({"error": f"Unknown query: {query_id}"}), 400

    try:
        g     = get_graph()
        entry = QUERIES[query_id]
        cypher = entry["cypher"].format(year_from=year_from)
        start  = time.time()
        r      = g.query(cypher, timeout=TIMEOUT)
        elapsed = round(time.time() - start, 2)
        return jsonify({
            "title":   entry["title"],
            "columns": r.header,
            "rows":    r.result_set,
            "elapsed": elapsed,
            "count":   len(r.result_set),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/forecast')
def api_forecast():
    try:
        n      = int(request.args.get("neighborhoods", 6))
        months = int(request.args.get("months", 4))
        year_from = int(request.args.get("year_from", 2020))
        data   = generate_forecast(n, months, year_from)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/neighborhoods')
def api_neighborhoods():
    try:
        g = get_graph()
        r = g.query("""
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
RETURN n.uri AS neighborhood, count(i) AS incidents
ORDER BY incidents DESC
""", timeout=TIMEOUT)
        return jsonify([{
            "id": row[0],
            "name": row[0].replace("Neighborhood_","").replace("_"," "),
            "incidents": row[1],
        } for row in r.result_set])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# FORECAST
# =============================================================================

def generate_forecast(n_neighborhoods=6, n_months=4, year_from=2020):
    g = get_graph()

    r = g.query("""
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
RETURN n.uri AS neighborhood, count(i) AS total
ORDER BY total DESC
""", timeout=TIMEOUT)
    neighborhoods = [row[0] for row in r.result_set[:n_neighborhoods]]

    r2 = g.query(f"""
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
MATCH (i)-[:IN_TIME_WINDOW]->(tw:TimeWindow)
WHERE tw.year >= '{year_from}'
RETURN n.uri AS neighborhood, tw.year AS year, tw.month AS month, count(i) AS incidents
ORDER BY neighborhood, year, month
""", timeout=TIMEOUT)

    rows = []
    for row in r2.result_set:
        n, year, month, incidents = row
        if n in neighborhoods:
            rows.append({"neighborhood": n, "year": int(str(year)),
                         "month": int(month), "incidents": int(incidents)})

    if not rows:
        return {"historical": [], "forecast": [], "neighborhoods": []}

    df = pd.DataFrame(rows)

    # Graph features
    graph_features = {}
    for n in neighborhoods:
        r3 = g.query(f"""
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(nb:Neighborhood {{uri: '{n}'}})
RETURN count(i) AS total
""", timeout=TIMEOUT)
        total = r3.result_set[0][0] if r3.result_set else 0

        r4 = g.query(f"""
MATCH (resp:PoliceResponse)-[:RESPONSE_TO]->(i:Incident)
MATCH (i)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(nb:Neighborhood {{uri: '{n}'}})
WHERE resp.responseTimeMinutes > 0 AND resp.responseTimeMinutes < 120
RETURN avg(resp.responseTimeMinutes) AS avg_rt
""", timeout=TIMEOUT)
        val = r4.result_set[0][0] if r4.result_set else None
        police_rt = round(float(val), 2) if val else 0.0

        graph_features[n] = {
            "total_incidents": total,
            "medical_ratio": 0.18,
            "avg_police_response_min": police_rt,
            "concept_diversity": 9,
        }

    # Build and train model
    monthly = df.groupby(["neighborhood","year","month"])["incidents"].sum().reset_index()
    le = LabelEncoder()
    monthly["neighborhood_enc"] = le.fit_transform(monthly["neighborhood"])
    monthly["month_sin"] = np.sin(2 * np.pi * monthly["month"] / 12)
    monthly["month_cos"] = np.cos(2 * np.pi * monthly["month"] / 12)
    monthly["year_norm"]  = (monthly["year"] - 2020) / 6.0
    monthly["is_summer"]  = monthly["month"].isin([6,7,8]).astype(int)
    monthly["is_winter"]  = monthly["month"].isin([12,1,2]).astype(int)

    for feat in ["total_incidents","medical_ratio","avg_police_response_min","concept_diversity"]:
        monthly[feat] = monthly["neighborhood"].map({n: graph_features[n][feat] for n in neighborhoods})

    monthly["dominant_concept_enc"] = 0
    monthly = monthly.sort_values(["neighborhood","year","month"])
    monthly["lag_1"]      = monthly.groupby("neighborhood")["incidents"].shift(1)
    monthly["lag_2"]      = monthly.groupby("neighborhood")["incidents"].shift(2)
    monthly["lag_3"]      = monthly.groupby("neighborhood")["incidents"].shift(3)
    monthly["rolling_3m"] = monthly.groupby("neighborhood")["incidents"].transform(
        lambda x: x.shift(1).rolling(3).mean())
    monthly = monthly.dropna().reset_index(drop=True)

    FEATURES = ["neighborhood_enc","year_norm","month_sin","month_cos",
                "is_summer","is_winter","lag_1","lag_2","lag_3","rolling_3m",
                "total_incidents","medical_ratio","avg_police_response_min",
                "concept_diversity","dominant_concept_enc"]

    model = Ridge(alpha=1.0)
    model.fit(monthly[FEATURES].values, monthly["incidents"].values)

    last_known = monthly.sort_values(["year","month"]).groupby("neighborhood").last()
    forecast_rows = []

    for n in neighborhoods:
        if n not in last_known.index:
            continue
        row      = last_known.loc[n]
        lag1     = float(row["incidents"])
        lag2     = float(row["lag_1"])
        lag3     = float(row["lag_2"])
        rolling3 = float(row["rolling_3m"])
        n_enc    = float(row["neighborhood_enc"])
        gf       = graph_features[n]
        year_m   = int(row["year"])
        month_m  = int(row["month"])

        for step in range(1, n_months + 1):
            month_m += 1
            if month_m > 12: month_m = 1; year_m += 1
            x = np.array([[n_enc, (year_m-2020)/6.0,
                           np.sin(2*np.pi*month_m/12), np.cos(2*np.pi*month_m/12),
                           int(month_m in [6,7,8]), int(month_m in [12,1,2]),
                           lag1, lag2, lag3, rolling3,
                           gf["total_incidents"], gf["medical_ratio"],
                           gf["avg_police_response_min"], gf["concept_diversity"], 0]])
            pred = max(0, model.predict(x)[0])
            forecast_rows.append({
                "neighborhood": n.replace("Neighborhood_",""),
                "year": year_m, "month": month_m,
                "forecast": round(pred), "type": "forecast",
            })
            lag3, lag2, lag1 = lag2, lag1, pred
            rolling3 = (rolling3 * 2 + pred) / 3

    historical = [{"neighborhood": r["neighborhood"].replace("Neighborhood_",""),
                   "year": r["year"], "month": r["month"],
                   "incidents": r["incidents"], "type": "historical"}
                  for _, r in df.iterrows() if r["neighborhood"] in neighborhoods]

    return {
        "historical": historical,
        "forecast": forecast_rows,
        "neighborhoods": [n.replace("Neighborhood_","") for n in neighborhoods],
    }


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    print(f"""
╔══════════════════════════════════════════════════╗
║   SF EMERGENCY KG — DASHBOARD v2               ║
╚══════════════════════════════════════════════════╝""")
    try:
        db    = FalkorDB(host=FALKOR_HOST, port=FALKOR_PORT)
        graph = db.select_graph(GRAPH_NAME)
        print(f"  FalkorDB ✓  →  http://localhost:{PORT}")
    except Exception as e:
        print(f"  FalkorDB ✗  {e}")
        print("  Run: docker-compose up -d")
        sys.exit(1)

    app.run(host='0.0.0.0', port=PORT, debug=False)