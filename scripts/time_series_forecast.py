#!/usr/bin/env python3
"""
SF Emergency KG — Phase 4: Time Series Forecasting
====================================================
Predicts monthly incident volume per neighborhood using two models:

  BASELINE: temporal features only (year, month, lags)
  GRAPH:    baseline + graph-derived features from FalkorDB

Usage:
    python3 scripts/time_series_forecast.py
    python3 scripts/time_series_forecast.py --neighborhoods 6 --forecast-weeks 4
"""

import sys
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from pathlib import Path
import argparse

try:
    from falkordb import FalkorDB
except ImportError:
    print("ERROR: pip3 install falkordb"); sys.exit(1)

try:
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.linear_model import Ridge
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.preprocessing import LabelEncoder
    from sklearn.model_selection import TimeSeriesSplit
except ImportError:
    print("ERROR: pip3 install scikit-learn"); sys.exit(1)

GRAPH_NAME  = "SF_KG"
FALKOR_HOST = "localhost"
FALKOR_PORT = 6379
TIMEOUT     = 120000
OUTPUT_DIR  = Path("docs")


# =============================================================================
# STEP 1: EXTRACT DATA FROM FALKORDB
# =============================================================================

def extract_monthly_series(graph, top_n=6):
    print("\n  Extracting monthly incident series from FalkorDB...")

    # Get top N neighborhoods by volume
    r = graph.query("""
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
RETURN n.uri AS neighborhood, count(i) AS total
ORDER BY total DESC
""", timeout=TIMEOUT)
    top_neighborhoods = [row[0] for row in r.result_set[:top_n]]
    print(f"  Top {top_n}: {[n.replace('Neighborhood_','') for n in top_neighborhoods]}")

    # Monthly counts per neighborhood
    r2 = graph.query("""
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(n:Neighborhood)
MATCH (i)-[:IN_TIME_WINDOW]->(tw:TimeWindow)
WHERE tw.year >= '2020'
RETURN n.uri AS neighborhood, tw.year AS year, tw.month AS month, count(i) AS incidents
ORDER BY neighborhood, year, month
""", timeout=TIMEOUT)

    rows = []
    for row in r2.result_set:
        neighborhood, year, month, incidents = row
        if neighborhood in top_neighborhoods:
            rows.append({
                "neighborhood": neighborhood,
                "year":         int(str(year)),
                "month":        int(month),
                "incidents":    int(incidents),
            })

    df = pd.DataFrame(rows)
    print(f"  Extracted {len(df):,} monthly data points across {df['neighborhood'].nunique()} neighborhoods")
    return df, top_neighborhoods


def extract_graph_features(graph, neighborhoods):
    print("\n  Extracting graph-derived features per neighborhood...")
    features = {}

    for n in neighborhoods:
        f = {}

        r = graph.query(f"""
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(nb:Neighborhood {{uri: '{n}'}})
RETURN count(i) AS total
""", timeout=TIMEOUT)
        f["total_incidents"] = r.result_set[0][0] if r.result_set else 0

        r = graph.query(f"""
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(nb:Neighborhood {{uri: '{n}'}})
MATCH (i)-[:HAS_CALL_TYPE]->(ct:CallType)-[:REALIZATION_OF]->(c:IncidentConcept)
WHERE c.uri = 'Concept_MEDICAL_EMERGENCY'
RETURN count(i) AS medical
""", timeout=TIMEOUT)
        medical = r.result_set[0][0] if r.result_set else 0
        f["medical_ratio"] = medical / max(f["total_incidents"], 1)

        r = graph.query(f"""
MATCH (resp:PoliceResponse)-[:RESPONSE_TO]->(i:Incident)
MATCH (i)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(nb:Neighborhood {{uri: '{n}'}})
WHERE resp.responseTimeMinutes > 0 AND resp.responseTimeMinutes < 120
RETURN avg(resp.responseTimeMinutes) AS avg_rt
""", timeout=TIMEOUT)
        val = r.result_set[0][0] if r.result_set else None
        f["avg_police_response_min"] = round(float(val), 2) if val else 0.0

        r = graph.query(f"""
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(nb:Neighborhood {{uri: '{n}'}})
MATCH (i)-[:HAS_CALL_TYPE]->(ct:CallType)-[:REALIZATION_OF]->(c:IncidentConcept)
RETURN count(DISTINCT c.uri) AS diversity
""", timeout=TIMEOUT)
        f["concept_diversity"] = r.result_set[0][0] if r.result_set else 1

        r = graph.query(f"""
MATCH (i:Incident)-[:HAS_LOCATION]->(loc:Location)-[:IN_NEIGHBORHOOD]->(nb:Neighborhood {{uri: '{n}'}})
MATCH (i)-[:HAS_CALL_TYPE]->(ct:CallType)-[:REALIZATION_OF]->(c:IncidentConcept)
RETURN c.uri AS concept, count(i) AS cnt
ORDER BY cnt DESC LIMIT 1
""", timeout=TIMEOUT)
        f["dominant_concept"] = r.result_set[0][0] if r.result_set else "Concept_OTHER"

        features[n] = f
        short = n.replace("Neighborhood_", "")
        print(f"    {short:<35} total={f['total_incidents']:>6,}  "
              f"medical={f['medical_ratio']:.2f}  "
              f"police_rt={f['avg_police_response_min']:.1f}min  "
              f"diversity={f['concept_diversity']}")

    return features


# =============================================================================
# STEP 2: BUILD FEATURE MATRIX
# =============================================================================

def build_feature_matrix(df, graph_features, neighborhoods):
    monthly = df.groupby(["neighborhood", "year", "month"])["incidents"].sum().reset_index()

    le = LabelEncoder()
    monthly["neighborhood_enc"] = le.fit_transform(monthly["neighborhood"])

    monthly["month_sin"] = np.sin(2 * np.pi * monthly["month"] / 12)
    monthly["month_cos"] = np.cos(2 * np.pi * monthly["month"] / 12)
    monthly["year_norm"]  = (monthly["year"] - 2020) / 6.0
    monthly["is_summer"]  = monthly["month"].isin([6, 7, 8]).astype(int)
    monthly["is_winter"]  = monthly["month"].isin([12, 1, 2]).astype(int)

    for feat in ["total_incidents", "medical_ratio", "avg_police_response_min", "concept_diversity"]:
        monthly[feat] = monthly["neighborhood"].map({n: graph_features[n][feat] for n in neighborhoods})

    concept_map = {n: graph_features[n]["dominant_concept"] for n in neighborhoods}
    monthly["dominant_concept_enc"] = LabelEncoder().fit_transform(
        monthly["neighborhood"].map(concept_map)
    )

    monthly = monthly.sort_values(["neighborhood", "year", "month"])
    monthly["lag_1"]      = monthly.groupby("neighborhood")["incidents"].shift(1)
    monthly["lag_2"]      = monthly.groupby("neighborhood")["incidents"].shift(2)
    monthly["lag_3"]      = monthly.groupby("neighborhood")["incidents"].shift(3)
    monthly["rolling_3m"] = (monthly.groupby("neighborhood")["incidents"]
                             .transform(lambda x: x.shift(1).rolling(3).mean()))
    monthly = monthly.dropna().reset_index(drop=True)

    BASELINE_FEATURES = [
        "neighborhood_enc", "year_norm", "month_sin", "month_cos",
        "is_summer", "is_winter", "lag_1", "lag_2", "lag_3", "rolling_3m",
    ]
    GRAPH_FEATURES = BASELINE_FEATURES + [
        "total_incidents", "medical_ratio",
        "avg_police_response_min", "concept_diversity", "dominant_concept_enc",
    ]

    X_baseline = monthly[BASELINE_FEATURES].values
    X_graph    = monthly[GRAPH_FEATURES].values
    y          = monthly["incidents"].values

    return X_baseline, X_graph, y, monthly, BASELINE_FEATURES, GRAPH_FEATURES


# =============================================================================
# STEP 3: TRAIN AND EVALUATE
# =============================================================================

def evaluate_models(X_baseline, X_graph, y):
    print("\n  Training and evaluating models...")

    tscv = TimeSeriesSplit(n_splits=4)
    models = {
        "Baseline — Ridge":         (Ridge(alpha=1.0),                                    X_baseline),
        "Baseline — RandomForest":  (RandomForestRegressor(n_estimators=100, random_state=42), X_baseline),
        "Graph — Ridge":            (Ridge(alpha=1.0),                                    X_graph),
        "Graph — RandomForest":     (RandomForestRegressor(n_estimators=100, random_state=42), X_graph),
        "Graph — GradientBoosting": (GradientBoostingRegressor(n_estimators=100, random_state=42), X_graph),
    }

    results = {}
    for name, (model, X) in models.items():
        maes, rmses, r2s = [], [], []
        for train_idx, test_idx in tscv.split(X):
            model.fit(X[train_idx], y[train_idx])
            y_pred = model.predict(X[test_idx])
            maes.append(mean_absolute_error(y[test_idx], y_pred))
            rmses.append(np.sqrt(mean_squared_error(y[test_idx], y_pred)))
            r2s.append(r2_score(y[test_idx], y_pred))

        results[name] = {
            "mae":   round(np.mean(maes), 1),
            "rmse":  round(np.mean(rmses), 1),
            "r2":    round(np.mean(r2s), 3),
            "model": model,
        }
        tag = "📊 GRAPH" if "Graph" in name else "📋 BASE "
        print(f"    {tag}  {name:<35}  MAE={results[name]['mae']:>7,.1f}  "
              f"RMSE={results[name]['rmse']:>7,.1f}  R²={results[name]['r2']:.3f}")

    return results


# =============================================================================
# STEP 4: FEATURE IMPORTANCE
# =============================================================================

def get_feature_importance(results, feature_names):
    for model_name in ["Graph — RandomForest", "Graph — Ridge"]:
        model = results[model_name]["model"]
        if hasattr(model, "feature_importances_"):
            return list(zip(feature_names, model.feature_importances_))
        elif hasattr(model, "coef_"):
            imp = np.abs(model.coef_) / (np.abs(model.coef_).sum() + 1e-10)
            return list(zip(feature_names, imp))
    return []


# =============================================================================
# STEP 5: FORECAST
# =============================================================================

def forecast_next_months(results, monthly_df, graph_features, neighborhoods,
                         graph_feature_names, n_months=4):
    print(f"\n  Generating {n_months}-month forecast per neighborhood...")

    best_model = results["Graph — Ridge"]["model"]

    # Retrain on ALL data before forecasting
    X_all = monthly_df[graph_feature_names].values
    y_all = monthly_df["incidents"].values
    best_model.fit(X_all, y_all)

    # Last known row per neighborhood
    last_known = (monthly_df.sort_values(["year", "month"])
                  .groupby("neighborhood").last())

    forecasts = []

    for n in neighborhoods:
        if n not in last_known.index:
            print(f"  WARNING: {n} not in monthly data, skipping")
            continue

        row      = last_known.loc[n]
        lag1     = float(row["incidents"])
        lag2     = float(row["lag_1"])
        lag3     = float(row["lag_2"])
        rolling3 = float(row["rolling_3m"])

        gf       = graph_features[n]
        n_enc    = float(row["neighborhood_enc"])
        dc_enc   = float(row["dominant_concept_enc"])

        year_m  = int(row["year"])
        month_m = int(row["month"])

        for step in range(1, n_months + 1):
            month_m += 1
            if month_m > 12:
                month_m = 1
                year_m += 1

            month_sin = np.sin(2 * np.pi * month_m / 12)
            month_cos = np.cos(2 * np.pi * month_m / 12)
            year_norm  = (year_m - 2020) / 6.0
            is_summer  = int(month_m in [6, 7, 8])
            is_winter  = int(month_m in [12, 1, 2])

            x = np.array([[
                n_enc, year_norm, month_sin, month_cos,
                is_summer, is_winter,
                lag1, lag2, lag3, rolling3,
                gf["total_incidents"], gf["medical_ratio"],
                gf["avg_police_response_min"], gf["concept_diversity"],
                dc_enc,
            ]])

            pred = max(0, best_model.predict(x)[0])

            forecasts.append({
                "neighborhood": n.replace("Neighborhood_", ""),
                "year":   year_m,
                "month":  month_m,
                "forecast": round(pred),
                "step":   step,
            })

            lag3, lag2, lag1 = lag2, lag1, pred
            rolling3 = (rolling3 * 2 + pred) / 3

    forecast_df = pd.DataFrame(forecasts)
    print(f"  Generated {len(forecast_df)} forecast rows for {forecast_df['neighborhood'].nunique()} neighborhoods")
    return forecast_df


# =============================================================================
# STEP 6: VISUALIZATIONS
# =============================================================================

def plot_results(monthly_df, forecast_df, importance, output_dir, neighborhoods):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Chart 1: Historical + Forecast — one subplot per neighborhood
    n_hoods = len(neighborhoods)
    ncols   = min(3, n_hoods)
    nrows   = (n_hoods + ncols - 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=(6 * ncols, 5 * nrows), squeeze=False)
    axes_flat = axes.flatten()

    fig.suptitle("SF Emergency KG — Incident Forecast by Neighborhood\n"
                 "(Graph-enriched Ridge model, 2020–2026)", fontsize=13, fontweight='bold')

    # Hide unused subplots
    for idx in range(n_hoods, len(axes_flat)):
        axes_flat[idx].set_visible(False)

    for idx, n in enumerate(neighborhoods):
        ax = axes_flat[idx]
        short = n.replace("Neighborhood_", "").replace("_", " ")

        hist = (monthly_df[monthly_df["neighborhood"] == n]
                .copy().sort_values(["year", "month"]))
        hist["date"] = pd.to_datetime(hist[["year", "month"]].assign(day=1))

        short_no_prefix = n.replace("Neighborhood_", "")
        fore = (forecast_df[forecast_df["neighborhood"] == short_no_prefix]
                .copy().sort_values(["year", "month"]))
        fore["date"] = pd.to_datetime(fore[["year", "month"]].assign(day=1))

        ax.plot(hist["date"], hist["incidents"], color="#2196F3",
                linewidth=1.5, label="Historical", alpha=0.85)

        if not fore.empty:
            ax.plot(fore["date"], fore["forecast"], color="#FF5722",
                    linewidth=2, linestyle="--", marker="o", markersize=5,
                    label="Forecast")
            ax.axvline(x=hist["date"].max(), color="gray", linestyle=":", alpha=0.5)

        ax.set_title(short, fontsize=10, fontweight='bold')
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax.tick_params(axis='x', rotation=30, labelsize=7)
        ax.legend(fontsize=7)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    p1 = output_dir / "forecast_comparison.png"
    plt.savefig(p1, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Chart saved: {p1}")

    # Chart 2: Feature importance
    if importance:
        imp_sorted = sorted(importance, key=lambda x: x[1], reverse=True)
        names  = [i[0].replace("_", " ") for i in imp_sorted]
        values = [i[1] for i in imp_sorted]
        GRAPH_ONLY = {"total incidents", "medical ratio", "avg police response min",
                      "concept diversity", "dominant concept enc"}
        colors = ["#FF5722" if n in GRAPH_ONLY else "#2196F3" for n in names]

        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.barh(names, values, color=colors)
        ax.set_xlabel("Feature Importance", fontsize=11)
        ax.set_title("Feature Importance — Graph Ridge model\n"
                     "(🟠 Graph-derived   🔵 Baseline temporal)", fontsize=12, fontweight='bold')
        ax.invert_yaxis()
        ax.grid(True, axis='x', alpha=0.3)
        for bar, val in zip(bars, values):
            ax.text(val + 0.001, bar.get_y() + bar.get_height()/2,
                    f"{val:.3f}", va='center', fontsize=8)
        plt.tight_layout()
        p2 = output_dir / "feature_importance.png"
        plt.savefig(p2, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  Chart saved: {p2}")


# =============================================================================
# STEP 7: SUMMARY
# =============================================================================

def print_summary(results, forecast_df):
    print(f"\n{'='*65}")
    print(f"  PHASE 4 — TIME SERIES FORECASTING RESULTS")
    print(f"{'='*65}")
    print(f"\n  MODEL COMPARISON (TimeSeriesSplit CV, 4 folds):")
    print(f"  {'Model':<38} {'MAE':>8} {'RMSE':>8} {'R²':>7}")
    print(f"  {'─'*38} {'─'*8} {'─'*8} {'─'*7}")

    baseline_mae = None
    for name, r in results.items():
        tag = "📊" if "Graph" in name else "📋"
        print(f"  {tag} {name:<36} {r['mae']:>8,.1f} {r['rmse']:>8,.1f} {r['r2']:>7.3f}")
        if name == "Baseline — RandomForest":
            baseline_mae = r['mae']

    best_graph_mae = min(r['mae'] for n, r in results.items() if 'Graph' in n)
    if baseline_mae and best_graph_mae:
        imp = (baseline_mae - best_graph_mae) / baseline_mae * 100
        print(f"\n  Best graph model vs baseline RF: {imp:.1f}% MAE reduction")

    print(f"\n  4-MONTH FORECAST:")
    print(f"  {'Neighborhood':<35} {'Month':>10} {'Forecast':>10}")
    print(f"  {'─'*35} {'─'*10} {'─'*10}")
    for _, row in forecast_df.iterrows():
        month_label = f"{int(row['year'])}-{int(row['month']):02d}"
        print(f"  {row['neighborhood']:<35} {month_label:>10} {row['forecast']:>10,}")

    print(f"\n  WHY GRAPH FEATURES HELP:")
    print(f"  The graph extracts relational context impossible to get from a flat CSV:")
    print(f"  • medical_ratio   → Incident→CallType→IncidentConcept (3 hops)")
    print(f"  • police_rt       → PoliceResponse→Incident→Location→Neighborhood (4 hops)")
    print(f"  • concept_diversity → distinct concepts per neighborhood (aggregation over graph)")
    print(f"  These are computable from joins but require pre-built pipelines.")
    print(f"  The graph provides them as natural traversals with no schema changes.")
    print(f"{'='*65}\n")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='SF Emergency KG — Phase 4: Forecasting')
    parser.add_argument('--graph',          default=GRAPH_NAME)
    parser.add_argument('--host',           default=FALKOR_HOST)
    parser.add_argument('--port', type=int, default=FALKOR_PORT)
    parser.add_argument('--neighborhoods',  type=int, default=6)
    parser.add_argument('--forecast-weeks', type=int, default=4)
    parser.add_argument('--output',         default=str(OUTPUT_DIR))
    args = parser.parse_args()

    print(f"\n{'='*65}")
    print(f"  SF EMERGENCY KG — PHASE 4: TIME SERIES FORECASTING")
    print(f"{'='*65}")
    print(f"  Graph: {args.graph} @ {args.host}:{args.port}")
    print(f"  Neighborhoods: top {args.neighborhoods}")
    print(f"  Forecast: {args.forecast_weeks} months ahead")
    print(f"{'='*65}")

    print("\n  Connecting to FalkorDB...", end=' ')
    try:
        db    = FalkorDB(host=args.host, port=args.port)
        graph = db.select_graph(args.graph)
        print("✓")
    except Exception as e:
        print(f"✗\n  ERROR: {e}"); sys.exit(1)

    df, neighborhoods = extract_monthly_series(graph, args.neighborhoods)
    graph_feats       = extract_graph_features(graph, neighborhoods)

    print("\n  Building feature matrices...")
    X_base, X_graph, y, monthly_df, base_names, graph_names = \
        build_feature_matrix(df, graph_feats, neighborhoods)
    print(f"  Baseline features: {len(base_names)}")
    print(f"  Graph features:    {len(graph_names)}")
    print(f"  Training samples:  {len(y):,}")

    results    = evaluate_models(X_base, X_graph, y)
    importance = get_feature_importance(results, graph_names)

    forecast_df = forecast_next_months(
        results, monthly_df, graph_feats, neighborhoods,
        graph_names, n_months=args.forecast_weeks
    )

    print("\n  Generating charts...")
    plot_results(monthly_df, forecast_df, importance, args.output, neighborhoods)

    print_summary(results, forecast_df)


if __name__ == '__main__':
    main()