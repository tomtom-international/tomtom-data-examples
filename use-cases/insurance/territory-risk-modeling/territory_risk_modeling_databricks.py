# Databricks notebook source
# MAGIC %md
# MAGIC # Territory risk modeling
# MAGIC
# MAGIC Road data as features for a territory risk model.
# MAGIC
# MAGIC A motor insurer rates territory because that is how the evidence arrives. Claims come with
# MAGIC a postcode, so the model has to predict for a postcode. Everything the model knows about a
# MAGIC place has to be attached to that postcode too.
# MAGIC
# MAGIC The awkward part is that a postcode is not one kind of road. It can hold a motorway and a
# MAGIC cul-de-sac. Averaging across them describes neither, and the rating ends up covering roads
# MAGIC that behave nothing alike. Most insurers know this and have nothing finer to put in its
# MAGIC place.
# MAGIC
# MAGIC TomTom measures roads one at a time. How much traffic each one carries, how fast that
# MAGIC traffic moves, how steady it is. This notebook turns those measurements into a few numbers
# MAGIC per postcode, puts them into a claims model, and then asks the two questions that decide
# MAGIC whether they are worth buying: how much did they add, and which of them did the work.
# MAGIC
# MAGIC ```
# MAGIC roads -> map cells -> postcodes -> model -> explanation
# MAGIC ```
# MAGIC
# MAGIC **What you need.** Two free TomTom samples from Databricks Marketplace: Traffic Stats,
# MAGIC hourly speeds per road, and Traffic Volumes, average daily traffic per road. The notebook
# MAGIC runs on the `london` extract, which reaches across southern England.
# MAGIC
# MAGIC **What stands in for claims.** Claims are private, so the notebook downloads two open
# MAGIC datasets instead: injury collisions reported to the police in Great Britain
# MAGIC ([STATS19](https://www.data.gov.uk/dataset/cb7ae6f0-4be6-4935-9277-47e5ce24a11f/road-accidents-safety-data))
# MAGIC and postcode locations from Ordnance Survey
# MAGIC ([Code-Point Open](https://www.ordnancesurvey.co.uk/products/code-point-open)).
# MAGIC Collisions are not claims, but they are the event behind most of them. Your own claims
# MAGIC table replaces them in section 2.

# COMMAND ----------

# MAGIC %pip install xgboost shap h3==4.5.0 pyproj folium==0.20.0 scipy shapely==2.1.2

# COMMAND ----------

import contextlib
import io
import json
import tempfile
import urllib.request
import zipfile
from pathlib import Path

import branca
import folium
import h3
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import shapely
from pyproj import Transformer
from pyspark.sql.types import DecimalType
from scipy.spatial import cKDTree
from scipy.stats import spearmanr

# The runtime's threadpoolctl prints a harmless traceback the first time scikit-learn loads.
with contextlib.redirect_stderr(io.StringIO()):
    import shap
    import xgboost as xgb
    from sklearn.metrics import mean_poisson_deviance
    from sklearn.model_selection import GroupKFold

sns.set_theme(style="whitegrid", palette="colorblind")

# Both open datasets give British National Grid coordinates, in metres. Distances are measured
# there; latitude and longitude are only for H3 and the maps.
TO_WGS84 = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)
TO_GRID = Transformer.from_crs("EPSG:4326", "EPSG:27700", always_xy=True)


def collect(query):
    """Run a query into pandas. Spark types expressions built from literals such as `1.0` as
    DECIMAL, which pandas receives as decimal.Decimal objects that matplotlib cannot plot."""
    frame = spark.sql(query)
    decimals = {f.name: float for f in frame.schema if isinstance(f.dataType, DecimalType)}
    return frame.toPandas().astype(decimals)


def download(url, name):
    """Fetch a public file once per session into the driver's temp directory."""
    path = Path(tempfile.gettempdir()) / name
    if not path.exists():
        urllib.request.urlretrieve(url, path)
    return path

# COMMAND ----------

dbutils.widgets.text("stats_catalog", "TomTom_Traffic_Stats", "Traffic Stats catalog")
dbutils.widgets.text("volumes_catalog", "TomTom_Traffic_Volumes", "Traffic Volumes catalog")
dbutils.widgets.text("vintage_year", "2025", "Traffic Volumes vintage")
dbutils.widgets.text("first_date", "2025-09-01", "First Traffic Stats date, UTC")
dbutils.widgets.text("last_date", "2025-09-07", "Last Traffic Stats date, UTC")
dbutils.widgets.dropdown("postcode_level", "sector", ["sector", "district"], "Postcode level")

stats = dbutils.widgets.get("stats_catalog") + ".traffic_stats_batch"
volumes = dbutils.widgets.get("volumes_catalog") + ".traffic_volumes"
vintage_year = int(dbutils.widgets.get("vintage_year"))
first_date = dbutils.widgets.get("first_date")
last_date = dbutils.widgets.get("last_date")
postcode_level = dbutils.widgets.get("postcode_level")

REGION = "london"  # the collisions and postcodes are British, so the region is fixed

for schema in (stats, volumes):
    try:
        spark.sql(f"DESCRIBE SCHEMA {schema}")
    except Exception as error:
        raise ValueError(
            f"Cannot read {schema}. The error was: {error}. Set the catalog widgets at the top "
            f"of this notebook to the catalogs you accepted when you installed the two "
            f"listings. Run SHOW CATALOGS if you are not sure what they were called."
        ) from None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Roads into map cells
# MAGIC
# MAGIC The two datasets describe the same roads from different angles, and both carry an
# MAGIC [H3](https://h3geo.org/) cell of about a tenth of a square kilometre. That cell is where
# MAGIC they meet, and it is small enough to keep a motorway apart from the streets beside it.
# MAGIC
# MAGIC Combining roads is where this usually goes wrong, so two rules apply. Traffic is summed,
# MAGIC which keeps a road that happens to be drawn as ten short segments from counting ten times.
# MAGIC Speed is averaged with length as the weight, so a long stretch counts for more than a
# MAGIC stub. Each dataset is reduced on its own before they are joined, so hourly rows never
# MAGIC multiply road rows.
# MAGIC
# MAGIC The summary shows how much of the network each dataset reaches. Nothing is filled in where
# MAGIC they fall short; that gap becomes a feature later.
# MAGIC
# MAGIC The dates default to the first week of September. The sample holds two months, and
# MAGIC reading all of it measures about 5% more map cells, mostly quiet roads that need longer to
# MAGIC collect enough vehicles. The results in section 3 do not change, so one week is the
# MAGIC cheaper default.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW volume_cells AS
    SELECT h3_r9,
           sum(length_m) / 1000                                           AS volume_km,
           sum(aadt * length_m) / 1000                                    AS vehicle_km_per_day,
           sum(CASE WHEN frc <= 2 THEN aadt * length_m ELSE 0 END) / 1000 AS major_vehicle_km_per_day,
           sum(aggregate(slice(aadt_by_day, 1, 5), 0D, (acc, x) -> acc + x) / 5 * length_m) / 1000
                                                                          AS weekday_vehicle_km_per_day,
           sum(aggregate(slice(aadt_by_day, 6, 2), 0D, (acc, x) -> acc + x) / 2 * length_m) / 1000
                                                                          AS weekend_vehicle_km_per_day
    FROM {volumes}.aadt_segments
    WHERE region = '{REGION}' AND vintage_year = {vintage_year}
    GROUP BY h3_r9
    """)

# One row per road, from its observed hours. An hour measured from a single vehicle carries no
# speed distribution: it still counts towards the average, and its share becomes a feature.
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW road_signals AS
    WITH hours AS (
        SELECT s.dseg_id, s.h3_r9, s.length_m, s.speed_limit_kph,
               h.harmonic_speed_kph                    AS speed,
               h.stddev_speed_kph                      AS stddev,
               element_at(h.speed_percentiles_kph, 17) AS p85,
               from_utc_timestamp(timestampadd(HOUR, h.hour_utc, timestamp(h.observation_date)),
                                  s.time_zone)         AS local_time
        FROM {stats}.segments s
        JOIN {stats}.hourly_stats h USING (dseg_id)
        WHERE s.region = '{REGION}' AND s.speed_limit_kph > 0 AND h.harmonic_speed_kph > 0
          AND h.observation_date BETWEEN DATE '{first_date}' AND DATE '{last_date}'
    )
    SELECT dseg_id, any_value(h3_r9) AS h3_r9, any_value(length_m) AS length_m,
           avg(speed)                                     AS mean_speed_kph,
           avg(CASE WHEN dayofweek(local_time) BETWEEN 2 AND 6
                     AND hour(local_time) IN (7, 8, 9, 16, 17, 18) THEN speed END)
                                                          AS peak_speed_kph,
           avg(stddev / speed)                            AS speed_variability,
           100 * avg(int(speed < 0.6 * speed_limit_kph))  AS congested_hours_pct,
           100 * avg(int(p85 > speed_limit_kph))          AS speeding_hours_pct,
           100 * avg(int(p85 IS NULL))                    AS single_observation_pct
    FROM hours
    GROUP BY dseg_id
    """)

SPEED_SIGNALS = ["mean_speed_kph", "peak_speed_kph", "speed_variability",
                 "congested_hours_pct", "speeding_hours_pct", "single_observation_pct"]
weighted = ", ".join(
    f"sum(CASE WHEN {column} IS NOT NULL THEN length_m * {column} END) "
    f"/ sum(CASE WHEN {column} IS NOT NULL THEN length_m END) AS {column}"
    for column in SPEED_SIGNALS
)

# Every mapped cell stays, so the ones a dataset does not reach are visible instead of missing.
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW cell_layer AS
    SELECT n.h3_r9, n.map_km,
           v.volume_km, v.vehicle_km_per_day, v.major_vehicle_km_per_day,
           v.weekday_vehicle_km_per_day, v.weekend_vehicle_km_per_day,
           s.observed_segments, s.naive_speed_kph, {", ".join("s." + c for c in SPEED_SIGNALS)}
    FROM (SELECT h3_r9, sum(length_m) / 1000 AS map_km FROM {stats}.segments
          WHERE region = '{REGION}' GROUP BY h3_r9) n
    LEFT JOIN volume_cells v USING (h3_r9)
    LEFT JOIN (SELECT h3_r9, count(*) AS observed_segments, avg(mean_speed_kph) AS naive_speed_kph,
                      {weighted}
               FROM road_signals GROUP BY h3_r9) s USING (h3_r9)
    """)

cells = collect("SELECT * FROM cell_layer")

has_volume, has_speed = cells.vehicle_km_per_day.notna(), cells.mean_speed_kph.notna()
display(pd.DataFrame({
    "map cells": [len(cells), has_volume.sum(), has_speed.sum(), (has_volume & has_speed).sum()],
    "road km": [cells.map_km.sum(), cells.map_km[has_volume].sum(), cells.map_km[has_speed].sum(),
                cells.map_km[has_volume & has_speed].sum()],
}, index=["on the map", "with a traffic estimate", "with observed speeds", "with both"]).round())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Map cells into a modelling table
# MAGIC
# MAGIC Postcode outlines are licensed, so the notebook uses the open alternative: every cell
# MAGIC joins the postcode whose addresses are nearest. Collisions are placed the same way, which
# MAGIC keeps the features and the target on one definition of a postcode. That rule also draws
# MAGIC the postcode, so the maps later on show the areas the model actually used.
# MAGIC
# MAGIC Then comes the step that decides what the features mean. Traffic is summed, and speed
# MAGIC behaviour is averaged **weighted by traffic**, so each road speaks as loudly as the number
# MAGIC of vehicles on it. The chart is the argument for doing it that way: on the left, what every
# MAGIC postcode looks like under both methods, and on the right, the one where they disagree most.
# MAGIC A plain average of that postcode's roads suggests a quiet place. Its drivers are on a
# MAGIC motorway.

# COMMAND ----------

CODEPOINT_URL = ("https://api.os.uk/downloads/v1/products/CodePointOpen/downloads"
                 "?area=GB&format=CSV&redirect")
MAX_ASSIGNMENT_M = 1500  # further than this from an address, nobody lives there to insure

with zipfile.ZipFile(download(CODEPOINT_URL, "codepo_gb.zip")) as archive:
    postcodes = pd.concat(
        pd.read_csv(archive.open(name), header=None, usecols=[0, 1, 2, 3],
                    names=["postcode", "quality", "easting", "northing"])
        for name in archive.namelist() if name.startswith("Data/CSV/") and name.endswith(".csv")
    )
postcodes = postcodes[(postcodes.quality < 90) & (postcodes.easting > 0)].copy()
postcodes["postcode"] = postcodes.postcode.str.upper().str.replace(r"\s+", " ", regex=True)
outward, inward = postcodes.postcode.str[:-3].str.strip(), postcodes.postcode.str[-3:]
postcodes["district"] = outward                      # SW10
postcodes["sector"] = outward + " " + inward.str[0]  # SW10 0
postcodes["lon"], postcodes["lat"] = TO_WGS84.transform(postcodes.easting.values,
                                                          postcodes.northing.values)

centroids = np.array([h3.cell_to_latlng(cell) for cell in cells.h3_r9])
cells["lat"], cells["lon"] = centroids[:, 0], centroids[:, 1]
cells["easting"], cells["northing"] = TO_GRID.transform(cells.lon.values, cells.lat.values)

# Postcodes reaching outside the extract see only part of their traffic, so they are left out.
inside = (postcodes.lat.between(cells.lat.min(), cells.lat.max())
          & postcodes.lon.between(cells.lon.min(), cells.lon.max()))
at_edge = set(postcodes.loc[~inside, postcode_level])
postcodes = postcodes[inside].reset_index(drop=True)

addresses = cKDTree(postcodes[["easting", "northing"]].values)
distance, nearest = addresses.query(cells[["easting", "northing"]].values,
                                    distance_upper_bound=MAX_ASSIGNMENT_M)
found = np.isfinite(distance)
cells["postcode"] = np.where(found, postcodes[postcode_level].values[np.where(found, nearest, 0)], None)

units = postcodes.groupby(postcode_level).agg(addresses=("postcode", "size")).rename_axis("postcode")
units[["lat", "lon"]] = postcodes.groupby(postcode_level)[["lat", "lon"]].mean()

# Nearest address is a Voronoi rule, so drawing it gives the boundary. One tile per address
# point, merged by postcode, then held to the same reach the assignment uses so that coastal
# postcodes stop at the coast. Thinned to 150 m, which is far below anything the map shows.
corners = postcodes.drop_duplicates(["easting", "northing"])
grid = corners[["easting", "northing"]].values
tiles = shapely.voronoi_polygons(
    shapely.MultiPoint(grid), ordered=True,
    extend_to=shapely.box(*grid.min(axis=0) - MAX_ASSIGNMENT_M,
                          *grid.max(axis=0) + MAX_ASSIGNMENT_M))
outlines = (pd.Series(np.asarray(tiles.geoms, dtype=object))
            .groupby(corners[postcode_level].values)
            .apply(lambda group: shapely.coverage_union_all(np.asarray(group, dtype=object))))
spread = (corners.groupby(postcode_level)[["easting", "northing"]]
          .apply(lambda group: shapely.multipoints(group.to_numpy())))
reach = shapely.buffer(shapely.envelope(spread.loc[outlines.index].values),
                       MAX_ASSIGNMENT_M, join_style="mitre")
outlines[:] = shapely.transform(
    shapely.simplify(shapely.intersection(outlines.values, reach), 150),
    lambda xy: np.column_stack(TO_WGS84.transform(xy[:, 0], xy[:, 1])))

# COMMAND ----------

TOMTOM_FEATURES = {
    "vehicle_km_per_day": "How much traffic the postcode's roads carry",
    "major_road_share": "How much of that traffic is on motorways and major roads",
    "network_coverage_pct": "How much of the road network has a traffic estimate",
    "mean_speed_kph": "How fast the traffic moves",
    "peak_speed_kph": "How fast it moves in the weekday peaks",
    "speed_variability": "How much the speed swings from hour to hour",
    "congested_hours_pct": "How often the traffic is jammed",
    "speeding_hours_pct": "How often it runs above the limit",
    "single_observation_pct": "How thin the measurement is",
    "weekend_traffic_ratio": "How much busier or quieter the weekend is",
}


def traffic_weighted(frame, column, weight):
    ok = frame[column].notna() & frame[weight].notna()
    rows = frame[ok]
    return ((rows[column] * rows[weight]).groupby(rows.postcode).sum()
            / rows[weight].groupby(rows.postcode).sum())


members = cells[cells.postcode.notna()].fillna({"observed_segments": 0})
total = members.groupby("postcode").sum(numeric_only=True)
features = pd.DataFrame({
    "cells": members.groupby("postcode").size(),
    "vehicle_km_per_day": total.vehicle_km_per_day,
    "major_road_share": total.major_vehicle_km_per_day / total.vehicle_km_per_day,
    "weekend_traffic_ratio": total.weekend_vehicle_km_per_day / total.weekday_vehicle_km_per_day,
    "network_coverage_pct": (100 * total.volume_km / total.map_km).clip(upper=100),
    **{column: traffic_weighted(members, column, "vehicle_km_per_day") for column in SPEED_SIGNALS},
    # What a plain average over roads would have said, for the chart below.
    "naive_speed_kph": traffic_weighted(members, "naive_speed_kph", "observed_segments"),
}).join(units)
features = features[(features.vehicle_km_per_day > 0) & ~features.index.isin(at_edge)]

display(pd.Series(TOMTOM_FEATURES).rename("what it measures").to_frame())
display(features[list(TOMTOM_FEATURES)].describe().T.round(2))

# COMMAND ----------

big = features[(features.cells >= 50) & (features.addresses >= 100)]
example = (big.mean_speed_kph - big.naive_speed_kph).idxmax()
plain, weighted_speed = features.loc[example, ["naive_speed_kph", "mean_speed_kph"]]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
dots = ax1.scatter(features.naive_speed_kph, features.mean_speed_kph, s=8, alpha=0.4,
                   c=features.major_road_share, cmap="viridis")
ax1.plot([15, 110], [15, 110], color="grey", linestyle="--", linewidth=1)
ax1.scatter(plain, weighted_speed, s=160, facecolors="none", edgecolors="crimson", linewidths=2)
ax1.annotate(example, (plain, weighted_speed), textcoords="offset points", xytext=(12, -4),
             color="crimson", fontweight="bold")
ax1.set(xlabel="Plain average over roads (km/h)", ylabel="Traffic-weighted average (km/h)",
        title=f"Every {postcode_level}, both ways of averaging")
fig.colorbar(dots, ax=ax1, label="Share of traffic on major roads")

roads = cells[cells.postcode == example].dropna(subset=["mean_speed_kph", "vehicle_km_per_day"])
ax2.scatter(roads.mean_speed_kph, roads.vehicle_km_per_day, s=40, alpha=0.75, color="steelblue")
ax2.axvline(plain, color="grey", linestyle="--", label=f"plain average {plain:.0f} km/h")
ax2.axvline(weighted_speed, color="crimson", label=f"traffic-weighted {weighted_speed:.0f} km/h")
ax2.set(yscale="log", xlabel="Speed of the cell (km/h)", ylabel="Traffic per day (log scale)",
        title=f"Inside {example}: most roads are slow, most driving is not")
ax2.legend(loc="upper left")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The target comes next, laid out the way a frequency model expects: one row per postcode
# MAGIC and month, with a count in each. Last year's count rides along as the claims history an
# MAGIC insurer already has. Serious collisions are kept apart, because frequency and severity
# MAGIC rarely reward the same features.
# MAGIC
# MAGIC Swap these rows for your own claims and the rest of the notebook does not change.

# COMMAND ----------

STATS19_URL = ("https://data.dft.gov.uk/road-accidents-safety-data/"
               "dft-road-casualty-statistics-collision-{year}.csv")
TARGET_YEAR, HISTORY_YEAR = 2025, 2024


def load_collisions(year):
    frame = pd.read_csv(
        download(STATS19_URL.format(year=year), f"collisions-{year}.csv"),
        usecols=["location_easting_osgr", "location_northing_osgr", "date", "collision_severity"],
        low_memory=False,
    ).dropna(subset=["location_easting_osgr", "location_northing_osgr"])
    frame["month"] = pd.to_datetime(frame.date, format="%d/%m/%Y").dt.month
    frame["ksi"] = frame.collision_severity.isin([1, 2])  # killed or seriously injured
    distance, nearest = addresses.query(
        frame[["location_easting_osgr", "location_northing_osgr"]].values,
        distance_upper_bound=MAX_ASSIGNMENT_M)
    found = np.isfinite(distance)
    frame = frame[found].copy()
    frame["postcode"] = postcodes[postcode_level].values[nearest[found]]
    return frame[frame.postcode.isin(features.index)]


target, history = load_collisions(TARGET_YEAR), load_collisions(HISTORY_YEAR)
panel = pd.MultiIndex.from_product([features.index, range(1, 13)],
                                   names=["postcode", "month"]).to_frame(index=False)
panel = panel.merge(
    target.groupby(["postcode", "month"]).agg(collisions=("ksi", "size"), ksi=("ksi", "sum")).reset_index(),
    how="left", on=["postcode", "month"]).fillna({"collisions": 0, "ksi": 0})
panel = panel.join(
    history.groupby("postcode").agg(prior_collisions=("ksi", "size"), prior_ksi=("ksi", "sum")),
    on="postcode").fillna({"prior_collisions": 0, "prior_ksi": 0})
panel = panel.join(features[["addresses", "lat", "lon", *TOMTOM_FEATURES]], on="postcode")

print(f"{len(panel):,} {postcode_level}-months, {panel.collisions.sum():,.0f} collisions, "
      f"{panel.ksi.sum():,.0f} of them serious")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. What the traffic data is worth
# MAGIC
# MAGIC The honest way to price a data feed is to build the model without it and then with it. So
# MAGIC the same gradient boosted Poisson model is fitted four times, on the same rows, with the
# MAGIC same settings. Only the features change.
# MAGIC
# MAGIC | Model | Features | Stands for |
# MAGIC |---|---|---|
# MAGIC | size | month, addresses in the postcode | a territory with no claims yet |
# MAGIC | size + TomTom | plus the ten road features | that territory with road data |
# MAGIC | size + history | plus last year's collisions | the model an insurer already has |
# MAGIC | size + history + TomTom | everything | that model with road data added |
# MAGIC
# MAGIC The address count is in every model deliberately. It stands for policies in force, and it
# MAGIC is also the density measure that traffic features are accused of copying, so it has to be
# MAGIC there for the comparison to mean anything. Training uses the first nine months and testing
# MAGIC the last three, so no model sees the months it is judged on.

# COMMAND ----------

FEATURE_SETS = {
    "size": ["month", "addresses"],
    "size + TomTom": ["month", "addresses", *TOMTOM_FEATURES],
    "size + history": ["month", "addresses", "prior"],
    "size + history + TomTom": ["month", "addresses", "prior", *TOMTOM_FEATURES],
}
TRAIN_MONTHS, TEST_MONTHS = range(1, 10), range(10, 13)


def fit_poisson(X, y):
    model = xgb.XGBRegressor(
        objective="count:poisson", n_estimators=600, learning_rate=0.03, max_depth=4,
        min_child_weight=20, subsample=0.8, colsample_bytree=0.8, reg_lambda=5.0,
        random_state=0, n_jobs=4,
    )
    return model.fit(X, y)


def columns_for(name, target):
    return [f"prior_{target}" if column == "prior" else column for column in FEATURE_SETS[name]]


def score(y, prediction, average):
    """Share of Poisson deviance explained, and how well the ranking separates the counts."""
    explained = 1 - mean_poisson_deviance(y, prediction) / mean_poisson_deviance(
        y, np.full(len(y), average))
    ranked = np.asarray(y)[np.argsort(-prediction, kind="stable")]
    share = np.arange(1, len(ranked) + 1) / len(ranked)
    gini = 2 * (np.trapz(np.cumsum(ranked) / ranked.sum(), share) - 0.5)
    return {"deviance_explained_pct": 100 * explained, "gini": gini}


def run(target, train, test):
    """Fit the four models on `train` and score them on `test`."""
    models, predictions, scores = {}, {}, {}
    for name in FEATURE_SETS:
        columns = columns_for(name, target)
        models[name] = fit_poisson(train[columns], train[target])
        predictions[name] = models[name].predict(test[columns])
        scores[name] = score(test[target], predictions[name], train[target].mean())
    return models, predictions, pd.DataFrame(scores).T


train, test = panel[panel.month.isin(TRAIN_MONTHS)], panel[panel.month.isin(TEST_MONTHS)]
models, predictions, frequency = run("collisions", train, test)
_, _, severity = run("ksi", train, test)
display(frequency.join(severity.deviance_explained_pct.rename("serious_only_pct")).round(3))

# For scale: each postcode's own rate over both years, test months included. No model can know
# that much, so this is a generous ceiling on what monthly counts allow.
own_rate = (panel.groupby("postcode").collisions.sum()
            + panel.groupby("postcode").prior_collisions.first()) / 24
ceiling = score(test.collisions, test.postcode.map(own_rate).clip(lower=1e-3).values,
                train.collisions.mean())
print(f"Ceiling: {ceiling['deviance_explained_pct']:.0f}% of deviance explained")

# COMMAND ----------

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5.5), gridspec_kw={"width_ratios": [1, 1.15]})
colors = ["#bbbbbb", "#e15759", "#4e79a7", "#a0132f"]
position = np.arange(len(FEATURE_SETS))

ax1.barh(position - 0.2, frequency.deviance_explained_pct, 0.38, color=colors, label="all collisions")
ax1.barh(position + 0.2, severity.deviance_explained_pct, 0.38, color=colors, alpha=0.45,
         label="serious collisions only")
for y, value in zip(position, frequency.deviance_explained_pct):
    ax1.text(value + 0.5, y - 0.2, f"{value:.0f}%", va="center", fontsize=9)
for y, value in zip(position, severity.deviance_explained_pct):
    ax1.text(value + 0.5, y + 0.2, f"{value:.0f}%", va="center", fontsize=9, color="dimgrey")
ax1.set(yticks=position, yticklabels=frequency.index, xlabel="Deviance explained (%)",
        title="What each model explains",
        xlim=(0, 1.2 * frequency.deviance_explained_pct.max()))
ax1.invert_yaxis()
ax1.legend(loc="upper right", fontsize=9)

actual = test.collisions.values
share = np.linspace(0, 1, len(actual))
for (name, prediction), color in zip(predictions.items(), colors):
    captured = np.cumsum(actual[np.argsort(-prediction, kind="stable")]) / actual.sum()
    ax2.plot(share, captured, color=color, label=f"{name} ({frequency.loc[name, 'gini']:.3f})")
ax2.plot([0, 1], [0, 1], color="grey", linestyle="--", label="random")
ax2.set(xlabel=f"Share of {postcode_level}-months, worst first", ylabel="Share of collisions found",
        title="Finding the collisions, held-out months (Gini)")
ax2.legend(loc="lower right", fontsize=9)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Held-out months are a soft test, because every postcode in them was also in training. The
# MAGIC harder test cuts the map into five blocks and holds each one out in turn, so a tested
# MAGIC postcode has no trained neighbour. If the road features were only memorising places, this
# MAGIC is where the gain would disappear.

# COMMAND ----------

blocks = [h3.latlng_to_cell(lat, lon, 4) for lat, lon in zip(panel.lat, panel.lon)]
territory = []
for keep_rows, hold_rows in GroupKFold(n_splits=5).split(panel, groups=blocks):
    keep, hold = panel.iloc[keep_rows], panel.iloc[hold_rows]
    territory.append(run("collisions", keep, hold)[2].deviance_explained_pct)

print("Deviance explained (%) over five held-out map blocks:")
display(pd.DataFrame(territory).agg(["mean", "min", "max"]).T.round(1))

# COMMAND ----------

# MAGIC %md
# MAGIC Read the results the way an insurer would, starting from the model you already have.
# MAGIC
# MAGIC **With a claims history, road data adds a little.** About two points on held-out months
# MAGIC and one on held-out territory. That is small, and it should be. Most of the change in a
# MAGIC postcode's count from one month to the next is chance, and the ceiling above shows how
# MAGIC little room there is. A model that already knows each postcode's history is close to it.
# MAGIC
# MAGIC **Without a history, road data does most of the work.** It takes the model from about 10%
# MAGIC to about 28%, nearly as far as a full year of claims gets it. That is the position of a new
# MAGIC territory, a new postcode or a thin book, where history is exactly what is missing.
# MAGIC
# MAGIC **For serious collisions, road data beats the history.** Severity follows speed, which the
# MAGIC road data measures and last year's count does not.
# MAGIC
# MAGIC The held-out blocks rank the four models the same way, so the gain is not the model
# MAGIC memorising places. Your numbers will move a little with the library versions; the order
# MAGIC should not.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. What the model used, and where
# MAGIC
# MAGIC A number on a slide is not enough to put a feature into a rating. Someone will ask which
# MAGIC features moved the price, in which direction, and whether the answer is the same in every
# MAGIC region. SHAP answers all three from the fitted model.
# MAGIC
# MAGIC Each dot below is one postcode-month, placed by how much a feature pushed its prediction
# MAGIC and coloured by the value of that feature. The model works on a log scale, so a push of
# MAGIC 0.2 multiplies the expected count by about 1.2.

# COMMAND ----------

FULL = columns_for("size + history + TomTom", "collisions")
full_model = models["size + history + TomTom"]
held_out = test[FULL].reset_index(drop=True)
explanation = shap.TreeExplainer(full_model)(held_out)

shap.plots.beeswarm(explanation, max_display=13, show=False)
plt.gcf().set_size_inches(12, 6)
plt.title("How each feature moves the prediction", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The same values answer a more practical question: which postcodes does this change, and by
# MAGIC how much. Below are the two the existing model cannot tell apart, one pushed up by the road
# MAGIC data and one pushed down. The bars are how unusual each of their road features is, and the
# MAGIC waterfall builds the raised prediction one term at a time.

# COMMAND ----------

october = np.flatnonzero((test.month == TEST_MONTHS[0]).values)
road_effect = explanation.values[:, [FULL.index(column) for column in TOMTOM_FEATURES]].sum(axis=1)
existing = predictions["size + history"]

middle = np.quantile(existing[october], [0.4, 0.6])
alike = october[(existing[october] >= middle[0]) & (existing[october] <= middle[1])]
up, down = alike[road_effect[alike].argmax()], alike[road_effect[alike].argmin()]
pair = test.iloc[[up, down]].set_index("postcode")

standardised = ((pair[list(TOMTOM_FEATURES)] - held_out[list(TOMTOM_FEATURES)].mean())
                / held_out[list(TOMTOM_FEATURES)].std())
axis = standardised.T.plot.barh(figsize=(12, 5.5), color=["#a0132f", "#4e79a7"], width=0.8)
axis.axvline(0, color="black", linewidth=0.8)
axis.set(xlabel="Standard deviations from the average postcode", ylabel="",
         title="Same rating today, different roads")
axis.legend(title=postcode_level, loc="upper right")
plt.tight_layout()
plt.show()

# shap.plots.waterfall resizes whatever figure it is handed, so it gets one to itself.
shap.plots.waterfall(explanation[up], max_display=11, show=False)
plt.gcf().set_size_inches(12, 5.5)
plt.title(f"{pair.index[0]}: building the raised prediction, one term at a time", fontsize=12)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Across the whole territory the effect looks like this. Red is where the road data raises
# MAGIC the rating and blue is where it lowers it. Red follows the major roads more closely than
# MAGIC it follows the towns, which is the sign that the features carry road information and not
# MAGIC a headcount. Hover a postcode to see what its roads did to the prediction.

# COMMAND ----------

shown = test.iloc[october][["postcode", "collisions"]].copy()
shown["effect"] = road_effect[october]
shown["prediction"] = predictions["size + history + TomTom"][october].round(2)
shown["multiplier"] = np.exp(shown.effect).round(2)
shown = shown[shown.postcode.isin(outlines.index)]

limit = round(float(np.quantile(np.abs(shown.effect), 0.95)), 2)
shade = plt.get_cmap("RdBu_r")
scale = branca.colormap.LinearColormap(
    [mcolors.to_hex(shade(x)) for x in np.linspace(0, 1, 11)], vmin=-limit, vmax=limit,
    caption="Effect of the road features, log scale")
scale.tick_labels = [-limit, -limit / 2, 0, limit / 2, limit]

chart = folium.Map(tiles="OpenStreetMap")
chart.get_root().header.add_child(folium.Element(
    "<style>.leaflet-tile-pane{filter:grayscale(1) contrast(0.9) brightness(1.1);}</style>"))
drawn = folium.GeoJson(
    {"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": row._asdict(),
         "geometry": json.loads(shapely.to_geojson(outlines[row.postcode], 4))}
        for row in shown.itertuples(index=False)]},
    style_function=lambda feature: {
        "fillColor": scale(feature["properties"]["effect"]), "fillOpacity": 0.85,
        "color": "#ffffff", "weight": 0.2},
    highlight_function=lambda feature: {"weight": 2, "color": "#222222"},
    tooltip=folium.GeoJsonTooltip(
        fields=["postcode", "multiplier", "prediction", "collisions"],
        aliases=[postcode_level.title(), "Road features multiply the prediction by",
                 "Predicted collisions", "Observed collisions"]),
    smooth_factor=0.5,
).add_to(chart)
scale.add_to(chart)
chart.fit_bounds(drawn.get_bounds())
display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Before this goes near a rate filing
# MAGIC
# MAGIC The notebook settles some of this on a sample. The rest needs your data.
# MAGIC
# MAGIC | Question | Here | With your data |
# MAGIC |---|---|---|
# MAGIC | Does it beat a claims-only model? | Section 3, on held-out months and held-out territory | Rerun against your own model, with policies in force as the exposure |
# MAGIC | Frequency and severity together? | Both are scored; they favour different features | Keep two models and expect two feature sets |
# MAGIC | Does the gain hold everywhere? | Five map blocks held out in turn | Watch it by month once the features arrive monthly |
# MAGIC | Road signal or just population? | Address count is in every model; the chart below shows how far each feature tracks it | Add your own density controls and see which effects survive |
# MAGIC | Postcodes the data only half covers? | Coverage is a feature, and edge postcodes are dropped | Keep coverage as a feature and a quality flag, and never fill in speeds |

# COMMAND ----------

tracking = pd.Series({column: spearmanr(features.addresses, features[column],
                                        nan_policy="omit").correlation
                      for column in TOMTOM_FEATURES}).sort_values()

axis = tracking.plot.barh(figsize=(11, 4.5), width=0.75,
                          color=np.where(tracking.abs() > 0.5, "#a0132f", "#4e79a7"))
axis.axvline(0, color="black", linewidth=0.8)
axis.set(xlim=(-1, 1), ylabel="", xlabel="Rank correlation with the number of addresses",
         title="How much of each road feature is really just population")
for name, value in enumerate(tracking):
    axis.annotate(f"{value:+.2f}", (value, name), xytext=(6 if value > 0 else -6, 0),
                  textcoords="offset points", va="center",
                  ha="left" if value > 0 else "right", fontsize=9)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Three ways to take this further. Feed the model your own claims, with policies in force
# MAGIC as the exposure. Build the features by month once the claims are monthly too, which is
# MAGIC where more than a week of traffic starts to pay. And keep the map cells from section 1,
# MAGIC because they are already a road-level view for address-level quotes or telematics
# MAGIC scoring, which the `territory-risk-assessment` notebook picks up.
# MAGIC
# MAGIC **Getting the full data.** The Marketplace samples hold two months of traffic across four
# MAGIC metropolitan areas. That is enough to measure what the features are worth, which is what
# MAGIC this notebook does. It is not enough to rate on. A commercial extract can cover the
# MAGIC markets you write and the years your claims cover. Ask for one at
# MAGIC `marketplacesupport@tomtom.com`.
