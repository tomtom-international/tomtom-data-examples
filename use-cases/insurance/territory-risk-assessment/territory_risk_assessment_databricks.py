# Databricks notebook source
# MAGIC %md
# MAGIC # Territory risk assessment
# MAGIC
# MAGIC Scoring areas, and single routes, for road risk.
# MAGIC
# MAGIC An insurer rating territory by postcode is averaging a motorway together with a
# MAGIC cul-de-sac. One rating has to cover both, and it describes neither.
# MAGIC
# MAGIC Two measurements of the roads themselves get closer. Traffic volume says how many
# MAGIC vehicles are there to collide. Speed says how hard they would hit, and how the road is
# MAGIC driven. Neither is a risk model on its own, so this notebook builds both, scores areas of a
# MAGIC few city blocks, and then runs the same recipe over the roads of one driver's trip.
# MAGIC
# MAGIC **What you need.** Both free samples from Databricks Marketplace: TomTom Traffic Stats,
# MAGIC hourly speeds per road, and TomTom Traffic Volumes, average daily traffic per road. They
# MAGIC cover the same four metropolitan areas, and the `region` widget picks one. The notebook
# MAGIC scores one day, which keeps a full run to about ten minutes on serverless.

# COMMAND ----------

# MAGIC %pip install folium==0.20.0 h3==4.5.0 shapely==2.1.2

# COMMAND ----------

import folium
import h3
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.types import DecimalType
from shapely import wkt

sns.set_theme(style="whitegrid", palette="colorblind")
YLORRD = plt.get_cmap("YlOrRd")


def collect(query):
    """Run a query into pandas. Spark types expressions built from literals such as `1.0` as
    DECIMAL, which pandas receives as decimal.Decimal objects that matplotlib cannot plot."""
    frame = spark.sql(query)
    decimals = {f.name: float for f in frame.schema if isinstance(f.dataType, DecimalType)}
    return frame.toPandas().astype(decimals)


def basemap(lat, lon, zoom):
    """A muted OpenStreetMap basemap. CartoDB's tiles now need an API key; this does not."""
    chart = folium.Map(location=[lat, lon], zoom_start=zoom, tiles="OpenStreetMap")
    chart.get_root().header.add_child(folium.Element(
        "<style>.leaflet-tile-pane{filter:grayscale(1) contrast(0.92) brightness(1.06);}</style>"
    ))
    return chart

# COMMAND ----------

dbutils.widgets.text("stats_catalog", "TomTom_Traffic_Stats", "Traffic Stats catalog")
dbutils.widgets.text("volumes_catalog", "TomTom_Traffic_Volumes", "Traffic Volumes catalog")
dbutils.widgets.text("observation_date", "2025-09-03", "Date to score, UTC")
dbutils.widgets.text("region", "london", "Region: london, austin, losangeles or melbourne")
dbutils.widgets.text("vintage_year", "2025", "Traffic Volumes vintage")

stats = dbutils.widgets.get("stats_catalog") + ".traffic_stats_batch"
volumes = dbutils.widgets.get("volumes_catalog") + ".traffic_volumes"
observation_date = dbutils.widgets.get("observation_date")
region = dbutils.widgets.get("region")
vintage_year = int(dbutils.widgets.get("vintage_year"))

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
# MAGIC ## 1. How much traffic each area carries
# MAGIC
# MAGIC Both datasets carry the same [H3](https://h3geo.org/) cell of about a tenth of a square
# MAGIC kilometre, so they join on a string with no map matching in between.
# MAGIC
# MAGIC Exposure is vehicle-kilometres per day: traffic times length, summed over the cell.
# MAGIC Summing keeps a road drawn as ten segments from counting ten times, and it lets a cell
# MAGIC holding a motorway read as busy even with three quiet streets beside it.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW exposure AS
    SELECT h3_r9, count(*) AS segments, min(frc) AS most_major_road_class,
           round(sum(length_m) / 1000, 2) AS network_km,
           round(sum(aadt * length_m) / 1000) AS vehicle_km_per_day
    FROM {volumes}.aadt_segments
    WHERE region = '{region}' AND vintage_year = {vintage_year}
    GROUP BY h3_r9
    """)

display(spark.sql("SELECT * FROM exposure ORDER BY vehicle_km_per_day DESC LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. How that traffic behaves
# MAGIC
# MAGIC Volume cannot tell a jam from a motorway, so four factors come from the hourly speeds.
# MAGIC How fast the traffic moves, how much that swings, how often the faster traffic is above
# MAGIC the limit, and how often everything is crawling.
# MAGIC
# MAGIC An hour measured from a single vehicle has no speed distribution. It still counts towards
# MAGIC the average, the speeding rate skips it, and its share sits beside the factors so that
# MAGIC thin evidence stays visible.

# COMMAND ----------

# One row per segment-hour on the date, with the road attributes joined on. `speeding` is null
# for a single-observation hour, so its average is a rate over the hours with a distribution.
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW hours AS
    SELECT s.dseg_id, s.h3_r9, s.length_m, s.speed_limit_kph, s.street_name, s.geometry_wkt,
           s.osm_way_ids, h.harmonic_speed_kph, h.stddev_speed_kph, h.speed_percentiles_kph,
           hour(from_utc_timestamp(timestampadd(HOUR, h.hour_utc, timestamp(h.observation_date)),
                                   s.time_zone))                       AS hour_local,
           element_at(h.speed_percentiles_kph, 17) > s.speed_limit_kph AS speeding,
           h.harmonic_speed_kph < 0.6 * s.speed_limit_kph              AS congested,
           h.speed_percentiles_kph IS NULL                             AS single_observation
    FROM {stats}.segments s JOIN {stats}.hourly_stats h USING (dseg_id)
    WHERE h.observation_date = DATE '{observation_date}' AND s.region = '{region}'
      AND s.speed_limit_kph > 0 AND h.harmonic_speed_kph > 0
    """)

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW severity AS
    SELECT h3_r9,
           avg(harmonic_speed_kph)                    AS mean_speed_kph,
           avg(stddev_speed_kph / harmonic_speed_kph) AS variability,
           avg(int(speeding))                         AS speeding_rate,
           avg(int(congested))                        AS congestion_rate,
           avg(int(single_observation))               AS single_observation_rate
    FROM hours GROUP BY h3_r9 HAVING count(speeding) > 0
    """)

display(spark.sql("""
        SELECT count(*) AS cells,
               round(avg(mean_speed_kph), 1)                 AS mean_speed_kph,
               round(avg(variability), 3)                    AS mean_variability,
               round(100 * avg(speeding_rate), 1)            AS mean_speeding_pct,
               round(100 * avg(congestion_rate), 1)          AS mean_congestion_pct,
               round(100 * avg(single_observation_rate), 1)  AS mean_single_observation_pct
        FROM severity
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. One score, and what it hides
# MAGIC
# MAGIC Each factor is scaled across the cells in this extract, weighted and summed. Exposure
# MAGIC takes the largest weight, because a collision needs traffic before it needs speed. The
# MAGIC weights are illustrative and the parts stay visible, so you can re-weight them against
# MAGIC your own claims. Scores rank cells inside this extract and nowhere else.
# MAGIC
# MAGIC The factor charts show the trap. A few very busy cells stretch the exposure scale, which
# MAGIC leaves every ordinary cell near zero on that term. Ranking or log-scaling exposure before
# MAGIC weighting it usually works better.

# COMMAND ----------

FACTORS = {  # factor: (column, weight)
    "exposure": ("vehicle_km_per_day", 0.35),
    "speed": ("mean_speed_kph", 0.20),
    "variability": ("variability", 0.20),
    "speeding": ("speeding_rate", 0.15),
    "congestion": ("congestion_rate", 0.10),
}
parts = [f"{factor}_part" for factor in FACTORS]

scaled = ", ".join(
    f"{weight} * ({column} - min({column}) OVER ()) "
    f"/ nullif(max({column}) OVER () - min({column}) OVER (), 0) AS {factor}_part"
    for factor, (column, weight) in FACTORS.items()
)

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW territory_risk AS
    WITH scored AS (
        SELECT e.h3_r9, e.vehicle_km_per_day, v.mean_speed_kph, v.variability, v.speeding_rate,
               v.congestion_rate, v.single_observation_rate, {scaled}
        FROM exposure e JOIN severity v USING (h3_r9)
    )
    SELECT h3_r9, vehicle_km_per_day,
           round(mean_speed_kph, 1)                AS mean_speed_kph,
           round(variability, 3)                   AS variability,
           round(100 * speeding_rate, 1)           AS speeding_pct,
           round(100 * congestion_rate, 1)         AS congestion_pct,
           round(100 * single_observation_rate, 1) AS single_observation_pct,
           round({" + ".join(parts)}, 3)           AS risk_score,
           {", ".join(f"round({part}, 3) AS {part}" for part in parts)}
    FROM scored
    """)

# One row per scored cell. Everything below is drawn from this frame or from the view.
cells = collect("SELECT * FROM territory_risk")
print(f"{len(cells):,} scored cells in {region} on {observation_date}")

display(spark.sql("""
        SELECT h3_r9, vehicle_km_per_day, mean_speed_kph, variability, speeding_pct,
               congestion_pct, single_observation_pct, risk_score
        FROM territory_risk ORDER BY risk_score DESC LIMIT 15
        """))

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(13, 8))

panels = [
    ("vehicle_km_per_day", "Exposure (vehicle-km per day)", "steelblue"),
    ("variability", "Speed variability (std / mean)", "mediumpurple"),
    ("speeding_pct", "Hours with p85 above the limit (%)", "coral"),
    ("congestion_pct", "Hours below 60% of the limit (%)", "seagreen"),
]
for ax, (column, title, color) in zip(axes.flat, panels):
    ax.hist(cells[column].clip(upper=cells[column].quantile(0.99)), bins=50, color=color,
            edgecolor="white")
    ax.set(title=title, ylabel="Cells")
axes[0, 0].set(yscale="log", ylabel="Cells (log scale)")

plt.suptitle(f"The four factors across {len(cells):,} cells, 99th percentile clipped",
             fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 4.5))

median, p95 = cells.risk_score.quantile([0.5, 0.95])
ax1.hist(cells.risk_score, bins=60, color="crimson", edgecolor="white")
ax1.axvline(median, color="black", linestyle="--", label=f"Median: {median:.2f}")
ax1.axvline(p95, color="darkred", label=f"95th percentile: {p95:.2f}")
ax1.set(title="Composite risk score", xlabel="Score (0 lowest, 1 highest)", ylabel="Cells")
ax1.legend()

top = cells.nlargest(15, "risk_score")
(top[parts].rename(columns=lambda part: part.removesuffix("_part"))
    .set_axis(top.mean_speed_kph.round().astype(int))
    .plot.bar(stacked=True, ax=ax2, rot=0, width=0.8,
              color=["#800026", "#e31a1c", "#fd8d3c", "#feb24c", "#ffeda0"]))
ax2.set(title="What lifts the top cells", ylabel="Weighted contribution",
        xlabel="The 15 highest-scoring cells, labelled by mean speed in km/h")
ax2.legend(fontsize=8)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The stacked bars above hold two different risks inside one number. Some cells are
# MAGIC lifted by exposure: central, slow and busy, where collisions are common and mild. Others
# MAGIC are lifted by speed: motorway cells where collisions are rare and severe.
# MAGIC
# MAGIC The scatter puts the two at opposite ends of the speed axis and colours both as high
# MAGIC risk. Frequency and severity models would weight them apart, which is the reason to keep
# MAGIC the factor columns and not only the score.

# COMMAND ----------

display(spark.sql("""
        SELECT
            CASE WHEN mean_speed_kph >= 80 THEN 'motorway speeds'
                 WHEN mean_speed_kph >= 40 THEN 'arterial speeds'
                 ELSE 'urban speeds' END          AS cell_type,
            count(*)                              AS cells,
            round(avg(vehicle_km_per_day))        AS mean_vehicle_km_per_day,
            round(avg(mean_speed_kph), 1)         AS mean_speed_kph,
            round(avg(speeding_pct), 1)           AS mean_speeding_pct,
            round(avg(congestion_pct), 1)         AS mean_congestion_pct,
            round(avg(single_observation_pct), 1) AS mean_single_observation_pct,
            round(avg(risk_score), 3)             AS mean_risk_score
        FROM territory_risk
        GROUP BY cell_type
        ORDER BY mean_speed_kph DESC
        """))

busy = cells[cells.vehicle_km_per_day > 0]
sample = busy.sample(min(8000, len(busy)), random_state=0)

fig, ax = plt.subplots(figsize=(11, 6))
points = ax.scatter(sample.mean_speed_kph, sample.vehicle_km_per_day, c=sample.risk_score,
                    cmap=YLORRD, s=6, alpha=0.5)
ax.set(yscale="log", xlabel="Mean speed (km/h)", ylabel="Vehicle-km per day (log scale)")
ax.set_title(f"Exposure against speed, {len(sample):,} sampled cells in {region}",
             fontsize=13, fontweight="bold")
for x in (40, 80):
    ax.axvline(x, color="grey", linestyle=":", alpha=0.7)
for x, band in [(20, "urban"), (60, "arterial"), (100, "motorway")]:
    ax.text(x, sample.vehicle_km_per_day.max(), band, ha="center", fontsize=9, color="grey")
fig.colorbar(points, ax=ax, label="Risk score")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The map
# MAGIC
# MAGIC Cells of a tenth of a square kilometre are right for scoring and far too fine to draw
# MAGIC across a whole region, so the map rolls them up into hexagons of about 36 km².
# MAGIC
# MAGIC Each hexagon takes the exposure-weighted average of the cells inside it, so it shows the
# MAGIC risk vehicles meet rather than treating an empty lane like a motorway. Colour follows
# MAGIC rank, because the long tail would otherwise leave nearly every hexagon the same pale
# MAGIC shade. The second layer holds the scored cells around the busiest point.

# COMMAND ----------

MAP_RESOLUTION = 6

cells["parent"] = [h3.cell_to_parent(cell, MAP_RESOLUTION) for cell in cells.h3_r9]
cells["weighted_score"] = cells.risk_score * cells.vehicle_km_per_day
area = cells.groupby("parent").agg(
    vehicle_km_per_day=("vehicle_km_per_day", "sum"), weighted_score=("weighted_score", "sum"),
    mean_speed_kph=("mean_speed_kph", "mean"), speeding_pct=("speeding_pct", "mean"),
    cells_r9=("h3_r9", "size"),
).reset_index()
area["risk_score"] = area.weighted_score / area.vehicle_km_per_day
area["rank"] = area.risk_score.rank(pct=True)
print(f"{len(area):,} hexagons at resolution {MAP_RESOLUTION}, "
      f"rolled up from {area.cells_r9.sum():,} scored cells")

busiest = cells.loc[cells.vehicle_km_per_day.idxmax(), "h3_r9"]
chart = basemap(*h3.cell_to_latlng(busiest), 9)

overview = folium.FeatureGroup(name=f"Region, resolution {MAP_RESOLUTION}").add_to(chart)
for row in area.itertuples():
    color = mcolors.to_hex(YLORRD(row.rank))
    folium.Polygon(
        h3.cell_to_boundary(row.parent),
        color=color, fill=True, fill_color=color, fill_opacity=0.55, weight=1,
        tooltip=(f"score {row.risk_score:.2f}, {row.vehicle_km_per_day:,.0f} vehicle-km/day, "
                 f"{row.mean_speed_kph:.0f} km/h, speeding in {row.speeding_pct:.0f}% of hours, "
                 f"{row.cells_r9} scored cells"),
    ).add_to(overview)

# The scored cells themselves, within 40 steps of the busiest one
near = cells[cells.h3_r9.isin(h3.grid_disk(busiest, 40))]
detail = folium.FeatureGroup(name=f"Busiest area, resolution 9 ({len(near):,} cells)",
                             show=False).add_to(chart)
norm = mcolors.Normalize(near.risk_score.min(), near.risk_score.max())
for row in near.itertuples():
    color = mcolors.to_hex(YLORRD(norm(row.risk_score)))
    folium.Polygon(
        h3.cell_to_boundary(row.h3_r9),
        color=color, fill=True, fill_color=color, fill_opacity=0.6, weight=0.5,
        tooltip=(f"{row.h3_r9}<br>score {row.risk_score:.2f}<br>"
                 f"{row.vehicle_km_per_day:,.0f} vehicle-km/day<br>"
                 f"{row.mean_speed_kph:.0f} km/h, speeding {row.speeding_pct:.0f}%, "
                 f"congested {row.congestion_pct:.0f}%"),
    ).add_to(detail)

folium.LayerControl(collapsed=False).add_to(chart)
display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Scoring a single route
# MAGIC
# MAGIC Territory rating asks where a driver lives. Telematics lets you ask the better question:
# MAGIC which roads do they actually drive?
# MAGIC
# MAGIC Raw GPS will not answer it, because the points land tens of metres off the road and do
# MAGIC not name it. A map matcher such as [Fast Map Matching](https://github.com/cyang-kth/fmm)
# MAGIC turns a trace into OpenStreetMap way IDs, and `segments.osm_way_ids` carries the same
# MAGIC IDs, so the two join with no second matching step:
# MAGIC
# MAGIC ```
# MAGIC GPS trace -> map matcher -> OSM way IDs -> segments.osm_way_ids -> hourly speeds
# MAGIC ```
# MAGIC
# MAGIC The example uses two ways from the M20 motorway in Kent. In production, the matcher
# MAGIC provides one list per trip.

# COMMAND ----------

MATCHED_OSM_WAYS = [4394118, 4394117]

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW route_hours AS
    SELECT * FROM hours
    WHERE arrays_overlap(osm_way_ids, array({", ".join(f"{way}L" for way in MATCHED_OSM_WAYS)}))
    """)

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW route_segments AS
    SELECT dseg_id, any_value(street_name) AS street_name, any_value(geometry_wkt) AS geometry_wkt,
           any_value(length_m) AS length_m, any_value(speed_limit_kph) AS speed_limit_kph,
           avg(harmonic_speed_kph) AS mean_speed_kph, avg(int(speeding)) AS speeding_rate,
           avg(int(congested)) AS congestion_rate, avg(int(single_observation)) AS single_observation_rate
    FROM route_hours GROUP BY dseg_id HAVING count(speeding) > 0
    """)

display(spark.sql("""
        SELECT count(*)                                     AS segments_on_route,
               round(sum(length_m) / 1000, 1)               AS route_km,
               round(avg(mean_speed_kph), 1)                AS mean_speed_kph,
               round(100 * avg(speeding_rate), 1)           AS pct_hours_speeding,
               round(100 * avg(congestion_rate), 1)         AS pct_hours_congested,
               round(100 * avg(single_observation_rate), 1) AS pct_hours_single_observation,
               round(100 * sum(length_m * speeding_rate) / sum(length_m), 1) AS pct_speeding_by_length
        FROM route_segments
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC Two views of the same route. The profile follows the whole route through the day against
# MAGIC the posted limit, so every hour above the dashed line is an hour when the faster traffic
# MAGIC was speeding. The map breaks the route into segments, which is where a route that looks
# MAGIC uniform turns out to have uneven stretches.

# COMMAND ----------

route = collect("SELECT * FROM route_segments")

if route.empty:
    raise ValueError(
        f"None of the OSM ways {MATCHED_OSM_WAYS} lie in region '{region}'. They are on the M20 "
        f"in Kent, which is in the london extract. Set the region widget back to london, or put "
        f"way IDs from your own matched trace in MATCHED_OSM_WAYS."
    )

profile = collect("""
        SELECT hour_local,
               round(avg(harmonic_speed_kph), 1)                    AS harmonic_mean,
               round(avg(element_at(speed_percentiles_kph, 17)), 1) AS p85,
               round(avg(element_at(speed_percentiles_kph, 3)), 1)  AS p15,
               round(avg(speed_limit_kph), 1)                       AS speed_limit
        FROM route_hours GROUP BY hour_local ORDER BY hour_local
        """)

fig, ax = plt.subplots(figsize=(11, 5))
ax.fill_between(profile.hour_local, profile.p15, profile.p85, alpha=0.2, color="steelblue",
                label="p15 to p85")
ax.plot(profile.hour_local, profile.harmonic_mean, marker="o", color="steelblue",
        label="Harmonic mean")
ax.plot(profile.hour_local, profile.speed_limit, linestyle="--", color="black",
        label="Speed limit")
ax.set(xlabel="Local hour", ylabel="km/h", xticks=range(0, 24, 2))
ax.set_title(f"The route through {observation_date}", fontsize=13, fontweight="bold")
ax.legend()
plt.tight_layout()
plt.show()

centre_lon, centre_lat = wkt.loads(route.geometry_wkt.iloc[len(route) // 2]).coords[0]
chart = basemap(centre_lat, centre_lon, 11)

for row in route.itertuples():
    folium.PolyLine(
        [(lat, lon) for lon, lat in wkt.loads(row.geometry_wkt).coords],
        color=mcolors.to_hex(YLORRD(row.speeding_rate)), weight=4, opacity=0.9,
        tooltip=(f"{row.street_name or 'unnamed'} | "
                 f"speeding in {100 * row.speeding_rate:.0f}% of hours | "
                 f"{row.mean_speed_kph:.0f} km/h against a {row.speed_limit_kph:.0f} limit | "
                 f"congested {100 * row.congestion_rate:.0f}%"),
    ).add_to(chart)

display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the route says
# MAGIC
# MAGIC This stretch of motorway runs fast and clear, with the faster traffic above the limit in
# MAGIC almost every hour and hardly any congestion. No driver on it stands out.
# MAGIC
# MAGIC That is exactly why the road is worth measuring. Speeding this widespread belongs to the
# MAGIC road rather than to anyone driving on it, and it is the baseline a driver's own telematics
# MAGIC has to be read against. Without it, a model charges people for the roads they happen to
# MAGIC use.
# MAGIC
# MAGIC ## Where to take it next
# MAGIC
# MAGIC - **Measure the factors before trusting the weights.** The `territory-risk-modeling`
# MAGIC   notebook puts the same kind of road features into a claims model and shows how much they
# MAGIC   add, and where.
# MAGIC - **Score frequency and severity separately.** The factor columns are already there.
# MAGIC - **Rank or log-scale exposure** before weighting it, for the reason in section 3.
# MAGIC - **Weight a route by time rather than distance.** A congested kilometre carries more
# MAGIC   exposure than a clear one, and `aadt_by_day_hour` holds the hourly weights.
# MAGIC - **Check coverage first.** `traffic_volumes.coverage` gives the share of each road class
# MAGIC   with an estimate. A road without one is missing, not empty.
# MAGIC - **Compare days.** The default score uses one Wednesday. The sample runs from
# MAGIC   1 September to 31 October 2025, so set `observation_date` to a Sunday, or to a day in
# MAGIC   October, and see which cells move.
