# Databricks notebook source
# MAGIC %md
# MAGIC # Territory risk assessment
# MAGIC
# MAGIC Rating geographic areas, and individual routes, for road risk from traffic speed and
# MAGIC volume.
# MAGIC
# MAGIC Insurers rate territory by postcode, and a postcode can hold a motorway and a cul-de-sac.
# MAGIC This notebook rates a finer grain, an H3 cell of a few city blocks, and then rates the
# MAGIC roads a single driver used. Two questions, one method:
# MAGIC
# MAGIC 1. **Which areas are risky?** Territory rating.
# MAGIC 2. **Which roads does this driver use?** Route rating, from a telematics trace.
# MAGIC
# MAGIC Both need the same two ingredients. **Volume** is exposure: how many vehicles are there
# MAGIC to collide with. **Speed** is severity and behaviour: how fast, how erratic, how often
# MAGIC above the limit. Neither alone is a risk model, so the notebook runs in that order:
# MAGIC exposure, then severity, then one score, then what the score hides, then the map, then
# MAGIC the same recipe on a single route.
# MAGIC
# MAGIC ## What you need
# MAGIC
# MAGIC Both free samples from Databricks Marketplace, attached as catalogs:
# MAGIC
# MAGIC - **TomTom Traffic Stats**, hourly speeds per road segment
# MAGIC - **TomTom Traffic Volumes**, annual average daily traffic per road segment
# MAGIC
# MAGIC They cover the same four metropolitan areas. The `region` widget below picks one. Each
# MAGIC region is an extract far wider than the city: `london` runs from the Dorset coast to the
# MAGIC Peak District and takes in Birmingham, Bristol and Southampton.

# COMMAND ----------

# MAGIC %pip install folium==0.20.0 h3==4.5.0 shapely==2.1.2

# COMMAND ----------

import folium
import h3
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import seaborn as sns
from shapely import wkt

sns.set_theme(style="whitegrid", palette="colorblind")


def basemap(lat, lon, zoom):
    """A muted OpenStreetMap basemap. CartoDB's tiles now need an API key; this does not."""
    chart = folium.Map(location=[lat, lon], zoom_start=zoom, tiles="OpenStreetMap")
    chart.get_root().header.add_child(folium.Element(
        "<style>.leaflet-tile-pane{filter:grayscale(1) contrast(0.92) brightness(1.06);}</style>"
    ))
    return chart


# COMMAND ----------

dbutils.widgets.text("stats_catalog", "TomTom_Traffic_Stats", "Traffic Stats catalog")
dbutils.widgets.text(
    "volumes_catalog", "TomTom_Traffic_Volumes", "Traffic Volumes catalog"
)
dbutils.widgets.text("observation_date", "2025-09-03", "Date to score, UTC")
dbutils.widgets.text(
    "region", "london", "Region: london, austin, losangeles or melbourne"
)
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
# MAGIC ## 1. Exposure: how much traffic each cell carries
# MAGIC
# MAGIC Both datasets carry `h3_r9`, an [H3](https://h3geo.org/) cell at resolution 9, about
# MAGIC 0.1 km² or a few city blocks. Everything here joins on that string, so no map matching or
# MAGIC spatial index is needed.
# MAGIC
# MAGIC Exposure is **vehicle-kilometres per day**: `aadt` times `length_m`, summed over the cell.
# MAGIC A road drawn as ten segments counts once, and a cell holding a motorway and three side
# MAGIC streets reads as busy, which an average of AADT would hide.

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
# MAGIC ## 2. Severity: how traffic behaves in each cell
# MAGIC
# MAGIC Four factors from the hourly speeds of one date, averaged over every segment-hour in the
# MAGIC cell:
# MAGIC
# MAGIC | Factor | Why it matters |
# MAGIC |---|---|
# MAGIC | Mean speed | severity. Energy in a collision goes with the square of speed |
# MAGIC | Variability | stop-and-go traffic, the pattern behind rear-end collisions |
# MAGIC | Speeding rate | hours where p85 is above the limit, so speeding is normal rather than rare |
# MAGIC | Congestion rate | hours below 60% of the limit: dense, slow, frequent contact |
# MAGIC
# MAGIC Hours with a single observation carry no percentile array. They count towards mean speed
# MAGIC and congestion, the speeding rate is taken over the hours that have a distribution, and
# MAGIC `single_observation_pct` is kept beside the factors as a measure of how thin the evidence
# MAGIC is. `hourly_stats` is partitioned by `observation_date`, so one date reads one partition.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW severity AS
    SELECT
        s.h3_r9,
        avg(h.harmonic_speed_kph)                          AS mean_speed_kph,
        avg(h.stddev_speed_kph / h.harmonic_speed_kph)     AS variability,
        avg(CASE WHEN h.speed_percentiles_kph IS NULL THEN NULL
                 WHEN element_at(h.speed_percentiles_kph, 17) > s.speed_limit_kph
                 THEN 1.0 ELSE 0.0 END)                    AS speeding_rate,
        avg(CASE WHEN h.harmonic_speed_kph < 0.6 * s.speed_limit_kph
                 THEN 1.0 ELSE 0.0 END)                    AS congestion_rate,
        avg(CASE WHEN h.speed_percentiles_kph IS NULL
                 THEN 1.0 ELSE 0.0 END)                    AS single_observation_rate
    FROM {stats}.segments s
    JOIN {stats}.hourly_stats h USING (dseg_id)
    WHERE h.observation_date = DATE '{observation_date}'
      AND s.region = '{region}'
      AND s.speed_limit_kph > 0
      AND h.harmonic_speed_kph > 0
    GROUP BY s.h3_r9
    HAVING count(h.speed_percentiles_kph) > 0
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
# MAGIC ## 3. One score per cell
# MAGIC
# MAGIC Each factor is scaled from 0 to 1 across the cells in this extract, weighted, and summed.
# MAGIC Exposure takes the largest weight because a collision needs traffic before it needs
# MAGIC speed. **The weights are illustrative.** Calibrate them with your claims history. The
# MAGIC scaling is relative to this extract, so scores rank cells within it and cannot be
# MAGIC compared across releases.
# MAGIC
# MAGIC The four factor panels say why the scaling matters. Exposure is heavily skewed, a long
# MAGIC tail of a few very busy cells, so on a linear scale almost every cell scores near zero on
# MAGIC that term; speeding and congestion are closer to uniform. A factor that is skewed
# MAGIC contributes almost nothing to the ranking of ordinary cells, which is an argument for
# MAGIC ranking or log-scaling the exposure term before you weight it.

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW territory_risk AS
    WITH cells AS (
        SELECT e.h3_r9, e.vehicle_km_per_day, v.mean_speed_kph, v.variability,
               v.speeding_rate, v.congestion_rate, v.single_observation_rate
        FROM exposure e JOIN severity v USING (h3_r9)
    ),
    bounds AS (
        SELECT min(vehicle_km_per_day) lo_e, max(vehicle_km_per_day) hi_e,
               min(mean_speed_kph) lo_s, max(mean_speed_kph) hi_s,
               min(variability) lo_v, max(variability) hi_v,
               min(speeding_rate) lo_p, max(speeding_rate) hi_p,
               min(congestion_rate) lo_c, max(congestion_rate) hi_c
        FROM cells
    ),
    weighted AS (
        SELECT c.*,
               0.35 * (c.vehicle_km_per_day - b.lo_e) / nullif(b.hi_e - b.lo_e, 0) AS exposure_part,
               0.20 * (c.mean_speed_kph  - b.lo_s) / nullif(b.hi_s - b.lo_s, 0)    AS speed_part,
               0.20 * (c.variability     - b.lo_v) / nullif(b.hi_v - b.lo_v, 0)    AS variability_part,
               0.15 * (c.speeding_rate   - b.lo_p) / nullif(b.hi_p - b.lo_p, 0)    AS speeding_part,
               0.10 * (c.congestion_rate - b.lo_c) / nullif(b.hi_c - b.lo_c, 0)    AS congestion_part
        FROM cells c CROSS JOIN bounds b
    )
    SELECT h3_r9, vehicle_km_per_day,
           round(mean_speed_kph, 1)                AS mean_speed_kph,
           round(variability, 3)                   AS variability,
           round(100 * speeding_rate, 1)           AS speeding_pct,
           round(100 * congestion_rate, 1)         AS congestion_pct,
           round(100 * single_observation_rate, 1) AS single_observation_pct,
           round(exposure_part + speed_part + variability_part + speeding_part + congestion_part, 3)
                                                   AS risk_score,
           round(exposure_part, 3)    AS exposure_part,
           round(speed_part, 3)       AS speed_part,
           round(variability_part, 3) AS variability_part,
           round(speeding_part, 3)    AS speeding_part,
           round(congestion_part, 3)  AS congestion_part
    FROM weighted
    """)

# One row per scored cell. Everything below is drawn from this frame.
cells = spark.sql("SELECT * FROM territory_risk").toPandas()
print(f"{len(cells):,} scored cells in {region} on {observation_date}")

display(spark.sql("""
        SELECT h3_r9, vehicle_km_per_day, mean_speed_kph, variability, speeding_pct,
               congestion_pct, single_observation_pct, risk_score
        FROM territory_risk ORDER BY risk_score DESC LIMIT 15
        """))

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(13, 8))

panels = [
    ("vehicle_km_per_day", "Exposure (vehicle-km per day)", "steelblue", True),
    ("variability", "Speed variability (std / mean)", "mediumpurple", False),
    ("speeding_pct", "Hours with p85 above the limit (%)", "coral", False),
    ("congestion_pct", "Hours below 60% of the limit (%)", "seagreen", False),
]
for ax, (column, title, color, log) in zip(axes.flat, panels):
    values = cells[column].clip(upper=cells[column].quantile(0.99))
    ax.hist(values, bins=50, color=color, edgecolor="white")
    ax.set_title(title)
    if log:
        ax.set_yscale("log")
        ax.set_ylabel("Cells (log scale)")
    else:
        ax.set_ylabel("Cells")

plt.suptitle(f"The four factors across {len(cells):,} cells, 99th percentile clipped",
             fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 4.5))

ax1.hist(cells.risk_score, bins=60, color="crimson", edgecolor="white")
ax1.axvline(cells.risk_score.median(), color="black", linestyle="--",
            label=f"Median: {cells.risk_score.median():.2f}")
ax1.axvline(cells.risk_score.quantile(0.95), color="darkred",
            label=f"95th percentile: {cells.risk_score.quantile(0.95):.2f}")
ax1.set_title("Composite risk score")
ax1.set_xlabel("Score (0 lowest, 1 highest)")
ax1.set_ylabel("Cells")
ax1.legend()

top = cells.nlargest(15, "risk_score")
parts = [c for c in cells.columns if c.endswith("_part")]
bottom = [0] * len(top)
for part, color in zip(parts, ["#800026", "#e31a1c", "#fd8d3c", "#feb24c", "#ffeda0"]):
    ax2.bar(range(len(top)), top[part], bottom=bottom, color=color,
            label=part.replace("_part", ""))
    bottom = [b + v for b, v in zip(bottom, top[part])]
ax2.set_xticks(range(len(top)))
ax2.set_xticklabels([f"{s:.0f}" for s in top.mean_speed_kph])
ax2.set_xlabel("The 15 highest-scoring cells, labelled by mean speed in km/h")
ax2.set_ylabel("Weighted contribution")
ax2.set_title("What lifts the top cells")
ax2.legend(fontsize=8)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Two kinds of risky cell
# MAGIC
# MAGIC The stacked bars above already show it: some top cells are lifted by exposure, others by
# MAGIC speed and speeding. They are two different risks wearing one number.
# MAGIC
# MAGIC - **Dense and slow.** Central cells carry the most vehicle-kilometres at 15 to 25 km/h.
# MAGIC   Exposure is high, but each event is less severe.
# MAGIC - **Fast and free-flowing.** Motorway cells run above 100 km/h. Their 85th percentile is
# MAGIC   above the limit in almost every hour, with very little congestion. Exposure per cell is
# MAGIC   lower, but each event is more severe.
# MAGIC
# MAGIC Claims frequency and claims severity models weight these differently, so in practice
# MAGIC score them separately rather than as one number. Keeping the factor columns makes that
# MAGIC possible. In the scatter the two types sit at opposite ends of the speed axis, and the
# MAGIC colour shows that the single score rates both highly.

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

sample = cells[cells.vehicle_km_per_day > 0].sample(min(8000, len(cells)), random_state=0)

fig, ax = plt.subplots(figsize=(11, 6))
points = ax.scatter(sample.mean_speed_kph, sample.vehicle_km_per_day, c=sample.risk_score,
                    cmap="YlOrRd", s=6, alpha=0.5)
ax.set_yscale("log")
ax.set_xlabel("Mean speed (km/h)")
ax.set_ylabel("Vehicle-km per day (log scale)")
ax.set_title(f"Exposure against speed, {len(sample):,} sampled cells in {region}",
             fontsize=13, fontweight="bold")
ax.axvline(40, color="grey", linestyle=":", alpha=0.7)
ax.axvline(80, color="grey", linestyle=":", alpha=0.7)
ax.text(20, sample.vehicle_km_per_day.max(), "urban", ha="center", fontsize=9, color="grey")
ax.text(60, sample.vehicle_km_per_day.max(), "arterial", ha="center", fontsize=9, color="grey")
ax.text(100, sample.vehicle_km_per_day.max(), "motorway", ha="center", fontsize=9, color="grey")
fig.colorbar(points, ax=ax, label="Risk score")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The map
# MAGIC
# MAGIC Resolution 9 is right for scoring and too fine for a map of a whole region: this extract
# MAGIC has hundreds of thousands of scored cells, and a browser renders a few thousand polygons
# MAGIC comfortably. Rolling up to resolution 6, about 36 km² per hexagon, covers the extract
# MAGIC with roughly two thousand.
# MAGIC
# MAGIC Each hexagon takes the **exposure-weighted** mean of its children's scores, so it shows
# MAGIC the risk vehicles actually meet rather than treating an empty lane like a motorway.
# MAGIC Colour is by rank, because exposure is skewed and a linear scale would leave almost every
# MAGIC hexagon the same pale shade. Switch the layer to see the scored cells at full resolution
# MAGIC around the busiest point.

# COMMAND ----------

MAP_RESOLUTION = 6

cells["parent"] = [h3.cell_to_parent(c, MAP_RESOLUTION) for c in cells.h3_r9]
cells["weighted_score"] = cells.risk_score * cells.vehicle_km_per_day

area = cells.groupby("parent").agg(
    vehicle_km_per_day=("vehicle_km_per_day", "sum"),
    weighted_score=("weighted_score", "sum"),
    mean_speed_kph=("mean_speed_kph", "mean"),
    speeding_pct=("speeding_pct", "mean"),
    cells_r9=("h3_r9", "count"),
).reset_index()
area["risk_score"] = area.weighted_score / area.vehicle_km_per_day

print(f"{len(area):,} hexagons at resolution {MAP_RESOLUTION}, "
      f"rolled up from {area.cells_r9.sum():,} scored cells")

busiest = cells.nlargest(1, "vehicle_km_per_day").h3_r9.iloc[0]
centre = h3.cell_to_latlng(busiest)

chart = basemap(centre[0], centre[1], 9)
shade = cm.YlOrRd
rank = area.risk_score.rank(pct=True)

overview = folium.FeatureGroup(name=f"Region, resolution {MAP_RESOLUTION}")
for row, value in zip(area.itertuples(), rank):
    color = mcolors.to_hex(shade(value))
    folium.Polygon(
        locations=h3.cell_to_boundary(row.parent),
        color=color, fill=True, fill_color=color, fill_opacity=0.55, weight=1,
        tooltip=(f"score {row.risk_score:.2f}, {row.vehicle_km_per_day:,.0f} vehicle-km/day, "
                 f"{row.mean_speed_kph:.0f} km/h, speeding in {row.speeding_pct:.0f}% of hours, "
                 f"{row.cells_r9} scored cells"),
    ).add_to(overview)
overview.add_to(chart)

# The scored cells themselves, within 40 steps of the busiest one
near = cells[cells.h3_r9.isin(set(h3.grid_disk(busiest, 40)))]
detail = folium.FeatureGroup(name=f"Busiest area, resolution 9 ({len(near):,} cells)", show=False)
norm = mcolors.Normalize(vmin=near.risk_score.min(), vmax=near.risk_score.max())
for row in near.itertuples():
    color = mcolors.to_hex(shade(norm(row.risk_score)))
    folium.Polygon(
        locations=h3.cell_to_boundary(row.h3_r9),
        color=color, fill=True, fill_color=color, fill_opacity=0.6, weight=0.5,
        tooltip=(f"{row.h3_r9}<br>score {row.risk_score:.2f}<br>"
                 f"{row.vehicle_km_per_day:,.0f} vehicle-km/day<br>"
                 f"{row.mean_speed_kph:.0f} km/h, speeding {row.speeding_pct:.0f}%, "
                 f"congested {row.congestion_pct:.0f}%"),
    ).add_to(detail)
detail.add_to(chart)

folium.LayerControl(collapsed=False).add_to(chart)
display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Route risk, from a telematics trace
# MAGIC
# MAGIC Territory rating asks where a driver lives. Route rating asks which roads they drive, and
# MAGIC telematics makes that possible.
# MAGIC
# MAGIC Raw GPS is not enough. Points can be 10 to 50 metres off the road and do not identify the
# MAGIC road. **Map matching** links the trace to road segments. [Fast Map Matching](https://github.com/cyang-kth/fmm)
# MAGIC is an open source tool that outputs OpenStreetMap way IDs, and `segments.osm_way_ids`
# MAGIC lists the OpenStreetMap ways for each TomTom segment, so `arrays_overlap` joins the two
# MAGIC without a second matching step:
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
    SELECT s.dseg_id, s.length_m, s.speed_limit_kph, s.street_name, s.geometry_wkt,
           h.harmonic_speed_kph, h.speed_percentiles_kph,
           hour(from_utc_timestamp(make_timestamp(year(h.observation_date), month(h.observation_date),
                                                  day(h.observation_date), h.hour_utc, 0, 0),
                                   s.time_zone))                                          AS hour_local,
           CASE WHEN h.speed_percentiles_kph IS NULL THEN NULL
                WHEN element_at(h.speed_percentiles_kph, 17) > s.speed_limit_kph
                THEN 1.0 ELSE 0.0 END                                                     AS speeding,
           CASE WHEN h.harmonic_speed_kph < 0.6 * s.speed_limit_kph THEN 1.0 ELSE 0.0 END AS congested,
           CASE WHEN h.speed_percentiles_kph IS NULL THEN 1.0 ELSE 0.0 END                AS single_observation
    FROM {stats}.segments s
    JOIN {stats}.hourly_stats h USING (dseg_id)
    WHERE arrays_overlap(s.osm_way_ids, array({", ".join(f"{w}L" for w in MATCHED_OSM_WAYS)}))
      AND s.region = '{region}'
      AND s.speed_limit_kph > 0
      AND h.observation_date = DATE '{observation_date}'
      AND h.harmonic_speed_kph > 0
    """)

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW route_segments AS
    SELECT dseg_id, any_value(street_name) AS street_name, any_value(geometry_wkt) AS geometry_wkt,
           any_value(length_m) AS length_m, any_value(speed_limit_kph) AS speed_limit_kph,
           avg(harmonic_speed_kph) AS mean_speed_kph, avg(speeding) AS speeding_rate,
           avg(congested) AS congestion_rate, avg(single_observation) AS single_observation_rate
    FROM route_hours GROUP BY dseg_id HAVING count(speed_percentiles_kph) > 0
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
# MAGIC The route in two views. The profile is the whole route through the day, with the p85 line
# MAGIC against the posted limit: where p85 sits above the dashed line, the faster sixth of the
# MAGIC traffic is over the limit. The map is the same route segment by segment, shaded by how
# MAGIC much of its day was spent that way, which is where a uniform-looking route turns out to
# MAGIC have uneven stretches.

# COMMAND ----------

route = spark.sql("SELECT * FROM route_segments").toPandas()

if route.empty:
    raise ValueError(
        f"None of the OSM ways {MATCHED_OSM_WAYS} lie in region '{region}'. They are on the M20 "
        f"in Kent, which is in the london extract. Set the region widget back to london, or put "
        f"way IDs from your own matched trace in MATCHED_OSM_WAYS."
    )

profile = spark.sql("""
        SELECT hour_local,
               round(avg(harmonic_speed_kph), 1)                    AS harmonic_mean,
               round(avg(element_at(speed_percentiles_kph, 17)), 1) AS p85,
               round(avg(element_at(speed_percentiles_kph, 3)), 1)  AS p15,
               round(avg(speed_limit_kph), 1)                       AS speed_limit
        FROM route_hours GROUP BY hour_local ORDER BY hour_local
        """).toPandas()

fig, ax = plt.subplots(figsize=(11, 5))
ax.fill_between(profile.hour_local, profile.p15, profile.p85, alpha=0.2, color="steelblue",
                label="p15 to p85")
ax.plot(profile.hour_local, profile.harmonic_mean, marker="o", color="steelblue",
        label="Harmonic mean")
ax.plot(profile.hour_local, profile.speed_limit, linestyle="--", color="black",
        label="Speed limit")
ax.set_xlabel("Local hour")
ax.set_ylabel("km/h")
ax.set_title(f"The route through {observation_date}", fontsize=13, fontweight="bold")
ax.set_xticks(range(0, 24, 2))
ax.legend()
plt.tight_layout()
plt.show()

centre_point = wkt.loads(route.geometry_wkt.iloc[len(route) // 2]).coords[0]

chart = basemap(centre_point[1], centre_point[0], 11)
norm = mcolors.Normalize(vmin=0, vmax=1)

for row in route.itertuples():
    folium.PolyLine(
        [(lat, lon) for lon, lat in wkt.loads(row.geometry_wkt).coords],
        color=mcolors.to_hex(cm.YlOrRd(norm(row.speeding_rate))), weight=4, opacity=0.9,
        tooltip=(f"{row.street_name or 'unnamed'} | "
                 f"speeding in {100 * row.speeding_rate:.0f}% of hours | "
                 f"{row.mean_speed_kph:.0f} km/h against a {row.speed_limit_kph:.0f} limit | "
                 f"congested {100 * row.congestion_rate:.0f}%"),
    ).add_to(chart)

display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this route says
# MAGIC
# MAGIC This route has 137 segments and covers 26 km of motorway. The average speed is 105 km/h.
# MAGIC The 85th percentile is above the limit in 94% of hours, with almost no congestion. This
# MAGIC means the route has rare but severe risk. The widespread speeding reflects the road, not
# MAGIC any single driver.
# MAGIC
# MAGIC This matters for pricing. These results are the road's **baseline**. A driver's telematics
# MAGIC speeds only make sense compared with this baseline. Without it, a model may charge people
# MAGIC for the roads they use.
# MAGIC
# MAGIC ## Where to take it next
# MAGIC
# MAGIC - **Score frequency and severity separately.** Section 4 shows why one number hides two
# MAGIC   kinds of risk; the factor columns are already there.
# MAGIC - **Rank or log-scale the exposure term.** Section 3 shows how skewed it is. Min-max
# MAGIC   scaling on a long tail gives ordinary cells almost no exposure signal.
# MAGIC - **Weight the route by time, not distance.** A congested kilometre has more exposure than
# MAGIC   a clear one. Use `aadt_by_day_hour` to apply hourly weights.
# MAGIC - **Join the two datasets on OpenStreetMap as well as H3.** `segments.osm_way_ids` is an
# MAGIC   array; in Traffic Volumes the way IDs sit inside `osm_id` as `way:length:offset` triples,
# MAGIC   so `cast(split(triple, ':')[0] AS BIGINT)` after `explode(split(osm_id, ','))` gives the
# MAGIC   same key.
# MAGIC - **Check coverage first.** `traffic_volumes.coverage` shows the share of each road class
# MAGIC   with an estimate. Segments without an estimate are missing, not zero. Many minor roads
# MAGIC   have no estimate.
# MAGIC - **Separate weekdays and weekends.** `observation_date` shows the day of week. Combining
# MAGIC   the two patterns can hide important differences.
