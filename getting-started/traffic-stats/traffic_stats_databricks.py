# Databricks notebook source
# MAGIC %md
# MAGIC # Getting started with TomTom Traffic Stats Batch
# MAGIC
# MAGIC  Hourly speed and travel time statistics for individual road segments, from anonymised probe vehicles.
# MAGIC
# MAGIC ## Database tables
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |---|---|
# MAGIC | `traffic_stats_batch.segments` | One directional road segment and its map, road, and location attributes. |
# MAGIC | `traffic_stats_batch.hourly_stats` | One segment's speed measurements for one UTC date and hour. Join to `segments` on `dseg_id`. |
# MAGIC
# MAGIC ### `segments` columns
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `dseg_id` | Directional segment ID; join key to `hourly_stats`. |
# MAGIC | `segment_id` | Legacy segment ID used before 2023. |
# MAGIC | `new_segment_id` | Current segment identifier as text. |
# MAGIC | `frc` | Functional Road Class: 0 is motorway and 7 is a local road. The deliveries carry 0 to 4, 6 and 7. |
# MAGIC | `speed_limit_kph` | Posted speed limit in km/h. |
# MAGIC | `has_verified_speed` | Whether the speed limit has been field verified. |
# MAGIC | `street_name` | Road name; may be null. |
# MAGIC | `length_m` | Segment length in metres. |
# MAGIC | `form_of_way` | Physical road form, such as a single carriageway or roundabout. |
# MAGIC | `has_hov_lane` | Whether a high-occupancy-vehicle lane is present. |
# MAGIC | `bearing_deg` | Direction of travel in degrees. |
# MAGIC | `is_navigable` | Whether through traffic can use the segment. |
# MAGIC | `is_under_construction` | Whether the segment is under construction. |
# MAGIC | `has_restricted_access` | Whether access is private or restricted. |
# MAGIC | `geometry_wkt` | Segment geometry as a WGS84 WKT LineString. |
# MAGIC | `min_lon` | West edge of the segment bounding box. |
# MAGIC | `min_lat` | South edge of the segment bounding box. |
# MAGIC | `max_lon` | East edge of the segment bounding box. |
# MAGIC | `max_lat` | North edge of the segment bounding box. |
# MAGIC | `h3_r9` | H3 cell containing the segment centre, at resolution 9. |
# MAGIC | `time_zone` | IANA time zone used to convert UTC measurements to local time. |
# MAGIC | `country_iso3` | Three-letter ISO country code. |
# MAGIC | `osm_way_ids` | OpenStreetMap ways the segment lies on. |
# MAGIC | `osm_offsets` | The segment's offset along each of those ways, in the same order. |
# MAGIC | `region` | Metropolitan sample extract. |
# MAGIC | `tile_id` | Morton tile identifier. |
# MAGIC | `map_version` | TomTom map version. |
# MAGIC
# MAGIC ### `hourly_stats` columns
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `dseg_id` | Directional segment ID; join key to `segments`. |
# MAGIC | `observation_date` | UTC measurement date. |
# MAGIC | `hour_utc` | UTC hour from 0 to 23. |
# MAGIC | `avg_speed_kph` | Arithmetic mean speed in km/h. |
# MAGIC | `harmonic_speed_kph` | Harmonic mean speed in km/h; use for travel-time analysis. |
# MAGIC | `median_speed_kph` | Median speed in km/h. |
# MAGIC | `stddev_speed_kph` | Standard deviation of speed in km/h. |
# MAGIC | `speed_percentiles_kph` | Nineteen speed percentiles from p5 to p95; null when the hour rests on a single observation. |
# MAGIC | `tile_id` | Morton tile identifier. |
# MAGIC | `map_version` | TomTom map version. |
# MAGIC
# MAGIC **TomTom Traffic Volumes** is a separate listing holding how much traffic each road carries, rather than how fast it moves.
# MAGIC Take it too and volume and speed can be read together; the last section here
# MAGIC joins them.
# MAGIC
# MAGIC Four things to know before you start.
# MAGIC
# MAGIC 1. Speeds are km/h.
# MAGIC 2. Hours are UTC. Use `segments.time_zone` before you say anything about rush hour.
# MAGIC 3. `speed_percentiles_kph` is null for roughly one row in six. A null means the hour
# MAGIC    rests on a single observation; the mean is still there. It is the one sample-size
# MAGIC    signal in the data, so carry it as a flag rather than drop the rows.
# MAGIC 4. Regions are metropolitan extracts far wider than the city. `london` runs from the
# MAGIC    Dorset coast to the Peak District and includes Birmingham, Bristol and Southampton.
# MAGIC    Section 1 prints the extent.

# COMMAND ----------

# MAGIC %pip install folium==0.20.0 shapely==2.1.2

# COMMAND ----------

import folium
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.collections import LineCollection
from shapely import wkt

sns.set_theme(style="whitegrid", palette="colorblind")


def basemap(lat, lon, zoom):
    """A muted OpenStreetMap basemap. CartoDB's tiles now need an API key; this does not."""
    chart = folium.Map(location=[lat, lon], zoom_start=zoom, tiles="OpenStreetMap")
    chart.get_root().header.add_child(folium.Element(
        "<style>.leaflet-tile-pane{filter:grayscale(1) contrast(0.92) brightness(1.06);}</style>"
    ))
    return chart


FRC_LABELS = {
    0: "Motorway", 1: "Major road", 2: "Other major road", 3: "Secondary road",
    4: "Local connecting", 5: "Local high importance", 6: "Local road", 7: "Minor local",
}
FRC_COLORS = {
    0: "#800026", 1: "#bd0026", 2: "#e31a1c", 3: "#fc4e2a",
    4: "#fd8d3c", 5: "#feb24c", 6: "#fed976", 7: "#ffeda0",
}


def histogram(source, expr, lo, hi, bins=50, where="TRUE"):
    """Bucket counts computed in Spark, so no per-row data reaches the driver."""
    return spark.sql(f"""
        SELECT {lo} + ({hi} - {lo}) * (bucket + 0.5) / {bins} AS value, count(*) AS rows
        FROM (
            SELECT least({bins} - 1, greatest(0, floor(({expr} - {lo}) * {bins} / ({hi} - {lo})))) AS bucket
            FROM {source} WHERE {where} AND {expr} IS NOT NULL
        )
        GROUP BY bucket ORDER BY bucket
        """).toPandas()

# COMMAND ----------

# The default is what Marketplace suggests when you install this listing, so accepting the
# suggested name means this notebook runs unedited.
dbutils.widgets.text(
    "catalog", "TomTom_Traffic_Stats", "Catalog you attached the dataset as"
)
catalog = dbutils.widgets.get("catalog")

try:
    spark.sql(f"DESCRIBE SCHEMA {catalog}.traffic_stats_batch")
except Exception as error:
    raise ValueError(
        f"Cannot read {catalog}.traffic_stats_batch. The error was: {error}. "
        f"If that is the name being wrong rather than a permission problem, set the `catalog` "
        f"widget at the top of this notebook to the catalog you accepted when you installed "
        f"the listing. Run SHOW CATALOGS if you are not sure what it was called."
    ) from None

# The dataset covers four metropolitan areas. A whole-dataset query is a fair amount of data for a
# first look, so the notebook scopes itself to one of them
dbutils.widgets.text(
    "region", "london", "Region: london, austin, losangeles or melbourne"
)
region = dbutils.widgets.get("region")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW segments AS
    SELECT * FROM {catalog}.traffic_stats_batch.segments WHERE region = '{region}'
    """)

tiles = [
    row.tile_id for row in spark.sql("SELECT DISTINCT tile_id FROM segments").collect()
]
if not tiles:
    raise ValueError(
        f"No segments in region '{region}'. Run "
        f"SELECT DISTINCT region FROM {catalog}.traffic_stats_batch.segments to see the names."
    )

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW hourly_stats AS
    SELECT * FROM {catalog}.traffic_stats_batch.hourly_stats
    WHERE tile_id IN ({", ".join(repr(t) for t in tiles)})
    """)

segments = "segments"
hourly = "hourly_stats"

# `hourly_stats` contains 922 million rows for a full week. To keep reads fast, this
# notebook processes one day at a time.
#
# The default is Wednesday, 2025-09-03, a typical midweek baseline. Set the dates to
# 2025-09-01 and 2025-09-07 to process the full week; the charts below then show one line
# per day, and weekday and weekend separate cleanly.
dbutils.widgets.text("date_from", "2025-09-03", "First observation date")
dbutils.widgets.text("date_to", "2025-09-03", "Last observation date")
date_from = dbutils.widgets.get("date_from")
date_to = dbutils.widgets.get("date_to")

# One row per segment-hour in the window, with the road attributes joined on. Everything
# after section 2 reads this view.
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW measured AS
    SELECT h.dseg_id, h.observation_date, h.hour_utc, h.avg_speed_kph, h.harmonic_speed_kph,
           h.median_speed_kph, h.stddev_speed_kph, h.speed_percentiles_kph,
           s.frc, s.speed_limit_kph, s.length_m, s.street_name, s.h3_r9,
           date_format(h.observation_date, 'EEEE') AS day_of_week,
           dayofweek(h.observation_date) IN (1, 7) AS is_weekend,
           hour(from_utc_timestamp(
               make_timestamp(year(h.observation_date), month(h.observation_date),
                              day(h.observation_date), h.hour_utc, 0, 0), s.time_zone)) AS hour_local
    FROM {hourly} h
    JOIN {segments} s USING (dseg_id)
    WHERE h.observation_date BETWEEN DATE '{date_from}' AND DATE '{date_to}'
      AND h.harmonic_speed_kph > 0
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is in the database
# MAGIC
# MAGIC Every column carries a description, so `DESCRIBE TABLE` is worth a minute. The tables
# MAGIC below give the extent, the dates, and the share of hours that rest on a single
# MAGIC observation.
# MAGIC
# MAGIC The four panels are the road network itself, before any speed is read: how many segments
# MAGIC of each class, what the posted limits look like, how long a segment typically is, and
# MAGIC which physical road forms occur. Segment length matters more than it looks. Half of all
# MAGIC segments are shorter than a city block, so anything averaged per segment is dominated by
# MAGIC short ones unless you weight by length.

# COMMAND ----------

display(spark.sql(f"""
        SELECT count(*) AS segments, round(sum(length_m) / 1000) AS network_km,
               count(DISTINCT country_iso3) AS countries, min(min_lat) AS south,
               max(max_lat) AS north, min(min_lon) AS west, max(max_lon) AS east
        FROM {segments}
        """))

display(spark.sql(f"""
        SELECT min(observation_date) AS first_date, max(observation_date) AS last_date,
               count(DISTINCT observation_date) AS days, count(*) AS hourly_rows,
               round(100.0 * avg(CASE WHEN speed_percentiles_kph IS NULL THEN 1 ELSE 0 END), 1)
                   AS pct_single_observation
        FROM {hourly}
        """))

# COMMAND ----------

frc_counts = spark.sql(f"SELECT frc, count(*) AS segments FROM {segments} GROUP BY frc ORDER BY frc").toPandas()
limits = histogram(segments, "speed_limit_kph", 0, 140, bins=28)
lengths = histogram(segments, "length_m", 0, 500, bins=50)
fow = spark.sql(f"""
        SELECT form_of_way, count(*) AS segments FROM {segments}
        GROUP BY form_of_way ORDER BY segments DESC LIMIT 8
        """).toPandas()

fig, axes = plt.subplots(2, 2, figsize=(13, 9))

axes[0, 0].bar(frc_counts.frc, frc_counts.segments,
               color=[FRC_COLORS.get(f, "#999") for f in frc_counts.frc])
axes[0, 0].set_title("Functional road class")
axes[0, 0].set_xlabel("FRC (0 = motorway, 7 = local)")
axes[0, 0].set_ylabel("Segment count")

axes[0, 1].bar(limits.value, limits.rows, width=140 / 28 * 0.9, color="coral")
axes[0, 1].set_title("Posted speed limit")
axes[0, 1].set_xlabel("km/h")

axes[1, 0].bar(lengths.value, lengths.rows, width=10 * 0.9, color="seagreen")
axes[1, 0].set_title("Segment length (clipped at 500 m)")
axes[1, 0].set_xlabel("metres")

axes[1, 1].barh(fow.form_of_way[::-1], fow.segments[::-1], color="mediumpurple")
axes[1, 1].set_title("Form of way (top 8)")

plt.suptitle(f"The road network in {region}", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The network on a map
# MAGIC
# MAGIC Two views of the same roads. The static one covers the whole extract, coloured by class,
# MAGIC which is the quickest way to see how wide these regional samples really are. The
# MAGIC interactive one is limited to the major roads around the centre, with a layer per class,
# MAGIC so you can switch classes on and off and hover a road for its name and limit.

# COMMAND ----------

overview = spark.sql(f"""
        SELECT geometry_wkt, frc FROM {segments} WHERE frc <= 4
        ORDER BY frc, length_m DESC LIMIT 25000
        """).toPandas()

lines = [list(wkt.loads(g).coords) for g in overview.geometry_wkt]
mid_lat = np.mean([c[0][1] for c in lines])

fig, ax = plt.subplots(figsize=(13, 9))
collection = LineCollection(lines, array=overview.frc.values, cmap="RdYlGn_r", linewidths=0.4)
ax.add_collection(collection)
ax.autoscale()
ax.set_aspect(1 / np.cos(np.radians(mid_lat)))
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
ax.set_title(f"Major roads in the {region} extract ({len(overview):,} of the longest drawn)",
             fontsize=13, fontweight="bold")
fig.colorbar(collection, ax=ax, shrink=0.7, label="FRC (0 = motorway, 4 = local connecting)")
plt.tight_layout()
plt.show()

# COMMAND ----------

centre = spark.sql(f"SELECT avg(min_lon) AS lon, avg(min_lat) AS lat FROM {segments}").first()
AREA = (centre.lon - 0.12, centre.lat - 0.06, centre.lon + 0.12, centre.lat + 0.06)

major = spark.sql(f"""
        SELECT geometry_wkt, frc, street_name, speed_limit_kph
        FROM {segments}
        WHERE frc <= 3
          AND max_lon >= {AREA[0]} AND min_lon <= {AREA[2]}
          AND max_lat >= {AREA[1]} AND min_lat <= {AREA[3]}
        LIMIT 4000
        """).toPandas()

chart = basemap(centre.lat, centre.lon, 12)

for frc, color in {0: "red", 1: "darkorange", 2: "blue", 3: "green"}.items():
    subset = major[major.frc == frc]
    group = folium.FeatureGroup(name=f"FRC {frc} ({len(subset):,} segments)")
    for row in subset.itertuples():
        folium.PolyLine(
            [(lat, lon) for lon, lat in wkt.loads(row.geometry_wkt).coords],
            color=color, weight=2 if frc <= 1 else 1, opacity=0.7,
            tooltip=f"{row.street_name or 'unnamed'} (FRC {frc}, {row.speed_limit_kph} km/h)",
        ).add_to(group)
    group.add_to(chart)

folium.LayerControl().add_to(chart)
display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Coverage: which roads report, and when
# MAGIC
# MAGIC A road can be on the map without a probe vehicle ever using it. Coverage falls with road
# MAGIC class, because small roads carry less traffic, and it falls at night for the same reason.
# MAGIC
# MAGIC The two lines are different things. **Reporting** is the share of segments that produced
# MAGIC any measurement in that hour. **Single observation** is the share of the measurements
# MAGIC that rest on one vehicle, which is where `speed_percentiles_kph` comes back null. Both
# MAGIC matter before you trust a number: the first says whether the road is represented at all,
# MAGIC the second says how much weight the reading carries.

# COMMAND ----------

by_frc = spark.sql(f"""
        WITH observed AS (SELECT dseg_id, count(*) AS hours FROM measured GROUP BY dseg_id)
        SELECT s.frc, count(*) AS segments, round(sum(s.length_m) / 1000) AS network_km,
               round(avg(s.speed_limit_kph), 1)              AS avg_speed_limit_kph,
               round(100.0 * count(o.dseg_id) / count(*), 1) AS pct_with_measurements,
               round(avg(o.hours), 1)                        AS avg_hours_measured
        FROM {segments} s LEFT JOIN observed o USING (dseg_id)
        GROUP BY s.frc ORDER BY s.frc
        """).toPandas()

display(by_frc)

active = spark.sql("SELECT count(DISTINCT dseg_id) AS n FROM measured").first().n
by_hour = spark.sql(f"""
        SELECT hour_utc,
               round(100.0 * count(DISTINCT dseg_id) / {active}, 1) AS pct_reporting,
               round(100.0 * avg(CASE WHEN speed_percentiles_kph IS NULL THEN 1 ELSE 0 END), 1)
                   AS pct_single_observation
        FROM measured GROUP BY hour_utc ORDER BY hour_utc
        """).toPandas()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 4.5))

ax1.bar(by_frc.frc, by_frc.pct_with_measurements,
        color=[FRC_COLORS.get(f, "#999") for f in by_frc.frc])
ax1.set_title("Segments with any measurement, by road class")
ax1.set_xlabel("FRC (0 = motorway, 7 = local)")
ax1.set_ylabel("% of segments")
for f, v in zip(by_frc.frc, by_frc.pct_with_measurements):
    ax1.text(f, v, f"{v:.0f}%", ha="center", va="bottom", fontsize=8)

ax2.plot(by_hour.hour_utc, by_hour.pct_reporting, marker="o", color="steelblue", label="Reporting")
ax2.plot(by_hour.hour_utc, by_hour.pct_single_observation, marker="o", color="coral",
         label="Resting on one vehicle")
ax2.set_title("Coverage through the day")
ax2.set_xlabel("Hour (UTC)")
ax2.set_ylabel("% of active segments / of rows")
ax2.set_xticks(range(0, 24, 2))
ax2.legend()

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Speed distributions
# MAGIC
# MAGIC Three views. The first is every measured segment-hour in the window, which is bimodal:
# MAGIC urban traffic around 30 km/h and free-flowing major roads well above it. The second
# MAGIC separates that by road class, from a sample small enough to estimate a density on. The
# MAGIC third compares the three averages the data carries.
# MAGIC
# MAGIC The harmonic mean sits below the arithmetic mean, and that is the point of it. Averaging
# MAGIC speeds over-weights the fast vehicles; averaging the time they take does not. Use the
# MAGIC harmonic mean for anything that becomes a travel time.

# COMMAND ----------

speeds = histogram("measured", "avg_speed_kph", 0, 150, bins=60)
sample = spark.sql("""
        SELECT frc, least(avg_speed_kph, 150) AS avg_speed_kph FROM measured
        WHERE frc <= 4 AND rand() < 0.02 LIMIT 150000
        """).toPandas()
metrics = spark.sql("""
        SELECT round(avg(avg_speed_kph), 1)      AS arithmetic,
               round(avg(median_speed_kph), 1)   AS median,
               round(avg(harmonic_speed_kph), 1) AS harmonic
        FROM measured
        """).toPandas().iloc[0]

fig, axes = plt.subplots(1, 3, figsize=(16, 4.5))

axes[0].bar(speeds.value, speeds.rows, width=2.3, color="steelblue")
axes[0].set_title(f"Average speed, all {speeds.rows.sum():,} segment-hours")
axes[0].set_xlabel("km/h")
axes[0].set_ylabel("Segment-hours")

for frc in sorted(sample.frc.unique()):
    sns.kdeplot(sample.avg_speed_kph[sample.frc == frc], ax=axes[1], label=f"FRC {frc}",
                color=FRC_COLORS.get(frc), alpha=0.8)
axes[1].set_title("Speed by road class (2% sample)")
axes[1].set_xlabel("km/h")
axes[1].set_xlim(0, 150)
axes[1].legend(fontsize=8)

axes[2].bar(metrics.index, metrics.values, color=["steelblue", "coral", "seagreen"])
axes[2].set_title("The three averages compared")
axes[2].set_ylabel("km/h")
for i, v in enumerate(metrics.values):
    axes[2].text(i, v, f"{v:.1f}", ha="center", va="bottom")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. What the percentile array adds
# MAGIC
# MAGIC `speed_percentiles_kph` holds 19 values, p5 to p95 in steps of 5, for every segment-hour
# MAGIC that saw more than one vehicle. `element_at` counts from 1, so p85 is `element_at(arr, 17)`;
# MAGIC the `arr[16]` form counts from 0 and would give you p80.
# MAGIC
# MAGIC The curve on the left is the average shape of that distribution. It is not a straight
# MAGIC line: the gap between p5 and p25 is much wider than the gap between p50 and p70, which is
# MAGIC the signature of a few very slow vehicles in an otherwise free-flowing hour.
# MAGIC
# MAGIC The right panel turns the array into a congestion measure that needs no speed limit. For
# MAGIC each segment it compares the slow tail of the peak (p15 at 07:00 to 09:00 and 16:00 to
# MAGIC 18:00) with the free-flowing night (p85 between 22:00 and 05:00). A ratio near 1 means
# MAGIC the peak is as free as the night. Everything left of the dashed line is a road that slows
# MAGIC down when it is busy.

# COMMAND ----------

curve = spark.sql("""
        SELECT pos + 1 AS position, round(avg(value), 1) AS speed_kph
        FROM measured LATERAL VIEW posexplode(speed_percentiles_kph) t AS pos, value
        WHERE speed_percentiles_kph IS NOT NULL
        GROUP BY pos ORDER BY pos
        """).toPandas()

spark.sql("""
    CREATE OR REPLACE TEMP VIEW congestion_ratio AS
    SELECT dseg_id,
           avg(CASE WHEN hour_utc BETWEEN 7 AND 9 OR hour_utc BETWEEN 16 AND 18
                    THEN element_at(speed_percentiles_kph, 3) END)  AS peak_p15,
           avg(CASE WHEN hour_utc >= 22 OR hour_utc <= 5
                    THEN element_at(speed_percentiles_kph, 17) END) AS night_p85
    FROM measured GROUP BY dseg_id
    """)

ratios = histogram("congestion_ratio", "peak_p15 / night_p85", 0, 2, bins=50,
                   where="night_p85 > 0 AND peak_p15 IS NOT NULL")
median_ratio = spark.sql("""
        SELECT round(percentile_approx(peak_p15 / night_p85, 0.5), 2) AS m
        FROM congestion_ratio WHERE night_p85 > 0 AND peak_p15 IS NOT NULL
        """).first().m

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 4.5))

ax1.plot(curve.position, curve.speed_kph, marker="o", color="steelblue", linewidth=2)
ax1.fill_between(curve.position, curve.speed_kph, alpha=0.2, color="steelblue")
ax1.set_title("Average speed percentile curve")
ax1.set_xlabel("Percentile")
ax1.set_ylabel("km/h")
ax1.set_xticks(curve.position)
ax1.set_xticklabels([f"p{p}" for p in range(5, 100, 5)], rotation=45)

ax2.bar(ratios.value, ratios.rows, width=0.036, color="coral")
ax2.axvline(1.0, color="black", linestyle="--", alpha=0.6, label="No slowdown")
ax2.axvline(median_ratio, color="red", label=f"Median: {median_ratio:.2f}")
ax2.set_title("Congestion severity: peak p15 over night p85")
ax2.set_xlabel("Speed ratio (lower is more congested)")
ax2.set_ylabel("Segments")
ax2.legend()

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Speed and speeding through the day
# MAGIC
# MAGIC Hours are UTC in the data and local time in these charts, converted with
# MAGIC `segments.time_zone`. The shaded bands are the commute peaks.
# MAGIC
# MAGIC Read the left chart per road class, not across classes. The set of segments that report
# MAGIC changes through the day: at night the mix tilts towards motorways, by day towards local
# MAGIC streets, so a single all-class line would fall in the morning largely because different
# MAGIC roads started reporting.
# MAGIC
# MAGIC Speeding here means **p85 above the posted limit**, an hour in which the faster sixth of
# MAGIC traffic is over the limit rather than one unusual vehicle. It peaks where congestion does
# MAGIC not: at night, and on the fastest classes.

# COMMAND ----------

by_class_hour = spark.sql("""
        SELECT hour_local, frc, round(avg(harmonic_speed_kph), 1) AS speed_kph
        FROM measured WHERE frc <= 4 AND speed_limit_kph > 0
        GROUP BY 1, 2 ORDER BY 1, 2
        """).toPandas()

speeding = spark.sql("""
        SELECT hour_local, frc, is_weekend,
               round(100.0 * avg(CASE WHEN element_at(speed_percentiles_kph, 17) > speed_limit_kph
                                      THEN 1.0 ELSE 0.0 END), 1) AS speeding_pct
        FROM measured
        WHERE speed_limit_kph > 0 AND speed_percentiles_kph IS NOT NULL
        GROUP BY 1, 2, 3
        """).toPandas()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 4.5))

for frc in sorted(by_class_hour.frc.unique()):
    subset = by_class_hour[by_class_hour.frc == frc]
    ax1.plot(subset.hour_local, subset.speed_kph, marker="o", markersize=3,
             color=FRC_COLORS.get(frc), label=f"FRC {frc}")
ax1.axvspan(7, 9, alpha=0.1, color="red")
ax1.axvspan(16, 18, alpha=0.1, color="orange")
ax1.set_title("Harmonic speed by local hour and road class")
ax1.set_xlabel("Local hour (shaded: commute peaks)")
ax1.set_ylabel("km/h")
ax1.set_xticks(range(0, 24, 2))
ax1.legend(fontsize=8)

for weekend, color, label in [(False, "steelblue", "Weekday"), (True, "coral", "Weekend")]:
    subset = speeding[speeding.is_weekend == weekend].groupby("hour_local").speeding_pct.mean()
    if not subset.empty:
        ax2.plot(subset.index, subset.values, marker="o", color=color, linewidth=2, label=label)
ax2.set_title("Hours with p85 above the limit")
ax2.set_xlabel("Local hour")
ax2.set_ylabel("% of measured hours")
ax2.set_xticks(range(0, 24, 2))
ax2.legend()

plt.tight_layout()
plt.show()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 4.5), gridspec_kw={"width_ratios": [2, 1]})

grid = by_class_hour.pivot(index="frc", columns="hour_local", values="speed_kph")
sns.heatmap(grid, ax=ax1, cmap="RdYlGn", cbar_kws={"label": "km/h"},
            yticklabels=[f"FRC {f}: {FRC_LABELS.get(f, '')}" for f in grid.index])
ax1.set_title("Speed by road class and local hour")
ax1.set_xlabel("Local hour")
ax1.set_ylabel("")

by_frc_speeding = speeding.groupby("frc").speeding_pct.mean()
ax2.bar(by_frc_speeding.index, by_frc_speeding.values,
        color=[FRC_COLORS.get(f, "#999") for f in by_frc_speeding.index])
ax2.set_title("Speeding prevalence by road class")
ax2.set_xlabel("FRC (0 = motorway, 7 = local)")
ax2.set_ylabel("% of measured hours")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Joining on H3 cells
# MAGIC
# MAGIC Road segments are hard to join with address and journey data, because their boundaries
# MAGIC are arbitrary. `segments.h3_r9` stores the segment centroid's [H3](https://h3geo.org/)
# MAGIC cell at resolution 9, about 174 m across. Put the same cell on your own points and the
# MAGIC join is one string comparison.
# MAGIC
# MAGIC Weight by length when you aggregate. A 2 km motorway section should not count the same
# MAGIC as a 30 m slip road. The cells below are the ones that spend most of the day congested,
# MAGIC taking congestion as an hour below 60% of the posted limit.

# COMMAND ----------

display(spark.sql("""
        WITH per_segment AS (
            SELECT dseg_id, any_value(h3_r9) AS h3_r9, any_value(length_m) AS length_m,
                   100.0 * avg(CASE WHEN harmonic_speed_kph < 0.6 * speed_limit_kph
                                    THEN 1.0 ELSE 0.0 END) AS congestion_pct,
                   100.0 * avg(CASE WHEN element_at(speed_percentiles_kph, 17) > speed_limit_kph
                                    THEN 1.0 ELSE 0.0 END) AS speeding_pct
            FROM measured WHERE speed_limit_kph > 0
            GROUP BY dseg_id HAVING count(speed_percentiles_kph) > 0
        )
        SELECT h3_r9, count(*) AS segments,
               round(sum(congestion_pct * length_m) / sum(length_m), 1) AS congestion_pct_by_length,
               round(sum(speeding_pct * length_m) / sum(length_m), 1)   AS speeding_pct_by_length,
               round(sum(length_m) / 1000, 2)                           AS network_km
        FROM per_segment GROUP BY h3_r9 HAVING count(*) >= 3
        ORDER BY congestion_pct_by_length DESC LIMIT 25
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC **Use the harmonic mean for travel time.** A regular average gives too much weight to
# MAGIC the fastest vehicles and can make a slow road look faster than it is.
# MAGIC
# MAGIC **Use the whole distribution.** Percentiles show details that an average hides, such as
# MAGIC how often speeds exceed the limit and how much speeds vary at 08:00 versus 14:00.
# MAGIC
# MAGIC **Join through OpenStreetMap.** If journeys already have an OpenStreetMap match,
# MAGIC `segments.osm_way_ids` lets you skip another matching step. `arrays_overlap` joins a
# MAGIC list of matched way IDs to the segments in one step; the use case notebook shows it.
# MAGIC
# MAGIC **Separate weekdays and weekends.** Use `observation_date` to identify the day of week.
# MAGIC Their traffic patterns differ, so combining them can hide useful information.
# MAGIC
# MAGIC **Turn the signals into a score.** The territory risk assessment use case in this
# MAGIC repository scales these signals, weights them, and joins them to traffic volume for a
# MAGIC scored territory and a scored route.
# MAGIC
# MAGIC **Add volume.** Speed shows how bad traffic is; volume shows how many people it affects.
# MAGIC TomTom Traffic Volumes is a separate Marketplace listing for the same four metropolitan
# MAGIC areas. Both datasets include `h3_r9`, so you can join them without map matching.
# MAGIC
# MAGIC The listings use separate catalogs. The query below uses the suggested names,
# MAGIC `TomTom_Traffic_Stats` and `TomTom_Traffic_Volumes`; replace them with your catalog names.
# MAGIC
# MAGIC First total each dataset to one row per cell. If you join first, each volume row is
# MAGIC repeated for all 21 hourly rows in a segment, which makes the totals far too large:
# MAGIC
# MAGIC ```sql
# MAGIC WITH speed AS (
# MAGIC   SELECT s.h3_r9, avg(h.harmonic_speed_kph) AS peak_speed_kph
# MAGIC   FROM TomTom_Traffic_Stats.traffic_stats_batch.segments s
# MAGIC   JOIN TomTom_Traffic_Stats.traffic_stats_batch.hourly_stats h ON h.dseg_id = s.dseg_id
# MAGIC   WHERE h.observation_date = DATE '2025-09-03'
# MAGIC     AND h.hour_utc BETWEEN 7 AND 9
# MAGIC   GROUP BY s.h3_r9
# MAGIC ),
# MAGIC volume AS (
# MAGIC   SELECT h3_r9, round(sum(aadt * length_m) / 1000) AS vehicle_km_per_day
# MAGIC   FROM TomTom_Traffic_Volumes.traffic_volumes.aadt_segments
# MAGIC   WHERE region = 'london' AND vintage_year = 2025  -- same region as the widget above
# MAGIC   GROUP BY h3_r9
# MAGIC )
# MAGIC SELECT v.h3_r9, round(s.peak_speed_kph) AS peak_speed_kph, v.vehicle_km_per_day
# MAGIC FROM volume v JOIN speed s ON s.h3_r9 = v.h3_r9
# MAGIC ORDER BY v.vehicle_km_per_day DESC
# MAGIC LIMIT 20
# MAGIC ```
# MAGIC
# MAGIC The busiest cells in central London are typically 14 to 22 km/h during the morning peak.
# MAGIC To compare another metro, change the region in both datasets.
