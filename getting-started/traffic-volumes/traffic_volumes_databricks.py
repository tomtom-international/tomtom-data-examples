# Databricks notebook source
# MAGIC %md
# MAGIC # Getting started with TomTom Traffic Volumes (AADT)
# MAGIC
# MAGIC Annual Average Daily Traffic for individual road segments, broken down to every hour of
# MAGIC every day of the week. Where Traffic Stats tells you how fast traffic moved, this tells
# MAGIC you how much of it there was, which is the exposure term in most road risk models.
# MAGIC
# MAGIC ## Database tables
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |---|---|
# MAGIC | `traffic_volumes.aadt_segments` | Annual Average Daily Traffic estimate and weekly profile for a road segment. |
# MAGIC | `traffic_volumes.coverage` | Network coverage by region, vintage, and road class. |
# MAGIC
# MAGIC ### `aadt_segments` columns
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `region` | Metropolitan sample extract. |
# MAGIC | `vintage_year` | Year of the AADT estimate. |
# MAGIC | `segment_id` | TomTom segment ID, unique within a region and vintage. |
# MAGIC | `frc` | Functional Road Class: 0 is motorway and 7 is a minor local road. The deliveries carry 0 to 4, 6 and 7. |
# MAGIC | `frc_delivered` | Functional Road Class exactly as delivered. |
# MAGIC | `openlr` | OpenLR location reference, for any map with an OpenLR decoder. |
# MAGIC | `osm_id` | OpenStreetMap ways the segment lies on, as `way:length:offset` triples separated by commas. The first field is the OSM way ID. Null where no way matched. |
# MAGIC | `gers_id` | Overture GERS segment the road element lies on, as `id:start:end`. |
# MAGIC | `aadt` | Annual Average Daily Traffic estimate, in vehicles. |
# MAGIC | `aadt_by_day` | Seven daily AADT values, ordered Monday through Sunday. |
# MAGIC | `aadt_by_day_hour` | 168 hourly AADT values, ordered Monday 00:00 through Sunday 23:00. |
# MAGIC | `geometry_wkt` | Segment geometry as a WGS84 WKT LineString. |
# MAGIC | `min_lon` | West edge of the segment bounding box. |
# MAGIC | `min_lat` | South edge of the segment bounding box. |
# MAGIC | `max_lon` | East edge of the segment bounding box. |
# MAGIC | `max_lat` | North edge of the segment bounding box. |
# MAGIC | `h3_r9` | H3 cell containing the segment centre, at resolution 9. |
# MAGIC | `length_m` | Segment length in metres. |
# MAGIC | `source_file` | Source file that supplied the row. |
# MAGIC
# MAGIC ### `coverage` columns
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `region` | Metropolitan sample extract. |
# MAGIC | `vintage_year` | Year of the AADT estimate. |
# MAGIC | `frc_key` | Road-class key exactly as delivered, such as `FRC0`. |
# MAGIC | `frc` | Functional Road Class from 0 to 8, with a delivered 9 stored as 8. |
# MAGIC | `frc_label` | Human-readable road-class name. |
# MAGIC | `total_length_m` | Total network length in the road class, in metres. |
# MAGIC | `covered_length_m` | Length with an AADT estimate, in metres. |
# MAGIC | `coverage_pct` | Percentage of the road class with an AADT estimate. |
# MAGIC | `source_file` | Source file name. |
# MAGIC
# MAGIC **TomTom Traffic Stats** is a separate listing holding hourly speeds for the same
# MAGIC roads. Take it too and volume and speed can be read together; the last section here
# MAGIC joins them.
# MAGIC
# MAGIC Four things to know before you start.
# MAGIC
# MAGIC 1. **Read `coverage` first.** This table holds only the segments that have an
# MAGIC    estimate.
# MAGIC 2. **AADT comes in two arrays.** `aadt_by_day` has 7 values, starting with Monday.
# MAGIC    `aadt_by_day_hour` has 168 values, ordered by day and then hour. Position `(day * 24) +
# MAGIC    hour` uses Monday as day 0. This notebook uses `posexplode`, which counts from 0.
# MAGIC 3. **Regions are metropolitan extracts far wider than the city.** `london` runs from the
# MAGIC    Dorset coast to the Peak District and includes Birmingham, Bristol and Southampton;
# MAGIC    `melbourne` covers most of Victoria. Section 1 prints each extent.
# MAGIC 4. **Exposure is `aadt` times `length_m`.** Vehicle-kilometres do not change when a road
# MAGIC    is drawn as one segment or ten, so sum that rather than `aadt` when comparing areas.

# COMMAND ----------

# MAGIC %pip install folium==0.20.0 shapely==2.1.2 h3==4.5.0

# COMMAND ----------

import folium
import h3
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from branca.colormap import LinearColormap
from matplotlib.collections import LineCollection
from pyspark.sql.types import DecimalType
from shapely import wkt

sns.set_theme(style="whitegrid", palette="colorblind")

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
HEAT = ["#ffffcc", "#fd8d3c", "#e31a1c", "#800026"]
FRC_LABELS = {
    0: "Motorway", 1: "Major road", 2: "Other major road", 3: "Secondary road",
    4: "Local connecting", 5: "Local high importance", 6: "Local road",
    7: "Minor local", 8: "Other road",
}
FRC_COLORS = {
    0: "#800026", 1: "#bd0026", 2: "#e31a1c", 3: "#fc4e2a", 4: "#fd8d3c",
    5: "#feb24c", 6: "#fed976", 7: "#ffeda0", 8: "#ffffcc",
}


def collect(query):
    """Run a query into pandas. Spark types expressions built from literals such as `1.0` as
    DECIMAL, which pandas receives as decimal.Decimal objects that matplotlib cannot plot."""
    frame = spark.sql(query)
    decimals = {f.name: float for f in frame.schema if isinstance(f.dataType, DecimalType)}
    return frame.toPandas().astype(decimals)


def overlaps(area):
    """SQL predicate for segments whose bounding box touches `area` = (west, south, east, north)."""
    west, south, east, north = area
    return f"max_lon >= {west} AND min_lon <= {east} AND max_lat >= {south} AND min_lat <= {north}"


def basemap(lat, lon, zoom):
    """A muted OpenStreetMap basemap. CartoDB's tiles now need an API key; this does not."""
    chart = folium.Map(location=[lat, lon], zoom_start=zoom, tiles="OpenStreetMap")
    chart.get_root().header.add_child(folium.Element(
        "<style>.leaflet-tile-pane{filter:grayscale(1) contrast(0.92) brightness(1.06);}</style>"
    ))
    return chart

# COMMAND ----------

# The defaults are what Marketplace suggests when you install this listing.
dbutils.widgets.text("catalog", "TomTom_Traffic_Volumes", "Catalog you attached the share as")
dbutils.widgets.text("region", "melbourne", "Region: london, austin, losangeles or melbourne")
dbutils.widgets.text("vintage_year", "2025", "Vintage year")

catalog = dbutils.widgets.get("catalog")
region = dbutils.widgets.get("region")
vintage_year = int(dbutils.widgets.get("vintage_year"))
aadt = f"{catalog}.traffic_volumes.aadt_segments"
coverage = f"{catalog}.traffic_volumes.coverage"

try:
    spark.sql(f"DESCRIBE SCHEMA {catalog}.traffic_volumes")
except Exception as error:
    raise ValueError(
        f"Cannot read {catalog}.traffic_volumes. The error was: {error}. "
        f"If that is the name being wrong rather than a permission problem, set the `catalog` "
        f"widget at the top of this notebook to the catalog you accepted when you installed "
        f"the listing. Run SHOW CATALOGS if you are not sure what it was called."
    ) from None

# Everything after section 1 reads one region and one vintage through this view.
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW segments AS
    SELECT * FROM {aadt} WHERE region = '{region}' AND vintage_year = {vintage_year}
    """)

if spark.table("segments").isEmpty():
    raise ValueError(
        f"No segments for region '{region}', vintage {vintage_year}. "
        f"The table below the next cell lists what this share holds."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is in the database
# MAGIC
# MAGIC Every region and vintage in the share, with its extent. The rest of the notebook reads
# MAGIC the one picked by the widgets above.

# COMMAND ----------

display(spark.sql(f"""
        SELECT region, vintage_year, count(*) AS segments,
               round(sum(length_m) / 1000) AS network_km,
               round(percentile_approx(aadt, 0.5)) AS median_aadt,
               min(aadt) AS min_aadt, max(aadt) AS max_aadt,
               min(min_lat) AS south, max(max_lat) AS north,
               min(min_lon) AS west, max(max_lon) AS east
        FROM {aadt}
        GROUP BY region, vintage_year
        ORDER BY region, vintage_year
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check coverage first
# MAGIC
# MAGIC Coverage follows probe vehicles: it is high on motorways and low on residential streets.
# MAGIC Read it before anything else, because `aadt_segments` holds only the covered part of the
# MAGIC network. A missing road is missing, not empty.
# MAGIC
# MAGIC The left panel is absolute: how many kilometres of each class carry an estimate, against
# MAGIC how many kilometres exist. The right panel is the rate, coloured green above 70%, amber
# MAGIC above 30% and red below.
# MAGIC
# MAGIC `frc_key` is the value in the source data. The coverage files report buckets `FRC8` and
# MAGIC `FRC9` with network length and no covered length, and no segment carries either value.
# MAGIC Group or sum by `frc`; use `frc_key` to reconcile with published figures.

# COMMAND ----------

cov = collect(f"""
        SELECT frc, frc_label, frc_key,
               round(total_length_m / 1000, 1)   AS total_km,
               round(covered_length_m / 1000, 1) AS covered_km,
               coverage_pct
        FROM {coverage}
        WHERE region = '{region}' AND vintage_year = {vintage_year}
        ORDER BY frc
        """)

display(cov)

labels = [f"FRC {f}" for f in cov.frc]
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))

ax1.bar(labels, cov.covered_km, color="#2ca02c", label="Covered")
ax1.bar(labels, cov.total_km - cov.covered_km, bottom=cov.covered_km, color="#d3d3d3",
        label="Not covered")
ax1.set(ylabel="Road network length (km)",
        title=f"Network length by road class, {region} {vintage_year}")
ax1.legend()

bars = ax2.barh([f"{label}: {name}" for label, name in zip(labels, cov.frc_label)], cov.coverage_pct,
                color=["#2ca02c" if c > 70 else "#ff7f0e" if c > 30 else "#d62728"
                       for c in cov.coverage_pct])
ax2.bar_label(bars, fmt="%.0f%%", padding=3, fontsize=9)
ax2.set(xlabel="Coverage (%)", xlim=(0, 105), title="Share of each class carrying an AADT estimate")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Volume by road class
# MAGIC
# MAGIC Traffic is unevenly distributed. Most segments are small roads, and most of the traffic
# MAGIC is on the few big ones, so a mean across all segments describes a quiet street rather
# MAGIC than a city. The box plot is on a log axis for that reason: each class spans more than
# MAGIC an order of magnitude and the classes barely overlap.

# COMMAND ----------

by_class = collect("""
        SELECT frc, count(*) AS segments,
               percentile_approx(cast(aadt AS DOUBLE), array(0.05, 0.25, 0.5, 0.75, 0.95)) AS quantiles,
               round(100.0 * sum(aadt * length_m) / sum(sum(aadt * length_m)) OVER (), 1)
                                                   AS pct_of_vehicle_km
        FROM segments GROUP BY frc ORDER BY frc
        """)

colors = [FRC_COLORS.get(f, "#999") for f in by_class.frc]
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))

bars = ax1.bar([f"FRC {f}\n{FRC_LABELS.get(f, '')}" for f in by_class.frc], by_class.segments,
               color=colors)
ax1.bar_label(bars, [f"{n:,}\n{pct:.0f}% of veh-km"
                     for n, pct in zip(by_class.segments, by_class.pct_of_vehicle_km)], fontsize=8)
ax1.tick_params(axis="x", labelsize=8)
ax1.set(ylabel="Number of segments", title="Road segments by class")

# The quantiles come back from Spark, so no per-segment row reaches the driver
box = ax2.bxp([dict(zip(["whislo", "q1", "med", "q3", "whishi"], row.quantiles),
                    label=f"FRC {row.frc}", fliers=[]) for row in by_class.itertuples()],
              patch_artist=True, showfliers=False)
for patch, color in zip(box["boxes"], colors):
    patch.set_facecolor(color)
ax2.set(yscale="log", ylabel="AADT (log scale), 5th to 95th percentile",
        title="Traffic volume distribution by road class")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Temporal traffic patterns
# MAGIC
# MAGIC Every segment carries **168 hourly values**, 7 days by 24 hours. That temporal depth is
# MAGIC what separates this dataset from a single annual number: it says *when* the traffic is
# MAGIC there, not just how much of it.
# MAGIC
# MAGIC The heatmaps below average those values across the segments of one road class. Look for
# MAGIC the double hump of commuter traffic on weekdays and the later, flatter weekend profile.
# MAGIC The line chart normalises each curve to a share of its own daily total, so the shapes of
# MAGIC classes with very different volumes can be compared directly.

# COMMAND ----------

PROFILE_CLASSES = [0, 2, 4, 7]

profile = collect(f"""
        SELECT frc, floor(pos / 24) AS day_index, pos % 24 AS hour_of_day,
               avg(hour_aadt) AS mean_hour_aadt
        FROM segments LATERAL VIEW posexplode(aadt_by_day_hour) t AS pos, hour_aadt
        WHERE frc IN ({", ".join(map(str, PROFILE_CLASSES))})
        GROUP BY 1, 2, 3
        """)

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
for ax, frc in zip(axes.flat, PROFILE_CLASSES):
    subset = profile[profile.frc == frc]
    if subset.empty:
        ax.set_title(f"FRC {frc} (not in this extract)")
        ax.set_axis_off()
        continue
    grid = subset.pivot(index="day_index", columns="hour_of_day", values="mean_hour_aadt")
    sns.heatmap(grid, ax=ax, cmap="YlOrRd", yticklabels=DAYS,
                xticklabels=[f"{h:02d}" for h in range(24)], cbar_kws={"label": "Mean vehicles/hour"})
    ax.set_title(f"FRC {frc}: {FRC_LABELS[frc]}", fontsize=11, fontweight="bold")
    ax.set(xlabel="Hour of day", ylabel="")

plt.suptitle(f"Weekly traffic patterns by road class, {region} {vintage_year}",
             fontsize=14, fontweight="bold", y=1.01)
plt.tight_layout()
plt.show()

shape = (profile.assign(period=np.where(profile.day_index < 5, "weekday", "weekend"))
         .groupby(["frc", "period", "hour_of_day"]).mean_hour_aadt.mean())

fig, ax = plt.subplots(figsize=(12, 6))
for (frc, period), curve in shape.groupby(level=["frc", "period"]):
    ax.plot(range(24), 100 * curve.to_numpy() / curve.sum(), "-" if period == "weekday" else "--",
            color=FRC_COLORS[frc], label=f"FRC {frc} {period}")
ax.set(xlabel="Hour of day", ylabel="% of daily traffic",
       title="Hourly traffic distribution: weekday (solid) against weekend (dashed)")
ax.set_xticks(range(0, 24, 2), [f"{h:02d}:00" for h in range(0, 24, 2)])
ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Traffic intensity on a map
# MAGIC
# MAGIC Traffic is spatial, and a map shows what a table cannot: the corridors, the density
# MAGIC gradient out of the centre, and how road class and volume line up.
# MAGIC
# MAGIC Filter on the bounding box columns before parsing any geometry, so only the segments
# MAGIC about to be drawn get read. The box below is centred on the busiest part of the region;
# MAGIC set `AREA` by hand for somewhere else.

# COMMAND ----------

# Centre the view on the busiest 1% of the extract rather than a hardcoded city
focus = spark.sql("""
        SELECT percentile_approx((min_lon + max_lon) / 2, 0.5) AS lon,
               percentile_approx((min_lat + max_lat) / 2, 0.5) AS lat
        FROM (SELECT * FROM segments ORDER BY aadt DESC LIMIT 2000)
        """).first()

AREA = (focus.lon - 0.12, focus.lat - 0.06, focus.lon + 0.12, focus.lat + 0.06)

box = collect(f"""
        SELECT geometry_wkt, aadt FROM segments
        WHERE {overlaps(AREA)}
        ORDER BY aadt DESC LIMIT 4000
        """)

fig, ax = plt.subplots(figsize=(14, 8))
roads = LineCollection([list(wkt.loads(g).coords) for g in box.geometry_wkt],
                       array=box.aadt.to_numpy(), cmap="YlOrRd", linewidths=0.9,
                       norm=mcolors.LogNorm(vmin=max(box.aadt.min(), 1), vmax=box.aadt.max()))
ax.add_collection(roads)
ax.autoscale()
ax.set_aspect(1 / np.cos(np.radians(focus.lat)))
ax.set(xlabel="Longitude", ylabel="Latitude")
ax.set_title(f"Traffic volume intensity, {region} {vintage_year} ({len(box):,} busiest segments)",
             fontsize=14, fontweight="bold")
fig.colorbar(roads, ax=ax, shrink=0.7, label="AADT (log scale)")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The same roads, interactively
# MAGIC
# MAGIC A static picture shows the pattern; an interactive one lets you check it. Click a segment
# MAGIC to see its AADT and its Monday-to-Sunday breakdown, and ask whether the busy roads are
# MAGIC the ones you would expect to be busy. That is the quickest validation of the data there
# MAGIC is.

# COMMAND ----------

CBD = (focus.lon - 0.02, focus.lat - 0.012, focus.lon + 0.02, focus.lat + 0.012)

cbd = collect(f"""
        SELECT segment_id, frc, aadt, osm_id, aadt_by_day, geometry_wkt FROM segments
        WHERE {overlaps(CBD)}
        ORDER BY aadt DESC LIMIT 1500
        """)

chart = basemap(focus.lat, focus.lon, 15)
scale = LinearColormap(HEAT, vmin=cbd.aadt.quantile(0.05), vmax=cbd.aadt.quantile(0.95),
                       caption="AADT (annual average daily traffic)")

for row in cbd.itertuples():
    daily = "<br>".join(f"{day[:3]}: {value:,.0f}" for day, value in zip(DAYS, row.aadt_by_day))
    folium.PolyLine(
        [(lat, lon) for lon, lat in wkt.loads(row.geometry_wkt).coords],
        weight=3 + min(row.aadt / 5000, 5), color=scale(row.aadt), opacity=0.8,
        popup=folium.Popup(
            f"<b>Segment:</b> {row.segment_id}<br>"
            f"<b>FRC:</b> {row.frc} ({FRC_LABELS.get(row.frc, '')})<br>"
            f"<b>OSM:</b> {row.osm_id}<br>"
            f"<b>AADT:</b> {row.aadt:,.0f}<hr><b>Daily averages:</b><br>{daily}",
            max_width=260,
        ),
    ).add_to(chart)

scale.add_to(chart)
display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Exposure per H3 cell
# MAGIC
# MAGIC Joining addresses or journeys to road segments is awkward: segment boundaries are
# MAGIC arbitrary and a spatial join costs a lookup per point. `h3_r9` holds the
# MAGIC [H3](https://h3geo.org/) cell of each segment's centre at resolution 9, about 174 m
# MAGIC across. Put the same cell on your own points and the join is one string comparison, with
# MAGIC no spatial library and no map matching.
# MAGIC
# MAGIC The value mapped here is derived, not raw: **vehicle-kilometres per day**, `aadt` times
# MAGIC `length_m` summed over the cell. A cell holding a motorway and three side streets reads
# MAGIC as busy, and the total does not change when a road is drawn as one segment or ten, which
# MAGIC is what makes cells comparable with each other.

# COMMAND ----------

cells = collect(f"""
        SELECT h3_r9,
               count(*)                           AS segments,
               round(sum(aadt * length_m) / 1000) AS vehicle_km_per_day,
               round(sum(length_m) / 1000, 2)     AS network_km,
               max(aadt)                          AS busiest_segment_aadt,
               min(frc)                           AS most_major_road_class
        FROM segments
        WHERE (min_lon + max_lon) / 2 BETWEEN {AREA[0]} AND {AREA[2]}
          AND (min_lat + max_lat) / 2 BETWEEN {AREA[1]} AND {AREA[3]}
        GROUP BY h3_r9
        ORDER BY vehicle_km_per_day DESC
        """)

display(cells.head(25))

chart = basemap(focus.lat, focus.lon, 12)
scale = LinearColormap(HEAT, vmin=cells.vehicle_km_per_day.quantile(0.1),
                       vmax=cells.vehicle_km_per_day.quantile(0.9),
                       caption="Vehicle-kilometres per day per H3 cell (exposure)")

for row in cells.itertuples():
    color = scale(row.vehicle_km_per_day)
    folium.Polygon(
        h3.cell_to_boundary(row.h3_r9),
        color=color, fill=True, fill_color=color, fill_opacity=0.6, weight=1,
        popup=folium.Popup(
            f"<b>H3 cell:</b> {row.h3_r9}<br>"
            f"<b>Vehicle-km/day:</b> {row.vehicle_km_per_day:,.0f}<br>"
            f"<b>Network:</b> {row.network_km:,.2f} km over {row.segments} segments<br>"
            f"<b>Busiest segment:</b> {row.busiest_segment_aadt:,.0f} AADT<br>"
            f"<b>Most major class:</b> FRC {row.most_major_road_class}",
            max_width=260,
        ),
    ).add_to(chart)

scale.add_to(chart)
display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC **Join to your own network.** Four identifiers, one per situation:
# MAGIC
# MAGIC | You have | Join on |
# MAGIC |---|---|
# MAGIC | Other TomTom products, or another vintage of this one | `segment_id` |
# MAGIC | Data referenced to OpenStreetMap, such as map-matched GPS traces | the way ID inside `osm_id` |
# MAGIC | Data from another vendor referenced to Overture | the segment ID inside `gers_id` |
# MAGIC | Your own map and an OpenLR decoder | `openlr` |
# MAGIC
# MAGIC `osm_id` holds one `way:length:offset` triple per OpenStreetMap way the segment lies on,
# MAGIC separated by commas. One row per way, with the way ID as a number:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT segment_id, cast(split(triple, ':')[0] AS BIGINT) AS osm_way_id
# MAGIC FROM TomTom_Traffic_Volumes.traffic_volumes.aadt_segments
# MAGIC LATERAL VIEW explode(split(osm_id, ',')) t AS triple
# MAGIC WHERE osm_id IS NOT NULL
# MAGIC ```
# MAGIC
# MAGIC `gers_id` is `id:start:end`; `split(gers_id, ':')[0]` is the Overture segment ID.
# MAGIC
# MAGIC **Combine with Traffic Stats.** Volume shows exposure and speed shows severity. Together
# MAGIC they give a better risk picture. TomTom Traffic Stats is a separate Marketplace listing
# MAGIC for the same four metropolitan areas. Both datasets have `h3_r9`, so you can join them
# MAGIC without map matching. The territory risk assessment use case in this repository builds
# MAGIC a scored territory and a scored route from the two.
# MAGIC
# MAGIC The listings use separate catalogs. The query below uses the suggested names,
# MAGIC `TomTom_Traffic_Volumes` and `TomTom_Traffic_Stats`. Replace them with your catalog names.
# MAGIC
# MAGIC First total each dataset to one row per cell. If you join first, each volume row is
# MAGIC multiplied by the 21 hourly rows for each segment. The totals can then reach the billions:
# MAGIC
# MAGIC ```sql
# MAGIC WITH volume AS (
# MAGIC   SELECT h3_r9, round(sum(aadt * length_m) / 1000) AS vehicle_km_per_day
# MAGIC   FROM TomTom_Traffic_Volumes.traffic_volumes.aadt_segments
# MAGIC   WHERE region = 'london' AND vintage_year = 2025
# MAGIC   GROUP BY h3_r9
# MAGIC ),
# MAGIC speed AS (
# MAGIC   SELECT s.h3_r9, avg(h.harmonic_speed_kph) AS peak_speed_kph
# MAGIC   FROM TomTom_Traffic_Stats.traffic_stats_batch.segments s
# MAGIC   JOIN TomTom_Traffic_Stats.traffic_stats_batch.hourly_stats h ON h.dseg_id = s.dseg_id
# MAGIC   WHERE h.observation_date = DATE '2025-09-03'
# MAGIC     AND h.hour_utc BETWEEN 7 AND 9
# MAGIC   GROUP BY s.h3_r9
# MAGIC )
# MAGIC SELECT v.h3_r9, v.vehicle_km_per_day, round(s.peak_speed_kph) AS peak_speed_kph
# MAGIC FROM volume v JOIN speed s ON s.h3_r9 = v.h3_r9
# MAGIC ORDER BY v.vehicle_km_per_day DESC
# MAGIC LIMIT 20
# MAGIC ```
# MAGIC
# MAGIC A busy, slow cell has a different risk from a busy, fast cell. The busiest central London
# MAGIC cells have morning peak speeds of 14 to 22 km/h.
# MAGIC
# MAGIC **Compare vintages.** Use the same segment identifiers from two years to calculate growth.
# MAGIC Check `coverage` for both years first. Better coverage can look like more traffic.
# MAGIC
# MAGIC **Remember what is not here.** Every row has an estimate, so no filtering is needed.
# MAGIC Segments without estimates are absent, not zero. Use `coverage` for network-wide claims,
# MAGIC not the row count in this table.
