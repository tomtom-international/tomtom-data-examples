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

# The default is what Marketplace suggests when you install this listing.
dbutils.widgets.text(
    "catalog", "TomTom_Traffic_Volumes", "Catalog you attached the share as"
)
catalog = dbutils.widgets.get("catalog")

try:
    spark.sql(f"DESCRIBE SCHEMA {catalog}.traffic_volumes")
except Exception as error:
    raise ValueError(
        f"Cannot read {catalog}.traffic_volumes. The error was: {error}. "
        f"If that is the name being wrong rather than a permission problem, set the `catalog` "
        f"widget at the top of this notebook to the catalog you accepted when you installed "
        f"the listing. Run SHOW CATALOGS if you are not sure what it was called."
    ) from None

aadt = f"{catalog}.traffic_volumes.aadt_segments"
coverage = f"{catalog}.traffic_volumes.coverage"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is in the database

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
# MAGIC Coverage follows probe vehicles: it is high on motorways and low on residential
# MAGIC streets. Use this table to decide which road classes to include.
# MAGIC
# MAGIC `frc_key` is the value in the source data. The coverage files report buckets `FRC8` and
# MAGIC `FRC9` with network length and no covered length, and no segment carries either value.
# MAGIC Group or sum by `frc`; use `frc_key` to reconcile with published figures.

# COMMAND ----------

import plotly.express as px

px.defaults.template = "plotly_white"

cov = spark.sql(f"""
        SELECT region, vintage_year, frc_key, frc, frc_label,
               round(total_length_m / 1000, 1)   AS total_km,
               round(covered_length_m / 1000, 1) AS covered_km,
               coverage_pct
        FROM {coverage}
        ORDER BY region, vintage_year, frc_key
        """).toPandas()

display(cov)
px.bar(cov, x="frc_label", y="coverage_pct", color="region", barmode="group", facet_col="vintage_year",
       labels=dict(frc_label="", coverage_pct="% of network with an estimate"),
       title="Coverage by road class").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Volume by road class
# MAGIC
# MAGIC Traffic is unevenly distributed. A few motorway segments carry more traffic than many
# MAGIC local roads, so medians and percentiles are more useful than means.

# COMMAND ----------

display(spark.sql(f"""
        SELECT frc, count(*) AS segments,
               round(percentile_approx(aadt, 0.5)) AS median_aadt,
               round(percentile_approx(aadt, 0.9)) AS p90_aadt,
               max(aadt)                           AS max_aadt,
               round(100.0 * sum(aadt) / sum(sum(aadt)) OVER (), 1) AS pct_of_all_traffic
        FROM {aadt}
        GROUP BY frc
        ORDER BY frc
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The weekly shape
# MAGIC
# MAGIC `posexplode` turns the 168-slot array into rows and keeps each slot's position, which
# MAGIC gives the day and the hour. Each cell below is the mean share of a segment's AADT that
# MAGIC falls in that hour, over classes 0 to 4.

# COMMAND ----------

profile = spark.sql(f"""
        SELECT floor(pos / 24) AS day_index, pos % 24 AS hour_of_day,
               round(100.0 * avg(hour_aadt / aadt), 2) AS pct_of_aadt
        FROM {aadt} LATERAL VIEW posexplode(aadt_by_day_hour) t AS pos, hour_aadt
        WHERE frc <= 4 AND aadt > 0
        GROUP BY 1, 2
        """).toPandas().pivot(index="day_index", columns="hour_of_day", values="pct_of_aadt")

px.imshow(profile, x=[f"{h:02d}" for h in range(24)], y=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
          color_continuous_scale="YlOrRd", labels=dict(x="hour", y="", color="% of AADT"),
          title="Hourly share of AADT, classes 0 to 4").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. How concentrated is the traffic
# MAGIC
# MAGIC A segment with 20,000 vehicles spread across the day is different from one with half its
# MAGIC traffic in four hours. The hourly array shows this difference. Below, we calculate the
# MAGIC share of weekday traffic during the morning peak (07:00 to 09:59) and evening peak
# MAGIC (16:00 to 18:59).

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW peaks AS
    WITH spread AS (
        SELECT region, vintage_year, segment_id, frc, aadt, h3_r9,
               floor(pos / 24) AS day_index, pos % 24 AS hour_of_day, hour_aadt
        FROM {aadt}
        LATERAL VIEW posexplode(aadt_by_day_hour) t AS pos, hour_aadt
    )
    SELECT
        region, vintage_year, segment_id, frc, aadt, h3_r9,
        round(100.0 * sum(CASE WHEN day_index < 5 AND hour_of_day BETWEEN 7 AND 9
                               THEN hour_aadt ELSE 0 END)
                    / nullif(sum(CASE WHEN day_index < 5 THEN hour_aadt ELSE 0 END), 0), 1)
            AS morning_peak_pct,
        round(100.0 * sum(CASE WHEN day_index < 5 AND hour_of_day BETWEEN 16 AND 18
                               THEN hour_aadt ELSE 0 END)
                    / nullif(sum(CASE WHEN day_index < 5 THEN hour_aadt ELSE 0 END), 0), 1)
            AS evening_peak_pct
    FROM spread
    GROUP BY region, vintage_year, segment_id, frc, aadt, h3_r9
    """)

display(spark.sql("""
        SELECT frc, count(*) AS segments,
               round(avg(morning_peak_pct), 1) AS avg_morning_peak_pct,
               round(avg(evening_peak_pct), 1) AS avg_evening_peak_pct
        FROM peaks GROUP BY frc ORDER BY frc
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. An exposure tier per segment
# MAGIC
# MAGIC Volume alone does not show exposure. The same AADT means different things on a motorway
# MAGIC and a residential street. These tiers rank roads within each road class.
# MAGIC This makes busy local roads easier to compare with other local roads.

# COMMAND ----------

display(spark.sql(f"""
        WITH tiered AS (
            SELECT region, vintage_year, frc, aadt,
                   ntile(5) OVER (PARTITION BY region, vintage_year, frc ORDER BY aadt)
                       AS exposure_tier
            FROM {aadt}
        )
        SELECT exposure_tier, count(*) AS segments,
               round(min(aadt)) AS min_aadt,
               round(percentile_approx(aadt, 0.5)) AS median_aadt,
               round(max(aadt)) AS max_aadt
        FROM tiered GROUP BY exposure_tier ORDER BY exposure_tier
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Scoring a portfolio through H3
# MAGIC
# MAGIC `h3_r9` holds the [H3](https://h3geo.org/) cell of each segment's center at resolution 9,
# MAGIC about 174 m across. Add the same cell to your addresses or journeys to join them with
# MAGIC one string comparison. No spatial library or map matching is needed.
# MAGIC
# MAGIC Score each cell by **vehicle-kilometres per day**: `aadt` times `length_m`, summed. A
# MAGIC cell with a motorway and three side streets is busy, and an average hides that. The sum
# MAGIC is the same whether a road is drawn as one segment or ten.

# COMMAND ----------

display(spark.sql(f"""
        SELECT region, vintage_year, h3_r9,
               count(*)                                AS segments,
               round(sum(aadt * length_m) / 1000)      AS vehicle_km_per_day,
               round(sum(length_m) / 1000, 2)          AS network_km,
               max(aadt)                               AS busiest_segment_aadt,
               min(frc)                                AS most_major_road_class
        FROM {aadt}
        GROUP BY region, vintage_year, h3_r9
        ORDER BY vehicle_km_per_day DESC
        LIMIT 25
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Putting it on a map
# MAGIC
# MAGIC Geometry is a WKT LineString in EPSG:4326. Filter on the bounding box columns first so
# MAGIC only the segments you are about to draw get parsed. Set `REGION` and `AREA` to somewhere
# MAGIC your extract covers.

# COMMAND ----------

# MAGIC %pip install folium==0.20.0 shapely==2.1.2

# COMMAND ----------

import folium
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import numpy as np
from shapely import wkt

REGION = "melbourne"
AREA = (144.94, -37.83, 144.99, -37.80)  # west, south, east, north

# Each segment becomes its own SVG path with its own tooltip, so we limit the rendered segments
MAX_SEGMENTS_ON_MAP = 1500

city = spark.sql(f"""
    SELECT geometry_wkt, aadt, frc, aadt_by_day
    FROM {aadt}
    WHERE region = '{REGION}'
      AND max_lon >= {AREA[0]} AND min_lon <= {AREA[2]}
      AND max_lat >= {AREA[1]} AND min_lat <= {AREA[3]}
    ORDER BY aadt DESC
    LIMIT {MAX_SEGMENTS_ON_MAP}
    """).toPandas()

print(f"{len(city):,} busiest segments in that box")

if len(city):
    chart = folium.Map(
        location=((AREA[1] + AREA[3]) / 2, (AREA[0] + AREA[2]) / 2),
        zoom_start=15,
        tiles="OpenStreetMap",
    )
    chart.get_root().header.add_child(
        folium.Element(
            "<style>.leaflet-tile-pane{filter:grayscale(1) contrast(0.92) brightness(1.06);}"
            "</style>"
        )
    )
    shade = mcolors.LogNorm(vmin=max(city.aadt.min(), 1), vmax=city.aadt.max())

    for row in city.itertuples():
        folium.PolyLine(
            [(lat, lon) for lon, lat in wkt.loads(row.geometry_wkt).coords],
            color=mcolors.to_hex(cm.YlOrRd(shade(row.aadt))),
            weight=2 + 4 * shade(row.aadt),
            opacity=0.85,
            tooltip=(
                f"class {row.frc}, AADT {row.aadt:,}, "
                f"busiest weekday {int(np.max(row.aadt_by_day[:5])):,}"
            ),
        ).add_to(chart)
    display(chart)
else:
    print("Nothing there. Regions in this share:")
    display(spark.sql(f"SELECT DISTINCT region FROM {aadt} ORDER BY region"))

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
# MAGIC without map matching.
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
