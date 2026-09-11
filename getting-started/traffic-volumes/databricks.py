# Databricks notebook source
# MAGIC %md
# MAGIC # Getting started with TomTom Traffic Volumes (AADT)
# MAGIC
# MAGIC Annual Average Daily Traffic for individual road segments, broken down to every hour of
# MAGIC every day of the week. Where Traffic Stats tells you how fast traffic moved, this tells
# MAGIC you how much of it there was, which is the exposure term in most road risk models.
# MAGIC
# MAGIC | Table | One row per | Carries |
# MAGIC |---|---|---|
# MAGIC | `traffic_volumes.aadt_segments` | segment and vintage | AADT, its weekly and hourly profile, geometry, identifiers |
# MAGIC | `traffic_volumes.coverage` | region, vintage and road class | how much of the network has an estimate |
# MAGIC
# MAGIC **TomTom Traffic Stats** is a separate listing holding hourly speeds for the same
# MAGIC roads. Take it too and volume and speed can be read together; the last section here
# MAGIC joins them.
# MAGIC
# MAGIC Two things to know before you start.
# MAGIC
# MAGIC 1. **Read `coverage` first.** This table holds only the segments that have an
# MAGIC    estimate. On small roads much of the network has none, so an average taken here is
# MAGIC    really an average over the roads that were busy enough to measure. `coverage` is
# MAGIC    what tells you how much is missing.
# MAGIC 2. **The two arrays.** `aadt_by_day` holds 7 values, Monday first. `aadt_by_day_hour`
# MAGIC    holds 168, ordered by day then hour, so position `(day * 24) + hour` with Monday as
# MAGIC    day 0. This notebook uses `element_at` and `posexplode`, which count from 1 and 0
# MAGIC    respectively, so watch which one you are reading.

# COMMAND ----------

# The default is what Marketplace suggests when you install this listing, so accepting the
# suggested name means this notebook runs unedited. Change the widget if you named it something
# else. Capitalisation does not matter: Unity Catalog resolves identifiers case insensitively.
dbutils.widgets.text("catalog", "TomTom_Traffic_Volumes", "Catalog you attached the share as")
catalog = dbutils.widgets.get("catalog")

try:
    # Checked through SQL rather than `spark.catalog.databaseExists`, so the check resolves the
    # name exactly the way every query below it will. Better to say this now than to let the
    # first real query fail with a table-not-found several cells later.
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
# MAGIC ## 1. What is in the share

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT region, vintage_year, count(*) AS segments,
               round(percentile_approx(aadt, 0.5)) AS median_aadt,
               min(aadt) AS min_aadt, max(aadt) AS max_aadt
        FROM {aadt}
        GROUP BY region, vintage_year
        ORDER BY region, vintage_year
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Coverage, before anything else
# MAGIC
# MAGIC Coverage follows probe vehicles, so it is high on motorways and low on residential
# MAGIC streets. This is the table that decides which road classes your analysis can honestly
# MAGIC include.
# MAGIC
# MAGIC Note `frc_key` next to `frc`. Road class 8, meaning other roads, is written as the value
# MAGIC 9 in the source data, and some vintages publish both 8 and 9. `frc` is the corrected
# MAGIC code and is not unique on its own, so sum over it. `frc_key` is the key exactly as
# MAGIC delivered, for anyone reconciling against published figures.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT region, vintage_year, frc_key, frc, frc_label,
               round(total_length_m / 1000, 1)   AS total_km,
               round(covered_length_m / 1000, 1) AS covered_km,
               coverage_pct
        FROM {coverage}
        ORDER BY region, vintage_year, frc_key
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Volume by road class
# MAGIC
# MAGIC The distribution is heavily skewed. A handful of motorway segments carry more traffic
# MAGIC than thousands of local roads, so medians and percentiles describe it and means do not.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT frc, count(*) AS segments,
               round(percentile_approx(aadt, 0.5)) AS median_aadt,
               round(percentile_approx(aadt, 0.9)) AS p90_aadt,
               max(aadt)                           AS max_aadt,
               round(100.0 * sum(aadt) / sum(sum(aadt)) OVER (), 1) AS pct_of_all_traffic
        FROM {aadt}
        GROUP BY frc
        ORDER BY frc
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The weekly shape
# MAGIC
# MAGIC `posexplode` turns the array into rows and keeps the position, which is what maps a
# MAGIC slot back to a weekday.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            element_at(
                array('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday',
                      'Saturday', 'Sunday'),
                day_index + 1
            )                                           AS day_of_week,
            round(avg(day_aadt))                        AS mean_aadt,
            round(100.0 * avg(day_aadt) / avg(aadt), 1) AS pct_of_annual_average
        FROM (
            SELECT aadt, posexplode(aadt_by_day) AS (day_index, day_aadt)
            FROM {aadt} WHERE frc <= 4
        )
        GROUP BY day_index
        ORDER BY day_index
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. How concentrated is the traffic
# MAGIC
# MAGIC A segment carrying 20,000 vehicles evenly through the day is a different risk from one
# MAGIC carrying 20,000 with half of them in four hours. The hourly array is what separates the
# MAGIC two. Below, the share of weekday traffic in the morning peak, 07:00 to 09:59, and the
# MAGIC evening peak, 16:00 to 18:59.

# COMMAND ----------

spark.sql(
    f"""
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
    """
)

display(
    spark.sql(
        """
        SELECT frc, count(*) AS segments,
               round(avg(morning_peak_pct), 1) AS avg_morning_peak_pct,
               round(avg(evening_peak_pct), 1) AS avg_evening_peak_pct
        FROM peaks GROUP BY frc ORDER BY frc
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. An exposure tier per segment
# MAGIC
# MAGIC Volume on its own is not exposure. The same AADT on a motorway and on a residential
# MAGIC street are not comparable risks, so the tiers below are ranked inside each road class.
# MAGIC A busy local road then reads as busy for a local road.

# COMMAND ----------

display(
    spark.sql(
        f"""
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
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Scoring a portfolio through H3
# MAGIC
# MAGIC `h3_r9` holds the [H3](https://h3geo.org/) cell of the segment centroid at resolution 9,
# MAGIC about 174 m across. Put the same cell on your own addresses or journeys and the join is
# MAGIC one string comparison, with no spatial library and no map matching.
# MAGIC
# MAGIC Total the volume per cell rather than averaging it. A cell holding a motorway and three
# MAGIC side streets is a busy place, and an average hides that.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT region, vintage_year, h3_r9,
               count(*)  AS segments,
               sum(aadt) AS total_aadt,
               max(aadt) AS busiest_segment_aadt,
               min(frc)  AS most_major_road_class
        FROM {aadt}
        GROUP BY region, vintage_year, h3_r9
        ORDER BY total_aadt DESC
        LIMIT 25
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Putting it on a map
# MAGIC
# MAGIC Geometry is a WKT LineString in EPSG:4326. Filter on the bounding box columns first so
# MAGIC only the segments you are about to draw get parsed. Set `REGION` and `AREA` to somewhere
# MAGIC your extract covers.

# COMMAND ----------

# MAGIC %pip install folium shapely

# COMMAND ----------

import folium
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import numpy as np
from shapely import wkt

REGION = "melbourne"
AREA = (144.94, -37.83, 144.99, -37.80)  # west, south, east, north

# Each segment becomes its own SVG path with its own tooltip, so a wide box produces an output
# larger than a notebook will render, and a truncated map shows nothing at all. Busiest first,
# which is also the right order to lose the tail in.
MAX_SEGMENTS_ON_MAP = 1500

city = spark.sql(
    f"""
    SELECT geometry_wkt, aadt, frc, aadt_by_day
    FROM {aadt}
    WHERE region = '{REGION}'
      AND max_lon >= {AREA[0]} AND min_lon <= {AREA[2]}
      AND max_lat >= {AREA[1]} AND min_lat <= {AREA[3]}
    ORDER BY aadt DESC
    LIMIT {MAX_SEGMENTS_ON_MAP}
    """
).toPandas()

print(f"{len(city):,} busiest segments in that box")

if len(city):
    chart = folium.Map(
        location=((AREA[1] + AREA[3]) / 2, (AREA[0] + AREA[2]) / 2),
        zoom_start=15,
        # OpenStreetMap rather than one of the CartoDB styles: those now need an API key, and
        # folium still offers them, so the map renders with a warning and no basemap.
        tiles="OpenStreetMap",
    )
    # Volume spans orders of magnitude, so shade on a log scale or every road but the
    # busiest looks the same.
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
# MAGIC **Join to your own network.** `osm_id` links to OpenStreetMap and `gers_id` to Overture
# MAGIC GERS, so data already matched to either needs no further matching. `openlr` is there for
# MAGIC when it is matched to neither.
# MAGIC
# MAGIC **Combine with Traffic Stats.** Volume is exposure and speed is severity. Together they
# MAGIC make a far better risk surface than either alone. TomTom Traffic Stats is a separate
# MAGIC Marketplace listing covering the same London box, and both datasets carry `h3_r9`, so
# MAGIC the two join without any map matching.
# MAGIC
# MAGIC Because they are separate listings they arrive as separate catalogs. The query below
# MAGIC assumes the suggested names, `TomTom_Traffic_Volumes` and `TomTom_Traffic_Stats`;
# MAGIC substitute whatever you called them.
# MAGIC
# MAGIC Total each side to one row per cell **before** joining. Joining first multiplies every
# MAGIC volume row by the 21 hourly rows behind each segment, and the totals come out in the
# MAGIC billions:
# MAGIC
# MAGIC ```sql
# MAGIC WITH volume AS (
# MAGIC   SELECT h3_r9, sum(aadt) AS total_aadt
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
# MAGIC SELECT v.h3_r9, v.total_aadt, round(s.peak_speed_kph) AS peak_speed_kph
# MAGIC FROM volume v JOIN speed s ON s.h3_r9 = v.h3_r9
# MAGIC ORDER BY v.total_aadt DESC
# MAGIC LIMIT 20
# MAGIC ```
# MAGIC
# MAGIC A cell that is busy and slow is a different risk from one that is busy and fast. The
# MAGIC busiest cells in central London come back at 14 to 22 km/h in the morning peak.
# MAGIC
# MAGIC **Compare vintages.** Two years on the same segment identifiers turns growth into a
# MAGIC subtraction. Check `coverage` for both years first, so you do not read better coverage
# MAGIC as more traffic.
# MAGIC
# MAGIC **Remember what is not here.** Every row has an estimate, so nothing needs filtering
# MAGIC out, but the segments with no estimate are absent rather than zero. Any statement about
# MAGIC a whole network has to come from `coverage`, not from counting rows in this table.
