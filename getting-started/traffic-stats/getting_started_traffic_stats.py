# Databricks notebook source
# MAGIC %md
# MAGIC # Getting started with TomTom Traffic Stats Batch
# MAGIC
# MAGIC Historical hourly speed statistics for individual road segments, measured from
# MAGIC anonymised probe vehicles. This notebook goes from a share you just attached to a
# MAGIC segment level risk score you can join your own data against.
# MAGIC
# MAGIC | Table | One row per | Carries |
# MAGIC |---|---|---|
# MAGIC | `traffic_stats_batch.segments` | directional road segment | road class, speed limit, geometry, time zone |
# MAGIC | `traffic_stats_batch.hourly_stats` | segment, date and hour | mean, harmonic mean, median, standard deviation and 19 speed percentiles |
# MAGIC
# MAGIC They join on `dseg_id`.
# MAGIC
# MAGIC **TomTom Traffic Volumes** is a separate listing, and it is the other half of most
# MAGIC risk questions: how much traffic each road carries, rather than how fast it moves.
# MAGIC Take it too and the last section here joins the two.
# MAGIC
# MAGIC Three things to know before you start.
# MAGIC
# MAGIC 1. Speeds are km/h.
# MAGIC 2. Hours are UTC. Use `segments.time_zone` before you say anything about rush hour.
# MAGIC 3. `speed_percentiles_kph` is null for roughly one row in six, where too few vehicles
# MAGIC    were seen in that hour to publish a distribution. The mean is still there. Filter
# MAGIC    rather than assume.

# COMMAND ----------

# The default is what Marketplace suggests when you install this listing, so accepting the
# suggested name means this notebook runs unedited. Change the widget if you named it something
# else. Capitalisation does not matter: Unity Catalog resolves identifiers case insensitively.
dbutils.widgets.text("catalog", "TomTom_Traffic_Stats", "Catalog you attached the share as")
catalog = dbutils.widgets.get("catalog")

try:
    # Checked through SQL rather than `spark.catalog.databaseExists`, so the check resolves the
    # name exactly the way every query below it will. Better to say this now than to let the
    # first real query fail with a table-not-found several cells later.
    spark.sql(f"DESCRIBE SCHEMA {catalog}.traffic_stats_batch")
except Exception as error:
    raise ValueError(
        f"Cannot read {catalog}.traffic_stats_batch. The error was: {error}. "
        f"If that is the name being wrong rather than a permission problem, set the `catalog` "
        f"widget at the top of this notebook to the catalog you accepted when you installed "
        f"the listing. Run SHOW CATALOGS if you are not sure what it was called."
    ) from None

segments = f"{catalog}.traffic_stats_batch.segments"
hourly = f"{catalog}.traffic_stats_batch.hourly_stats"

# `hourly_stats` is partitioned by `observation_date` and holds 322 million rows across a
# complete week. Reading all of it to demonstrate a scoring method is waste on both sides, so
# this notebook works one day at a time and the day is a widget.
#
# Wednesday 2025-09-03 by default: an ordinary midweek day, which is the usual baseline for
# traffic work. Widen it to 2025-09-01 and 2025-09-07 for the whole week, but read the note
# under the risk score first, because pooling weekdays with the weekend changes what the score
# means rather than just making it more accurate.
dbutils.widgets.text("date_from", "2025-09-03", "First observation date")
dbutils.widgets.text("date_to", "2025-09-03", "Last observation date")
date_from = dbutils.widgets.get("date_from")
date_to = dbutils.widgets.get("date_to")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is in the share
# MAGIC
# MAGIC Every column carries a description. `DESCRIBE TABLE` is worth a minute here, because
# MAGIC several fields have an edge that is cheaper to learn now than inside a model.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT count(*) AS segments, round(sum(length_m) / 1000) AS network_km,
               count(DISTINCT country_iso3) AS countries, min(min_lat) AS south,
               max(max_lat) AS north, min(min_lon) AS west, max(max_lon) AS east
        FROM {segments}
        """
    )
)

display(
    spark.sql(
        f"""
        SELECT min(observation_date) AS first_date, max(observation_date) AS last_date,
               count(DISTINCT observation_date) AS days, count(*) AS hourly_rows,
               round(100.0 * avg(CASE WHEN speed_percentiles_kph IS NULL THEN 1 ELSE 0 END), 1)
                   AS pct_rows_without_percentiles
        FROM {hourly}
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Road class decides almost everything
# MAGIC
# MAGIC Functional Road Class runs from 0, a motorway, to 7, a quiet residential street. Most
# MAGIC segments in any extract are minor roads, and most traffic is on the few major ones. An
# MAGIC average taken across segments therefore describes a quiet suburb, not a city.
# MAGIC
# MAGIC Coverage matters just as much. A segment exists in the map whether or not a probe
# MAGIC vehicle ever drove it, and coverage follows traffic, so it falls away on small roads.

# COMMAND ----------

display(
    spark.sql(
        f"""
        WITH observed AS (
            SELECT dseg_id, count(*) AS hours FROM {hourly} GROUP BY dseg_id
        )
        SELECT
            s.frc,
            count(*)                                        AS segments,
            round(sum(s.length_m) / 1000)                   AS network_km,
            round(avg(s.speed_limit_kph), 1)                AS avg_speed_limit_kph,
            round(100.0 * count(o.dseg_id) / count(*), 1)   AS pct_with_measurements,
            round(avg(o.hours), 1)                          AS avg_hours_measured
        FROM {segments} s
        LEFT JOIN observed o USING (dseg_id)
        GROUP BY s.frc
        ORDER BY s.frc
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Speed through the day, in local time
# MAGIC
# MAGIC `from_utc_timestamp` plus `segments.time_zone` puts the measurements in the hour a
# MAGIC driver would recognise. Skip that step and the morning peak lands in the wrong bucket.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            hour(from_utc_timestamp(
                make_timestamp(year(h.observation_date), month(h.observation_date),
                               day(h.observation_date), h.hour_utc, 0, 0),
                s.time_zone))                                           AS hour_local,
            round(avg(h.harmonic_speed_kph), 1)                         AS avg_speed_kph,
            round(100.0 * avg(h.harmonic_speed_kph / s.speed_limit_kph), 1)
                                                                        AS pct_of_speed_limit
        FROM {hourly} h
        JOIN {segments} s USING (dseg_id)
        WHERE s.frc <= 4 AND s.speed_limit_kph > 0
        GROUP BY hour_local
        ORDER BY hour_local
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Three signals from the percentile array
# MAGIC
# MAGIC The percentiles are what make this more than an average.
# MAGIC
# MAGIC **Speeding** is p85 above the posted limit. Traffic engineers set limits from the 85th
# MAGIC percentile, so this marks roads where speeding is normal rather than occasional.
# MAGIC
# MAGIC **Congestion** is any hour whose harmonic mean falls below 60% of the limit. Congestion
# MAGIC drives how often claims happen: low speeds, dense traffic, more contact.
# MAGIC
# MAGIC **Variability** is the standard deviation over the mean. Stop and go traffic on a road
# MAGIC with an ordinary average is the pattern behind rear end collisions.
# MAGIC
# MAGIC The array is ascending, 19 values, p5 through p95 in steps of 5, so p85 is the 17th.
# MAGIC `element_at` counts from 1. The `arr[i]` form counts from 0 and would quietly give you
# MAGIC p80.

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMPORARY VIEW segment_metrics AS
    WITH flagged AS (
        SELECT
            h.dseg_id,
            h.harmonic_speed_kph,
            h.stddev_speed_kph,
            s.frc,
            s.length_m,
            CASE WHEN element_at(h.speed_percentiles_kph, 17) > s.speed_limit_kph
                 THEN 1 ELSE 0 END                                  AS speeding,
            CASE WHEN h.harmonic_speed_kph < 0.6 * s.speed_limit_kph
                 THEN 1 ELSE 0 END                                  AS congested
        FROM {hourly} h
        JOIN {segments} s USING (dseg_id)
        WHERE h.observation_date BETWEEN DATE '{date_from}' AND DATE '{date_to}'
          AND s.speed_limit_kph > 0
          AND h.harmonic_speed_kph > 0
          AND h.speed_percentiles_kph IS NOT NULL
    )
    SELECT
        dseg_id,
        any_value(frc)                                              AS frc,
        any_value(length_m)                                         AS length_m,
        count(*)                                                    AS hours_measured,
        round(avg(harmonic_speed_kph), 2)                           AS avg_speed_kph,
        round(avg(stddev_speed_kph) / avg(harmonic_speed_kph), 3)   AS variability,
        round(100.0 * avg(speeding), 1)                             AS speeding_pct,
        round(100.0 * avg(congested), 1)                            AS congestion_pct
    FROM flagged
    GROUP BY dseg_id
    HAVING count(*) >= 24
    """
)

display(
    spark.sql(
        """
        SELECT frc, count(*) AS segments,
               round(avg(speeding_pct), 1)     AS avg_speeding_pct,
               round(avg(congestion_pct), 1)   AS avg_congestion_pct,
               round(avg(variability), 3)      AS avg_variability
        FROM segment_metrics GROUP BY frc ORDER BY frc
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. One score per segment
# MAGIC
# MAGIC Five factors, each scaled to the range 0 to 1 across this extract, then weighted. The
# MAGIC weights are illustrative. What matters is the shape of the join, not these numbers, so
# MAGIC calibrate them against your own loss history before the score means anything. Because
# MAGIC the scaling is relative to this extract, the score ranks segments within it and is not
# MAGIC comparable across releases.
# MAGIC
# MAGIC It is also relative to the **date window** at the top of this notebook, which is one
# MAGIC Wednesday by default: 2.59M segments and 38.4M hourly rows, an eighth of the week and
# MAGIC ample for ranking. Widening it to the full week does not simply sharpen the score. Two
# MAGIC of the five components, `congestion_pct` and `speeding_pct`, are shares of measured
# MAGIC hours, so adding Saturday and Sunday dilutes both on commuter roads and leaves roads
# MAGIC busy at weekends looking comparatively worse. Score weekdays and weekends separately if
# MAGIC you care about either.

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TEMPORARY VIEW segment_risk AS
    WITH span AS (
        SELECT min(avg_speed_kph) AS lo_speed, max(avg_speed_kph) AS hi_speed,
               min(variability) AS lo_var, max(variability) AS hi_var,
               min(congestion_pct) AS lo_cong, max(congestion_pct) AS hi_cong,
               min(speeding_pct) AS lo_speed_pct, max(speeding_pct) AS hi_speed_pct,
               min(hours_measured) AS lo_hours, max(hours_measured) AS hi_hours
        FROM segment_metrics
    )
    SELECT
        m.*,
        round(
              0.25 * (m.avg_speed_kph  - s.lo_speed)     / nullif(s.hi_speed - s.lo_speed, 0)
            + 0.25 * (m.variability    - s.lo_var)       / nullif(s.hi_var - s.lo_var, 0)
            + 0.20 * (m.congestion_pct - s.lo_cong)      / nullif(s.hi_cong - s.lo_cong, 0)
            + 0.15 * (m.speeding_pct   - s.lo_speed_pct) / nullif(s.hi_speed_pct - s.lo_speed_pct, 0)
            + 0.15 * (m.hours_measured - s.lo_hours)     / nullif(s.hi_hours - s.lo_hours, 0)
        , 4) AS risk_score
    FROM segment_metrics m CROSS JOIN span s
    """
)

display(
    spark.sql(
        """
        SELECT dseg_id, frc, avg_speed_kph, variability, speeding_pct, congestion_pct,
               risk_score
        FROM segment_risk ORDER BY risk_score DESC LIMIT 20
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Joining through H3 cells
# MAGIC
# MAGIC A road segment is an awkward thing to join against, because your own data is addresses
# MAGIC and journeys and segment boundaries are arbitrary. `segments.h3_r9` holds the
# MAGIC [H3](https://h3geo.org/) cell of the segment centroid at resolution 9, about 174 m
# MAGIC across. Put the same cell on your own points and the join is one string comparison.
# MAGIC
# MAGIC Weight by length. A 2 km stretch of motorway should not count the same as a 30 m slip
# MAGIC road.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            s.h3_r9,
            count(*)                                                    AS segments,
            round(sum(r.risk_score * r.length_m) / sum(r.length_m), 4)  AS risk_by_length,
            round(sum(r.length_m) / 1000, 2)                            AS network_km
        FROM segment_risk r
        JOIN {segments} s USING (dseg_id)
        GROUP BY s.h3_r9
        HAVING count(*) >= 3
        ORDER BY risk_by_length DESC
        LIMIT 25
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Putting it on a map
# MAGIC
# MAGIC Geometry is a WKT LineString in EPSG:4326. The bounding box columns exist so you can
# MAGIC pick out an area first and parse geometry second, which is the difference between
# MAGIC seconds and minutes. Change `AREA` to somewhere your extract covers.

# COMMAND ----------

# MAGIC %pip install folium shapely

# COMMAND ----------

import folium
import matplotlib.cm as cm
import matplotlib.colors as mcolors
from shapely import wkt

AREA = (-0.25, 51.45, 0.05, 51.58)  # west, south, east, north

# Each segment becomes its own SVG path with its own tooltip, so the map is the one output in
# this notebook that can outgrow what a notebook will render. The whole top fifth inside this
# box is around 26,000 segments, which Databricks truncates, and a truncated map shows nothing
# at all. Raise this if you want more and know your renderer can take it.
MAX_SEGMENTS_ON_MAP = 1500

worst = spark.sql(
    f"""
    SELECT s.geometry_wkt, s.street_name, s.frc, r.risk_score, r.avg_speed_kph,
           r.speeding_pct
    FROM segment_risk r
    JOIN {segments} s USING (dseg_id)
    WHERE s.frc <= 4
      AND s.max_lon >= {AREA[0]} AND s.min_lon <= {AREA[2]}
      AND s.max_lat >= {AREA[1]} AND s.min_lat <= {AREA[3]}
      AND r.risk_score >= (SELECT percentile_approx(risk_score, 0.8) FROM segment_risk)
    ORDER BY r.risk_score DESC
    LIMIT {MAX_SEGMENTS_ON_MAP}
    """
).toPandas()

print(f"{len(worst):,} highest scoring major road segments in that box")

if len(worst):
    chart = folium.Map(
        location=((AREA[1] + AREA[3]) / 2, (AREA[0] + AREA[2]) / 2),
        zoom_start=12,
        # OpenStreetMap rather than one of the CartoDB styles: those now need an API key, and
        # folium still offers them, so the map renders with a warning and no basemap.
        tiles="OpenStreetMap",
    )
    shade = mcolors.Normalize(vmin=worst.risk_score.min(), vmax=worst.risk_score.max())

    for row in worst.itertuples():
        folium.PolyLine(
            [(lat, lon) for lon, lat in wkt.loads(row.geometry_wkt).coords],
            color=mcolors.to_hex(cm.YlOrRd(shade(row.risk_score))),
            weight=3,
            opacity=0.85,
            tooltip=(
                f"{row.street_name or 'unnamed'}, class {row.frc}, "
                f"score {row.risk_score:.2f}, {row.avg_speed_kph:.0f} km/h, "
                f"speeding in {row.speeding_pct:.0f}% of hours"
            ),
        ).add_to(chart)
    display(chart)
else:
    print("Nothing in that box. Widen AREA or check it against the extent above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC **Use the harmonic mean for anything about travel time.** The plain average gives too
# MAGIC much weight to the fastest vehicles and will make a slow road look acceptable.
# MAGIC
# MAGIC **Use the whole distribution.** The percentiles answer questions an average cannot. How
# MAGIC heavy is the tail above the limit. How much wider is the spread at 08:00 than at 14:00.
# MAGIC
# MAGIC **Join through OpenStreetMap.** `segments.osm_way_ids` means journeys already matched to
# MAGIC OpenStreetMap need no second matching step.
# MAGIC
# MAGIC **Split weekdays from weekends.** `observation_date` gives you the day of week, and the
# MAGIC two patterns differ enough that pooling them hides both.
# MAGIC
# MAGIC **Add volume.** Speed says how bad it is, volume says how many people it happens to.
# MAGIC TomTom Traffic Volumes is a separate Marketplace listing covering the same London box,
# MAGIC and both datasets carry `h3_r9`, so the two join without any map matching.
# MAGIC
# MAGIC Because they are separate listings they arrive as separate catalogs. The query below
# MAGIC assumes the suggested names, `TomTom_Traffic_Stats` and `TomTom_Traffic_Volumes`;
# MAGIC substitute whatever you called them.
# MAGIC
# MAGIC Total each side to one row per cell **before** joining. Joining first multiplies every
# MAGIC volume row by the 21 hourly rows behind each segment, and the totals come out in the
# MAGIC billions:
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
# MAGIC   SELECT h3_r9, sum(aadt) AS total_aadt
# MAGIC   FROM TomTom_Traffic_Volumes.traffic_volumes.aadt_segments
# MAGIC   WHERE region = 'london' AND vintage_year = 2025
# MAGIC   GROUP BY h3_r9
# MAGIC )
# MAGIC SELECT v.h3_r9, round(s.peak_speed_kph) AS peak_speed_kph, v.total_aadt
# MAGIC FROM volume v JOIN speed s ON s.h3_r9 = v.h3_r9
# MAGIC ORDER BY v.total_aadt DESC
# MAGIC LIMIT 20
# MAGIC ```
# MAGIC
# MAGIC The busiest cells in central London come back at 14 to 22 km/h in the morning peak.
