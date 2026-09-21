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
# 2025-09-01 and 2025-09-07 to process the full week. See the risk score note first:
# combining weekdays and weekends changes what the score means.
dbutils.widgets.text("date_from", "2025-09-03", "First observation date")
dbutils.widgets.text("date_to", "2025-09-03", "Last observation date")
date_from = dbutils.widgets.get("date_from")
date_to = dbutils.widgets.get("date_to")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is in the database
# MAGIC
# MAGIC Every column carries a description. `DESCRIBE TABLE` is worth a minute here, because
# MAGIC several fields have an edge that is cheaper to learn now than inside a model.

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

display(spark.sql(f"""
        SELECT hour_utc,
               count(*) AS hourly_rows,
               round(100.0 * avg(CASE WHEN speed_percentiles_kph IS NULL THEN 1 ELSE 0 END), 1)
                   AS pct_single_observation
        FROM {hourly}
        GROUP BY hour_utc
        ORDER BY hour_utc
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Road class matters most
# MAGIC
# MAGIC Functional Road Class goes from 0 (motorway) to 7 (quiet residential street). Most
# MAGIC segments are small roads, but most traffic uses the few major roads. An average across
# MAGIC all segments therefore describes a quiet suburb, not a city.
# MAGIC
# MAGIC Coverage also matters. A road can be on the map even if no probe vehicle used it. Since
# MAGIC traffic is lower on small roads, they usually have less coverage.

# COMMAND ----------

display(spark.sql(f"""
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
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Daily speed by local time
# MAGIC
# MAGIC `from_utc_timestamp` and `segments.time_zone` convert measurements to local time.
# MAGIC
# MAGIC Speed by hour is read **within each road class**. The set of segments that report changes
# MAGIC through the day: at night the mix tilts towards motorways and major roads, by day towards
# MAGIC local streets. The chart has one line per class; the table reads speed as a share of the
# MAGIC posted limit, which compares across classes, next to the share of rows on classes 0 to 2
# MAGIC in that hour.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW local_hours AS
    SELECT
        hour(from_utc_timestamp(
            make_timestamp(year(h.observation_date), month(h.observation_date),
                           day(h.observation_date), h.hour_utc, 0, 0),
            s.time_zone))                                               AS hour_local,
        s.frc,
        h.harmonic_speed_kph,
        h.harmonic_speed_kph / s.speed_limit_kph                        AS share_of_limit
    FROM {hourly} h
    JOIN {segments} s USING (dseg_id)
    WHERE s.frc <= 4 AND s.speed_limit_kph > 0
    """)

import plotly.express as px

px.defaults.template = "plotly_white"

by_hour = spark.sql("""
        SELECT hour_local, cast(frc AS STRING) AS frc, round(avg(harmonic_speed_kph), 1) AS speed_kph
        FROM local_hours GROUP BY 1, 2 ORDER BY 1, 2
        """).toPandas()

px.line(by_hour, x="hour_local", y="speed_kph", color="frc", markers=True,
        labels=dict(hour_local="local hour", speed_kph="harmonic speed, km/h", frc="class"),
        title=f"Speed by local hour and road class, {region}").show()

display(spark.sql("""
        SELECT hour_local,
               round(100.0 * avg(share_of_limit), 1)                    AS pct_of_speed_limit,
               round(100.0 * avg(CASE WHEN frc <= 2 THEN 1 ELSE 0 END), 1)
                                                                        AS pct_rows_on_frc_0_to_2
        FROM local_hours
        GROUP BY hour_local
        ORDER BY hour_local
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Three signals from the percentile array
# MAGIC
# MAGIC Percentiles show more than the average speed.
# MAGIC
# MAGIC **Speeding** means p85 is above the posted limit. This shows where speeding is common.
# MAGIC
# MAGIC **Congestion** means the harmonic mean is below 60% of the limit. Low speeds and dense
# MAGIC traffic can lead to more crashes.
# MAGIC
# MAGIC **Variability** is the standard deviation divided by the mean. It shows stop-and-go traffic,
# MAGIC even when the average speed looks normal.
# MAGIC
# MAGIC The array has 19 values, from p5 to p95 in steps of 5. p85 is the 17th value.
# MAGIC `element_at` starts counting at 1. The `arr[i]` form starts at 0 and would return p80.
# MAGIC
# MAGIC Hours with a single observation have no array. They stay in the view: the mean counts
# MAGIC towards speed and congestion, the speeding rate is taken over the hours that have a
# MAGIC distribution, and `single_observation_pct` says how much of each segment rests on one
# MAGIC vehicle.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW segment_metrics AS
    WITH flagged AS (
        SELECT
            h.dseg_id,
            h.harmonic_speed_kph,
            h.stddev_speed_kph,
            s.frc,
            s.length_m,
            CASE WHEN h.speed_percentiles_kph IS NULL THEN NULL
                 WHEN element_at(h.speed_percentiles_kph, 17) > s.speed_limit_kph
                 THEN 1 ELSE 0 END                                  AS speeding,
            CASE WHEN h.harmonic_speed_kph < 0.6 * s.speed_limit_kph
                 THEN 1 ELSE 0 END                                  AS congested,
            CASE WHEN h.speed_percentiles_kph IS NULL THEN 1 ELSE 0 END
                                                                    AS single_observation
        FROM {hourly} h
        JOIN {segments} s USING (dseg_id)
        WHERE h.observation_date BETWEEN DATE '{date_from}' AND DATE '{date_to}'
          AND s.speed_limit_kph > 0
          AND h.harmonic_speed_kph > 0
    )
    SELECT
        dseg_id,
        any_value(frc)                                              AS frc,
        any_value(length_m)                                         AS length_m,
        count(*)                                                    AS hours_measured,
        round(avg(harmonic_speed_kph), 2)                           AS avg_speed_kph,
        round(avg(stddev_speed_kph) / avg(harmonic_speed_kph), 3)   AS variability,
        round(100.0 * avg(speeding), 1)                             AS speeding_pct,
        round(100.0 * avg(congested), 1)                            AS congestion_pct,
        round(100.0 * avg(single_observation), 1)                   AS single_observation_pct
    FROM flagged
    GROUP BY dseg_id
    HAVING count(*) >= 24 AND count(speeding) > 0
    """)

display(spark.sql("""
        SELECT frc, count(*) AS segments,
               round(avg(speeding_pct), 1)            AS avg_speeding_pct,
               round(avg(congestion_pct), 1)          AS avg_congestion_pct,
               round(avg(variability), 3)             AS avg_variability,
               round(avg(single_observation_pct), 1)  AS avg_single_observation_pct
        FROM segment_metrics GROUP BY frc ORDER BY frc
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. One score per segment
# MAGIC
# MAGIC Five factors are scaled from 0 to 1 across this extract and then weighted. The weights
# MAGIC are examples. Calibrate them with your own loss history before using the score. Because
# MAGIC the scaling is relative to this extract, the score ranks segments only within this extract
# MAGIC and cannot be compared across releases.
# MAGIC
# MAGIC A factor with no variation adds no information, so it contributes nothing to the score.
# MAGIC The `coalesce` around each term handles this case. For one day, every segment has exactly
# MAGIC 24 measured hours, so `hours_measured` is constant and drops out.
# MAGIC
# MAGIC `single_observation_pct` is kept beside the score rather than inside it. A segment whose
# MAGIC hours mostly rest on one vehicle deserves a wider confidence band, not a different rank.
# MAGIC
# MAGIC The score also depends on the **date window** at the top of this notebook. By default,
# MAGIC it covers one Wednesday: 2.59M segments and 38.4M hourly rows. This is enough data for
# MAGIC ranking. A full week does not necessarily improve the score. `congestion_pct` and
# MAGIC `speeding_pct` are based on measured hours, so adding Saturday and Sunday dilutes them
# MAGIC on commuter roads. Roads that are busy on weekends may then look worse. Score weekdays
# MAGIC and weekends separately if needed.
# MAGIC
# MAGIC The chart shows how the score spreads within each road class. Section 7 draws the
# MAGIC segments above its 80th percentile.

# COMMAND ----------

spark.sql("""
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
              0.25 * coalesce((m.avg_speed_kph  - s.lo_speed)     / nullif(s.hi_speed - s.lo_speed, 0), 0)
            + 0.25 * coalesce((m.variability    - s.lo_var)       / nullif(s.hi_var - s.lo_var, 0), 0)
            + 0.20 * coalesce((m.congestion_pct - s.lo_cong)      / nullif(s.hi_cong - s.lo_cong, 0), 0)
            + 0.15 * coalesce((m.speeding_pct   - s.lo_speed_pct) / nullif(s.hi_speed_pct - s.lo_speed_pct, 0), 0)
            + 0.15 * coalesce((m.hours_measured - s.lo_hours)     / nullif(s.hi_hours - s.lo_hours, 0), 0)
        , 4) AS risk_score
    FROM segment_metrics m CROSS JOIN span s
    """)

display(spark.sql("""
        SELECT dseg_id, frc, avg_speed_kph, variability, speeding_pct, congestion_pct,
               single_observation_pct, risk_score
        FROM segment_risk ORDER BY risk_score DESC LIMIT 20
        """))

scores = spark.sql("""
        SELECT cast(frc AS STRING) AS frc, round(risk_score, 2) AS risk_score, count(*) AS segments
        FROM segment_risk GROUP BY 1, 2 ORDER BY 1, 2
        """).toPandas()

px.line(scores, x="risk_score", y="segments", color="frc", log_y=True,
        labels=dict(risk_score="risk score", frc="class"),
        title="Segments per risk score, by road class").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Joining on H3 cells
# MAGIC
# MAGIC Road segments can be hard to join with address and journey data. Their boundaries
# MAGIC are arbitrary. `segments.h3_r9` stores the segment centroid's
# MAGIC [H3](https://h3geo.org/) cell at resolution 9, about 174 m across. Add the same cell
# MAGIC to your points, then join on it.
# MAGIC
# MAGIC Weight results by length. A 2 km motorway section should count more than a 30 m slip
# MAGIC road.

# COMMAND ----------

display(spark.sql(f"""
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
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Map the results
# MAGIC
# MAGIC Geometry is a WKT LineString in EPSG:4326. Set `AREA` to a region in your extract.

# COMMAND ----------

# MAGIC %pip install folium==0.20.0 shapely==2.1.2

# COMMAND ----------

import folium
import matplotlib.cm as cm
import matplotlib.colors as mcolors
from shapely import wkt

# Centred on where the region's segments are
centre = spark.sql(
    f"SELECT avg(min_lon) AS lon, avg(min_lat) AS lat FROM {segments}"
).first()
AREA = (centre.lon - 0.15, centre.lat - 0.065, centre.lon + 0.15, centre.lat + 0.065)

# Each segment becomes its own SVG path with its own tooltip, so we limit the rendered segments
MAX_SEGMENTS_ON_MAP = 1500

worst = spark.sql(f"""
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
    """).toPandas()

print(f"{len(worst):,} highest scoring major road segments in that box")

if len(worst):
    chart = folium.Map(
        location=((AREA[1] + AREA[3]) / 2, (AREA[0] + AREA[2]) / 2),
        zoom_start=12,
        tiles="OpenStreetMap",
    )
    chart.get_root().header.add_child(
        folium.Element(
            "<style>.leaflet-tile-pane{filter:grayscale(1) contrast(0.92) brightness(1.06);}"
            "</style>"
        )
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
