# Databricks notebook source
# MAGIC %md
# MAGIC # Territory risk assessment
# MAGIC
# MAGIC Rating geographic areas, and individual routes, for road risk from traffic speed and
# MAGIC volume.
# MAGIC
# MAGIC Two questions, because insurers ask both:
# MAGIC
# MAGIC 1. **Which areas are risky?** Territory rating, at a finer grain than a postcode.
# MAGIC 2. **Which roads does this driver use?** Route rating, from a telematics trace.
# MAGIC
# MAGIC Both need the same two ingredients. **Volume** is exposure: how many vehicles are there
# MAGIC to collide with. **Speed** is severity and behaviour: how fast, how erratic, how often
# MAGIC above the limit. Neither alone is a risk model.
# MAGIC
# MAGIC ## What you need
# MAGIC
# MAGIC Both free samples from Databricks Marketplace, attached as catalogs:
# MAGIC
# MAGIC - **TomTom Traffic Stats** — hourly speeds per road segment
# MAGIC - **TomTom Traffic Volumes** — annual average daily traffic per road segment
# MAGIC
# MAGIC They cover the same four metropolitan areas. The `region` widget below picks one. Each
# MAGIC region is an extract far wider than the city: `london` runs from the Dorset coast to the
# MAGIC Peak District and takes in Birmingham, Bristol and Southampton.

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
# MAGIC ## 1. Territory risk, per H3 cell
# MAGIC
# MAGIC Both datasets use `h3_r9`, an H3 cell at resolution 9. Each cell is about 0.1 km², or a
# MAGIC few city blocks. We join on this value, so no map matching or spatial index is needed.
# MAGIC It is more useful than a postcode, which can include both a motorway and a cul-de-sac.
# MAGIC
# MAGIC We use five factors: four from speed and one from volume.
# MAGIC
# MAGIC | Factor | Source | Why it matters |
# MAGIC |---|---|---|
# MAGIC | Vehicle-km per day | volumes | exposure. `aadt` times `length_m`, so a road drawn as ten segments counts once |
# MAGIC | Mean speed | speeds | severity. Energy in a collision goes with the square of speed |
# MAGIC | Variability | speeds | stop-and-go traffic, the pattern behind rear-end collisions |
# MAGIC | Speeding rate | speeds | p85 above the limit, so speeding is normal rather than rare |
# MAGIC | Congestion rate | speeds | hours below 60% of the limit: dense, slow, frequent contact |
# MAGIC
# MAGIC Each factor is scaled from 0 to 1 across the cells in this extract, then weighted.
# MAGIC **The weights are illustrative.** Calibrate them with your claims history. The scaling is
# MAGIC relative to this extract, so scores rank cells within it and cannot be compared across
# MAGIC releases.
# MAGIC
# MAGIC Hours with a single observation carry no percentile array. They count towards mean speed
# MAGIC and congestion; the speeding rate is taken over the hours that have a distribution, and
# MAGIC `single_observation_pct` is kept beside the score as a measure of how thin the evidence is.
# MAGIC
# MAGIC The default score uses one date. `hourly_stats` is partitioned by `observation_date`, so
# MAGIC one date reads one partition instead of the whole week.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW territory_risk AS
    WITH severity AS (
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
    ),
    exposure AS (
        SELECT h3_r9, round(sum(aadt * length_m) / 1000) AS vehicle_km_per_day
        FROM {volumes}.aadt_segments
        WHERE region = '{region}' AND vintage_year = {vintage_year}
        GROUP BY h3_r9
    ),
    cells AS (
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
    )
    SELECT
        c.h3_r9,
        c.vehicle_km_per_day,
        round(c.mean_speed_kph, 1)                AS mean_speed_kph,
        round(c.variability, 3)                   AS variability,
        round(100 * c.speeding_rate, 1)           AS speeding_pct,
        round(100 * c.congestion_rate, 1)         AS congestion_pct,
        round(100 * c.single_observation_rate, 1) AS single_observation_pct,
        round(
              0.35 * (c.vehicle_km_per_day - b.lo_e) / nullif(b.hi_e - b.lo_e, 0)
            + 0.20 * (c.mean_speed_kph  - b.lo_s) / nullif(b.hi_s - b.lo_s, 0)
            + 0.20 * (c.variability     - b.lo_v) / nullif(b.hi_v - b.lo_v, 0)
            + 0.15 * (c.speeding_rate   - b.lo_p) / nullif(b.hi_p - b.lo_p, 0)
            + 0.10 * (c.congestion_rate - b.lo_c) / nullif(b.hi_c - b.lo_c, 0)
        , 3) AS risk_score
    FROM cells c CROSS JOIN bounds b
    """)

display(spark.sql("SELECT * FROM territory_risk ORDER BY risk_score DESC LIMIT 15"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Review the top rows before trusting the score
# MAGIC
# MAGIC Two very different types of cell can score highly:
# MAGIC
# MAGIC - **Dense and slow.** Central London cells carry the most vehicle-kilometres at speeds of
# MAGIC   15 to 25 km/h. Exposure is high, but each event is less severe.
# MAGIC - **Fast and free-flowing.** Motorway cells run above 100 km/h. Their 85th percentile is
# MAGIC   above the limit in almost every hour, with very little congestion. Exposure per cell is
# MAGIC   lower, but each event is more severe.
# MAGIC
# MAGIC Claims frequency and severity models use different weights. In practice, score these separately
# MAGIC instead of combining them into one number. Keeping the component columns makes this possible.
# MAGIC
# MAGIC The breakdown below uses the full extract, not just the top rows. The pattern is the same:
# MAGIC motorway cells speed twice as often as urban cells and congest twenty times less. The
# MAGIC scatter draws a random sample of cells; the two types sit at opposite ends of the speed
# MAGIC axis.

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

import plotly.express as px

sample = spark.sql("""
        SELECT vehicle_km_per_day, mean_speed_kph, speeding_pct, congestion_pct, risk_score
        FROM territory_risk WHERE vehicle_km_per_day > 0 ORDER BY rand() LIMIT 5000
        """).toPandas()

px.scatter(sample, x="mean_speed_kph", y="vehicle_km_per_day", color="risk_score", log_y=True, opacity=0.6,
           color_continuous_scale="YlOrRd", hover_data=["speeding_pct", "congestion_pct"], template="plotly_white",
           labels=dict(mean_speed_kph="mean speed, km/h", vehicle_km_per_day="vehicle-km per day", risk_score="score"),
           title=f"5,000 sampled cells, {region}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The map
# MAGIC
# MAGIC Resolution 9 is useful for scoring but too detailed for the map. This extract has
# MAGIC 224,000 cells at about 0.1 km2 each. The notebook can only render a few hundred
# MAGIC polygons. Resolution 6 uses cells of about 36 km2 and covers the extract with about
# MAGIC 2,000 hexagons.
# MAGIC
# MAGIC Exposure weights the scores by traffic. This shows the risk vehicles actually face
# MAGIC instead of treating an empty lane like a motorway.

# COMMAND ----------

# MAGIC %pip install folium==0.20.0 h3==4.5.0

# COMMAND ----------

import folium
import matplotlib.cm as cm
import matplotlib.colors as mcolors
from h3 import cell_to_boundary, cell_to_latlng

MAP_RESOLUTION = 6

area = spark.sql(f"""
    SELECT h3_toparent(h3_r9, {MAP_RESOLUTION})                                        AS cell,
           sum(vehicle_km_per_day)                                                     AS vehicle_km_per_day,
           round(sum(risk_score * vehicle_km_per_day) / sum(vehicle_km_per_day), 3)     AS risk_score,
           round(sum(mean_speed_kph * vehicle_km_per_day) / sum(vehicle_km_per_day), 1) AS mean_speed_kph,
           round(sum(speeding_pct * vehicle_km_per_day) / sum(vehicle_km_per_day), 1)   AS speeding_pct,
           count(*)                                                                     AS cells_r9
    FROM territory_risk
    GROUP BY 1
    """).toPandas()

print(
    f"{len(area):,} cells at resolution {MAP_RESOLUTION}, "
    f"rolled up from {area.cells_r9.sum():,} scored cells"
)

centres = [cell_to_latlng(c) for c in area.cell]
chart = folium.Map(
    location=[
        sum(c[0] for c in centres) / len(centres),
        sum(c[1] for c in centres) / len(centres),
    ],
    zoom_start=9,
    tiles="OpenStreetMap",
)
chart.get_root().header.add_child(
    folium.Element(
        "<style>.leaflet-tile-pane{filter:grayscale(1) contrast(0.92) brightness(1.06);}</style>"
    )
)

# Shade by rank instead of score. Exposure is skewed, so a linear scale would leave most cells
# with little colour.
rank = area.risk_score.rank(pct=True)

for row, shade in zip(area.itertuples(), rank):
    folium.Polygon(
        locations=cell_to_boundary(row.cell),
        color=None,
        fill=True,
        fill_color=mcolors.to_hex(cm.YlOrRd(shade)),
        fill_opacity=0.55,
        tooltip=(
            f"score {row.risk_score:.2f}, {row.vehicle_km_per_day:,.0f} vehicle-km/day, "
            f"{row.mean_speed_kph:.0f} km/h, speeding in {row.speeding_pct:.0f}% of hours"
        ),
    ).add_to(chart)

display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Route risk, from a telematics trace
# MAGIC
# MAGIC Territory rating asks where a driver lives. Route rating asks which roads they drive.
# MAGIC Telematics makes this possible.
# MAGIC
# MAGIC Raw GPS is not enough. Points can be 10 to 50 metres off the road and do not identify the
# MAGIC road. **Map matching** links the trace to road segments. [Fast Map Matching](https://github.com/cyang-kth/fmm)
# MAGIC is an open source tool that outputs OpenStreetMap way IDs.
# MAGIC
# MAGIC These IDs provide the join. `segments.osm_way_ids` lists the OpenStreetMap ways for each
# MAGIC TomTom segment. The matched output joins with `arrays_overlap`, so no second matching step
# MAGIC is needed:
# MAGIC
# MAGIC ```
# MAGIC GPS trace -> map matcher -> OSM way IDs -> segments.osm_way_ids -> hourly speeds
# MAGIC ```
# MAGIC
# MAGIC The example uses two ways from the M20 motorway in Kent. In production, the matcher
# MAGIC provides one list per trip.

# COMMAND ----------

MATCHED_OSM_WAYS = [4394118, 4394117]

display(spark.sql(f"""
        WITH on_route AS (
            SELECT s.dseg_id, s.street_name, s.speed_limit_kph, s.length_m
            FROM {stats}.segments s
            WHERE arrays_overlap(s.osm_way_ids, array({", ".join(f"{w}L" for w in MATCHED_OSM_WAYS)}))
              AND s.region = '{region}'
              AND s.speed_limit_kph > 0
        ),
        per_segment AS (
            SELECT
                r.dseg_id,
                any_value(r.length_m)     AS length_m,
                avg(h.harmonic_speed_kph) AS mean_speed_kph,
                avg(CASE WHEN h.speed_percentiles_kph IS NULL THEN NULL
                         WHEN element_at(h.speed_percentiles_kph, 17) > r.speed_limit_kph
                         THEN 1.0 ELSE 0.0 END) AS speeding_rate,
                avg(CASE WHEN h.harmonic_speed_kph < 0.6 * r.speed_limit_kph
                         THEN 1.0 ELSE 0.0 END) AS congestion_rate,
                avg(CASE WHEN h.speed_percentiles_kph IS NULL
                         THEN 1.0 ELSE 0.0 END) AS single_observation_rate
            FROM on_route r
            JOIN {stats}.hourly_stats h USING (dseg_id)
            WHERE h.observation_date = DATE '{observation_date}'
              AND h.harmonic_speed_kph > 0
            GROUP BY r.dseg_id
            HAVING count(h.speed_percentiles_kph) > 0
        )
        SELECT
            count(*)                                       AS segments_on_route,
            round(sum(length_m) / 1000, 1)                 AS route_km,
            round(avg(mean_speed_kph), 1)                  AS mean_speed_kph,
            round(100 * avg(speeding_rate), 1)             AS pct_hours_speeding,
            round(100 * avg(congestion_rate), 1)           AS pct_hours_congested,
            round(100 * avg(single_observation_rate), 1)   AS pct_hours_single_observation,
            round(100 * sum(length_m * speeding_rate) / sum(length_m), 1)
                                                           AS pct_speeding_by_length
        FROM per_segment
        """))

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
# MAGIC - **Weight the route by time, not distance.** A congested kilometre has more exposure than
# MAGIC   a clear one. Use `aadt_by_day_hour` to apply hourly weights.
# MAGIC - **Join the two datasets on OpenStreetMap as well as H3.** `segments.osm_way_ids` is an
# MAGIC   array; in Traffic Volumes the way IDs sit inside `osm_id` as `way:length:offset` triples,
# MAGIC   so `cast(split(triple, ':')[0] AS BIGINT)` after `explode(split(osm_id, ','))` gives the
# MAGIC   same key.
# MAGIC - **Score frequency and severity separately.** Section 2 shows why these factors need
# MAGIC   separate scores.
# MAGIC - **Check coverage first.** `traffic_volumes.coverage` shows the share of each road class
# MAGIC   with an estimate. Segments without an estimate are missing, not zero. Many minor roads
# MAGIC   have no estimate.
# MAGIC - **Separate weekdays and weekends.** `observation_date` shows the day of week. Combining
# MAGIC   the two patterns can hide important differences.
