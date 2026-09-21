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
# MAGIC above the limit. Neither alone is a risk model, so the story runs exposure, then
# MAGIC severity, then the two combined, then the same recipe on one route.
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

# MAGIC %pip install folium==0.20.0 h3==4.5.0

# COMMAND ----------

import folium
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import plotly.express as px
from h3 import cell_to_boundary, cell_to_latlng

px.defaults.template = "plotly_white"

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
# MAGIC A road drawn as ten segments counts once, and a cell with a motorway and three side
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
               round(avg(mean_speed_kph), 1)              AS mean_speed_kph,
               round(avg(variability), 3)                 AS mean_variability,
               round(100 * avg(speeding_rate), 1)         AS mean_speeding_pct,
               round(100 * avg(congestion_rate), 1)       AS mean_congestion_pct,
               round(100 * avg(single_observation_rate), 1) AS mean_single_observation_pct
        FROM severity
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. One score per cell
# MAGIC
# MAGIC Each factor is scaled from 0 to 1 across the cells in this extract, weighted, and summed.
# MAGIC Exposure takes the largest weight because a collision needs traffic before it needs
# MAGIC speed. **The weights are illustrative.** Calibrate them with your claims history. The
# MAGIC scaling is relative to this extract, so scores rank cells within it and cannot be compared
# MAGIC across releases.
# MAGIC
# MAGIC The weighted contributions stay as columns, so the chart can show what lifts each of the
# MAGIC top cells: mostly exposure for the urban ones, mostly speed and speeding for the
# MAGIC motorway ones.

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
           round(exposure_part, 3) AS exposure_part, round(speed_part, 3) AS speed_part,
           round(variability_part, 3) AS variability_part, round(speeding_part, 3) AS speeding_part,
           round(congestion_part, 3) AS congestion_part
    FROM weighted
    """)

top = spark.sql("SELECT * FROM territory_risk ORDER BY risk_score DESC LIMIT 15").toPandas()

display(top.iloc[:, :8])
px.bar(top.melt(["h3_r9", "mean_speed_kph"], [c for c in top.columns if c.endswith("_part")], "factor", "contribution"),
       x="h3_r9", y="contribution", color="factor", hover_data=["mean_speed_kph"],
       labels=dict(h3_r9="cell", contribution="weighted contribution to the score", factor=""),
       title=f"What lifts the 15 highest-scoring cells, {region}").update_xaxes(categoryorder="total descending").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Two kinds of risky cell
# MAGIC
# MAGIC Two very different types of cell score highly:
# MAGIC
# MAGIC - **Dense and slow.** Central cells carry the most vehicle-kilometres at 15 to 25 km/h.
# MAGIC   Exposure is high, but each event is less severe.
# MAGIC - **Fast and free-flowing.** Motorway cells run above 100 km/h. Their 85th percentile is
# MAGIC   above the limit in almost every hour, with very little congestion. Exposure per cell is
# MAGIC   lower, but each event is more severe.
# MAGIC
# MAGIC Claims frequency and claims severity models weight these differently, so in practice score
# MAGIC them separately rather than as one number. Keeping the factor columns makes that possible.
# MAGIC
# MAGIC The breakdown uses the full extract: motorway cells speed about twice as often as urban
# MAGIC cells and congest twenty times less. The scatter samples 5,000 cells, with the score as
# MAGIC colour, and the two types sit at opposite ends of the speed axis.

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

sample = spark.sql("""
        SELECT vehicle_km_per_day, mean_speed_kph, speeding_pct, congestion_pct, risk_score
        FROM territory_risk WHERE vehicle_km_per_day > 0 ORDER BY rand() LIMIT 5000
        """).toPandas()

px.scatter(sample, x="mean_speed_kph", y="vehicle_km_per_day", color="risk_score", log_y=True, opacity=0.6,
           color_continuous_scale="YlOrRd", hover_data=["speeding_pct", "congestion_pct"],
           labels=dict(mean_speed_kph="mean speed, km/h", vehicle_km_per_day="vehicle-km per day", risk_score="score"),
           title=f"Exposure against speed for 5,000 sampled cells, {region}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The map
# MAGIC
# MAGIC Resolution 9 is right for scoring and too fine for a map: this extract has about 224,000
# MAGIC scored cells, and the notebook renders a few hundred polygons comfortably. Resolution 6
# MAGIC cells are about 36 km² and cover the extract with about 2,000 hexagons.
# MAGIC
# MAGIC Each hexagon takes the exposure-weighted mean of its children's scores, so it shows the
# MAGIC risk vehicles actually face rather than treating an empty lane like a motorway. Colour is
# MAGIC by rank, because exposure is skewed and a linear scale would leave most cells pale.

# COMMAND ----------

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
# MAGIC provides one list per trip. The table summarises the route for the day; the chart is the
# MAGIC route's speed profile by local hour, with the p85 line showing how far above the limit the
# MAGIC faster traffic runs.

# COMMAND ----------

MATCHED_OSM_WAYS = [4394118, 4394117]

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW route_hours AS
    SELECT s.dseg_id, s.length_m, s.speed_limit_kph, h.harmonic_speed_kph, h.speed_percentiles_kph,
           hour(from_utc_timestamp(make_timestamp(year(h.observation_date), month(h.observation_date),
                                                  day(h.observation_date), h.hour_utc, 0, 0), s.time_zone)) AS hour_local,
           CASE WHEN h.speed_percentiles_kph IS NULL THEN NULL
                WHEN element_at(h.speed_percentiles_kph, 17) > s.speed_limit_kph THEN 1.0 ELSE 0.0 END AS speeding,
           CASE WHEN h.harmonic_speed_kph < 0.6 * s.speed_limit_kph THEN 1.0 ELSE 0.0 END               AS congested,
           CASE WHEN h.speed_percentiles_kph IS NULL THEN 1.0 ELSE 0.0 END                             AS single_observation
    FROM {stats}.segments s
    JOIN {stats}.hourly_stats h USING (dseg_id)
    WHERE arrays_overlap(s.osm_way_ids, array({", ".join(f"{w}L" for w in MATCHED_OSM_WAYS)}))
      AND s.region = '{region}'
      AND s.speed_limit_kph > 0
      AND h.observation_date = DATE '{observation_date}'
      AND h.harmonic_speed_kph > 0
    """)

display(spark.sql("""
        WITH per_segment AS (
            SELECT dseg_id, any_value(length_m) AS length_m, avg(harmonic_speed_kph) AS mean_speed_kph,
                   avg(speeding) AS speeding_rate, avg(congested) AS congestion_rate,
                   avg(single_observation) AS single_observation_rate
            FROM route_hours GROUP BY dseg_id HAVING count(speed_percentiles_kph) > 0
        )
        SELECT count(*)                                     AS segments_on_route,
               round(sum(length_m) / 1000, 1)               AS route_km,
               round(avg(mean_speed_kph), 1)                AS mean_speed_kph,
               round(100 * avg(speeding_rate), 1)           AS pct_hours_speeding,
               round(100 * avg(congestion_rate), 1)         AS pct_hours_congested,
               round(100 * avg(single_observation_rate), 1) AS pct_hours_single_observation,
               round(100 * sum(length_m * speeding_rate) / sum(length_m), 1) AS pct_speeding_by_length
        FROM per_segment
        """))

route_day = spark.sql("""
        SELECT hour_local,
               round(avg(harmonic_speed_kph), 1)                      AS `harmonic mean`,
               round(avg(element_at(speed_percentiles_kph, 17)), 1)   AS `p85`,
               round(avg(speed_limit_kph), 1)                         AS `speed limit`
        FROM route_hours GROUP BY hour_local ORDER BY hour_local
        """).toPandas()

px.line(route_day.melt("hour_local", var_name="line", value_name="kph"), x="hour_local", y="kph", color="line",
        markers=True, line_dash="line", line_dash_map={"speed limit": "dash"},
        labels=dict(hour_local="local hour", kph="km/h", line=""),
        title=f"The route through the day, {observation_date}").show()

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
