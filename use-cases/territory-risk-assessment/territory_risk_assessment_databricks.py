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
# MAGIC They cover the same four metropolitan areas. The `region` widget below picks one.

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
# MAGIC ## 1. Territory risk, per H3 cell
# MAGIC
# MAGIC Both datasets carry `h3_r9`, an H3 cell at resolution 9, roughly 0.1 km² and about the
# MAGIC size of a few city blocks. That is the join: no map matching, no spatial index, one
# MAGIC string comparison. It is also a more useful grain than a postcode, which can span a
# MAGIC motorway and a cul-de-sac.
# MAGIC
# MAGIC Five factors, four from speed and one from volume:
# MAGIC
# MAGIC | Factor | Source | Why it matters |
# MAGIC |---|---|---|
# MAGIC | Total AADT | volumes | exposure. More vehicles, more opportunities for contact |
# MAGIC | Mean speed | speeds | severity. Energy in a collision goes with the square of speed |
# MAGIC | Variability | speeds | stop-and-go traffic, the pattern behind rear-end collisions |
# MAGIC | Speeding rate | speeds | p85 above the limit, so speeding is normal rather than rare |
# MAGIC | Congestion rate | speeds | hours below 60% of the limit: dense, slow, frequent contact |
# MAGIC
# MAGIC Each is scaled to 0–1 across the cells in this extract, then weighted. **The weights are
# MAGIC illustrative.** Calibrate them against your own claims history before the score means
# MAGIC anything, and note that because the scaling is relative to this extract, scores rank
# MAGIC cells within it and are not comparable across releases.
# MAGIC
# MAGIC One date is scored by default. `hourly_stats` is partitioned by `observation_date`, so a
# MAGIC single date reads one partition instead of the whole week.

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMPORARY VIEW territory_risk AS
    WITH severity AS (
        SELECT
            s.h3_r9,
            avg(h.harmonic_speed_kph)                          AS mean_speed_kph,
            avg(h.stddev_speed_kph / h.harmonic_speed_kph)     AS variability,
            avg(CASE WHEN element_at(h.speed_percentiles_kph, 17) > s.speed_limit_kph
                     THEN 1.0 ELSE 0.0 END)                    AS speeding_rate,
            avg(CASE WHEN h.harmonic_speed_kph < 0.6 * s.speed_limit_kph
                     THEN 1.0 ELSE 0.0 END)                    AS congestion_rate
        FROM {stats}.segments s
        JOIN {stats}.hourly_stats h USING (dseg_id)
        WHERE h.observation_date = DATE '{observation_date}'
          AND s.region = '{region}'
          AND s.speed_limit_kph > 0
          AND h.harmonic_speed_kph > 0
          AND h.speed_percentiles_kph IS NOT NULL
        GROUP BY s.h3_r9
    ),
    exposure AS (
        SELECT h3_r9, sum(aadt) AS total_aadt
        FROM {volumes}.aadt_segments
        WHERE region = '{region}' AND vintage_year = {vintage_year}
        GROUP BY h3_r9
    ),
    cells AS (
        SELECT e.h3_r9, e.total_aadt, v.mean_speed_kph, v.variability,
               v.speeding_rate, v.congestion_rate
        FROM exposure e JOIN severity v USING (h3_r9)
    ),
    bounds AS (
        SELECT min(total_aadt) lo_e, max(total_aadt) hi_e,
               min(mean_speed_kph) lo_s, max(mean_speed_kph) hi_s,
               min(variability) lo_v, max(variability) hi_v,
               min(speeding_rate) lo_p, max(speeding_rate) hi_p,
               min(congestion_rate) lo_c, max(congestion_rate) hi_c
        FROM cells
    )
    SELECT
        c.h3_r9,
        c.total_aadt,
        round(c.mean_speed_kph, 1)        AS mean_speed_kph,
        round(c.variability, 3)           AS variability,
        round(100 * c.speeding_rate, 1)   AS speeding_pct,
        round(100 * c.congestion_rate, 1) AS congestion_pct,
        round(
              0.35 * (c.total_aadt      - b.lo_e) / nullif(b.hi_e - b.lo_e, 0)
            + 0.20 * (c.mean_speed_kph  - b.lo_s) / nullif(b.hi_s - b.lo_s, 0)
            + 0.20 * (c.variability     - b.lo_v) / nullif(b.hi_v - b.lo_v, 0)
            + 0.15 * (c.speeding_rate   - b.lo_p) / nullif(b.hi_p - b.lo_p, 0)
            + 0.10 * (c.congestion_rate - b.lo_c) / nullif(b.hi_c - b.lo_c, 0)
        , 3) AS risk_score
    FROM cells c CROSS JOIN bounds b
    """
)

display(spark.sql("SELECT * FROM territory_risk ORDER BY risk_score DESC LIMIT 15"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read the top of that table before trusting the score
# MAGIC
# MAGIC Two very different kinds of cell score highly, and a single number hides which is which:
# MAGIC
# MAGIC - **Dense and slow.** Central London cells: 1.3 to 1.6 million AADT at 15 to 25 km/h.
# MAGIC   High exposure, low severity per event, frequent low-speed contact.
# MAGIC - **Fast and free-flowing.** Motorway cells: around 460,000 AADT at over 100 km/h with
# MAGIC   the 85th percentile above the limit in almost every hour, and next to no congestion.
# MAGIC   Lower exposure, far higher severity per event.
# MAGIC
# MAGIC A claims frequency model and a claims severity model want these weighted differently, so
# MAGIC in practice you would score them separately rather than collapse both into one number.
# MAGIC Keeping the component columns, as above, is what makes that possible later.
# MAGIC
# MAGIC The breakdown below is the same split across the whole extract rather than the top of the
# MAGIC table, and the pattern holds: motorway cells speed twice as often as urban ones and
# MAGIC congest twenty times less.

# COMMAND ----------

display(
    spark.sql(
        """
        SELECT
            CASE WHEN mean_speed_kph >= 80 THEN 'motorway speeds'
                 WHEN mean_speed_kph >= 40 THEN 'arterial speeds'
                 ELSE 'urban speeds' END          AS cell_type,
            count(*)                              AS cells,
            round(avg(total_aadt))                AS mean_aadt,
            round(avg(mean_speed_kph), 1)         AS mean_speed_kph,
            round(avg(speeding_pct), 1)           AS mean_speeding_pct,
            round(avg(congestion_pct), 1)         AS mean_congestion_pct,
            round(avg(risk_score), 3)             AS mean_risk_score
        FROM territory_risk
        GROUP BY cell_type
        ORDER BY mean_speed_kph DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The map
# MAGIC
# MAGIC Resolution 9 is the right grain to score on and the wrong grain to draw. There are
# MAGIC 224,000 of those cells here, each about 0.1 km2, and a notebook will render a few
# MAGIC hundred polygons before it gives up. Rolling the same scores up to resolution 6, about
# MAGIC 36 km2, covers the whole extract in roughly 2,000 hexagons.
# MAGIC
# MAGIC Exposure does the weighting, so a cell reads as the risk a vehicle there actually
# MAGIC meets rather than an average that counts an empty lane the same as a motorway.

# COMMAND ----------

# MAGIC %pip install folium h3

# COMMAND ----------

import folium
import matplotlib.cm as cm
import matplotlib.colors as mcolors
from h3 import cell_to_boundary, cell_to_latlng

MAP_RESOLUTION = 6

area = spark.sql(
    f"""
    SELECT h3_toparent(h3_r9, {MAP_RESOLUTION})                        AS cell,
           sum(total_aadt)                                             AS total_aadt,
           round(sum(risk_score * total_aadt) / sum(total_aadt), 3)     AS risk_score,
           round(sum(mean_speed_kph * total_aadt) / sum(total_aadt), 1) AS mean_speed_kph,
           round(sum(speeding_pct * total_aadt) / sum(total_aadt), 1)   AS speeding_pct,
           count(*)                                                     AS cells_r9
    FROM territory_risk
    GROUP BY 1
    """
).toPandas()

print(f"{len(area):,} cells at resolution {MAP_RESOLUTION}, "
      f"rolled up from {area.cells_r9.sum():,} scored cells")

centres = [cell_to_latlng(c) for c in area.cell]
chart = folium.Map(
    location=[sum(c[0] for c in centres) / len(centres),
              sum(c[1] for c in centres) / len(centres)],
    zoom_start=9,
    # OpenStreetMap rather than one of the CartoDB styles: those now need an API key, and
    # folium still offers them, so the map renders with a warning and no basemap.
    tiles="OpenStreetMap",
)
# The default tiles carry enough colour of their own to compete with the data drawn on top.
# Desaturating the tile pane leaves the basemap as context and the choropleth as the only
# colour. Leaflet keeps vectors in a separate pane, so this does not touch the hexagons.
chart.get_root().header.add_child(
    folium.Element(
        "<style>.leaflet-tile-pane{filter:grayscale(1) contrast(0.92) brightness(1.06);}</style>"
    )
)

# Shade on rank rather than on the score itself. Exposure is heavily skewed, so a linear scale
# spends most of its colour on a handful of motorway cells and leaves everything else flat.
rank = area.risk_score.rank(pct=True)

for row, shade in zip(area.itertuples(), rank):
    folium.Polygon(
        locations=cell_to_boundary(row.cell),
        color=None,
        fill=True,
        fill_color=mcolors.to_hex(cm.YlOrRd(shade)),
        fill_opacity=0.55,
        tooltip=(
            f"score {row.risk_score:.2f}, {row.total_aadt:,} AADT, "
            f"{row.mean_speed_kph:.0f} km/h, speeding in {row.speeding_pct:.0f}% of hours"
        ),
    ).add_to(chart)

display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Route risk, from a telematics trace
# MAGIC
# MAGIC Territory rating asks where a driver lives. Route rating asks which roads they actually
# MAGIC drive, which is a far better question and the reason telematics exists.
# MAGIC
# MAGIC Raw GPS is not enough on its own: points sit 10 to 50 metres off the road and carry no
# MAGIC record of which road they were on. **Map matching** resolves a trace to a sequence of
# MAGIC road segments. [Fast Map Matching](https://github.com/cyang-kth/fmm) is an open source
# MAGIC implementation that outputs OpenStreetMap way IDs.
# MAGIC
# MAGIC Those IDs are the join. `segments.osm_way_ids` holds the OpenStreetMap ways each TomTom
# MAGIC segment maps to, so matched output joins straight in with `arrays_overlap` and no second
# MAGIC matching step:
# MAGIC
# MAGIC ```
# MAGIC GPS trace -> map matcher -> OSM way IDs -> segments.osm_way_ids -> hourly speeds
# MAGIC ```
# MAGIC
# MAGIC The example below uses two ways from the M20 motorway in Kent. In production these come
# MAGIC from the matcher, one list per trip.

# COMMAND ----------

MATCHED_OSM_WAYS = [4394118, 4394117]

display(
    spark.sql(
        f"""
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
                avg(CASE WHEN element_at(h.speed_percentiles_kph, 17) > r.speed_limit_kph
                         THEN 1.0 ELSE 0.0 END) AS speeding_rate,
                avg(CASE WHEN h.harmonic_speed_kph < 0.6 * r.speed_limit_kph
                         THEN 1.0 ELSE 0.0 END) AS congestion_rate
            FROM on_route r
            JOIN {stats}.hourly_stats h USING (dseg_id)
            WHERE h.observation_date = DATE '{observation_date}'
              AND h.speed_percentiles_kph IS NOT NULL
              AND h.harmonic_speed_kph > 0
            GROUP BY r.dseg_id
        )
        SELECT
            count(*)                                       AS segments_on_route,
            round(sum(length_m) / 1000, 1)                 AS route_km,
            round(avg(mean_speed_kph), 1)                  AS mean_speed_kph,
            round(100 * avg(speeding_rate), 1)             AS pct_hours_speeding,
            round(100 * avg(congestion_rate), 1)           AS pct_hours_congested,
            round(100 * sum(length_m * speeding_rate) / sum(length_m), 1)
                                                           AS pct_speeding_by_length
        FROM per_segment
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this route says
# MAGIC
# MAGIC 137 segments, 26 km of motorway, averaging 105 km/h with the 85th percentile above the
# MAGIC limit in 94% of hours and congestion in almost none. A commuter on this route has a
# MAGIC low-frequency, high-severity profile, and the near-universal speeding is a property of
# MAGIC the road rather than of any one driver.
# MAGIC
# MAGIC That last point matters for pricing: this is the **baseline** for the road. A driver's
# MAGIC own telematics speeds are only informative relative to it, and a model that skips the
# MAGIC baseline ends up charging people for the roads available to them.
# MAGIC
# MAGIC ## Where to take it next
# MAGIC
# MAGIC - **Weight the route by time, not distance.** A congested kilometre carries more exposure
# MAGIC   than a free-flowing one. `aadt_by_day_hour` gives the hourly shape to weight by.
# MAGIC - **Score frequency and severity separately.** They want opposite things from these
# MAGIC   factors, as section 2 shows.
# MAGIC - **Check coverage before generalising.** `traffic_volumes.coverage` gives the share of
# MAGIC   each road class with an estimate, and segments without one are absent rather than zero.
# MAGIC   On minor roads much of the network has none.
# MAGIC - **Split weekdays from weekends.** `observation_date` gives the day of week, and the two
# MAGIC   patterns differ enough that pooling them hides both.
