# Databricks notebook source
# MAGIC %md
# MAGIC # Territory risk assessment
# MAGIC
# MAGIC Where, and when, the risky kilometres are driven.
# MAGIC
# MAGIC An insurer rating territory by postcode is averaging a motorway together with a
# MAGIC cul-de-sac. One rating covers both and describes neither. What the rating needs is how
# MAGIC much driving happens in a place, and how that driving goes.
# MAGIC
# MAGIC This notebook measures **risky kilometres**: vehicle-km per day driven in one of three
# MAGIC conditions. Above the speed limit, with a wide spread of speeds between vehicles, or in a
# MAGIC jam. Traffic Volumes says how much driving happens in each hour of the week. Traffic
# MAGIC Stats says how the traffic on each road drove in that hour. Neither dataset can answer
# MAGIC this alone. Together they answer it road by road and hour by hour.
# MAGIC
# MAGIC There are no claims here and no weights. This notebook measures. The
# MAGIC `territory-risk-modeling` notebook tests the measurements against real collisions.
# MAGIC
# MAGIC **What you need.** Both free samples from Databricks Marketplace: TomTom Traffic Stats,
# MAGIC hourly speeds per road, and TomTom Traffic Volumes, traffic per road for every hour of
# MAGIC the week. They cover the same four metropolitan areas, and the `region` widget picks one.
# MAGIC The notebook reads one week, Monday to Sunday.

# COMMAND ----------

# MAGIC %pip install folium==0.20.0 h3==4.5.0 shapely==2.1.2

# COMMAND ----------

import json

import branca
import folium
import h3
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import shapely
from matplotlib.collections import PolyCollection
from pyspark.sql.types import DecimalType

sns.set_theme(style="whitegrid", palette="colorblind")


def collect(query):
    """Run a query into pandas. Spark types expressions built from literals such as `1.0` as
    DECIMAL, which pandas receives as decimal.Decimal objects that matplotlib cannot plot."""
    frame = spark.sql(query)
    decimals = {f.name: float for f in frame.schema if isinstance(f.dataType, DecimalType)}
    return frame.toPandas().astype(decimals)


def basemap():
    """A muted OpenStreetMap basemap. CartoDB's tiles now need an API key; this does not."""
    chart = folium.Map(tiles="OpenStreetMap")
    chart.get_root().header.add_child(folium.Element(
        "<style>.leaflet-tile-pane{filter:grayscale(1) contrast(0.92) brightness(1.06);}</style>"
    ))
    return chart

# COMMAND ----------

dbutils.widgets.text("stats_catalog", "TomTom_Traffic_Stats", "Traffic Stats catalog")
dbutils.widgets.text("volumes_catalog", "TomTom_Traffic_Volumes", "Traffic Volumes catalog")
dbutils.widgets.text("region", "london", "Region: london, austin, losangeles or melbourne")
dbutils.widgets.text("vintage_year", "2025", "Traffic Volumes vintage")
dbutils.widgets.text("first_date", "2025-09-01", "First Traffic Stats date, UTC")
dbutils.widgets.text("last_date", "2025-09-07", "Last Traffic Stats date, UTC")

stats = dbutils.widgets.get("stats_catalog") + ".traffic_stats_batch"
volumes = dbutils.widgets.get("volumes_catalog") + ".traffic_volumes"
region = dbutils.widgets.get("region")
vintage_year = int(dbutils.widgets.get("vintage_year"))
place = {"london": "London", "austin": "Austin", "losangeles": "Los Angeles",
         "melbourne": "Melbourne"}.get(region, region)
first_date = dbutils.widgets.get("first_date")
last_date = dbutils.widgets.get("last_date")

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
# MAGIC ## 1. How much driving happens, hour by hour
# MAGIC
# MAGIC Both datasets carry the same [H3](https://h3geo.org/) map cell of about a tenth of a
# MAGIC square kilometre, so they join on it with no map matching in between.
# MAGIC
# MAGIC Traffic Volumes holds 168 numbers per road: the vehicles it carries in each hour of an
# MAGIC average week, Monday 00:00 first. Times the road length and summed over the cell, they
# MAGIC give vehicle-km for every cell and every hour of the week. Summing keeps a road drawn as
# MAGIC ten short segments from counting ten times.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW cell_week AS
    SELECT h3_r9, slot, sum(vehicles * length_m) / 1000 AS vehicle_km
    FROM {volumes}.aadt_segments LATERAL VIEW posexplode(aadt_by_day_hour) AS slot, vehicles
    WHERE region = '{region}' AND vintage_year = {vintage_year}
    GROUP BY h3_r9, slot
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. How that driving goes
# MAGIC
# MAGIC Traffic Stats gives each road, for each hour, the average speed of the vehicles on it,
# MAGIC how far apart their speeds were, and 19 percentiles of the speeds. That is enough to
# MAGIC read three conditions per road and hour.
# MAGIC
# MAGIC | Condition | Read as |
# MAGIC |---|---|
# MAGIC | Over the limit | The share of vehicles above the posted limit, counted from the 19 percentiles |
# MAGIC | Wide speed spread | Vehicles' speeds differ by more than 40% of their average (the top quarter of hours) |
# MAGIC | Jam | The average speed is below 60% of the limit, which in towns includes stop-start traffic at junctions |
# MAGIC
# MAGIC Each condition is averaged over the roads in the cell with length as the weight, then
# MAGIC multiplied by the cell's traffic in that same hour of the week. An hour measured from a
# MAGIC single vehicle has no percentiles and no spread, so it is left out. The driving it covers
# MAGIC is counted as unmeasured, not as safe.
# MAGIC
# MAGIC The notebook then adds the cells up into hexagons of about 36 km². It keeps every hour
# MAGIC of the week, so one pass over the data feeds both the week and the maps below.

# COMMAND ----------

WIDE_SPREAD = 0.4  # standard deviation over the mean; the top quarter of measured hours
JAM = 0.6          # average speed below 60% of the limit
HEXAGON_RESOLUTION = 6

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW cell_conditions AS
    WITH hours AS (
        SELECT s.h3_r9, s.length_m,
               from_utc_timestamp(timestampadd(HOUR, h.hour_utc, timestamp(h.observation_date)),
                                  s.time_zone) AS local_time,
               size(filter(h.speed_percentiles_kph, x -> x > s.speed_limit_kph)) * 0.05
                                                                         AS over_limit,
               int(h.stddev_speed_kph / h.harmonic_speed_kph > {WIDE_SPREAD}) AS wide_spread,
               int(h.harmonic_speed_kph < {JAM} * s.speed_limit_kph)    AS jammed
        FROM {stats}.segments s JOIN {stats}.hourly_stats h USING (dseg_id)
        WHERE s.region = '{region}' AND s.speed_limit_kph > 0 AND h.harmonic_speed_kph > 0
          AND h.speed_percentiles_kph IS NOT NULL
          AND h.observation_date BETWEEN DATE '{first_date}' AND DATE '{last_date}'
    )
    SELECT h3_r9, ((dayofweek(local_time) + 5) % 7) * 24 + hour(local_time) AS slot,
           sum(length_m * over_limit) / sum(length_m)  AS over_limit,
           sum(length_m * wide_spread) / sum(length_m) AS wide_spread,
           sum(length_m * jammed) / sum(length_m)      AS jammed
    FROM hours
    GROUP BY 1, 2
    """)

# One row per hexagon and hour of the week. Everything in sections 3 and 4 comes from it.
week = collect(f"""
    SELECT h3_toparent(w.h3_r9, {HEXAGON_RESOLUTION}) AS hexagon, w.slot,
           sum(w.vehicle_km)                                             AS vehicle_km,
           sum(CASE WHEN c.over_limit IS NOT NULL THEN w.vehicle_km END) AS measured_km,
           sum(w.vehicle_km * c.over_limit)                              AS over_limit_km,
           sum(w.vehicle_km * c.wide_spread)                             AS wide_spread_km,
           sum(w.vehicle_km * c.jammed)                                  AS jammed_km
    FROM cell_week w
    LEFT JOIN cell_conditions c ON c.h3_r9 = w.h3_r9 AND c.slot = w.slot
    GROUP BY 1, 2
    """).fillna(0)

CONDITIONS = {"over_limit_km": "over the limit", "wide_spread_km": "with a wide speed spread",
              "jammed_km": "in a jam"}
totals = week[["measured_km", *CONDITIONS]].sum() / 7
print(f"{week.vehicle_km.sum() / 7:,.0f} vehicle-km a day in {place}, "
      f"{100 * totals.measured_km / (week.vehicle_km.sum() / 7):.0f}% of it in cells and hours "
      f"with measured speeds")
display(pd.DataFrame({
    "driven": list(CONDITIONS.values()),
    "vehicle-km per day": totals[list(CONDITIONS)].round(-3).to_numpy(),
    "share of measured driving (%)": (100 * totals[list(CONDITIONS)] / totals.measured_km).round(1).to_numpy(),
}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. When the risky kilometres are driven
# MAGIC
# MAGIC Each square is one hour of the week, Monday at the top. The first panel is how much
# MAGIC driving happens in that hour. The other three are the share of the measured driving in
# MAGIC each condition.

# COMMAND ----------

by_slot = week.groupby("slot")[["vehicle_km", "measured_km", *CONDITIONS]].sum()
panels = [
    (by_slot.vehicle_km / 1e6, "Vehicle-km in the hour (millions)", "Blues"),
    *[(100 * by_slot[column] / by_slot.measured_km, f"Share driven {label} (%)", shade)
      for (column, label), shade in zip(CONDITIONS.items(), ["Reds", "Purples", "Oranges"])],
]

fig, axes = plt.subplots(2, 2, figsize=(15, 6.5), sharex=True, sharey=True)
for ax, (values, title, shade) in zip(axes.flat, panels):
    grid = values.reindex(range(168)).to_numpy().reshape(7, 24)
    image = ax.imshow(grid, cmap=shade, aspect="auto", vmin=0)
    ax.set_title(title, fontsize=11)
    ax.grid(False)
    fig.colorbar(image, ax=ax, fraction=0.03, pad=0.02)
for ax in axes.flat:
    ax.set_yticks(range(7), ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])
    ax.set_xticks(range(0, 24, 3), [f"{hour:02d}:00" for hour in range(0, 24, 3)])
fig.suptitle(f"A week of driving in {place}, hour by hour", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC In London the week splits in two. Driving over the limit belongs to the night and to
# MAGIC weekend mornings, when the roads are empty: more than a quarter of the driving before
# MAGIC 07:00 on a Sunday is over the limit. Wide speed spreads and jams belong to the weekday
# MAGIC peaks, when the roads are full. A territory rating sees one average of all of it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Where the risky kilometres are driven
# MAGIC
# MAGIC On the left, how much driving each hexagon carries. On the right, how much of the
# MAGIC measured driving there is over the limit. If the two maps looked alike, traffic volume
# MAGIC alone would be enough. Hexagons with too little measured driving to say are grey.

# COMMAND ----------

MIN_MEASURED_KM = 1000  # per day; below this the share rests on too few vehicles

hexagons = week.groupby("hexagon")[["vehicle_km", "measured_km", *CONDITIONS]].sum() / 7
hexagons = hexagons[hexagons.vehicle_km > 0]
for column in CONDITIONS:
    hexagons[column.replace("_km", "_pct")] = (100 * hexagons[column] / hexagons.measured_km
                                               ).where(hexagons.measured_km >= MIN_MEASURED_KM)

outlines = [np.array(h3.cell_to_boundary(hexagon))[:, ::-1] for hexagon in hexagons.index]
squash = 1 / np.cos(np.radians(np.mean([shape[:, 1].mean() for shape in outlines])))

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7.5))
for ax, values, shade, norm, label in [
    (ax1, hexagons.vehicle_km, "Blues",
     mcolors.LogNorm(hexagons.vehicle_km.quantile(0.05), hexagons.vehicle_km.max()),
     "Vehicle-km per day (log scale)"),
    (ax2, hexagons.over_limit_pct, "Reds",
     mcolors.Normalize(0, hexagons.over_limit_pct.quantile(0.98)),
     "Share of measured driving over the limit (%)"),
]:
    colormap = plt.get_cmap(shade).copy()
    colormap.set_bad("#e6e6e6")
    shapes = PolyCollection(outlines, array=np.ma.masked_invalid(values.to_numpy()),
                            cmap=colormap, norm=norm, edgecolors="face", linewidths=0.2)
    ax.add_collection(shapes)
    ax.autoscale_view()
    ax.set_aspect(squash)
    ax.set_axis_off()
    fig.colorbar(shapes, ax=ax, fraction=0.035, pad=0.01, label=label)
ax1.set_title("Where the driving is", fontsize=12)
ax2.set_title("How much of it is over the limit", fontsize=12)
fig.suptitle(f"{place}, hexagons of about 36 km²", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The two maps do not look alike. The driving piles up in the city. The driving over the
# MAGIC limit follows the motorways and the fast roads between towns, which carry far less
# MAGIC traffic. A rating built on traffic volume alone sees only the first map.
# MAGIC
# MAGIC The same hexagons to explore. Hover one to see all three conditions.

# COMMAND ----------

shown = hexagons.reset_index().round({"vehicle_km": -2, "over_limit_pct": 1,
                                      "wide_spread_pct": 1, "jammed_pct": 1})
limit = float(hexagons.over_limit_pct.quantile(0.98))
scale = branca.colormap.LinearColormap(
    [mcolors.to_hex(plt.get_cmap("Reds")(x)) for x in np.linspace(0, 1, 9)], vmin=0, vmax=limit,
    caption="Share of measured driving over the limit (%)")

chart = basemap()
drawn = folium.GeoJson(
    {"type": "FeatureCollection", "features": [
        {"type": "Feature",
         "properties": {key: (None if pd.isna(value) else value) for key, value in row._asdict().items()},
         "geometry": json.loads(shapely.to_geojson(shapely.Polygon(outline)))}
        for row, outline in zip(shown.itertuples(index=False), outlines)]},
    style_function=lambda feature: {
        "fillColor": ("#e6e6e6" if feature["properties"]["over_limit_pct"] is None
                      else scale(min(feature["properties"]["over_limit_pct"], limit))),
        "fillOpacity": 0.7, "color": "#ffffff", "weight": 0.3},
    highlight_function=lambda feature: {"weight": 2, "color": "#222222"},
    tooltip=folium.GeoJsonTooltip(
        fields=["hexagon", "vehicle_km", "over_limit_pct", "wide_spread_pct", "jammed_pct"],
        aliases=["Hexagon", "Vehicle-km per day", "Over the limit (%)", "Wide speed spread (%)",
                 "In a jam (%)"]),
).add_to(chart)
scale.add_to(chart)
chart.fit_bounds(drawn.get_bounds())
display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. A driver against the road
# MAGIC
# MAGIC Territory rating asks where a driver lives. Telematics asks which roads they drive, and
# MAGIC how. The percentiles make a third question possible: how did they drive compared with
# MAGIC everyone else on the same road in the same hour?
# MAGIC
# MAGIC A driver's trip reaches the data through OpenStreetMap. A map matcher such as
# MAGIC [Fast Map Matching](https://github.com/cyang-kth/fmm) turns a GPS trace into
# MAGIC OpenStreetMap way IDs, and `segments.osm_way_ids` carries the same IDs, so there is no
# MAGIC second matching step:
# MAGIC
# MAGIC ```
# MAGIC GPS trace -> map matcher -> OSM way IDs -> segments.osm_way_ids -> hourly speeds
# MAGIC ```
# MAGIC
# MAGIC The example uses two ways of the M20 motorway in Kent, and the A20, which runs beside
# MAGIC it with a lower limit. In production, the matcher gives one list of ways per trip.

# COMMAND ----------

MATCHED_OSM_WAYS = [4394118, 4394117]  # the M20 in Kent
NEARBY_ROAD, NEARBY_LIMIT_KPH = "A20", 96  # the 60 mph stretches of the road beside it

roads = collect(f"""
    SELECT s.dseg_id, s.street_name, s.speed_limit_kph, s.length_m, s.geometry_wkt,
           arrays_overlap(s.osm_way_ids, array({", ".join(f"{way}L" for way in MATCHED_OSM_WAYS)}))
                                                                          AS on_route,
           hour(from_utc_timestamp(timestampadd(HOUR, h.hour_utc, timestamp(h.observation_date)),
                                   s.time_zone))                          AS hour_local,
           h.harmonic_speed_kph, h.speed_percentiles_kph
    FROM {stats}.segments s JOIN {stats}.hourly_stats h USING (dseg_id)
    WHERE s.region = '{region}' AND h.speed_percentiles_kph IS NOT NULL
      AND h.observation_date BETWEEN DATE '{first_date}' AND DATE '{last_date}'
      AND (arrays_overlap(s.osm_way_ids, array({", ".join(f"{way}L" for way in MATCHED_OSM_WAYS)}))
           OR (s.street_name = '{NEARBY_ROAD}' AND round(s.speed_limit_kph) = {NEARBY_LIMIT_KPH}))
    """)

if not roads.on_route.any():
    raise ValueError(
        f"None of the OSM ways {MATCHED_OSM_WAYS} lie in region '{region}'. They are on the M20 "
        f"in Kent, which is in the london extract. Set the region widget back to london, or put "
        f"way IDs from your own matched trace in MATCHED_OSM_WAYS."
    )

route = roads[roads.on_route]
percentiles = np.stack(route.speed_percentiles_kph.to_numpy())
profile = pd.DataFrame({"hour_local": route.hour_local, "harmonic_mean": route.harmonic_speed_kph,
                        "p15": percentiles[:, 2], "p85": percentiles[:, 16],
                        "speed_limit": route.speed_limit_kph}).groupby("hour_local").mean()

fig, ax = plt.subplots(figsize=(11, 5))
ax.fill_between(profile.index, profile.p15, profile.p85, alpha=0.2, color="steelblue",
                label="p15 to p85")
ax.plot(profile.index, profile.harmonic_mean, marker="o", color="steelblue", label="Harmonic mean")
ax.plot(profile.index, profile.speed_limit, linestyle="--", color="black", label="Speed limit")
ax.set(xlabel="Local hour", ylabel="km/h", xticks=range(0, 24, 2))
ax.set_title("The M20 route through an average day of the week", fontsize=13, fontweight="bold")
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The band is the middle 70% of the traffic. A driver above it is faster than 85% of the
# MAGIC vehicles on that road in that hour, whatever the limit says.
# MAGIC
# MAGIC The table makes the same point with one speed in four places. It puts 100 km/h on each
# MAGIC road's own distribution, at night and in the morning peak. On the M20, 100 km/h is slower
# MAGIC than most of the traffic. On the A20 beside it, the same speed is faster than 95% of it.
# MAGIC The speed alone says little about the driver. The road and the hour say the rest.

# COMMAND ----------

SPEED_KPH = 100
LEVELS = np.arange(5, 100, 5)  # the 19 percentiles, p5 to p95


def where_it_falls(speed, distribution):
    if speed < distribution[0]:
        return "slower than 95%"
    if speed > distribution[-1]:
        return "faster than 95%"
    return f"faster than {np.interp(speed, distribution, LEVELS):.0f}%"


lookup = []
for name, rows in [("M20", roads[roads.on_route]), (f"{NEARBY_ROAD}, 60 mph stretches", roads[~roads.on_route])]:
    for hour in (3, 8):
        at_hour = rows[rows.hour_local == hour]
        typical = np.stack(at_hour.speed_percentiles_kph.to_numpy()).mean(axis=0)
        lookup.append({"road": name, "local hour": f"{hour:02d}:00",
                       "limit (km/h)": round(at_hour.speed_limit_kph.mean()),
                       "median (km/h)": round(typical[9]), "p85 (km/h)": round(typical[16]),
                       f"{SPEED_KPH} km/h is": where_it_falls(SPEED_KPH, typical)})
display(pd.DataFrame(lookup))

# COMMAND ----------

# MAGIC %md
# MAGIC The route itself, segment by segment. The colour is the share of vehicles over the
# MAGIC limit across the week, which is where a route that looks uniform turns out uneven.

# COMMAND ----------

over = (np.stack(route.speed_percentiles_kph.to_numpy()) > route.speed_limit_kph.to_numpy()[:, None])
segments = (route.assign(over_limit_pct=100 * over.mean(axis=1))
            .groupby("dseg_id")
            .agg(street_name=("street_name", "first"), geometry_wkt=("geometry_wkt", "first"),
                 speed_limit_kph=("speed_limit_kph", "first"),
                 mean_speed_kph=("harmonic_speed_kph", "mean"),
                 over_limit_pct=("over_limit_pct", "mean"))
            .round(1).reset_index())
# The 19 percentiles leave the top 5% unread, so a road where everyone speeds reads as 95%.
segments["over_limit_pct"] = (segments.over_limit_pct * 100 / 95).round(1)

scale = branca.colormap.LinearColormap(
    [mcolors.to_hex(plt.get_cmap("Reds")(x)) for x in np.linspace(0.15, 1, 9)], vmin=0, vmax=100,
    caption="Vehicles over the limit (%)")
chart = basemap()
drawn = folium.GeoJson(
    {"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": row.drop("geometry_wkt").to_dict(),
         "geometry": json.loads(shapely.to_geojson(shapely.from_wkt(row.geometry_wkt)))}
        for _, row in segments.iterrows()]},
    style_function=lambda feature: {"color": scale(feature["properties"]["over_limit_pct"]),
                                    "weight": 5, "opacity": 0.9},
    tooltip=folium.GeoJsonTooltip(
        fields=["street_name", "over_limit_pct", "mean_speed_kph", "speed_limit_kph"],
        aliases=["Road", "Vehicles over the limit (%)", "Mean speed (km/h)", "Limit (km/h)"]),
).add_to(chart)
scale.add_to(chart)
chart.fit_bounds(drawn.get_bounds())
display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to take it next
# MAGIC
# MAGIC - **Test the measurements before you weight them.** The `territory-risk-modeling`
# MAGIC   notebook puts road features into a claims model and measures what they add, and where.
# MAGIC - **Keep the three conditions apart.** Jams bring frequent, mild collisions and speed
# MAGIC   brings rare, severe ones, so frequency and severity models weight them differently.
# MAGIC - **Read a driver against the road.** With matched trips, place each stretch of a
# MAGIC   driver's speed on that road's percentiles for that hour, the way the table above does.
# MAGIC - **Check coverage first.** `traffic_volumes.coverage` gives the share of each road class
# MAGIC   with a traffic estimate. A road without one is missing, not empty.
# MAGIC - **Compare regions and weeks.** Set `region` to another city, or move the dates. The
# MAGIC   sample runs from 1 September to 31 October 2025.
