# Databricks notebook source
# MAGIC %md
# MAGIC # Getting started with TomTom Orbis Maps
# MAGIC
# MAGIC The road network of TomTom Orbis Maps in the [Overture](https://docs.overturemaps.org/)
# MAGIC schema: every road, path and railway as a segment, the junctions that join them, and
# MAGIC what a driver meets along the way: speed limits, typical speeds, access and turn
# MAGIC restrictions, traffic signs, traffic signals and signposts.
# MAGIC
# MAGIC ## Database tables
# MAGIC
# MAGIC One table per Overture feature type, all in the `transportation` theme.
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |---|---|
# MAGIC | `orbis_maps.segment` | A road, path, railway or waterway centreline and its attributes. |
# MAGIC | `orbis_maps.connector` | A point where segments meet or end. |
# MAGIC | `orbis_maps.traffic_sign` | A traffic sign placed on a segment: maximum speed, stop, give way, hazard and others. |
# MAGIC | `orbis_maps.traffic_signal` | A traffic signal placed on a segment. |
# MAGIC | `orbis_maps.signpost` | A direction sign at a junction, with the destinations it shows. |
# MAGIC
# MAGIC ### Columns every table has
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `region` | Metropolitan sample extract: `london`, `austin` or `losangeles`. |
# MAGIC | `id` | Orbis feature ID, a UUID. Tables refer to each other by it. |
# MAGIC | `geometry` | Feature geometry as WKB in WGS84 longitude and latitude. |
# MAGIC | `bbox` | Bounding box of the geometry: `xmin`, `ymin`, `xmax`, `ymax`. |
# MAGIC | `version` | Overture feature version. |
# MAGIC | `sources` | Where each property came from: dataset, licence and source record. |
# MAGIC | `theme`, `type` | Overture theme and feature type, the same on every row of a table. |
# MAGIC
# MAGIC ### `segment` columns used here
# MAGIC
# MAGIC `segment` has 45 columns; the
# MAGIC [Overture segment reference](https://docs.overturemaps.org/schema/reference/transportation/segment/)
# MAGIC describes the Overture ones. The ones this notebook reads:
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `subtype` | `road`, `rail` or `water`. |
# MAGIC | `class` | Road class, from `motorway` down to `footway` and `steps`. |
# MAGIC | `names` | `names.primary` is the road name. Null for unnamed roads. |
# MAGIC | `connectors` | The connectors on the segment, each with its position `at` along it. |
# MAGIC | `speed_limits` | Posted speed limits, each with a `max_speed` value and unit. |
# MAGIC | `speed_profiles` | Typical driven speeds for weekdays, weekends and the whole week, and the free-flow speed, per direction. |
# MAGIC | `access_restrictions` | Who may use the road, and when. |
# MAGIC | `prohibited_transitions` | Turn restrictions: segment sequences a vehicle may not take. |
# MAGIC | `gradient`, `curvature` | Slope and bend along the road. |
# MAGIC | `routing_class` | Routing importance from 1, the most important, to 5. |
# MAGIC
# MAGIC **TomTom Traffic Stats** and **TomTom Traffic Volumes** are separate listings covering the
# MAGIC same three regions, with how fast traffic moves and how much of it there is. The last
# MAGIC section here joins this map to them.
# MAGIC
# MAGIC Five things to know before you start.
# MAGIC
# MAGIC 1. **Attributes that change along a road are arrays.** Each element can carry `between`,
# MAGIC    the stretch it applies to as fractions of the segment from 0 to 1, and `when`, the
# MAGIC    vehicles or times it applies to. An element with neither covers the whole segment for
# MAGIC    everyone. This notebook reads those elements; the last section shows how to read the rest.
# MAGIC 2. **Not every segment is a street.** Footways, paths, cycleways and steps are segments
# MAGIC    too, and so are service roads such as driveways and car park aisles. In London those
# MAGIC    are more than half of all segments. Filter on `class`.
# MAGIC 3. **Units travel with the values.** Speed limits carry their own `unit`, which is `mph`
# MAGIC    in all three regions, and speed profiles are in km/h. This notebook converts by the
# MAGIC    unit rather than assuming one.
# MAGIC 4. **Regions are metropolitan extracts far wider than the city**, on the same extents as
# MAGIC    the traffic listings. `london` runs from the Dorset coast to the Peak District. A
# MAGIC    feature crossing the edge is kept whole, so a few long segments, such as ferry routes,
# MAGIC    reach far outside it. Section 2 prints each extent.
# MAGIC 5. **Geometry is WKB.** Filter on `bbox` first, then parse only what you draw.
# MAGIC
# MAGIC The map contains OpenStreetMap data, available under the Open Database License (ODbL).
# MAGIC `sources` says which properties came from it.

# COMMAND ----------

# MAGIC %pip install folium==0.20.0 shapely==2.1.2 h3==4.5.0 seaborn==0.13.2

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import folium
import h3
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from branca.colormap import LinearColormap
from pyspark.sql.types import DecimalType
from shapely import from_wkb

sns.set_theme(style="whitegrid", palette="colorblind")

HEAT = ["#ffffcc", "#fd8d3c", "#e31a1c", "#800026"]

# Road classes for cars, most important first, then everything else on the network.
DRIVEABLE = ["motorway", "trunk", "primary", "secondary", "tertiary", "unclassified",
             "residential", "living_street", "service"]
NOT_DRIVEABLE = ["pedestrian", "footway", "steps", "path", "cycleway", "bridleway", "track",
                 "unknown"]
CLASS_COLORS = dict(zip(DRIVEABLE, ["#800026", "#bd0026", "#e31a1c", "#fc4e2a", "#fd8d3c",
                                    "#feb24c", "#fed976", "#ffeda0", "#e0d9b8"]))
CLASS_COLORS.update({c: "#9e9e9e" for c in NOT_DRIVEABLE})


def collect(query):
    """Run a query into pandas. Spark types expressions built from literals such as `1.0` as
    DECIMAL, which pandas receives as decimal.Decimal objects that matplotlib cannot plot."""
    frame = spark.sql(query)
    decimals = {f.name: float for f in frame.schema if isinstance(f.dataType, DecimalType)}
    return frame.toPandas().astype(decimals)


def overlaps(area):
    """SQL predicate for features whose bounding box touches `area` = (west, south, east, north)."""
    west, south, east, north = area
    return (f"bbox.xmax >= {west} AND bbox.xmin <= {east} "
            f"AND bbox.ymax >= {south} AND bbox.ymin <= {north}")


def basemap(lat, lon, zoom):
    """A muted OpenStreetMap basemap. CartoDB's tiles now need an API key; this does not."""
    chart = folium.Map(location=[lat, lon], zoom_start=zoom, tiles="OpenStreetMap")
    chart.get_root().header.add_child(folium.Element(
        "<style>.leaflet-tile-pane{filter:grayscale(1) contrast(0.92) brightness(1.06);}</style>"
    ))
    return chart


def latlon(geometry):
    """Folium wants (lat, lon) pairs; the WKB holds (lon, lat)."""
    return [(lat, lon) for lon, lat in from_wkb(bytes(geometry)).coords]


def mph(speed):
    """SQL turning a `{value, unit}` speed into mph, whatever unit it was delivered in."""
    return (f"CASE {speed}.unit WHEN 'mph' THEN {speed}.value "
            f"WHEN 'km/h' THEN round({speed}.value / 1.609344) END")


# The posted limit for the whole segment and every vehicle: the first element with no `when`.
# `when` and `between` are SQL keywords, hence the backticks. `try_element_at` returns NULL
# for an empty array, where `element_at` fails under ANSI SQL, the default on serverless.
SPEED_LIMIT = "try_element_at(filter(speed_limits, s -> s.`when` IS NULL), 1).max_speed"
TYPICAL_SPEED = "try_element_at(speed_profiles, 1).week_speed"
FREEFLOW_SPEED = "try_element_at(speed_profiles, 1).freeflow_speed"

# COMMAND ----------

# The default is what Marketplace suggests when you install this listing.
dbutils.widgets.text("catalog", "TomTom_Orbis_Maps", "Catalog you attached the share as")
dbutils.widgets.text("region", "london", "Region: london, austin or losangeles")

catalog = dbutils.widgets.get("catalog")
region = dbutils.widgets.get("region")
maps = f"{catalog}.orbis_maps"

try:
    spark.sql(f"DESCRIBE SCHEMA {maps}")
except Exception as error:
    raise ValueError(
        f"Cannot read {maps}. The error was: {error}. "
        f"If that is the name being wrong rather than a permission problem, set the `catalog` "
        f"widget at the top of this notebook to the catalog you accepted when you installed "
        f"the listing. Run SHOW CATALOGS if you are not sure what it was called."
    ) from None

# Everything after section 2 reads one region through these views. `segments` is every
# segment as delivered; `roads` keeps the road subtype and pulls the two speeds out of their
# arrays. `class` is renamed because it is a keyword in Python, and a road without a name,
# which is most motorways, is called by its route instead, such as A303 or I-110.
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW segments AS
    SELECT * FROM {maps}.segment WHERE region = '{region}'
    """)

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW roads AS
    SELECT id, class AS road_class,
           coalesce(names.primary, try_element_at(routes, 1).name) AS name,
           bbox, geometry, connectors,
           {mph(SPEED_LIMIT)} AS speed_limit_mph,
           {mph(TYPICAL_SPEED)} AS typical_speed_mph,
           {mph(FREEFLOW_SPEED)} AS freeflow_speed_mph
    FROM segments
    WHERE subtype = 'road'
    """)

for layer in ["traffic_sign", "traffic_signal", "signpost"]:
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW {layer}s AS
        SELECT * FROM {maps}.{layer} WHERE region = '{region}'
        """)

if spark.table("roads").isEmpty():
    raise ValueError(
        f"No roads for region '{region}'. The table in section 2 lists what this share holds."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. How the data is organised
# MAGIC
# MAGIC ```
# MAGIC segment  1 row per ROAD, PATH OR RAILWAY between two connectors or more
# MAGIC   │
# MAGIC   ├── connectors[]            connector_id + at (0 → 1 along the segment) ──▶ connector
# MAGIC   ├── speed_limits[]          max_speed {value, unit}, between, when
# MAGIC   ├── speed_profiles[]        weekday / weekend / week speed, km/h
# MAGIC   └── access_restrictions[], prohibited_transitions[], gradient[], ...
# MAGIC         ▲
# MAGIC         │ segment.id + at
# MAGIC         │
# MAGIC   traffic_sign, traffic_signal   1 row per SIGN or SIGNAL, placed on a segment
# MAGIC
# MAGIC   signpost   1 row per DIRECTION SIGN: from_segment ──▶ to_segment, at a connector
# MAGIC ```
# MAGIC
# MAGIC **A segment is the row, and a road is many segments.** A segment ends where the road
# MAGIC meets another, so a motorway such as the M25 is many of them. Signs and signals do not
# MAGIC carry their own road: they point at a segment `id` and a position `at` along it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. What is in the database
# MAGIC
# MAGIC Every layer and region in the share, with its extent. The rest of the notebook reads the
# MAGIC region picked by the widget above.

# COMMAND ----------

display(spark.sql(" UNION ALL ".join(f"""
        SELECT '{layer}' AS layer, region, count(*) AS features,
               round(percentile_approx(bbox.ymin, 0.001), 2) AS south,
               round(percentile_approx(bbox.ymax, 0.999), 2) AS north,
               round(percentile_approx(bbox.xmin, 0.001), 2) AS west,
               round(percentile_approx(bbox.xmax, 0.999), 2) AS east
        FROM {maps}.{layer} GROUP BY region
        """ for layer in ["segment", "connector", "traffic_sign", "traffic_signal", "signpost"])
        + " ORDER BY layer, region"))

# COMMAND ----------

# MAGIC %md
# MAGIC The extents leave out the 0.1% of features furthest out on each side: the long segments
# MAGIC that cross the edge and run on, such as ferry routes, would otherwise stretch them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A first look at the roads
# MAGIC
# MAGIC Before any statistics, look at the data. The map is centred on the median position of
# MAGIC the region's traffic signals, which falls in its urban core. Click a road for its name,
# MAGIC class, posted speed limit and typical speed, and ask whether they are what you would
# MAGIC expect. That is the quickest validation of the data there is.

# COMMAND ----------

focus = spark.sql("""
        SELECT percentile_approx((bbox.xmin + bbox.xmax) / 2, 0.5) AS lon,
               percentile_approx((bbox.ymin + bbox.ymax) / 2, 0.5) AS lat
        FROM traffic_signals
        """).first()

CENTRE = (focus.lon - 0.015, focus.lat - 0.01, focus.lon + 0.015, focus.lat + 0.01)

centre = collect(f"""
        SELECT name, road_class, speed_limit_mph, typical_speed_mph, geometry FROM roads
        WHERE {overlaps(CENTRE)} AND road_class IN ({", ".join(f"'{c}'" for c in DRIVEABLE)})
        LIMIT 3000
        """)


def speed_text(value):
    return "" if pd.isna(value) else f"{value:.0f} mph"


chart = basemap(focus.lat, focus.lon, 16)
for row in centre.itertuples():
    folium.PolyLine(
        latlon(row.geometry), weight=max(2, 7 - DRIVEABLE.index(row.road_class)), opacity=0.85,
        color=CLASS_COLORS[row.road_class],
        popup=folium.Popup(
            f"<b>{row.name or 'Unnamed'}</b><br>"
            f"<b>Class:</b> {row.road_class}<br>"
            f"<b>Speed limit:</b> {speed_text(row.speed_limit_mph)}<br>"
            f"<b>Typical speed:</b> {speed_text(row.typical_speed_mph)}",
            max_width=260,
        ),
    ).add_to(chart)

display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. What the network is made of
# MAGIC
# MAGIC Segments by class, coloured from motorways in dark red to service roads in pale yellow,
# MAGIC with everything that is not for cars in grey. Most segments are the small ones: a
# MAGIC motorway is long and has few junctions, while a residential street is cut at every one.
# MAGIC The label on each bar is the share of those segments carrying a posted speed limit.

# COMMAND ----------

by_class = collect("""
        SELECT road_class, count(*) AS segments,
               round(100 * avg(CASE WHEN speed_limit_mph IS NOT NULL THEN 1 ELSE 0 END))
                 AS pct_limit
        FROM roads GROUP BY road_class
        """)
order = [c for c in DRIVEABLE + NOT_DRIVEABLE if c in set(by_class.road_class)]
by_class = by_class.set_index("road_class").loc[order].reset_index()

fig, ax = plt.subplots(figsize=(15, 5))
bars = ax.bar(by_class.road_class, by_class.segments,
              color=[CLASS_COLORS.get(c, "#9e9e9e") for c in by_class.road_class])
ax.bar_label(bars, [f"{pct:.0f}%" for pct in by_class.pct_limit], fontsize=8, padding=2)
ax.set(yscale="log", ylabel="Segments (log scale)",
       title=f"Road segments by class, {region}, labelled with the share carrying a speed limit")
ax.tick_params(axis="x", rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Which attributes are filled, and where
# MAGIC
# MAGIC An attribute array is empty where the attribute does not apply, so how full each one is
# MAGIC depends on the road. Speed limits are on almost every street and missing from footways,
# MAGIC because footways have none. Read this grid before you build on an attribute: it tells
# MAGIC you which roads you can say something about.
# MAGIC
# MAGIC Each cell is the share of segments of that class whose array holds at least one value.

# COMMAND ----------

ATTRIBUTES = ["speed_limits", "speed_profiles", "access_restrictions", "prohibited_transitions",
              "gradient", "curvature", "road_surface", "routes", "house_numbers",
              "dual_carriageway", "truck_routes", "hazmat", "toll"]

fill = collect(f"""
        SELECT class AS road_class,
               {", ".join(f"100 * avg(CASE WHEN size({a}) > 0 THEN 1 ELSE 0 END) AS {a}"
                          for a in ATTRIBUTES)}
        FROM segments WHERE subtype = 'road'
        GROUP BY class
        """)
fill = fill.set_index("road_class").loc[[c for c in order if c in set(fill.road_class)]]

fig, ax = plt.subplots(figsize=(15, 6))
sns.heatmap(fill[ATTRIBUTES].T, ax=ax, cmap="YlGn", vmin=0, vmax=100, annot=True, fmt=".0f",
            annot_kws={"fontsize": 8}, cbar_kws={"label": "% of segments with a value"})
ax.set(xlabel="", ylabel="", title=f"Attribute fill rate by road class, {region}")
ax.tick_params(axis="x", rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Posted limits against typical speeds
# MAGIC
# MAGIC Three speeds sit on the same segment. `speed_limits` is what the sign says.
# MAGIC `speed_profiles` holds what traffic typically does over a whole week, measured from probe
# MAGIC vehicles, and, on most major roads, the free-flow speed: what it does when the road is
# MAGIC empty. Free flow separates the two reasons traffic runs below the limit. Where it sits
# MAGIC close to the typical speed, congestion explains little of the gap, and the road itself,
# MAGIC its junctions, bends and crossings, explains the rest.
# MAGIC
# MAGIC The dots are medians per class; the bars span the middle half of the segments.

# COMMAND ----------

speeds = collect(f"""
        SELECT road_class, count(*) AS segments,
               percentile_approx(speed_limit_mph, array(0.25, 0.5, 0.75)) AS limit_q,
               percentile_approx(freeflow_speed_mph, array(0.25, 0.5, 0.75)) AS freeflow_q,
               percentile_approx(typical_speed_mph, array(0.25, 0.5, 0.75)) AS typical_q
        FROM roads
        WHERE speed_limit_mph IS NOT NULL AND typical_speed_mph IS NOT NULL
          AND road_class IN ({", ".join(f"'{c}'" for c in DRIVEABLE)})
        GROUP BY road_class
        """)
speeds = speeds.set_index("road_class").loc[[c for c in DRIVEABLE if c in set(speeds.road_class)]]

fig, ax = plt.subplots(figsize=(12, 7))
y = np.arange(len(speeds))
SERIES = [(-0.22, "limit_q", "#404040", "Posted limit"),
          (0, "freeflow_q", "#fd8d3c", "Free-flow speed"),
          (0.22, "typical_q", "#e31a1c", "Typical speed")]
for offset, column, color, label in SERIES:
    # A class with no free-flow values at all comes back as null rather than three nulls
    q = np.array([v if v is not None else [np.nan] * 3 for v in speeds[column]], dtype=float)
    ax.hlines(y + offset, q[:, 0], q[:, 2], color=color, linewidth=5, alpha=0.35)
    ax.plot(q[:, 1], y + offset, "o", color=color, label=label)
ax.set_yticks(y, [f"{c} ({n:,})" for c, n in zip(speeds.index, speeds.segments)])
ax.invert_yaxis()
ax.set(xlabel="mph", title=f"Posted limit, free-flow and typical speed by class, {region}")
ax.legend(loc="lower right")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Signs and signals
# MAGIC
# MAGIC Traffic signs and signals are features of their own, each placed on a segment. The chart
# MAGIC counts the signs by type. The map counts signals per [H3](https://h3geo.org/) cell at
# MAGIC resolution 8, about 0.7 km² each, over the area around section 3 zoomed out: dense
# MAGIC signals mark the junction-heavy urban core.
# MAGIC
# MAGIC `h3_longlatash3string` is built into Databricks, so the cell comes straight from SQL.

# COMMAND ----------

signs = collect("""
        SELECT subtype, count(*) AS signs FROM traffic_signs GROUP BY subtype ORDER BY signs
        """)

fig, ax = plt.subplots(figsize=(12, 5))
bars = ax.barh(signs.subtype, signs.signs, color="#fd8d3c")
ax.bar_label(bars, [f"{n:,}" for n in signs.signs], fontsize=8, padding=3)
ax.set(xlabel="Signs", title=f"Traffic signs by type, {region}")
plt.tight_layout()
plt.show()

# COMMAND ----------

AREA = (focus.lon - 0.15, focus.lat - 0.09, focus.lon + 0.15, focus.lat + 0.09)

cells = collect(f"""
        SELECT h3_longlatash3string((bbox.xmin + bbox.xmax) / 2, (bbox.ymin + bbox.ymax) / 2, 8)
                 AS h3_r8,
               count(*) AS signals
        FROM traffic_signals
        WHERE {overlaps(AREA)}
        GROUP BY 1
        """)

chart = basemap(focus.lat, focus.lon, 12)
scale = LinearColormap(HEAT, vmin=cells.signals.quantile(0.1), vmax=cells.signals.quantile(0.95),
                       caption="Traffic signals per H3 cell (resolution 8)")
for row in cells.itertuples():
    color = scale(row.signals)
    folium.Polygon(
        h3.cell_to_boundary(row.h3_r8), color=color, fill=True, fill_color=color,
        fill_opacity=0.6, weight=1,
        popup=f"<b>{row.signals}</b> signals in {row.h3_r8}",
    ).add_to(chart)

scale.add_to(chart)
display(chart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Junctions and signposts
# MAGIC
# MAGIC Segments meet at connectors, and counting the street segments at each one tells you what
# MAGIC kind of point it is: 1 is the end of a street, either a dead end or where it carries on as
# MAGIC a footway, 2 a bend or a change of attribute, and 3 or more a junction.
# MAGIC This is the network topology that routing runs on, and it comes with the data rather than
# MAGIC having to be derived from geometry.
# MAGIC
# MAGIC Signposts sit at those junctions. Each one says which segment it guides from and to, and
# MAGIC what it shows: the table reads a few of them back as a driver would see them.

# COMMAND ----------

degree = collect(f"""
        SELECT least(segments, 6) AS segments, count(*) AS connectors FROM (
            SELECT c.connector_id, count(*) AS segments
            FROM roads LATERAL VIEW explode(connectors) t AS c
            WHERE road_class IN ({", ".join(f"'{c}'" for c in DRIVEABLE)})
            GROUP BY c.connector_id)
        GROUP BY 1 ORDER BY 1
        """)

labels = {1: "1\nend of a street", 2: "2\ncontinuation", 6: "6+"}
fig, ax = plt.subplots(figsize=(10, 4))
bars = ax.bar([labels.get(n, str(n)) for n in degree.segments], degree.connectors,
              color=["#9e9e9e" if n < 3 else "#e31a1c" for n in degree.segments])
ax.bar_label(bars, [f"{n:,}" for n in degree.connectors], fontsize=8, padding=2)
ax.set(xlabel="Street segments meeting at a connector", ylabel="Connectors",
       title=f"Connectors by number of street segments, {region}")
plt.tight_layout()
plt.show()

display(spark.sql("""
        SELECT f.name AS from_road, f.road_class AS from_class, t.name AS to_road,
               concat_ws(' | ', transform(p.destination_info, d -> d.destination_name.primary))
                 AS destinations,
               concat_ws(' | ', transform(p.route_info, r -> r.route_text.primary)) AS routes
        FROM signposts p
        JOIN roads f ON f.id = p.from_segment.id
        JOIN roads t ON t.id = p.to_segment.id
        WHERE f.road_class IN ('motorway', 'trunk') AND size(p.destination_info) > 0
        LIMIT 15
        """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC **Read the whole of an attribute array.** This notebook took the one element that covers
# MAGIC a whole segment for every vehicle. To keep the rest, explode the array and carry
# MAGIC `between` and `when` with each value. Speed limits for heavy goods vehicles, for example:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT s.id, s.names.primary AS name, l.max_speed.value, l.max_speed.unit,
# MAGIC        l.`between`, l.`when`.mode
# MAGIC FROM TomTom_Orbis_Maps.orbis_maps.segment s
# MAGIC LATERAL VIEW explode(s.speed_limits) t AS l
# MAGIC WHERE s.region = 'london' AND array_contains(l.`when`.mode, 'hgv')
# MAGIC ```
# MAGIC
# MAGIC **Parse geometry where you need it.** `geometry` is WKB: `shapely.from_wkb` in Python,
# MAGIC or `st_geomfromwkb` in SQL on a runtime with the `st_` functions. Filter on `bbox` before
# MAGIC either.
# MAGIC
# MAGIC **Combine with the traffic listings.** TomTom Traffic Stats and TomTom Traffic Volumes
# MAGIC cover the same regions, but their segment IDs are not Orbis IDs, so join by place rather
# MAGIC than by ID. Both carry `h3_r9`, the H3 cell of each segment's centre, and the same cell
# MAGIC comes from `bbox` here. The query below sets traffic signals against how far vehicles
# MAGIC drive through each cell: junction exposure, which neither dataset gives alone.
# MAGIC
# MAGIC The listings use separate catalogs. The query uses the suggested names; replace them with
# MAGIC yours. Total each dataset to one row per cell before joining them.
# MAGIC
# MAGIC ```sql
# MAGIC WITH signals AS (
# MAGIC   SELECT h3_longlatash3string((bbox.xmin + bbox.xmax) / 2, (bbox.ymin + bbox.ymax) / 2, 9)
# MAGIC            AS h3_r9,
# MAGIC          count(*) AS signals
# MAGIC   FROM TomTom_Orbis_Maps.orbis_maps.traffic_signal
# MAGIC   WHERE region = 'london'
# MAGIC   GROUP BY 1
# MAGIC ),
# MAGIC volume AS (
# MAGIC   SELECT h3_r9, sum(aadt * length_m) / 1000 AS vehicle_km_per_day
# MAGIC   FROM TomTom_Traffic_Volumes.traffic_volumes.aadt_segments
# MAGIC   WHERE region = 'london' AND vintage_year = 2025
# MAGIC   GROUP BY h3_r9
# MAGIC )
# MAGIC SELECT v.h3_r9, round(v.vehicle_km_per_day) AS vehicle_km_per_day, s.signals,
# MAGIC        round(1e6 * s.signals / v.vehicle_km_per_day, 1) AS signals_per_million_vehicle_km
# MAGIC FROM volume v JOIN signals s ON s.h3_r9 = v.h3_r9
# MAGIC ORDER BY v.vehicle_km_per_day DESC
# MAGIC LIMIT 20
# MAGIC ```
# MAGIC
# MAGIC **Posted limits are in Traffic Stats too**, as `speed_limit_kph` on its own segments.
# MAGIC Orbis adds the typical speed, the conditional limits by vehicle and time, and the signs
# MAGIC that announce them.
