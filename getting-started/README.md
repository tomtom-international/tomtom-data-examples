# Getting started

One notebook per data product. Each takes you from a share you have just attached to a first
result, and runs unedited if you accepted the catalog name the marketplace suggested at install.

| Folder | Product | Platforms |
|---|---|---|
| `traffic-stats/` | TomTom Traffic Stats | Databricks |
| `traffic-volumes/` | TomTom Traffic Volumes | Databricks |

Each notebook covers what is in the tables and walks through an exploration of them: coverage
first, then distributions, then the temporal patterns, then the same roads on a map. They are
open explorations rather than worked models. For a scored, end-to-end answer see `use-cases/`.

Charts use matplotlib, seaborn and folium, all of which ship with Databricks Runtime except
folium, which each notebook installs in its first cell along with shapely and h3.
