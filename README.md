# TomTom traffic data sample notebooks

Runnable notebooks for TomTom traffic datasets — historical **traffic speed**, **travel times**
and **AADT traffic volumes** per road segment — published as free samples on the data
marketplaces and read here through Delta Sharing.

- **[`getting-started/`](getting-started/)** — one notebook per dataset. From a share you have
  just attached to a first result, in a few minutes.
- **[`use-cases/`](use-cases/)** — worked examples that answer a question end to end.
- **[`docs/`](docs/)** — how to get the data and run the notebooks on each platform.

Databricks is covered today. Microsoft Fabric is next.

## The datasets

| Dataset | What it measures | Documentation |
|---|---|---|
| TomTom Traffic Stats | Speed and travel time per road segment by hour, from anonymised probe vehicles: harmonic mean speed, speed percentiles, standard deviation, sample counts, and the road network the measurements sit on | [docs](https://docs.tomtom.com/traffic-stats/documentation/batch/introduction) |
| TomTom Traffic Volumes | Annual average daily traffic (AADT) per road segment, with day-of-week and hour-of-day profiles | [docs](https://docs.tomtom.com/historical-traffic-volumes/documentation/product-information/introduction) |

Both carry an H3 cell at resolution 9 on every segment, so they join to each other and to your
own data without map matching. Traffic Stats also carries OpenStreetMap way IDs, so the output of
a map matcher joins straight in.

## Getting the data

The data is not in this repository. Each dataset is published as a free sample on the data
marketplaces, and every notebook here expects one to be attached.

On Databricks Marketplace, search for either dataset name. The samples are free and instantly
available: accept the terms, keep the suggested catalog name, and the tables appear in it read
only. Step-by-step instructions are in **[docs/databricks.md](docs/databricks.md)**.

The samples are geographic extracts rather than whole countries, and coverage varies by road
class. Each listing description carries the measured figures, and the getting started notebooks
print them as they go.

For a wider extract, more history, or a commercial licence, contact
[TomTom sales](https://www.tomtom.com/contact-sales/).

## Running a notebook

Each notebook takes its catalog name as a parameter, defaulting to the name the marketplace
suggests at install. Accept the suggested name and nothing needs editing.

A filename is the notebook's subject and the platform it runs on, so
`traffic_stats_databricks.py` and `traffic_stats_fabric.ipynb` sit side by side in the same
folder. The analysis is the same in each; what differs is how the platform reads and displays
data, which is why they are separate files rather than one file with branches. The name has to
carry the subject because importing a notebook into a workspace drops its folder, and a
marketplace listing shows nothing but the name.

## Layout

```
docs/<platform>.md
getting-started/<dataset>/<dataset>_<platform>
use-cases/<use case>/<use case>_<platform>
```

## Licence and support

The code in this repository is Apache-2.0. It does not cover the data: use of the data is
governed by the
[TomTom terms and conditions](https://docs.tomtom.com/legal/terms-and-conditions).

Questions about a dataset or a marketplace listing: `marketplacesupport@tomtom.com`.
Problems with the code here: open an issue.
