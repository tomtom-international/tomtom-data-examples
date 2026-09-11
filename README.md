# TomTom data examples

Notebooks for working with TomTom traffic data on Databricks, Microsoft Fabric and Snowflake.

- **`getting-started/`** — one notebook per data product. From a share you have just attached to
  a first result, in a few minutes.
- **`use-cases/`** — worked examples that answer a question end to end.

## Getting the data

The data is not in this repository. It is published as free samples on the data marketplaces, and
every notebook here expects one to be attached.

| Product | What it is | Documentation |
|---|---|---|
| TomTom Traffic Stats | Hourly speed and travel time statistics per road segment, measured from anonymised probe vehicles | [docs](https://docs.tomtom.com/traffic-stats/documentation/batch/introduction) |
| TomTom Traffic Volumes | Annual average daily traffic per road segment, with a day and hour profile | [docs](https://docs.tomtom.com/historical-traffic-volumes/documentation/product-information/introduction) |

On Databricks Marketplace, search either name. The samples are free and instantly available: you
accept the terms, name a catalog, and the tables appear in it read only.

The samples are geographic extracts rather than whole countries, and coverage varies by road
class. Each listing description carries the measured figures, and the getting started notebooks
print them as they go.

For a wider extract, more history, or a commercial licence, contact
[TomTom sales](https://www.tomtom.com/contact-sales/).

## Running a notebook

Each notebook takes its catalog name as a parameter, defaulting to the name the marketplace
suggests at install. Accept the suggested name and nothing needs editing.

The platform is the filename, so `databricks.py` and `fabric.ipynb` sit side by side in the same
folder. The analysis is the same in each; what differs is how the platform reads and displays
data, which is why they are separate files rather than one file with branches.

## Layout

```
getting-started/<product>/<platform>
use-cases/<use case>/<platform>
```

## Licence and support

Use of the data is governed by the
[TomTom terms and conditions](https://docs.tomtom.com/legal/terms-and-conditions).

Questions about a dataset or a marketplace listing: `marketplacesupport@tomtom.com`.
Problems with the code here: open an issue.
