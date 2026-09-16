# TomTom Data Samples and Use Cases

This repo holds ready to run notebooks for TomTom data published as free samples on data marketplaces like databricks.

- **[`getting-started/`](getting-started/)**: one notebook per dataset and per marketplace to get you started in minutes.
- **[`use-cases/`](use-cases/)**: tailored notebook examples that answer practical real world scenarios end to end.


## Datasets

| Dataset | What it measures | Documentation |
|---|---|---|
| TomTom Traffic Stats | Hourly speed and travel time statistics for individual road segments, from anonymised probe vehicles. | [docs](https://docs.tomtom.com/traffic-stats/documentation/batch/introduction) |
| TomTom Traffic Volumes | Annual average daily traffic per road segment, with a profile for every hour of the week. | [docs](https://docs.tomtom.com/historical-traffic-volumes/documentation/product-information/introduction) |

## Getting the data

Each dataset is published as a free sample on different data marketplaces, and every notebook here expects one to be attached. To get started with step by step instructions for getting access to the data and running notebooks in each platform:

- **[`docs/databricks`](docs/databricks)**
- soon more platforms to come.

The samples are geographic extracts rather than whole countries, and samples are mean to overlap in regions provided so joined dataset uses cases are possible.

For a wider extract, more history, or a commercial licence, contact `marketplacesupport@tomtom.com`.

## Licence and support

The code in this repository is Apache-2.0. It does not cover the data: use of the data is
governed by the
[TomTom terms and conditions](https://docs.tomtom.com/legal/terms-and-conditions).

Questions about a marketplace listing, a wider extract, more history, or a commercial licence, contact: `marketplacesupport@tomtom.com`.

Problems with the code here: open an issue.
