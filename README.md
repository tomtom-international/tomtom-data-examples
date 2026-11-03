# TomTom Data Samples and Use Cases

This repo holds ready to run notebooks for TomTom data published as free samples on data marketplaces like databricks.

- **[`getting-started/`](getting-started/)**: one notebook per dataset and per marketplace to get you started in minutes.
- **[`use-cases/`](use-cases/)**: tailored notebook examples that answer practical real world scenarios end to end.

## Use cases

Each one answers a single question end to end and needs both the Traffic Stats and Traffic Volumes samples.

**Insurance**

- **[Territory risk assessment](use-cases/insurance/territory-risk-assessment/)**: where and when the risky kilometres are driven: above the speed limit, with a wide spread of speeds, or in a jam. It also shows how one driver's speed compares with the traffic on the same road. No claims data needed.
- **[Territory risk modeling](use-cases/insurance/territory-risk-modeling/)**: how far road data gets a pricing model for postcodes with no claims history, and which road features help.

## Getting started

Each dataset is published as a free sample on different data marketplaces, and every notebook here expects one to be attached. To get started with step by step instructions for getting access to the data and running notebooks in each platform:

| Dataset | What it measures | TomTom docs | Databricks |
|---|---|---|---|
| **Traffic Stats** | Hourly speed and travel time statistics for individual road segments, from anonymised probe vehicles. | [docs](https://docs.tomtom.com/traffic-stats/documentation/batch/introduction) | [docs](docs/databricks.md) |
| **Traffic Volumes** | Annual average daily traffic per road segment, with a profile for every hour of the week. | [docs](https://docs.tomtom.com/historical-traffic-volumes/documentation/product-information/introduction) | [docs](docs/databricks.md) |


The samples are geographic extracts rather than whole countries, and samples are meant to overlap in regions provided so joined dataset uses cases are possible.

Would you like to access this on another platform? Let us know which one, as we grow our presence on platforms continuously to make it as easy as possible for you.

## Licence and support

The code in this repository is Apache-2.0. It does not cover the data: use of the data is
governed by the
[TomTom terms and conditions](https://docs.tomtom.com/legal/terms-and-conditions).

Questions about a marketplace listing, a wider extract, more history, or a commercial licence, contact: `marketplacesupport@tomtom.com`.

Problems with the code here: open an issue.
