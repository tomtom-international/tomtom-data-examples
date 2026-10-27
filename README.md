# TomTom Data Samples and Use Cases

Ready-to-run notebooks for the free TomTom data samples on Databricks Marketplace: hourly
traffic speeds per road (Traffic Stats), average daily traffic per road (Traffic Volumes), and
the TomTom Orbis road network in the Overture schema (Orbis Maps). Install a sample, import a
notebook, and **Run all**.

- **[`getting-started/`](getting-started/)**: one notebook per dataset that takes you from a
  freshly installed sample to a first result in minutes.
- **[`use-cases/`](use-cases/)**: notebooks that join the datasets to answer one industry
  question end to end.

## Use cases

Each one answers a single question end to end. It needs the Traffic Stats and Traffic Volumes
samples, and draws its maps on the roads of the Orbis Maps sample.

**Insurance**

- **[Territory risk assessment](use-cases/insurance/territory-risk-assessment/)**: where and when the risky kilometres are driven: above the speed limit, with a wide spread of speeds, or in a jam. It also shows how one driver's speed compares with the traffic on the same road. No claims data needed.
- **[Territory risk modeling](use-cases/insurance/territory-risk-modeling/)**: how far road data gets a pricing model for postcodes with no claims history, and which road features help.

## Getting started

Install a sample from Databricks Marketplace, then open its getting started notebook. The
[Databricks guide](docs/databricks.md) has the steps, the compute to use, and what to check when
a notebook fails.

| Dataset | What it measures | Notebook | TomTom docs |
|---|---|---|---|
| **Traffic Stats** | Hourly speed and travel time statistics for individual road segments, from anonymised probe vehicles. | [traffic-stats](getting-started/traffic-stats/) | [docs](https://docs.tomtom.com/traffic-stats/documentation/batch/introduction) |
| **Traffic Volumes** | Annual average daily traffic per road segment, with a profile for every hour of the week. | [traffic-volumes](getting-started/traffic-volumes/) | [docs](https://docs.tomtom.com/historical-traffic-volumes/documentation/product-information/introduction) |
| **Orbis Maps** | The TomTom Orbis road network in the Overture schema, with speed limits, speed profiles, signs and signals. | [orbis-maps](getting-started/orbis-maps/) | [Overture schema](https://docs.overturemaps.org/schema/reference/transportation/segment/) |

The samples are metropolitan extracts rather than whole countries. Traffic Stats and Traffic
Volumes cover London, Austin, Los Angeles and Melbourne. Orbis Maps covers the first three, on
the same extents, so the datasets join with each other, which is what the use cases do.

Want the samples on another platform? Open an issue and say which one.

## Licence and support

The code in this repository is Apache-2.0. It does not cover the data: use of the data is
governed by the
[TomTom terms and conditions](https://docs.tomtom.com/legal/terms-and-conditions).

Questions about a marketplace listing, a wider extract, more history, or a commercial licence:
[marketplacesupport@tomtom.com](mailto:marketplacesupport@tomtom.com).

Problems with the code here: open an issue.
