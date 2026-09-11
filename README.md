# TomTom data examples

Notebooks for working with TomTom data products on Databricks, Microsoft Fabric and Snowflake.

Two kinds of thing live here, and they have different jobs.

**`getting-started/`** is the five minute on-ramp: one notebook per data product, taking you from
a share you have just attached to a result worth looking at. These run unedited. Each Marketplace
listing carries a copy of its own notebook, so most people meet these without ever seeing this
repository.

**`use-cases/`** is what comes after: worked examples that answer a real question end to end,
with the reasoning written down rather than just the code.

## Getting the data

The data itself is not in this repository. It is published as free samples on the data
marketplaces, and every notebook here assumes you have attached one:

| Product | What it is | Documentation |
|---|---|---|
| TomTom Traffic Stats | Historical hourly speeds per road segment, measured from anonymised probe vehicles | [docs](https://docs.tomtom.com/traffic-stats/documentation/batch/introduction) |
| TomTom Traffic Volumes | Annual average daily traffic per road segment, with a day and hour profile | [docs](https://docs.tomtom.com/historical-traffic-volumes/documentation/product-information/introduction) |

Search either name on Databricks Marketplace. The samples are free and instantly available: you
accept the terms, name a catalog, and the tables appear in it read only.

The samples are geographic extracts rather than whole countries, and coverage is by road class
rather than uniform, which is the first thing that trips people up. Each listing description
carries the measured figures, and the getting started notebooks print them on the way past.

To discuss a wider extract, more history, or a commercial licence, contact
[TomTom sales](https://www.tomtom.com/contact-sales/).

## Running the notebooks

Each notebook takes the catalog name as a parameter, defaulting to the name the marketplace
suggests when you install the listing. Accept the suggested name and nothing needs editing;
change the parameter if you called it something else.

Where a notebook exists for more than one platform, the analysis is the same and the difference
is how the platform reads and displays data. They are separate files rather than one file with
branches, because the notebook formats and the I/O libraries genuinely differ and a single file
pretending otherwise would be readable on none of them.

## Layout

```
getting-started/<product>/      one notebook per product, per platform
use-cases/<use case>/           a worked example, with a README explaining the question
```

A use case brings its own notebook rather than extending someone else's.

## Licence and support

Use of the data is governed by the
[TomTom terms and conditions](https://docs.tomtom.com/legal/terms-and-conditions).

Questions about a dataset or a marketplace listing: `marketplacesupport@tomtom.com`.
Problems with the code here: open an issue.
