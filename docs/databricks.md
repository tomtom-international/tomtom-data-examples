# Databricks

How to attach a TomTom dataset from Databricks Marketplace and run the notebooks in this
repository against it.

## Requirements

- A Databricks workspace enabled for Unity Catalog, on the Premium plan. A trial works; so does
  the free edition for browsing, though attaching a Marketplace share needs Unity Catalog.
- The `USE MARKETPLACE ASSETS` privilege on the metastore. It is granted to all users by default.

## 1. Attach the dataset

1. In the workspace sidebar, click **Marketplace**.
2. Search for **TomTom Traffic Stats** or **TomTom Traffic Volumes**.
3. Open the listing and click **Get instant access**.
4. Accept the terms. Under **More options** you can rename the catalog — **keep the suggested
   name** and the notebooks need no editing.
5. Click **Open** to see the dataset as a read-only catalog in Catalog Explorer.

The suggested catalog names are `TomTom_Traffic_Stats` and `TomTom_Traffic_Volumes`, which are
the defaults every notebook here uses. Unity Catalog resolves names case-insensitively, so the
casing does not matter.

Each dataset arrives as one catalog containing one schema:

| Dataset | Catalog | Schema | Tables |
|---|---|---|---|
| Traffic Stats | `TomTom_Traffic_Stats` | `traffic_stats_batch` | `segments`, `hourly_stats` |
| Traffic Volumes | `TomTom_Traffic_Volumes` | `traffic_volumes` | `aadt_segments`, `coverage` |

## 2. Get the notebooks

**Clone this repository as a Git folder.** This is the quickest route and gives you every
notebook at once.

1. In the sidebar, click **Workspace**, then **Create > Git folder**.
2. Git repository URL: `https://github.com/tomtom-international/tomtom-data-examples`
3. Git provider: **GitHub**. Leave the folder name as it is.
4. Click **Create Git folder**.

No credentials are needed — the repository is public. The `.py` files open as notebooks, and
**Pull** in the Git folder brings in any later changes.

| Notebook | Needs |
|---|---|
| `getting-started/traffic-stats/traffic_stats_databricks.py` | Traffic Stats |
| `getting-started/traffic-volumes/traffic_volumes_databricks.py` | Traffic Volumes |
| `use-cases/territory-risk-assessment/territory_risk_assessment_databricks.py` | both |

Individual notebooks are also attached to the Marketplace listings under **Sample notebook**,
where **Preview notebook** then **Import notebook** copies one into your workspace.

## 3. Run

Attach the notebook to serverless compute, or to a cluster on Databricks Runtime 16.2 or later.

Each notebook opens with widgets for the catalog names and, where relevant, a date and a region.
The defaults match the suggested catalog names and a date present in the sample, so **Run all**
works without changing anything.

The use-case notebooks draw maps with [folium](https://python-visualization.github.io/folium/).
If the import fails, run `%pip install folium` in the first cell.

## Troubleshooting

**`Cannot read <catalog>.<schema>`** — the notebook could not see the schema. Either the dataset
is not attached yet, or the catalog was given a different name at install. Set the catalog widget
to the name in Catalog Explorer.

**A use-case notebook needs both datasets.** `use-cases/territory-risk-assessment` joins Traffic
Stats to Traffic Volumes and needs both attached. They overlap on London, which is the area the
notebook uses.

**`DESCRIBE HISTORY` fails.** Expected. History is not shared, so the Delta commit log is not
available on a shared table. Nothing in these notebooks depends on it.

**Empty results for a date or region.** The samples are extracts. The getting started notebooks
print the dates and regions actually present — run those first.
