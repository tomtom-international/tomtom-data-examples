# Databricks

Here is the guide to attach a TomTom dataset from Databricks Marketplace and run the notebooks in this repository against it.

## Requirements

- A Databricks workspace enabled for Unity Catalog (both premium and free accounts work).

## 1. Attach the dataset

1. In the workspace sidebar, click **Marketplace**.
2. Search for **TomTom Traffic Stats** or **TomTom Traffic Volumes**.
3. Open the listing and click **Get instant access**.
4. Accept the terms. Under **More options** you can rename the catalog (**keep the suggested
   name** and the notebooks need no editing).
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

Each listing carries its notebooks under **Sample notebook**. Click **Preview notebook**, then
**Import notebook**, and it lands in your workspace ready to run.

| Notebook | Attached to | Datasets it needs |
|---|---|---|
| `traffic_stats_databricks` | TomTom Traffic Stats | Traffic Stats |
| `traffic_volumes_databricks` | TomTom Traffic Volumes | Traffic Volumes |
| `territory_risk_assessment_databricks` | both listings | both |

The same notebooks are the `.py` files in this repository, one folder per subject. You can connect your github account to databricks and checkout the latest version of this repo to get all the notebooks inside your workspace.

## 3. Run

Attach the notebook to serverless compute, or to a cluster on Databricks Runtime 16.2 or later.

Each notebook opens with widgets for the catalog names and, where relevant, a date and a region.
The defaults match the suggested catalog names and a date present in the sample, so **Run all**
works without changing anything.

## Troubleshooting

**`Cannot read <catalog>.<schema>`**: the notebook could not see the schema. Either the dataset
is not attached yet, or the catalog was given a different name at install. Set the catalog widget
to the name in Catalog Explorer.

**A use-case notebook needs both datasets.** `use-cases/territory-risk-assessment` joins Traffic
Stats to Traffic Volumes and needs both attached. They cover the same four metropolitan areas,
and the notebook picks one with a widget.

**Empty results for a date or region.** The samples are extracts. The getting started notebooks
print the dates and regions actually present, run those first.
