# Databricks

Each Marketplace listing explains how to get the data and carries its getting started
notebook. This page covers what the listings don't: the use case notebooks, which live only in
this repository, the compute to run them on, and what to check when a notebook fails.

## 1. Get the data

You need a Databricks workspace with Unity Catalog. Free accounts work.

In the workspace sidebar, click **Marketplace**, search for **TomTom Traffic Stats** or
**TomTom Traffic Volumes**, and click **Get instant access** on the listing. The use cases need
both.

**Keep the suggested catalog name** when you install. Every notebook here defaults to it, so
none of them needs editing. Unity Catalog ignores case in names.

| Dataset | Catalog | Schema | Tables |
|---|---|---|---|
| Traffic Stats | `TomTom_Traffic_Stats` | `traffic_stats_batch` | `segments`, `hourly_stats` |
| Traffic Volumes | `TomTom_Traffic_Volumes` | `traffic_volumes` | `aadt_segments`, `coverage` |

## 2. Get the notebooks

**Getting started notebooks** come with each listing. Under **Sample notebook**, click
**Preview notebook**, then **Import notebook**.

**Use case notebooks** are only here. To get them all and keep them up to date, add this
repository to your workspace as a Git folder:

1. In the workspace sidebar, click **Workspace**, then **Create** > **Git folder**.
2. Paste `https://github.com/tomtom-international/tomtom-data-examples` and click
   **Create Git folder**.
3. To get newer versions later, open the folder and click **Pull**.

To take just one notebook, download its `_databricks.py` file and use **Import** in the
workspace.

## 3. Run

Attach the notebook to serverless compute, or to a cluster on Databricks Runtime 16.2 or later.

Each notebook opens with widgets for the catalog names and, where relevant, a date and a region.
The defaults match the suggested catalog names and data present in the samples, so **Run all**
works without changing anything. Each use case README says how long a run takes and what it
costs.

## Troubleshooting

**`Cannot read <catalog>.<schema>`**: the notebook could not see the schema. Either the dataset
is not attached yet, or the catalog was given a different name at install. Set the catalog widget
to the name in Catalog Explorer.

**Empty results for a date or region**: the samples are extracts. The getting started notebooks
print the dates and regions actually present, so run those first.
