# Getting started

One notebook per data product. Each takes you from a share you have just attached to something
worth looking at, and each runs unedited if you accepted the catalog name the marketplace
suggested.

| Folder | Product | Platform |
|---|---|---|
| `traffic-stats/` | TomTom Traffic Stats | Databricks |
| `traffic-volumes/` | TomTom Traffic Volumes | Databricks |

## These files are also attached to the listings

Each Marketplace listing carries a **copy** of its notebook, taken at the moment it was attached.
Marketplace stores that copy in its own storage; it is not a link back here. So editing a file in
this folder does not change what a listing hands out.

Which means a change here needs a re-attach on the listing to reach a consumer: open the listing
in the Provider console, replace the notebook, and walk to the end of the wizard and publish,
because a draft is not what consumers see.

That is the price of the listing notebook being a reviewed artefact rather than whatever is on
`main` today, and it is worth paying for two files that change rarely. It is why `use-cases/`
works the other way round: those are linked, not attached, so they can move freely.

## What is fixed and what is not

Fixed, because a consumer meets these with no context:

- **They run unedited.** No credentials, no setup step, no file to download first.
- **They fail with an instruction.** If the catalog name is wrong, say so and name the parameter
  to change, rather than failing several cells later with a table not found.
- **They name the caveats.** Coverage is by road class rather than uniform, hours are UTC,
  segments without an estimate are absent rather than zero.

Not fixed: the analysis. Show something a data scientist would actually do with the data, not a
tour of the columns.
