# Use cases

A worked example that answers one real question end to end. One folder each.

What a folder holds:

```
<use-case>/
  README.md         the question, the approach, and what the answer turned out to be
  databricks.py     one notebook per platform it has been run on
  fabric.ipynb
```

The README is the part that matters. Code that runs is the easy half; the reason a particular
join, threshold or weighting was chosen is what someone reading this actually needs, and it is
the half that never survives in comments alone. Say what the numbers came out as, so a reader
can tell whether their own run went wrong.

Two conventions worth keeping:

- **Name the caveat that would invalidate the result.** Every one of these datasets has one, and
  a worked example that quietly steps around it teaches the wrong lesson. Coverage by road class,
  segments absent rather than zero, arrays whose order matters.
- **Keep the scan small enough to be free.** These are samples, but they are not tiny: the
  hourly speed table is 322 million rows. Filter to a partition unless the example needs more,
  and say why when it does.

**First up: territory risk assessment.** Scoring geographic areas for insurance risk from traffic
speed and volume, which is the question both data products were published to answer.
