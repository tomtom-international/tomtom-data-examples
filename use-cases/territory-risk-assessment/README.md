# Territory risk assessment

Rating geographic areas, and individual routes, for road risk from traffic speed and volume


## The question

Motor insurers rate areas. They usually use postcodes, but a postcode can include both a motorway
and a cul-de-sac. This means the rating averages roads with very different conditions.

Two factors make this rating better. **Volume** measures exposure: how many vehicles could be involved
in a collision. **Speed** measures severity and behaviour: how fast and erratically vehicles travel,
and how often they exceed the limit. Neither factor alone gives a complete risk model.

## The approach

**Territory score.** Both datasets use H3 resolution 9 cells. Each cell covers about 0.1 km². We
scale and combine five factors: total AADT, average speed, speed changes, speeding rate, and congestion. The weights are examples and should be tested against claims history.

**Route score.** `segments.osm_way_ids` lists the OpenStreetMap ways for each segment. A map matcher such as [Fast Map Matching](https://github.com/cyang-kth/fmm) can use this output directly. This lets us score a telematics trace against the roads it used.

## What the numbers come out as

Two types of cells score highly, for different reasons:

| | Mean AADT | Mean speed | Hours speeding | Hours congested |
|---|---|---|---|---|
| Motorway speeds | 82,600 | 91.8 km/h | 61.2% | 2.6% |
| Arterial speeds | 20,000 | 55.4 km/h | 28.2% | 33.0% |
| Urban speeds | 47,100 | 29.2 km/h | 26.4% | 52.3% |

The example route covers 26 km of the M20 motorway. It averages 105 km/h, with the 85th percentile
above the limit in 94% of hours and almost no congestion. Frequent speeding on this road is a road
feature. It provides a baseline for measuring drivers.

## Caveats that change the answer

- Scores are scaled within this extract. They rank cells against each other but cannot be
  compared with other areas.
- Claims frequency and severity need different weights. Separate scores are usually better than
  one combined score.
- `traffic_volumes.coverage` shows the share of each road class with an estimate. Segments without
  an estimate are absent, not zero. Check this before applying the results to a whole network.
- The default score uses one date. Weekday and weekend patterns differ, so combining them hides
  important details.
