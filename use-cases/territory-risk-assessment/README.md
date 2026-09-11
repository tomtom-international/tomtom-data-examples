# Territory risk assessment

Rating geographic areas, and individual routes, for road risk from traffic speed and volume.

**Data needed:** TomTom Traffic Stats and TomTom Traffic Volumes, both attached. They overlap on
London, which is the area used here.

## The question

Motor insurers price territory. The usual unit is a postcode, which can contain both a motorway
and a cul-de-sac, so the rating is an average over roads that behave nothing like each other.

Two ingredients make a better one. **Volume** is exposure: how many vehicles are there to collide
with. **Speed** is severity and behaviour: how fast, how erratic, how often above the limit.
Neither alone is a risk model.

## The approach

**Territory score.** Both datasets carry an H3 cell at resolution 9, roughly 0.1 km², so they
join on one string comparison with no map matching. Five factors are scaled across the extract
and weighted: total AADT for exposure, then mean speed, variability, speeding rate and congestion
rate from the hourly speeds. The weights are illustrative and want calibrating against claims
history.

**Route score.** `segments.osm_way_ids` holds the OpenStreetMap ways each segment maps to, so the
output of a map matcher such as [Fast Map Matching](https://github.com/cyang-kth/fmm) joins
straight in and a telematics trace can be scored against the roads it actually used.

## What the numbers come out as

Two kinds of cell score highly, for opposite reasons:

| | Mean AADT | Mean speed | Hours speeding | Hours congested |
|---|---|---|---|---|
| Motorway speeds | 82,600 | 91.8 km/h | 61.2% | 2.6% |
| Arterial speeds | 20,000 | 55.4 km/h | 28.2% | 33.0% |
| Urban speeds | 47,100 | 29.2 km/h | 26.4% | 52.3% |

The highest scoring cells are central London at 1.3 to 1.6 million AADT and 15 to 25 km/h, with
motorway cells close behind at around 460,000 AADT and over 100 km/h.

The example route, 26 km of the M20 motorway, averages 105 km/h with the 85th percentile above
the limit in 94% of hours and congestion in almost none. Near-universal speeding on a road like
that is a property of the road, which is what makes it a baseline to measure a driver against.

## Caveats that change the answer

- Scores are scaled within this extract, so they rank cells against each other and are not
  comparable across releases.
- Claims frequency and claims severity want these factors weighted differently. Scoring them
  separately is usually better than one combined number.
- `traffic_volumes.coverage` gives the share of each road class with an estimate. Segments without
  one are absent rather than zero, so read it before generalising to a whole network.
- One date is scored by default. Weekday and weekend patterns differ enough that pooling them
  hides both.
