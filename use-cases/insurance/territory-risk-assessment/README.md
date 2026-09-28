# Territory risk assessment

Where, and when, the risky kilometres are driven.

## The question

Motor insurers price by area, usually by postcode. A postcode can contain a motorway and a
cul-de-sac, so one rating has to cover roads that behave nothing alike. What a rating needs
is how much driving happens in a place, and how that driving goes.

## What the notebook does

It measures risky kilometres: vehicle-km per day driven above the speed limit, with a wide
spread of speeds between vehicles, or in a jam. Traffic Volumes gives the driving in each
hour of the week. Traffic Stats gives how the traffic on each road drove in that hour.
Neither dataset can do this alone.

It then shows the result two ways. A week, hour by hour, shows when each kind of risky
driving happens. Two maps side by side show where the driving is, and how much of it is over
the limit. The two maps do not look alike.

Last, it reads a driver against the road. A trip reaches the data through OpenStreetMap way
IDs, with no second map matching step. The notebook places one speed on the speed
distribution of two roads at two hours of the day. The same speed is ordinary on one and
faster than 95% of the traffic on the other.

## Why it is built this way

- **No single score.** An earlier version added the conditions into one number with weights
  nobody had measured. The notebook now keeps them apart and leaves the weighting to the
  `territory-risk-modeling` notebook, which measures it against collisions.
- **Unmeasured is not safe.** An hour measured from a single vehicle has no speed spread and
  no percentiles, so it is left out. The driving in it counts as unmeasured.
- **Real data only.** Telematics trips are private, so the driver comparison uses one speed
  in two real places. It does not invent a trip.
- **One week, Monday to Sunday.** That covers every hour of the week once.

## Before you use it

The conditions describe roads, not drivers. They rank places within one sample, so compare
regions with care. A jam is traffic below 60% of the limit, which in towns includes
stop-start traffic at junctions. Roads with no traffic estimate are missing from the data,
not quiet. The terms are defined in the [insurance README](../README.md#terms).

## What you need

The TomTom Traffic Stats and Traffic Volumes samples. With the defaults, a run takes about
four and a half minutes on serverless compute. As a serverless job it uses about 4 DBU, which
is about $2 at list price.
