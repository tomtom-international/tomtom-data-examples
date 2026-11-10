# Territory risk modeling

How far road data gets a postcode with no claims history, and which parts of it help.

## The question

Insurers price motor risk by postcode, because that is how claims are recorded. The weak spot
is a postcode with no claims yet: a new market, a new development, a thin book. The model
there knows how big the place is and nothing else. Does road data fill the gap, and can you
say which part of it helped?

## What the notebook does

It keeps the postcode as the thing being predicted, and builds the road inputs one road at a
time. Each road counts for the traffic it carries, so a busy motorway is not averaged away by
the quiet streets around it.

The result is a short list of numbers per postcode, each one plain enough to read out loud.
How much traffic there is, how much of it is on major roads, and how fast it moves. How far
apart the speeds of vehicles on the same road are, how often traffic is jammed or above the
limit, and how much of the area the data covers.

The notebook then trains the same model five ways. Size of the postcode alone, then with
traffic volume, with all the road data, with past claims, and with both. Comparing them
shows how far road data gets a postcode with no history, what each dataset adds, and what
road data adds to a model that already knows the claims. It explains the no-history model
feature by feature, and ends with two postcodes of the same size with very different roads.

Claims are private, so the notebook downloads police-reported road collisions, open UK data,
and uses them in place of claims. Your own claims table replaces them in one step.

## Why it is built this way

- **Collisions stand in for claims.** They are the event behind most motor claims, and they
  are open, so anyone can rerun the notebook.
- **Postcodes come from the roads.** Orbis Maps gives every road its postcode, so each
  postcode is the area whose roads carry it, and its size is how many full postcodes they
  carry.
- **The size of the postcode is in every model.** It stands for policies in force, and it is
  the density measure that road features are accused of copying.
- **Two tests.** Held-out months, and held-out blocks of the map, so a tested postcode has no
  trained neighbour.
- **One week of road data.** The sample holds two months, and reading all of it gives the same
  result, so a week is the cheaper default.
- **What did not help is left out.** Weighting each hour by its traffic, and counting the
  vehicle-km driven over the limit or in jams as features, changed nothing measurable. The
  model already sees the traffic. The `territory-risk-assessment` notebook maps them instead.

## Before you use it

Collisions are not claims, although they are the event behind most motor claims. The road
data is treated as a fixed picture of the roads. Postcode outlines are drawn from the
postcodes on the roads, so they are close to the official ones but not the same. The terms are
defined in the
[insurance README](https://github.com/tomtom-international/tomtom-data-examples/blob/main/use-cases/insurance/README.md#terms).

## What you need

The TomTom Traffic Stats and Traffic Volumes samples, and the TomTom Orbis Maps sample,
whose roads give the postcodes and draw the map. The notebook downloads the collisions
itself. With the defaults, a run takes about seven minutes on serverless compute. As
a serverless job it uses 2 to 3 DBU, which is $1 to $1.50 at list price.
