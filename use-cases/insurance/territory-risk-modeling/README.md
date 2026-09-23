# Territory risk modeling

Beyond postcode averages: road data in a claims model, explained.

## The question

Insurers price motor risk by postcode, because that is how claims are recorded. A postcode can
contain a motorway and a cul-de-sac, and an average over its roads describes neither. Does
detailed road data make the model better, and can you say which part of it helped?

## What the notebook does

It keeps the postcode as the thing being predicted, and builds the road inputs one road at a
time. Each road counts for the traffic it carries, so a busy motorway is not averaged away by
the quiet streets around it.

The result is a short list of numbers per postcode, each one plain enough to read out loud.
How much traffic there is, how much of it is on major roads, and how fast it moves. How much
the speed swings, how often traffic is jammed or above the limit, and how much of the area
the data covers.

The notebook then trains the same model four ways: on the size of the postcode alone, with
road data, with past claims, and with both. Comparing them shows what road data adds to a
model that already knows the claims history. It also shows how far road data gets you where
there is no history at all, which is where it helps most, along with serious collisions. The
notebook reports which signals mattered most, and ends with two postcodes that look alike on
paper but have very different roads.

Claims are private, so the notebook downloads police-reported road collisions and postcode
locations, both open UK data, and uses collisions in place of claims. Your own claims table
replaces them in one step.

## Before you use it

Collisions are not claims, although they are the event behind most motor claims. The
notebook reads one week of the road sample and treats it as a fixed picture of the roads. The
sample holds two months, and reading all of it gives the same result, so a week is the cheaper
default. Postcode outlines are not open data, so each postcode is drawn from the postcode
locations inside it.

## What you need

The TomTom Traffic Stats and Traffic Volumes samples. The notebook downloads the open UK data
itself.
