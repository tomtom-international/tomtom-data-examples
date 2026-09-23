# Territory risk assessment

Rating areas, and individual routes, for road risk.

## The question

Motor insurers price by area, usually by postcode. A postcode can contain a motorway and a
cul-de-sac, so one rating has to cover roads that behave nothing alike.

Two things make a rating better. How much traffic a road carries says how many vehicles are
there to collide. How fast and how erratically that traffic moves says how bad a collision
would be. Neither is enough on its own.

## What the notebook does

It scores small areas of a few city blocks each. The score combines how much traffic an area
carries with how that traffic behaves. Behaviour means average speed, how much the speed
swings through the day, how often traffic is jammed, and how often it runs above the limit.
Every part of the score stays visible. You can see why an area scored the way it did, and
change the weights to match your own claims.

It then scores a single route the same way. Given the roads a driver used, it reports what
those roads are normally like. A road where nearly everyone speeds tells you about the road
rather than the driver. That is the baseline a driver should be measured against.

## Before you use it

The scores rank areas against each other within one sample, so they do not carry over to other
places. Claim frequency and claim severity need different weights, so score them separately.
Roads with no traffic estimate are missing from the data rather than quiet.

## What you need

The TomTom Traffic Stats and Traffic Volumes samples.
