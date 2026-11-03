# Insurance

Road data for motor insurance pricing.

Both use cases start from the same problem. A postcode can hold a motorway and a cul-de-sac,
so one rating has to cover roads that behave nothing alike.

| Use case | What it answers |
|---|---|
| [`territory-risk-assessment/`](territory-risk-assessment/) | Where and when are the risky kilometres driven, and how does a driver compare with the road? No claims needed, any of the four regions. |
| [`territory-risk-modeling/`](territory-risk-modeling/) | How far does road data get a postcode with no claims history, and which parts of it help? |

Both need the TomTom Traffic Stats and Traffic Volumes samples.

## Terms

Both notebooks use these words the same way.

| Term | Meaning |
|---|---|
| Map cell | An [H3](https://h3geo.org/) cell at resolution 9, about a tenth of a square kilometre. Both datasets carry it, so they join on it. |
| Vehicle-km per day | Traffic times road length, summed over the roads in an area. It says how much driving happens there. |
| Speed spread | How far apart vehicles on the same road drive in the same hour: the standard deviation of their speeds over the mean. It is not how speed changes through the day. |
| Share over the limit | The share of vehicles above the posted limit in an hour, read from that hour's 19 speed percentiles. |
| Wide speed spread | A speed spread above 0.4: vehicles' speeds differ by more than 40% of their average. About a quarter of measured hours. |
| Jam | Average speed below 60% of the limit. In towns this includes stop-start traffic at junctions. |
| Risky kilometres | Vehicle-km per day driven in one condition: above the limit, with a wide speed spread, or in a jam. Each hour counts for the traffic it carries. |
