# Demo script: what's on screen

![River Air flight recovery architecture](./architecture.png)

Source: [`docs/architecture.excalidraw`](./architecture.excalidraw). Re-export the PNG after editing it.

This is the full demo script. Each row marked **▶ On screen** follows the script row that mentions it. It names the thing to show and the exact query or click path that shows it. The [data model](./data-model.md) defines the fields, and the [walkthrough](./walkthrough.md) covers deploy and run.

**Status tags:** ✅ built and works today · 🧪 built and passes local checks, but not yet run on Confluent Cloud · ⚠️ built, but it doesn't match the script yet (see [Gaps](#gaps-to-close-before-rehearsal)) · 🚧 not built; the query shown is a draft that hasn't been run.

**Keys:** the live day is the day the stream runs. The examples use 2026-09-28, so the main flight is `RA417-20260928`. `P-0928-417-081` is one of its passengers who misses a connection: RA417 from ORD in seat 19F, missing RA680 (seat 21C) to PHX, rebooked onto tomorrow's `RA605-20260929`. Swap in the date of your run.

**Lightning queries** are the exact SQL that `uv run airport-app` sends. Each returns at most 200 rows. The time window is now − 18 h to now + 6 h, shown here for a run at 17:00 UTC.

**Stream clock**, counted from when `uv run deploy` or `uv run airport-datagen` starts the live stream:

| Minutes | What happens |
| --- | --- |
| +5 to +25 | RA417's delay grows every 30 seconds, up to 115 minutes. |
| +6 to +16:30 | RA417's passengers go HIGH one destination at a time, about once a minute, until all 150 are at risk. |
| +40 | The Grand Hyatt at SFO sells out. |
| +90 | The stream stops. |

Start the stream at least 40 minutes before scene 2.4.

**Recording Demo 2 live:** run `uv run reset-demo-2` first. It removes what 2.3 creates (the Native Inference model, the `hotel_inventory_live_context` tool, the agent, and the `AI_RUN_AGENT` INSERT), so 2.3a–c run from a workspace instead of **Flink → Statements**, in the order of SQL 32, 29, 30, 31. Demo 1's tables, Lightning, RTCE, and the Bedrock model stay. The reset also restarts the stream and prints the clock above in UTC.

| Timing | ID | Visual | Narration |
| :---: | :---: | ----- | ----- |
| **Demo 1 Streamhouse for Apps: Always on, Always Current view of the operations (3 mins)** |  |  |  |
|  |  | Intro slide / plane-related graphics. | As Shaun mentioned earlier, one of the biggest challenges in the airline industry is something we can all relate to: **getting a single, reliable view of what’s actually happening with a flight.** The app says one thing. The airport website says another. And the flight tracker says something completely different. Meet **River Air**, a new airline business built to change that. Behind the scenes, River Air has the same fundamental problem. Its data is being created and stored across dozens of different places. Passenger and ticketing information lives in one system. Gate assignments, runway times, and flight status live in another,  often owned by the airport. Check-in and boarding data lives somewhere else. And crew assignments and operational decisions are managed in yet another system. These systems weren’t designed to work together. The airline owns part of the picture. The airport owns another part. Ground handlers own another. One simple question **“What’s our on-time performance today?”** suddenly becomes a data integration project. The challenge is bringing all of these siloed data sources together into one unified, real-time view of the business,  **right now. The Streamhouse architecture on Confluent’s Data Streaming Platform makes that possible. Transition**: Cue the demo please |
| ▶ | 1.0a | **On screen:** intro slide and plane graphics | Slides, not demo data. |
| ▶ | 1.0b | **On screen (optional):** “What’s our on-time performance today?” | ✅ The counts bar at the top of the app answers it later: **Flights today 180 · Delayed N**. Both counts come from the `flight_status` query in 1.3a. Delayed means an estimate 15 or more minutes past schedule. |
| 1 min | 1.1 | Start with Topic UI showing many topics Open Passenger Data and show one message Open Flight status one and open one message | So, here you can see data coming in from all of our different systems, in real time.  Let me highlight two.  First, **passenger itineraries**, coming directly from our ticketing systems, showing us who’s flying and which flight they’re on.  And then **flight status**, a universal, real-time data product showing  data from the Airport Operational Databases across our network. So for any flight, we can see **where it’s coming from, where it’s going, and its latest status — all in one place. Transition**: Now look at this….. |
| ▶ | 1.1a | **On screen:** Topic UI showing many topics | ⚠️ Confluent Cloud → the cluster → **Topics**. It lists 6 topics: `flight_status`, `passenger_itineraries`, `hotel_inventory`, `passenger_state`, `flight_impact` and `passenger_recommendations`. The script says "many topics", and 6 may look thin. |
| ▶ | 1.1b | **On screen:** one passenger itinerary message: “who’s flying and which flight they’re on” | ✅ **Topics → `passenger_itineraries` → Messages**, then filter by key `P-0928-417-081`. The key is who; the value is which flights and seats:<br>`{"inbound_flight_id": "RA417-20260928", "inbound_seat": "19F", "connecting_flight_id": "RA680-20260928", "connecting_seat": "21C"}` |
| ▶ | 1.1c | **On screen:** one flight status message: “where it’s coming from, where it’s going, and its latest status” | ✅ **Topics → `flight_status` → Messages**, then filter by key `RA417-20260928`. The newest message looks like:<br>`{"origin": "ORD", "destination": "SFO", "scheduled_time": "…17:50", "estimated_time": "…19:45", "status": "DELAYED"}`<br>New messages arrive about once a minute per flight. |
| 0.5 min | 1.2 | Enable LT on Passenger Enable LT on Flight status | With just a few clicks in the UI, we can enable **Lightning Tables** on both of these topics.  Now that same real-time data is available for low-latency querying.  **Transition**: So instead of just viewing the data, we can start querying it instantly and  build an **always-on** dashboard that shows us the current state of the business, in real-time. |
| ▶ | 1.2a | **On screen:** enable Lightning Tables on `passenger_itineraries` and `flight_status` | ✅ **Topics → the topic → Lightning Tables**. `uv run deploy` has already turned it on for all 6 topics, so show the setting as on and narrate the click. |
| ▶ | 1.2b | **On screen (optional):** “start querying it instantly” | ✅ One Lightning query in the console:<br>``SELECT * FROM `flight_status` WHERE "KEY" = 'RA417-20260928' LIMIT 1`` |
| 1.5 mins | 1.3 | Show dashboard Drill into one flight \[OPTIONAL\]Show passenger app (Figma \- design team) Show flight with 150 affected passengers | On the left, you can see the **latest status of every flight**. On the right, we’re highlighting the flights with the **biggest delays** and, more importantly, how many passengers are affected and may miss their connections. And we can drill down into any flight to see **exactly which passengers are impacted and where they’re going.** And all of this is **updated in real time**, with low-latency queries serving our operations teams. Because we have Lightning Tables enabled, we can expose the same real-time view to our passengers. And here’s where it gets really interesting. We can see that one delayed flight has **150 passengers impacted**, many of whom may miss their connecting flights. Now the question becomes: **how do we recover those passengers?** Which flights can we rebook them onto? How do we handle their bags? And who needs a hotel? That’s where **Streamhouse for AI** comes in. **Transition**: More on this a little bit later. For now, let’s jump back to the slides. |
| ▶ | 1.3a | **On screen:** dashboard, left side: “latest status of every flight” | ✅ App at http://127.0.0.1:8000, **Flights** table (Flight · Route · Scheduled · Status · Delay · At risk). It shows one service day, 180 flights, and opens on the flights around now. Status and delay come straight from `flight_status`, so they change within seconds; **At risk** comes from Flink's `flight_impact`. Both use the same window:<br>``SELECT * FROM `flight_status` WHERE scheduled_time >= TIMESTAMP '2026-09-27 23:00:00' AND scheduled_time < TIMESTAMP '2026-09-28 23:00:00' ORDER BY scheduled_time ASC LIMIT 200``<br>``SELECT * FROM `flight_impact` WHERE scheduled_time >= TIMESTAMP '2026-09-27 23:00:00' AND scheduled_time < TIMESTAMP '2026-09-28 23:00:00' ORDER BY scheduled_time ASC LIMIT 200`` |
| ▶ | 1.3b | **On screen:** dashboard, right side: “biggest delays … how many passengers are affected” | ✅ **Biggest delays** panel: the same 1.3a rows, sorted by `delay_minutes` in the app, top 10, each with its **At risk** count (`affected_passengers`). The app sorts because Lightning has no aggregation; Flink already computed each count. |
| ▶ | 1.3c | **On screen:** counts bar | ✅ **Affected** is the sum of `affected_passengers` over the 1.3a rows. **Booked** counts booked offers, paged by key:<br>``SELECT * FROM `passenger_recommendations` WHERE status = 'BOOKED' AND recommended_at >= TIMESTAMP '2026-09-27 23:00:00' ORDER BY "KEY" ASC LIMIT 200``<br>(then the same query with ``AND "KEY" > '<last key>'`` for each next page). |
| ▶ | 1.3d | **On screen:** “drill down into any flight … which passengers are impacted and where they’re going” | ✅ Click a flight. A seat map of the plane (nose on the left, First in rows 1–5) colors every seat: red at risk, green not at risk, grey open. Hover a seat for the passenger and their itinerary, or click it to open the passenger. Below it, the table shows Passenger · Seat · Going to (`final_destination`) · Connection · Minutes · Risk. A departure shows the passengers connecting onto it, in their connecting seats (`WHERE connecting_flight_id = …`), with **Arriving on** in place of **Connection**. Header:<br>``SELECT * FROM `flight_status` WHERE "KEY" = 'RA417-20260928' LIMIT 1`` and the same query on `flight_impact` for its at-risk count<br>Passengers, HIGH first. A flight carries at most 182, so one page returns all of RA417's 170:<br>``SELECT * FROM `passenger_state` WHERE inbound_flight_id = 'RA417-20260928' ORDER BY "KEY" ASC LIMIT 200`` |
| ▶ | 1.3e | **On screen:** “updated in real time” | ✅ The browser refreshes every 2.5 s, drill-down included. Every active flight updates once a minute, en-route arrivals revise their estimate by a minute or two along the way, and both hotels update every 15 s. From +5 to +25, RA417 updates every 30 s: its **Delay** climbs to 115, and its **At risk** count and seat map turn red one destination at a time, about once a minute until +16:30. |
| ▶ | 1.3f | **On screen (optional):** passenger app (Figma, design team): “the same real-time view to our passengers” | ⚠️ The design team's Figma screens aren't built here. Fallback: the app's passenger card (click a passenger):<br>``SELECT * FROM `passenger_state` WHERE "KEY" = 'P-0928-417-081' LIMIT 1``<br>``SELECT * FROM `passenger_recommendations` WHERE passenger_id = 'P-0928-417-081' LIMIT 3``<br>The click also swaps **Biggest delays** for **Hotel options** (rooms left, nightly rate, availability, and which hotels the passenger's offers use) until you click the passenger again or change flight:<br>``SELECT * FROM `hotel_inventory` LIMIT 200`` |
| ▶ | 1.3g | **On screen:** “one delayed flight has 150 passengers impacted” | ✅ RA417 is first in **Biggest delays**: ORD → SFO · DELAYED · 115 min · **At risk 150** (150 from +16:30). Its seat map shows 150 red, 20 green, and 12 open seats; the table lists 150 HIGH and 20 NO_CONNECTION. |
| ▶ | 1.3h | **Mentioned, not shown:** rebook flights, bags, hotel | Narration only. Offers (flights and hotels) appear in 2.3 and 2.4. There is no bag data. |
| **Demo 2 \- Streamhouse for AI (5 mins)** |  |  |  |
| 1.5 min | 2.1 | Show Flink Join query (ZOOM into the join) Show the results with all passengers (FILTER on the specific flight) | Now we have passenger information and flight status. So let’s use **Flink to bring that data together**. This query combines the passenger, flight, and connection data into **one current view for every passenger**. With all this real-time data, we can easily calculate every single passenger's risk of missing their flight, and update that risk score in real time. When any passenger's risk score tilts over into the **HIGH** category, that event kicks off a recovery process for that passenger. And because it’s running continuously, that view stays up to date as things change. A flight arrival time changes?**The passenger’s connection risk score changes too.** Now the airline can see exactly **who is affected and what they can do about it.** They can rebook the passenger, reroute their bags, arrange a hotel, whatever they need to keep the journey moving. And even when we can’t prevent the missed connection, we can remove the anxiety, stress, and uncertainty that comes along with missing a flight, for the passenger. By using data to be proactive rather than reactive, the airline can recover the passenger experience in real time. **Transition:** Now we want to give our **AI agent the information it needs to take action**. |
| ▶ | 2.1a | **On screen:** Flink join query (zoom into the join) | ✅ **Flink → Statements → `airport-materialize-passenger-state`**, or paste it into a workspace. The full query is in [Appendix A](#a-flink-join-passenger_state-21). Zoom in on:<br>`FROM passenger_itineraries p JOIN flight_status inbound ON p.inbound_flight_id = inbound.key LEFT JOIN flight_status onward ON p.connecting_flight_id = onward.key` |
| ▶ | 2.1b | **On screen:** results for all passengers, filtered to the specific flight | ✅ In a Flink workspace:<br>`SELECT * FROM passenger_state WHERE inbound_flight_id = 'RA417-20260928';`<br>Columns: key, inbound_flight_id, inbound_seat, connecting_flight_id, connecting_seat, final_destination, connection_minutes, risk. |
| ▶ | 2.1c | **On screen:** “risk score … tilts over into HIGH … changes too” | ⚠️ The same results. `connection_minutes` falls, and `risk` flips from OK to HIGH when it drops below 45. RA417's passengers flip between +6 and +16:30, so start the stream within about 15 minutes of this scene to see RA417 flip live. That fights the +40 sellout in 2.4 (see gaps). There is no numeric score: risk is HIGH, OK or NO_CONNECTION, plus `connection_minutes`. |
| 1.5 min | 2.2 | Show confluent Assistant prompt to create a webhook connector  Show the connector running in CC Show the topic in CC Enable RTCE in the Topics UI | For example, it needs to know what hotels have availability and their prices. So let’s bring that data into Confluent. We can use the **Webhooks Source Connector**. And this is really simple,  **you don’t need to be a Kafka expert.** Using **Confluent Assistant,** we provision a webhook endpoint and give that endpoint to our hotel partner. They simply send their updates to that endpoint. Every update is automatically ingested into Confluent. **No infrastructure to manage. No Kafka expertise required.** Now we have the hotel data in Confluent, next, we’ll make that real-time data available to the agent via MCP using the **Realtime Context Engine**. |
| ▶ | 2.2a | **On screen:** Confluent Assistant prompt that creates a webhook connector | 🚧 Draft prompt for Confluent Assistant: *“Create a Webhooks source connector named hotel-partner-webhook that writes hotel availability updates to the hotel_inventory topic, keyed by hotel_name, with available_rooms and nightly_rate in the value.”* Check that the connector can write the raw string key and the Avro value that `hotel_inventory` uses. |
| ▶ | 2.2b | **On screen:** the connector running in Confluent Cloud | 🚧 **Connectors → hotel-partner-webhook → Running**. Not deployed; today the generator publishes hotel rows directly. |
| ▶ | 2.2c | **On screen:** the topic in Confluent Cloud | ✅ **Topics → `hotel_inventory` → Messages**:<br>`Grand Hyatt at SFO {"available_rooms": 40, "nightly_rate": "329.00"}`, `SFO Airport Marriott Waterfront {"available_rooms": 150, "nightly_rate": "229.00"}`, and `Hilton SFO Airport Bayfront {"available_rooms": 120, "nightly_rate": "249.00"}`. All three update every 15 s as guests book and cancel; the Grand Hyatt drifts down about a room a minute until 0 at +40. |
| ▶ | 2.2d | **On screen:** enable RTCE in the Topics UI | ✅ **Topics → `hotel_inventory` → Real-Time Context Engine**. Deploy has already turned it on, and the agent needs it running, so show it as on (as in 1.2a). |
| 1.5 min | 2.3 | CREATE MODEL CREATE AGENT (with RTCE) Query to run the agent (ZOOM into AI\_RUN\_AGENT) Show the results on the Flight in question | Next, we create the model that our agent will use. Here, we’re using a model hosted directly in Confluent. So there’s no need to move your data anywhere. Everything stays in Confluent and is fully managed for you. Next, we create the agent. We give it the model we just created, a simple set of instructions, and access to live hotel information through the **Real-Time Context Engine.** Now, our agent is ready,  but it’s not running yet. So we use `AI_RUN_AGENT` to turn it on. And just like that \- we now have an always-on agent that understands what’s happening in the business right now. As flight data changes, the agent continuously adjusts \-  evaluating the latest context, and recommending the best actions to help all the passengers who will miss their connecting flights, in light of the latest data. **Transition:** But identifying the best option is only half the story. Now we need to get that recommendation to the passenger. |
| ▶ | 2.3a | **On screen:** CREATE MODEL, “a model hosted directly in Confluent” | 🧪 **Flink → Statements → `airport-agent-native-model`**. [Appendix B](#b-create-model-23a): `passenger_recovery_mode1`, Claude hosted in Confluent (Native Inference). Show this one. Native Inference can't call tools yet, so the agent runs on the Bedrock model `passenger_recovery_model` (statement `airport-agent-model`), which is named almost the same. Swap the agent over once tool calling works. |
| ▶ | 2.3b | **On screen:** CREATE AGENT with RTCE | ✅ **Flink → Statements → `airport-agent-passenger-recovery`**. [Appendix C](#c-rtce-tool-and-create-agent-23b): the `hotel_inventory_live_context` tool on the RTCE MCP connection, then `CREATE AGENT passenger_recovery_agent … USING TOOLS hotel_inventory_live_context`. |
| ▶ | 2.3c | **On screen:** the query that runs the agent (zoom into `AI_RUN_AGENT`) | ✅ **Flink → Statements → `airport-agent-insert-passenger-recommendations`**. [Appendix D](#d-ai_run_agent-23c). Zoom in on:<br>`LATERAL TABLE(AI_RUN_AGENT('passenger_recovery_agent', CONCAT(…), g.group_key))`<br>It runs for RA417 only, once per final destination (11 runs of about 40 s each), the first time a passenger going there turns HIGH. Every passenger in that group gets its two offers. |
| ▶ | 2.3d | **On screen:** the results for the flight in question | ✅ In a Flink workspace:<br>`SELECT * FROM passenger_recommendations WHERE passenger_id LIKE 'P-0928-417-%';`<br>That's 2 agent offers for each of the 150 passengers, for example `P-0928-417-081-O1`: RA605-20260929 · Grand Hyatt at SFO · $329 · OFFERED, and `-O2`: RA620-20260929 · SFO Airport Marriott Waterfront · $229. In the 2026-09-28 live run, when RA417 still had 200 connecting passengers, all 400 offers arrived within about 3 minutes of the statement starting. If the Grand Hyatt has already sold out when the agent runs, the offers are the Marriott and the Hilton instead. Other flights' offers come from the generator. Without Bedrock, deploy skips the agent and the generator also writes RA417's offers, 5–40 s after each passenger goes HIGH. |
| 0.5 min | 2.4 | Design Team App Push notification showing agent recommendation User picks an option App shows agent chain of thought, Hotel that was recommended is now not available and suggests another hotel | That’s where our external agent comes in. It takes the recommendation and presents it directly to the passenger in the app. If the passenger accepts, the agent takes care of the booking. But notice what happens here, the original hotel is no longer available. The agent gets the latest information, finds the next best option, and books it instead. And that’s the power of the Streamhouse. With access to the latest context, the agent can adapt in real time, proactively find the next best option, and take action for the passenger. **Transition:** Back to the slides please |
| ▶ | 2.4a | **On screen:** design team app, push notification with the agent's recommendation | ⚠️ The design team's screens (not in this repo). Data behind them, and the fallback in our app's passenger card:<br>``SELECT * FROM `passenger_recommendations` WHERE passenger_id = 'P-0928-417-081' LIMIT 3`` |
| ▶ | 2.4b | **On screen:** user picks an option | ✅ **Select** on offer O1 (Grand Hyatt at SFO). The app reads the offers (2.4a) and then:<br>``SELECT * FROM `hotel_inventory` LIMIT 200``<br>The Grand Hyatt has sold out (2.4c), so the app does not mark it `SELECTED`; see below. |
| ▶ | 2.4c | **On screen:** the recommended hotel is no longer available, so another is suggested | ✅ After +40, the Grand Hyatt has `available_rooms = 0`, and **Hotel options** shows it as **Sold out** (**Few left** from 10 rooms). On Select the app writes O1 back as `OFFERED` with the cheapest other hotel that has rooms, the Hilton ($249). The status line reads **Grand Hyatt at SFO is sold out. Now offering Hilton SFO Airport Bayfront.** The passenger now chooses between the Marriott (O2) and the Hilton (O1). **Select** the Hilton, then **Book** re-checks `hotel_inventory`, writes `BOOKED`, and closes O2. |
| ▶ | 2.4d | **On screen:** the agent's chain of thought | ⚠️ Not in our app. The design team's app would need to show it; alternatively, show the steps taken (the hotel read and the substitution) rather than the model's reasoning. |
| **Demo 3 Streamhouse for Analytics Reporting (2 mins)** |  |  |  |
| 1 min | 3-1 | Show Enable tableflow with Delta for the passenger\_recommedations data product Start in topic view Enable Tableflow Show pending \>\> syncing status End in topic view w/ tableflow enable  | Tableflow makes it push-button simple to turn your Kafka topics into Iceberg or Delta Lake tables. Here, we want to make our rebooking data available in Amazon Athena. And watch how easy this is — it’s just two steps. First, I choose the format — Iceberg. Then, I select our S3 bucket for storage. And we’re done. Our Kafka topic and its schema are now automatically synced to an Iceberg table in the AWS Glue Data Catalog. No custom code. No pipelines to build or manage. With Tableflow, your streams simply become tables, ready to query with the BI and SQL tools you already use. |
| ▶ | 3-1a | **On screen:** enable Tableflow on `passenger_recommendations` | ⚠️ **Topics → `passenger_recommendations` → Enable Tableflow**. The Visual column says **Delta**, but the narration and the Glue/Athena path use **Iceberg**, so pick Iceberg. |
| ▶ | 3-1b | **On screen:** “select our S3 bucket for storage” | ✅ Choose your own storage and the provider integration Terraform created, then the bucket from `terraform -chdir=terraform/airline-demo output analytics_bucket`. The Glue catalog integration already exists, so the table shows up in Glue on its own. |
| ▶ | 3-1c | **On screen:** Pending → Syncing, ending in the topic view with Tableflow enabled | ⚠️ Topic view, **Tableflow** column. Time the first sync in rehearsal, because 3-2 needs this table in Athena about a minute later. `flight_impact` is already synced by Terraform, off screen; question 1 needs it. |
| 1 min | 3-2 | In Quick ask a question  Show SQL In Quick ask question  Show SQL  | Now in AWS, we’re using Amazon Quick to interact with this data using natural language. So I can simply ask, *“**How many flights were delayed last month? How many passengers were impacted, and how much did it cost us?”*** Quick understands the question and uses Amazon Athena to get the answer. We can also measure how well our agent is performing. For example, *“**What’s the average time it takes to recover an impacted passenger?**”* And this is just the beginning. Because this data is continuously flowing into AWS in near real time, you can use it to power many more analytics and AI use cases. **Transition:** And with that, let’s go back to the slides. |
| ▶ | 3-2a | **On screen:** ask Quick: “How many flights were delayed last month? How many passengers were impacted, and how much did it cost us?” | ⚠️ Amazon Quick, with Athena as the data source and the Glue database from `terraform -chdir=terraform/airline-demo output glue_database`. The data covers a trailing 30 days, so say "**in the last 30 days**" rather than "last month". Quick writes its own SQL. The reference query and expected answer are in [Appendix E](#e-athena-questions-3-2). Quick isn't set up yet. |
| ▶ | 3-2b | **On screen:** show the SQL | ✅ Quick's generated SQL, which should match Appendix E, question 1. |
| ▶ | 3-2c | **On screen:** ask Quick: “What’s the average time it takes to recover an impacted passenger?” | ⚠️ The same Quick setup. Reference query: [Appendix E](#e-athena-questions-3-2), question 2. It measures from impacted to offer ready, so it leaves out the passenger's decision time. The answer is about 23 seconds. |
| ▶ | 3-2d | **On screen:** show the SQL | ✅ Quick's generated SQL, which should match Appendix E, question 2. |

## Gaps to close before rehearsal

**Must fix**

1. **Scene 2.3d: confirm on a fresh deploy that Lightning and RTCE see every flight.** On 2026-09-28, the agent ran live and wrote 2 offers for each of RA417's 200 HIGH passengers. It only worked after tomorrow's flights were republished, though: `flight_status` in Lightning showed only the 34 flights updated since enablement, while `hotel_inventory` and `passenger_itineraries` from the same deploy showed every row. After the next `uv run deploy`, check that ``SELECT * FROM `flight_status` WHERE origin = 'SFO' AND scheduled_time >= TIMESTAMP '<tomorrow> 00:00:00' LIMIT 200`` returns rows. If it doesn't, the agent answers NONE and writes no offers.
2. **Scenes 2.2a–b: the Webhooks connector and its Assistant prompt aren't built.** The generator publishes the hotel rows the connector would, straight to `hotel_inventory`.
3. **Scenes 3-2a–d: Amazon Quick isn't set up** over the Glue database.

**Decide or reword**

1. **Stream timing.** A live OK→HIGH flip on RA417 (2.1c) needs the stream started within about 15 minutes of scene 2.1. The sellout (2.4c) needs it started at least 40 minutes before scene 2.4. Either shorten the gaps in the stream clock or accept that RA417 has already flipped by 2.1; other flights keep updating.
2. **"A model hosted directly in Confluent" in 2.3:** the screen shows the Native Inference `CREATE MODEL`, but until Native Inference supports tool calling the agent calls Claude on Amazon Bedrock.
3. **"Delta" in 3-1:** change the visual to Iceberg. **"Last month" in 3-2:** say "in the last 30 days".
4. **"Risk score" in 2.1:** the data has a risk level (HIGH, OK or NO_CONNECTION) plus connection minutes, not a number.
5. **"Many topics" in 1.1** (there are 6) and **"chain of thought" in 2.4d** (not shown).

**Known limits**

1. The agent handles only today's RA417, the date taken from UTC when a passenger turns HIGH. RA417 is finished by +25, so start the stream before 23:30 UTC.
2. `impacted_at` is when a passenger's first HIGH row landed in `passenger_state_changes`. If that table or `impacted_passengers` is rebuilt, the rebuild time replaces it, and recovery times for the live day come out long. History days aren't affected.
3. The agent runs about 40 s per destination, one after another. That is fine for RA417's 11 destinations; widening it to every flight would take hours.
4. Flink commits its results to Kafka about once a minute (exactly-once), and Lightning only sees committed data. So the **At risk** counts and seat-map colors trail the stream by up to a minute (about 20–25 s in rehearsal), while flight status, delays, hotel rooms, and the generator's bookings show within seconds. Restarting the stream republishes every itinerary, so Flink needs a few minutes to recompute every passenger before the seat maps settle.

## Appendix: full queries

### A. Flink join: `passenger_state` (2.1)

Running statement `airport-materialize-passenger-state`, from [`sql/24-serving-passenger-state.sql`](../terraform/airline-demo/sql/24-serving-passenger-state.sql). The query part:

For a live `CREATE OR ALTER`, preserve the SQL file's schema and
`WITH ('key.format' = 'raw', ...)` options, and copy the exact distribution from
`SHOW CREATE MATERIALIZED TABLE passenger_state`. The query below is for showing the join;
wrapping it in a query-only materialized-table definition changes the key format defaults
and can make the existing topic unreadable.

```sql
SELECT
  p.`key`,
  p.inbound_flight_id,
  p.inbound_seat,
  p.connecting_flight_id,
  p.connecting_seat,
  COALESCE(onward.destination, inbound.destination) AS final_destination,
  TIMESTAMPDIFF(MINUTE, inbound.estimated_time, onward.estimated_time) AS connection_minutes,
  CASE
    WHEN p.connecting_flight_id IS NULL THEN 'NO_CONNECTION'
    WHEN TIMESTAMPDIFF(MINUTE, inbound.estimated_time, onward.estimated_time) < 45 THEN 'HIGH'
    ELSE 'OK'
  END AS risk
FROM passenger_itineraries p
JOIN flight_status inbound ON p.inbound_flight_id = inbound.`key`
LEFT JOIN flight_status onward ON p.connecting_flight_id = onward.`key`;
```

### B. CREATE MODEL (2.3a)

Statement `airport-agent-native-model`, from [`sql/32-model-passenger-recovery-native.sql`](../terraform/airline-demo/sql/32-model-passenger-recovery-native.sql). This is the one to show: Claude hosted in Confluent, with no connection. Nothing uses it yet, because Native Inference models can't call tools.

```sql
CREATE MODEL IF NOT EXISTS passenger_recovery_mode1
INPUT (prompt STRING)
OUTPUT (response STRING)
WITH (
  'provider' = 'confluent',
  'task' = 'text_generation',
  'confluent.model' = 'anthropic.claude-sonnet-4-6',
  'confluent.input_format' = 'confluent-chat',
  'confluent.params.max_tokens' = '1024'
);
```

The agent runs on statement `airport-agent-model`, from [`sql/28-model-passenger-recovery.sql`](../terraform/airline-demo/sql/28-model-passenger-recovery.sql): Claude on Amazon Bedrock, through the `bedrock-connection` that core creates when AWS credentials are present.

```sql
CREATE MODEL IF NOT EXISTS passenger_recovery_model
INPUT (prompt STRING)
OUTPUT (response STRING)
WITH (
  'provider' = 'bedrock',
  'task' = 'text_generation',
  'bedrock.connection' = 'bedrock-connection',
  'bedrock.params.max_tokens' = '1024'
);
```

### C. RTCE tool and CREATE AGENT (2.3b)

`uv run deploy` creates the MCP connection `rtce-connection` with the Confluent CLI. It points at the RTCE endpoint for the cluster, uses streamable HTTP, and signs in with the RTCE Global key from `credentials.env`. The key never appears in SQL or Terraform. The tool is statement `airport-agent-tool`, from [`sql/29-tool-hotel-inventory-live-context.sql`](../terraform/airline-demo/sql/29-tool-hotel-inventory-live-context.sql):

```sql
CREATE TOOL IF NOT EXISTS hotel_inventory_live_context
USING CONNECTION `rtce-connection`
WITH (
  'type' = 'mcp',
  'allowed_tools' = 'getMetadata,queryData',
  'request_timeout' = '30'
);
```

The agent is statement `airport-agent-passenger-recovery`, from [`sql/30-agent-passenger-recovery.sql`](../terraform/airline-demo/sql/30-agent-passenger-recovery.sql). Its rules match the generator's offers, so the Athena history and the live day agree.

```sql
CREATE AGENT IF NOT EXISTS passenger_recovery_agent
USING MODEL passenger_recovery_model
USING PROMPT 'You are the River Air recovery agent. The passengers on the inbound flight in the task
will miss their connections to the final destination, and they all get the same two offers.
Use the queryData tool (arguments: topic_name, query, max_result_rows) to run the three queries in the task.
The queries already use the right column names, so run them as given without calling getMetadata first.

1. inbound_query returns the inbound flight. Its estimated_time is when the passenger lands at SFO.
2. departures_query returns flights from SFO to the final destination. Offer the first two flights whose
   estimated_time is at least 45 minutes after the inbound estimated_time. Skip CANCELLED flights.
3. An offered flight that leaves 6 hours or more after the inbound estimated_time needs a hotel.
   Run hotel_query. List the hotels that have available_rooms above 0 in this order: Grand Hyatt at SFO,
   SFO Airport Marriott Waterfront, Hilton SFO Airport Bayfront. The first offer uses the
   first hotel in that list and the second offer uses the second. The hotel cost is that hotel nightly_rate.
   A flight that leaves sooner needs no hotel: write NONE for its hotel and cost.

Reply with exactly one line and nothing else:
O1_FLIGHT=<flight key>;O1_HOTEL=<hotel name or NONE>;O1_COST=<nightly_rate or NONE>;O2_FLIGHT=<flight key or NONE>;O2_HOTEL=<hotel name or NONE>;O2_COST=<nightly_rate or NONE>'
USING TOOLS hotel_inventory_live_context
WITH (
  'max_iterations' = '15',
  'handle_exception' = 'continue'
);
```

### D. AI_RUN_AGENT (2.3c)

Three off-diagram steps feed the agent. First, `passenger_state` is an upsert table, and `AI_RUN_AGENT` reads only append-only input. So a staging table, `passenger_state_changes`, gets one new row for every insert or update to `passenger_state`. Each row's `$rowtime` is when that change landed. It's statement `airport-agent-passenger-state-changes`, from [`sql/26-staging-passenger-state-changes.sql`](../terraform/airline-demo/sql/26-staging-passenger-state-changes.sql):

```sql
CREATE OR ALTER MATERIALIZED TABLE passenger_state_changes (
  `key` STRING NOT NULL,
  inbound_flight_id STRING,
  connecting_flight_id STRING,
  final_destination STRING,
  risk STRING
) DISTRIBUTED BY (`key`) INTO 4 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'raw',
  'value.format' = 'avro-registry'
)
START_MODE = FROM_BEGINNING
AS SELECT
  `key`,
  inbound_flight_id,
  connecting_flight_id,
  final_destination,
  risk
FROM TO_CHANGELOG(
  input      => TABLE passenger_state PARTITION BY `key`,
  op_mapping => MAP['INSERT, UPDATE_AFTER', 'UPSERT']
);
```

Second, `impacted_passengers` keeps one row per RA417 passenger, the first time they turn HIGH. `first_high` groups the HIGH change rows by passenger, and `TO_CHANGELOG` passes on only each group's first row, so later delay notices for someone already HIGH add nothing. `impacted_at` is when the first HIGH row landed. It's its own table because a Flink statement can hold only one `TO_CHANGELOG`. Statement `airport-agent-impacted-passengers`, from [`sql/27-staging-impacted-passengers.sql`](../terraform/airline-demo/sql/27-staging-impacted-passengers.sql):

```sql
CREATE OR ALTER MATERIALIZED TABLE impacted_passengers (
  `key` STRING NOT NULL,
  inbound_flight_id STRING,
  connecting_flight_id STRING,
  final_destination STRING,
  impacted_at TIMESTAMP(3)
) DISTRIBUTED BY (`key`) INTO 4 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'raw',
  'value.format' = 'avro-registry'
)
START_MODE = FROM_BEGINNING
AS WITH first_high AS (
  SELECT
    `key`,
    inbound_flight_id,
    connecting_flight_id,
    final_destination,
    CAST(MIN($rowtime) AS TIMESTAMP(3)) AS impacted_at
  FROM passenger_state_changes
  WHERE risk = 'HIGH'
    AND inbound_flight_id = CONCAT('RA417-', DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyyMMdd'))
  GROUP BY `key`, inbound_flight_id, connecting_flight_id, final_destination
)
SELECT `key`, inbound_flight_id, connecting_flight_id, final_destination, impacted_at
FROM TO_CHANGELOG(
  input      => TABLE first_high PARTITION BY `key`,
  op_mapping => MAP['INSERT', 'FIRST_HIGH']
);
```

Third, the agent query is statement `airport-agent-insert-passenger-recommendations`, from [`sql/31-insert-passenger-recommendations.sql`](../terraform/airline-demo/sql/31-insert-passenger-recommendations.sql). Passengers on the same inbound flight to the same final destination get the same offers, so `first_group` groups them and `TO_CHANGELOG` passes on each group's first row: the agent runs once per destination. The agent returns one line, the query splits it into two offers, and the join hands them to every passenger in the group. Zoom in on the `LATERAL TABLE(AI_RUN_AGENT(...))` block.

```sql
INSERT INTO passenger_recommendations
WITH first_group AS (
  SELECT
    CONCAT(inbound_flight_id, '>', final_destination) AS group_key,
    inbound_flight_id,
    final_destination,
    MIN(impacted_at) AS impacted_at
  FROM impacted_passengers
  GROUP BY inbound_flight_id, final_destination
),
new_groups AS (
  SELECT *
  FROM TO_CHANGELOG(
    input      => TABLE first_group PARTITION BY group_key,
    op_mapping => MAP['INSERT', 'FIRST_GROUP']
  )
),
plans AS (
  SELECT g.inbound_flight_id, g.final_destination, r.response
  FROM new_groups g,
  LATERAL TABLE(AI_RUN_AGENT(
    'passenger_recovery_agent',
    CONCAT(
      'inbound_flight_id=', g.inbound_flight_id,
      '; final_destination=', g.final_destination,
      '; inbound_query=SELECT * FROM `flight_status` WHERE "KEY" = ''', g.inbound_flight_id, ''' LIMIT 1',
      '; departures_query=SELECT * FROM `flight_status` WHERE origin = ''SFO'' AND destination = ''',
      g.final_destination, ''' AND scheduled_time >= TIMESTAMP ''',
      DATE_FORMAT(g.impacted_at - INTERVAL '1' HOUR, 'yyyy-MM-dd HH:mm:ss'),
      ''' ORDER BY scheduled_time ASC LIMIT 12',
      '; hotel_query=SELECT * FROM `hotel_inventory` LIMIT 10'
    ),
    g.group_key
  )) AS r(agent_status, response)
),
offers AS (
  SELECT
    h.`key` AS passenger_id,
    h.impacted_at,
    o.n,
    NULLIF(REGEXP_EXTRACT(p.response, CONCAT('O', o.n, '_FLIGHT=([^;\s]+)'), 1), 'NONE') AS recommended_flight_id,
    NULLIF(TRIM(REGEXP_EXTRACT(p.response, CONCAT('O', o.n, '_HOTEL=([^;\n]+)'), 1)), 'NONE') AS hotel_name,
    TRY_CAST(REGEXP_EXTRACT(p.response, CONCAT('O', o.n, '_COST=([0-9.]+)'), 1) AS DECIMAL(10, 2)) AS hotel_cost
  FROM impacted_passengers h
  JOIN plans p
    ON h.inbound_flight_id = p.inbound_flight_id AND h.final_destination = p.final_destination
  CROSS JOIN UNNEST(ARRAY['1', '2']) AS o(n)
)
SELECT
  CONCAT(passenger_id, '-O', n) AS `key`,
  passenger_id,
  recommended_flight_id,
  hotel_name,
  'OFFERED' AS status,
  hotel_cost,
  impacted_at,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS recommended_at
FROM offers
WHERE recommended_flight_id IS NOT NULL;
```

### E. Athena questions (3-2)

Run these against the Glue database Tableflow syncs into. They are the reference for what Quick should produce. The expected answers come from the default seed (42) over the trailing 30 days.

**Question 1.** *How many flights were delayed in the last 30 days? How many passengers were impacted, and how much did it cost us?* Expected for a run at 17:00 UTC on 2026-09-28: 1,643 delayed flights, 8,856 impacted passengers, and $547,521 of hotel spend. The counts shift slightly with the day and time you run.

```sql
SELECT
  (SELECT count_if(delay_minutes >= 15) FROM flight_impact
    WHERE scheduled_time >= current_date - INTERVAL '30' DAY)                  AS delayed_flights,
  (SELECT sum(affected_passengers) FROM flight_impact
    WHERE scheduled_time >= current_date - INTERVAL '30' DAY)                  AS impacted_passengers,
  (SELECT sum(hotel_cost) FROM passenger_recommendations
    WHERE status = 'BOOKED' AND recommended_at >= current_date - INTERVAL '30' DAY) AS hotel_cost;
```

**Question 2.** *What's the average time it takes to recover an impacted passenger?* Expected: 22.7 seconds.

```sql
SELECT avg(to_unixtime(recommended_at) - to_unixtime(impacted_at)) AS avg_recovery_seconds
FROM passenger_recommendations
WHERE status = 'BOOKED' AND recommended_at >= current_date - INTERVAL '30' DAY;
```
