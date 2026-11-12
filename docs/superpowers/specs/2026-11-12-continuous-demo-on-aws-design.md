# Always-on Flight Disruptions demo on AWS

## Intent

Today the Flight Disruptions demo only comes alive when someone runs it. `uv run deploy --with-labs`
plays a scripted 90-minute stream from the presenter's laptop, and when the stream ends, or the
laptop sleeps, the dashboard freezes. We want one always-on deployment of the demo that six Confluent
colleagues can open at any time and use in their own demos, with nobody setting it up first. They should
see a live board where flights get delayed, passengers' connections turn HIGH risk, and the recovery
agent books them new flights and hotels. They should also be able to show the Confluent Cloud side: the
Flink SQL, Lightning queries, and the agent statements.

## Goals

1. **Continuous data.** A new generator mode that runs forever and keeps the live day moving:
   flights are scheduled, delayed, land, and depart, day after day, at roughly today's rates (about 28
   of 90 arrivals delayed in a day, about 560 passengers turning HIGH a day).
2. **Real agent offers for everything.** In continuous mode the Flink recovery agent (`sql/26`–`32`)
   writes the offers for every HIGH passenger, not only RA417, RA183, and RA185. The generator writes
   no offers for the live day. Hotels still sell out and free up, so the offers stay hotel-aware.
3. **Hosted on AWS.** The app (`airport_app.py`) and the continuous generator run as always-on
   services in AWS, so neither depends on anyone's laptop.
4. **Password access to the app.** The app has one HTTPS URL behind one shared password.
5. **Six Confluent Cloud users, one environment.** Six demo users are invited to the one environment
   at addresses the operator supplies.

## Non-goals

- Replacing the 90-minute scripted mode. The workshop, the demo script, and anyone deploying from the
  public repo keep using it unchanged. Continuous mode is opt-in.
- Okta/SSO or VPN access for the app. The user chose a shared password.
- A copy per user. All six users share one deployment.
- The Agentic Pit Decisions lab in the always-on deployment (Flight Disruptions only).
- AWS access for the demo users. They use Confluent Cloud and the app only. Tableflow still writes to
  S3 and Glue, so the operator can run scene 3, but no IAM users, Athena workgroups, or Quick setup are
  built for colleagues.
- Autoscaling or high availability. One instance of each service, restarted if it crashes.

## Anti-goals: it fails even if it technically works when

- the board is empty or static whenever a colleague opens it, at any hour (they work in every time zone);
- credentials, the shared password, or the demo users' email addresses end up in tracked files (the
  repo is public, `confluentinc/quickstart-streamhouse`);
- the existing `uv run deploy`, workshop, or 90-minute stream stop working or change behavior;
- it needs hands-on care each day, or loses its place after a restart.

## Decisions made with the user

- Continuous data for an always-on board, with delays and agent bookings like today's demo.
- The agent handles every HIGH passenger (~560 runs a day), the cheaper option over replaying the
  RA417 story every 90 minutes (~3,500 runs a day).
- The app and the generator both run on AWS.
- One deployment, deployed once by the operator and left on. No one else deploys this mode.
- Six demo users, all with access to that one environment. The operator supplies their email addresses
  at deploy time (plus-addressed aliases of one mailbox).
- Access to the app with a shared password.
- Demo users get no AWS access.
- Demo users get full access to the environment (`EnvironmentAdmin`). The operator tells them not to
  delete topics.
- **Eight RA417s a day.** Eight flights a day from Chicago (ORD) to SFO, all numbered RA417, one every
  3 hours, each with RA417's growing delay and 150 of its 170 passengers missing their connection. Their
  IDs carry the scheduled time (`RA417-20261112-0040`, passengers `P-1112-417-0040-001`), so the eight
  don't collide. Whenever someone logs in, one RA417 is being delayed right now.
- Other flights keep their random delays.
- **Tableflow stays**, turned on automatically for `flight_impact` and `passenger_recommendations`, so
  the operator can demo scene 3. Colleagues see Tableflow in Confluent Cloud but can't run the Athena
  questions, since they have no AWS access.
- Before anyone gets the link, the deployment runs for two days, and the operator checks the Confluent
  and AWS bills.

## Calls I made (the user can override)

- **Flights around the clock in continuous mode.** Today's template lands flights from 06:00 to 23:00.
  Because colleagues are in every time zone, continuous mode spreads each day's 180 flights over 24 hours
  so something is always happening. Same 180 flights, same delay rates.
- **The clock comes from wall-clock time, not a state file.** Given the seed and the current UTC time,
  the generator works out which service days are live and what every flight looks like right now. It
  publishes that state on start, then streams forward. A restart, a redeploy, or a crash recovery just
  picks up where the clock says, with no `--resume` file. This keeps the seed-and-clock rule from
  `AGENTS.md`.
- **Rolling days.** The generator always has today and tomorrow published. Finished days stay in the
  topics, so they become the history for scene 3 (Tableflow/Athena). On first start it publishes 30 days
  of history as today. It doesn't publish history again on restart.
- **The eight RA417s.** They land at 00:40, 03:40, … 21:40 UTC and take the places of the eight regular
  arrival slots nearest those times, so each day still has 90 arrivals. Each one's delay climbs with
  every update from 75 to 25 minutes before its scheduled arrival, reaching 115 minutes. That keeps each
  one delayed and still in the air for about 3 hours, so the eight overlap with no gap. Each has 170
  passengers, of whom 150 connect and turn HIGH; the departures they connect to must have seats for all
  of them in the 182-seat layout. The rest of the day's flights follow the random delay rules.
- **Simulated customers still book.** Today the generator plays the customer: about 85% of offers get
  booked after a decision time, and the rest expire after 90 minutes. Agent offers stay OFFERED for the
  presenter. In continuous mode, nothing would ever book the agent's offers, so the board's booked panel
  and scene 3's hotel cost would fade towards zero. So the continuous generator also consumes
  `passenger_recommendations`. For each new agent offer, it decides (seeded on the passenger key) whether
  and when the customer books one, using today's booking share, decision times, and expiry. A colleague
  can still book from the app first, and the generator then leaves that passenger alone. This is the one
  place the generator reacts to live data. On restart, it rereads the topic and skips passengers who
  already have a decision.
- **Hotels stay about as common as today.** A hotel is offered when the next onward flight leaves more
  than 6 hours after landing (`OVERNIGHT`). Spreading flights over 24 hours would erase most of those
  gaps. So the continuous schedule gives each destination one long gap a day, staggered across
  destinations, so at any hour some routes are in their "night". The share of HIGH passengers who get a
  hotel offer should stay close to today's. `OVERNIGHT` and `sql/30` don't change.
- **Agent filter.** `sql/27` limits the agent to the three scripted flights with a hard-coded
  `IN ('RA417', 'RA183', 'RA185')`. Continuous mode needs that filter off, through a Terraform variable
  that renders the statement with or without the clause. The default stays as today, so the workshop
  guide's SQL (checked by `tests/test_workshop.py`) doesn't change.
- **Tableflow in always-on mode.** Deploy sets `tableflow_topics = ["flight_impact",
  "passenger_recommendations"]`, so Iceberg collects both from day one. `--always-on` requires AWS
  credentials, so Tableflow can't be skipped by accident. The scripted demo keeps its on-screen step of
  enabling `passenger_recommendations`.
- **IAM preflight.** The deployment creates three IAM roles and no users: Tableflow's
  (`analytics.tf`), the flight board's Lambda execution role, and the ECS task execution role. Before creating anything, deploy checks that the AWS credentials can create IAM roles and
  policies, and stops with a clear message if they can't.
- **Deploy command.** The operator runs `uv run deploy --always-on` once. It deploys the full demo
  (`--with-labs` behavior) with continuous mode, Tableflow (it needs AWS credentials), `setup-rtce`, the
  AWS services, and the demo users' environment role. Rerunning it updates the deployment in place, and
  `uv run destroy` tears it down. It saves `TF_VAR_always_on` in `credentials.env`, like
  `TF_VAR_workshop`, so later runs stay in this mode. Nothing guards against running the scripted
  stream against it. It's simply never done.
- **Naming.** Fixed names through the existing `deployment_name` set to `river-air-demo`. Core's
  existing convention then names the environment `RIVER-AIR-DEMO-ENV`, and the cluster, compute pool,
  and service accounts get their usual suffixes.
- **A new Terraform root, `terraform/always-on/`.** It reads the outputs of `core` and `airline-demo`,
  the same way `airline-demo` reads `core`, and holds the AWS services.
- **Terraform state stays on the operator's machine.** The operator deploys, rotates the password,
  changes users, and tears down from their checkout. The running services don't depend on that machine.
  A shared remote backend is out of scope.
- **AWS shape.** One AWS account, in the same region as the Confluent cluster (default `us-east-1`).
  - **Image:** one container image built from the repo and pushed to ECR, used for both services.
  - **App:** an AWS Lambda function (container image, with the AWS Lambda Web Adapter passing requests
    to the FastAPI app) behind a public function URL, with one instance kept warm on a `live` alias.
    Extra concurrent requests (one open board already makes a few in parallel) use on-demand instances,
    which can take a few seconds to start the first time; raise provisioned concurrency to 2–3 if the
    board feels slow. The function URL gives an HTTPS URL with no domain or certificate to
    manage. The first design used App Runner, but AWS closed App Runner to new customers and rejected it
    for this account, so the board moved to Lambda; the image, secret, and generator are unchanged.
  - **Generator:** an ECS Fargate service with one task and no load balancer. ECS restarts it if it exits.
  - **Credentials:** the Kafka, Schema Registry, and Lightning/RTCE keys, and the app password, are
    stored in AWS Secrets Manager. ECS passes them to the generator as environment variables; Lambda
    can't, so the app reads the secret at startup when `STREAMHOUSE_SETTINGS_SECRET_ARN` is set.
- **Password check in the app.** The public function URL has no login of its own, so the FastAPI app gets HTTP Basic
  auth, turned on when an `APP_PASSWORD` variable is set. Locally it stays off, as today. Deploy
  generates the password, prints the URL and password, and writes them to a gitignored card,
  `tmp/always-on-card.env`.
- **App and generator config from environment variables.** Today both read `credentials.env` and the
  Terraform state on disk. They also need to accept the same values from environment variables when no
  state is present. Local behavior is unchanged.
- **Demo users' permissions (the user's decision).** All six get `EnvironmentAdmin` on the one
  environment, so they can create Flink statements and materialized tables under any name, including
  pasting the workshop SQL as written. Nothing technical stops them from dropping topics or stopping the
  pipeline. The operator asks them not to, and a redeploy is the fix if it happens. Their access is
  limited to this environment: no organization-level roles.
- **Demo users are invited by hand, and deploy grants their role.** All six were invited with the
  Confluent CLI on 2026-11-12 into the deploying organization, and each got its own user ID.
  Plus-addressing works: the first one signed in as its own Username/Password user. Deploy only grants
  `EnvironmentAdmin` on the environment, through the CLI, once it exists. The emails live in
  `credentials.env` (`DEMO_USER_EMAILS`), never in tracked files.
- **Recovery is a redeploy.** If someone breaks something, rerunning `uv run deploy --always-on`
  restores the pipeline, and `--reset-data` also clears and republishes the data. Neither touches the
  users' accounts.

## Constraints

- Everything in `AGENTS.md` still applies: business-readable names, `uv` only, synthetic data, no
  secrets in tracked files, idempotent deploy and teardown, `uv run pytest` and `uv run ruff check
  scripts tests` pass.
- Lightning returns at most 200 rows per query. The app depends on one service day being 180 flights,
  so continuous mode must keep 180 flights per day.
- Flink output reaches Lightning only at its once-a-minute commit. Nothing changes here.
- Push with `git push-external` (Airlock repo).

## Risks to check early

1. **Plus-addressed invitations: resolved.** Tested on 2026-11-12, and each address is its own user.
   The Cloud API key used to deploy must belong to the same organization.
2. **Agent run cost and quota.** About 560 hosted-Claude runs a day, every day. Confirm the
   Confluent-hosted model's rate limits allow bursts, since one delayed flight can turn dozens of
   passengers HIGH within a minute.
3. **Topic retention.** The source tables (`sql/20`–`22`) set no cleanup policy, so their topics may
   delete records after Kafka's default 7 days, which would drop history. The compacted tables
   (`passenger_recommendations`, `passenger_state`, `flight_impact`) instead keep every key forever. So
   always-on mode compacts the three source topics, and the continuous generator writes tombstones for
   each day older than 35 days, which Flink carries through to the derived tables.
4. **The Lightning frozen-copy issue** (2026-10-28 run). The app already handles it, but it may show up
   more often over weeks of running.

## Left to the builder

- How continuous mode is spelled on the command line (for example `uv run airport-datagen --continuous`).
- How delays and hotel sellouts are spread through a 24-hour day, so the board always shows a few
  delayed flights.
- The Dockerfile, the ECR repository, the names of the Lambda and ECS resources (business-readable),
  the container sizes, and the logging (CloudWatch).
- The tests: continuous mode is deterministic for a given seed and clock, a restart produces the same
  state, the agent filter renders both ways, and basic auth is on only when `APP_PASSWORD` is set.
- The README section for the operator: deploy, rotate the password, add or remove users, reset the
  data, and tear down.
