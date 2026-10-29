# River Air Flight Recovery — Streamhouse Walkthrough

Built on [Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/overview.html),
[Lightning Tables](https://docs.confluent.io/cloud/current/lightning/overview.html),
[RTCE](https://docs.confluent.io/cloud/current/ai/real-time-context-engine/overview.html), and
[Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html).

River Air runs 180 flights a day through its SFO hub. Flight RA417 from Chicago slips by nearly two
hours, and 150 of its passengers miss their connections. The demo has three scenes:

1. **Live flights and passengers.** The app reads `flight_status`, `flight_impact`, and `passenger_state`
   through Lightning Tables: every flight today, its delay, how many passengers it puts at risk, and its seat map.
2. **Connection risk and hotel-aware recovery.** Flink recomputes each connection as flight updates
   arrive. Each HIGH-risk passenger gets two rebooking offers; overnight ones include a hotel. When
   the Grand Hyatt at SFO sells out, selecting it swaps in the Hilton, so the passenger chooses between the Marriott and the Hilton.
3. **History in Iceberg.** Tableflow writes `flight_impact` and `passenger_recommendations` to
   Iceberg in S3 with an AWS Glue catalog, and Athena answers: how many flights were delayed in the
   last 30 days, how many passengers were impacted, what it cost, and how long recovery takes.

The Flink SQL is in [`terraform/airline-demo/sql/`](../terraform/airline-demo/sql/) (files `20`–`25`),
and the [data model](./data-model.md) explains every field.

## Run the demo

`uv run deploy` provisions everything, publishes 30 days of history and today's flights, starts a
90-minute live stream in the background (log: `tmp/datagen.log`), enables Lightning Tables and RTCE,
and offers to start the app at http://127.0.0.1:8000.

RA417 is scheduled 50 minutes after the stream starts. From +5 to +25 minutes its delay grows every
30 seconds, and the Grand Hyatt at SFO sells out at +40. To restart the scene, run `uv run airport-datagen`; it
stops any stream already running, republishes the data, and streams for another 90 minutes. For a
quicker rehearsal, add `--skip-history`, and `--speed 5` plays the stream five times faster (the 90 minutes take 18). `--dry-run` prints the records without publishing, and
`--reset` writes tombstones for every generated key.

In the app, open RA417, hover its seat map, and pick a passenger (click a seat or a table row). **Hotel options** replaces **Biggest delays** and shows
the Grand Hyatt's rooms counting down. Select the Grand Hyatt offer after the sellout: it shows as
sold out and the app replaces it with a Hilton offer, so the passenger now chooses between the Marriott and the Hilton.
Select the Hilton and book it.

For Demo 3, enable Tableflow on `passenger_recommendations` on screen (Tableflow on `flight_impact`
is already enabled by Terraform), then run the Athena queries in the [data model](./data-model.md#work-backwards-from-the-questions).
Ask the questions as "in the last 30 days"; the data covers a trailing 30-day window.

When core has the Bedrock connection (AWS credentials present), deploy also starts the recovery
agent. It is Claude on Amazon Bedrock, reading live flights and hotel rooms through RTCE/MCP, and
it writes the live day's offers for RA417 ([SQL 26–31](../terraform/airline-demo/sql/)). The generator
writes every other flight's offers, and RA417's too without Bedrock. The Webhooks source connector is still planned, so the
generator publishes hotel updates directly to Kafka.

To record Demo 2 with its statements run live, run `uv run reset-demo-2` first. It keeps Demo 1
(the source tables, `passenger_state`, `flight_impact`, Lightning Tables, RTCE) and the Bedrock
model, drops the Native Inference model, the `hotel_inventory_live_context` tool, the agent, and its INSERT,
rebuilds the two staging tables off screen, deletes today's RA417 offers, and restarts the stream.
It prints when RA417 flips and when the Grand Hyatt sells out. Then run CREATE MODEL, CREATE TOOL,
CREATE AGENT, and the INSERT on screen ([SQL 32, 29, 30, 31](../terraform/airline-demo/sql/)).
Show the `passenger_state` join as `CREATE OR ALTER`; it already exists. To skip the live run,
`uv run reset-demo-2 --restore` deletes any on-screen copy and starts the agent the way deploy does.

When replaying `CREATE OR ALTER MATERIALIZED TABLE`, preserve the schema and `WITH` options in
[`sql/24-serving-passenger-state.sql`](../terraform/airline-demo/sql/24-serving-passenger-state.sql),
and take the existing table's exact distribution from `SHOW CREATE MATERIALIZED TABLE`
before evolving it. A query-only definition defaults to Avro keys and can mix Avro records
with the existing raw keys, making `SELECT` fail at the first old record.
`reset-demo-2`, even with `--full`,
preserves `passenger_state` and cannot repair mixed key encodings; its derived topic must
be rebuilt from the source feeds, along with any staging tables that copied the malformed keys.

## Prerequisites

### Local dependencies

```bash
brew install uv git python && brew tap hashicorp/tap && brew install hashicorp/tap/terraform && brew install --cask confluent-cli
```

**Windows:**

```powershell
winget install astral-sh.uv Git.Git Hashicorp.Terraform ConfluentInc.Confluent-CLI Python.Python
```

### API keys & access

> [!NOTE]
>
> This repository deploys to AWS `us-east-1` on a Standard Kafka cluster. To use another region, set `TF_VAR_cloud_region` in `credentials.env` before deploying. A region deploys a Dedicated cluster (1 CKU) when `DEDICATED_CLUSTER_REGIONS` in [tfvars.py](../scripts/tfvars.py) lists it (`us-west-2` does), or when you set `TF_VAR_kafka_cluster_type=dedicated`. Dedicated adds the hotel webhook connector; Standard has none, so the generator publishes hotel availability straight to Kafka. Changing the type of an existing deployment replaces its cluster.

- **Confluent Cloud** account with rights to create environments, clusters, Flink pools, and API keys.
- **AWS credentials** (optional) power the Tableflow S3/Glue analytics scene and core's Bedrock
  connection. Run `uv run api-keys create` to auto-generate credentials. Without them, everything
  else still deploys.

> [!WARNING]
>
> **AWS Bedrock users:** request access to the Claude model in your deployment region (`us-east-1` by default) via the [Model Catalog](https://console.aws.amazon.com/bedrock/home#/model-catalog) before deploying.

## Deploy the Demo

Clone and pull the latest:

```bash
git clone https://github.com/confluentinc/quickstart-streamhouse.git
cd quickstart-streamhouse
uv sync
```

Deploy the platform (`terraform/core` then `terraform/airline-demo` — cluster, Flink pool, the
source tables, `passenger_state`, `flight_impact`, and Tableflow to S3/Glue when AWS creds are present):

```bash
uv run deploy
```

It prompts for your Confluent Cloud login + API key, (optionally) AWS credentials, and which coding agent should get the RTCE MCP server. After Terraform it publishes the demo data, starts the live stream, and enables RTCE + Lightning on the demo topics. To re-register the MCP server later, run `uv run setup-rtce`.

> [!NOTE]
>
> Deploy mints an org-wide (Global) API key for the reader service account and writes it to the
> git-ignored `credentials.env`. It's the key the app and Lightning queries authenticate with —
> keep it out of screenshots and recordings.

## Conclusion

The demo maintains passenger state in Flink, serves current rows to the app through Lightning
Tables and to a connected agent through RTCE/MCP, and enables Tableflow for Iceberg history.

## Troubleshooting

<details>
<summary>Click to expand</summary>

- **App shows `503 … No RTCE/Lightning Global API key`** — run `uv run setup-rtce`; it writes the
  Global key to `credentials.env`.
- **`/api/state` empty or no passengers at risk** — the stream hasn't reached RA417's first delay
  (+5 minutes), the stream has stopped (check `tmp/datagen.log`), or Flink is catching up. Run
  `uv run airport-datagen` to restart the scene.
- **Lightning returns at most 200 rows** — every Lightning query is capped at 200 rows, and OFFSET,
  COUNT, and GROUP BY are rejected. The app filters to one service day (180 flights) and pages
  larger results by key (`WHERE "KEY" > 'last' ORDER BY "KEY"`).
- **Lightning `curl` / app read returns 401** — Global key still propagating (retry), or the topic
  lacks `DeveloperRead` for the reader SA.
- **`queryData` errors on missing args** — pass **all** of `topic_name`, `query`, and
  `max_result_rows`. `getMetadata` takes a single `topic_name` string.
- **Tableflow topics not listed** — Tableflow needs AWS credentials in core
  (`aws_tableflow_access_key`) and `enable_analytics = true` in `terraform/airline-demo`.
- **Targeted Terraform change** — never `apply` the full stack for a targeted change; use `-target`.

</details>

## 🧹 Clean-up

```bash
uv run destroy
```

Removes the Terraform-managed resources (core + airline-demo, including Tableflow and minted keys).
Confirm the environment is gone in the Confluent Cloud console, and rotate any credentials used for
a recording. Synthetic data only — no real PII; never commit secrets.

## Navigation

- **Overview:** [README](../README.md)
- **Workshop:** [workshop.md](./workshop.md)
- **Data model:** [data-model.md](./data-model.md)
