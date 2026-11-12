# Always-on demo: operator guide

`uv run deploy --always-on` runs the full River Air demo around the clock, so colleagues can open the flight board and the Confluent Cloud environment any time without a presenter. This guide is for the operator who deploys and looks after it. The 90-minute scripted demo (`uv run deploy --with-labs`) and the [workshop](./workshop.md) are unchanged.

## What colleagues see

- The flight board, behind a shared password, with 180 flights per UTC day.
- Eight RA417s a day from Chicago O'Hare (00:40, 03:40, ... 21:40 UTC), each delayed the way the scripted RA417 is. The Grand Hyatt sells out at 18:00 UTC and refills at midnight.
- Random delays on the other flights. The recovery agent writes offers for every HIGH-risk passenger, not just three flights.
- Simulated customers who book about 85% of offers, the rest expire after 90 minutes. A colleague who books in the app wins over the simulated customer.
- Scene 3 (Tableflow, Glue, Athena) is for the operator only: colleagues have no AWS access.

## Prerequisites

- The repository's usual [prerequisites](../../README.md#prerequisites) and a Confluent login (`confluent login` for SSO users).
- [Docker](https://www.docker.com), **running**. Deploy builds the demo image and pushes it to Amazon ECR.
- The [AWS CLI](https://aws.amazon.com/cli/) with credentials that can create IAM roles and policies (`iam:CreateRole`, `iam:PutRolePolicy`, `iam:AttachRolePolicy`, `iam:CreatePolicy`, `iam:PassRole`). Deploy checks this before it creates anything. It also asks for the AWS credentials Tableflow uses, saved in `credentials.env`.
- The demo users' email addresses, which deploy asks for the first time and keeps in `credentials.env` as `DEMO_USER_EMAILS` (comma-separated).

## Deploy

```bash
uv run deploy --always-on
```

It deploys the full Flight Disruptions demo with `terraform/core` and `terraform/airline-demo` (with `TF_VAR_continuous`, which drops `sql/27`'s three-flight filter so the agent handles every flight, and adds Tableflow on `passenger_recommendations` next to `flight_impact`). It then publishes 29 finished days plus the current state (on a rerun only the current state, so the agent's offers and the customers' answers in history stay as they are), enables Lightning Tables and RTCE, starts the recovery agent, and compacts the source topics.

Then [`terraform/always-on/`](../../terraform/always-on/) creates, on AWS:

- an Amazon ECR repository for the image deploy builds and pushes;
- a Secrets Manager secret holding the settings both services read, including a generated flight board password;
- an AWS Lambda function for the flight board, run from the same image through the Lambda Web Adapter, with a public function URL (the board's own password is the only gate), one provisioned instance on its `live` alias so it never cold-starts, and a CloudWatch log group for it;
- an ECS Fargate cluster and service for the continuous generator (`uv run airport-datagen --continuous`), and a CloudWatch log group for it;
- the IAM roles those need.

Last, deploy gives each demo user the `EnvironmentAdmin` role on the environment. Users not yet in your Confluent organization are listed. Invite them (below) and rerun deploy. Deploy is idempotent, so rerun it any time.

### The card

Deploy prints the card and writes it to `tmp/always-on-card.env` (gitignored):

- `ALWAYS_ON_APP_URL`: the flight board.
- `ALWAYS_ON_APP_PASSWORD`: the board takes any username with this password.
- `ALWAYS_ON_DEMO_USERS`: who has the environment role.

Keep the card out of tracked files and share it only through a private channel.

### After deploying (operator checks)

1. The app URL asks for the password, then shows flights.
2. Within 15 minutes, `passenger_recommendations` has agent rows for flights other than RA417, RA183, and RA185.
3. Within 2 hours, some agent offers turn BOOKED.
4. `aws ecs update-service --force-new-deployment` (see [Logs](#logs-and-restarts) for the cluster and service names) leaves the board where it was: the generator derives every row from the seed and the wall clock.
5. One demo user can sign in to Confluent Cloud and open the environment.
6. Scene 3 works for you: `flight_impact` and `passenger_recommendations` appear as tables in the Glue database, and Athena can query them.

## Cost

On AWS the steady costs are the flight board's Lambda provisioned concurrency (one 1 GB instance kept warm around the clock, billed whether or not anyone opens the board), the generator's Fargate task, and Secrets Manager, ECR, and CloudWatch logs. Leave it running for 2 days, then read the daily cost in the Confluent Cloud Console (**Billing & payment** → **Cost**) and in AWS Cost Explorer (filtered to the region). Record both as monthly projections here, and share the card only after the cost is accepted.

> TODO(operator): measured monthly cost, Confluent Cloud: ______ ; AWS: ______ (measured over 2 days, date: ______).

## Changing users

Add a user: invite them, add their email to `DEMO_USER_EMAILS` in `credentials.env`, then rerun deploy so it grants the role.

```bash
confluent iam user invitation create <email>
uv run deploy --always-on
```

Remove a user: delete them from the organization, and remove their email from `DEMO_USER_EMAILS`.

```bash
confluent iam user list
confluent iam user delete <user-id>
```

## Rotating the password

Mark the generated password for replacement, then rerun deploy. Deploy applies the new secret. The flight board picks it up on its own: a new secret version publishes a new Lambda version and moves the `live` alias to it. Deploy then restarts the generator so it reads the secret too (whenever the image is unchanged; a new image redeploys it anyway). Then give colleagues the new card.

```bash
terraform -chdir=terraform/always-on taint random_password.app
uv run deploy --always-on
```

## Resetting the data

```bash
uv run deploy --always-on --reset-data
```

This tombstones every generated key (scripted runs and the continuous days within the 35-day retention) before it publishes the history and the current state again. Use it when the board looks wrong or after a long outage. Without it, deploy republishes only the current state and leaves the finished days alone.

Flink keeps each passenger's first HIGH-risk state, so passengers already impacted before the reset get no new offers until their flights land; this heals within about 3 hours.

## Logs and restarts

- Generator: the CloudWatch log group `/ecs/<deployment name>-flight-generator` (Terraform output `generator_log_group`).
- Flight board: the CloudWatch log group `/aws/lambda/<deployment name>-flight-board` (Terraform output `app_log_group`; the function is `app_function_name`).
- Restart the generator: rerun `uv run deploy --always-on`, or use `aws ecs update-service --cluster <ecs_cluster_name> --service <generator_service_name> --force-new-deployment`. The names are Terraform outputs of `terraform/always-on`.
- Restart the board: rerun `uv run deploy --always-on`. A new image or setting publishes a new function version and the `live` alias moves to it; otherwise nothing needs restarting, since Lambda replaces a failed instance on the next request.

## Tearing down

```bash
uv run destroy
```

This destroys `terraform/always-on` first, then the labs and the core environment, and deletes the card.

## Tell the users

Anyone can follow the [workshop](./workshop.md) in this environment, but pasting its SQL as written replaces the shared pipeline's tables (the statements use `CREATE OR ALTER`), and the flight board and the agent stop working. Use new table names, such as `my_passenger_state`, in every statement you paste.
