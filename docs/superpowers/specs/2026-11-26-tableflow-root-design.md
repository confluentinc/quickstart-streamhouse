# Keep the AWS provider away from workshop attendees

## Intent
No workshop attendee (Flight Disruptions, Agentic Pit Decisions, or both; new or existing environment) downloads the
`hashicorp/aws` Terraform provider. Today `terraform/airline-demo` declares it for Tableflow (`analytics.tf`), and
`terraform init` downloads every declared provider even when every resource has `count = 0`.

## Goals / non-goals
- Goal: a workshop deploy initialises only roots that don't declare `hashicorp/aws`.
- Goal: full demos with Tableflow AWS creds, and always-on, keep working with Tableflow exactly as today.
- Non-goal: changing what Tableflow builds or how it's configured.
- Non-goal: migrating existing deployments (there are none).
- Anti-goal: anything that breaks the workshop path before the next workshop. PR #19 merges only after a workshop deploy
  test.

## Approaches
- **Chosen: a separate `terraform/tableflow` root**, applied only when Tableflow is on. It holds all of Tableflow, both
  the AWS and the Confluent side, because the two depend on each other: the IAM role needs the integration's external
  ID, and the topics need the role.
- Rejected: keeping Tableflow in `airline-demo` behind `count`, or in a child module. Terraform still downloads the
  provider.
- Rejected: moving only the AWS resources out. That creates a circular dependency between two roots.

## Design
- **`terraform/tableflow`**
  - Moved here: `analytics.tf`, the Tableflow API key, the `aws` and `time` providers, the variables
    `enable_analytics` and `tableflow_topics` (plus `continuous`), and the outputs `tableflow_topics`,
    `analytics_bucket`, `tableflow_role_name` and `glue_database`.
  - Reads core through `terraform_remote_state`, as `always-on` does.
  - The code moves; it isn't copied. The only new repeated parts are the provider block, the remote-state lookup and the
    naming prefix.
- **`airline-demo`** declares only `confluent`. Its Tableflow pieces are removed.
- **Deploy** applies `tableflow` after `airline-demo` only when Tableflow is on: Flights is deployed, it isn't a
  workshop, and Tableflow AWS creds are present. That covers `--with-labs` with creds, and always-on, which requires
  creds.
- **Destroy** removes `tableflow` before `airline-demo` (full destroy and `--lab flight-disruptions`). The
  provider-integration release moves with it.
- **`always-on`** reads the bucket and Glue database from `tableflow`'s state. Deploy's Tableflow `-replace` hint points
  at `terraform/tableflow`.
- **Test:** no root a workshop deploy initialises declares `hashicorp/aws`.

## Testing before merge
1. A Flights workshop deploy and destroy. `terraform init` shows no `hashicorp/aws`.
2. The F1 end-to-end run.
3. `--with-labs` with Tableflow creds, before demos or always-on rely on it.

## Left to the builder
File split inside `terraform/tableflow`; whether its resources keep a `count` guard or become unconditional (the root is
applied only when Tableflow is on).
