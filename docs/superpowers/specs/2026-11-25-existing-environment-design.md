# Deploy a workshop into an existing environment

## Intent

Most attendees run `uv run deploy` and get a new Confluent Cloud environment. A few attendees work
in an organization where they can't create environments: an admin gives each of them their own
environment and the **EnvironmentAdmin** role on it, nothing more. They need to deploy either lab's
workshop (Flight Disruptions or Agentic Pit Decisions) into that environment.

The admin can't create service accounts for them in advance, and EnvironmentAdmin can't create
service accounts. So in an existing environment, everything that runs as the `app-manager` and
`rtce-reader` service accounts today runs as the attendee instead.

Built on the `always-on-quick` branch. It reaches `master` with the Quick work, after the
2026-11-26 workshop.

## Goals

- `uv run deploy` supports both flows. Creating a new environment stays the default.
- An attendee with only EnvironmentAdmin on their own environment can deploy either lab's workshop
  into it, and run the workshop end to end (datagen, the app, Flink in the workspace, RTCE).
- `uv run destroy` removes the cluster, the pool and the keys, and keeps the environment.
- One codebase: a switch in the existing Terraform roots and scripts, not a copy of them.
- Both flows create the RTCE Global API key with Terraform instead of the CLI. Terraform then
  deletes it on destroy.

## Non-goals

- Full demos (`--with-labs`, `--video-2`) and `--always-on` in an existing environment. Flink
  statements that run as a user stop after 4 hours, so long-running demos need service accounts.
  Deploy refuses these combinations.
- Several attendees sharing one environment. Each attendee has their own.
- Reusing a cluster or Flink pool that's already in the environment. Deploy creates new ones.
- Cleaning up what lives at the environment level after destroy: Schema Registry subjects (created
  by Flink's `CREATE TABLE`) and the `rtce-connection` Flink connection (created by
  `uv run setup-rtce --agent-connection`). Neither is a Terraform resource. They stay in the
  environment. That's harmless when the attendee redeploys the same lab, because the schemas match.
- Tableflow in an existing environment. It's already off in workshop mode
  (`airline-demo/main.tf:82`).

## Anti-goals

- Anything that breaks or slows the default new-environment flow.
- Deleting or changing the attendee's environment.
- An error midway through a deploy that an attendee can't act on, such as a missing Schema
  Registry or a third Global key surfacing as a raw Terraform or API error.

## Constraints

What a spike on 2026-11-25 proved, signed in to the CLI as a user with only EnvironmentAdmin on
one ADVANCED environment:

- It can create, as the user:
  - a user-owned Cloud API key (`confluent api-key create --resource cloud`). That key could read
    the environment, clusters, Flink pools, Schema Registry and the service-account list through
    the API.
  - a user-owned Global API key
  - a Standard cluster
  - a Flink pool
  - a user-owned Kafka key
- A Flink `CREATE TABLE` submitted as the user (principal `u-…`) reached `COMPLETED` and created
  its topic.
- The first statement on a new cluster failed once with Confluent's generic "technical
  difficulties" error, and a retry a minute later worked.
- Not tested: creating a service account (assumed to be refused).
- Not tested: creating user-owned API keys **through Terraform**, Global keys included. The CLI
  calls the same API, but the plan's first task must prove it.
- Not tested: an environment on Stream Governance Essentials.

Constraints from Confluent and from the code:

- A user can own at most 2 Global API keys.
- Schema Registry must already be enabled in the environment.
- Terraform supports Global API keys from Confluent provider 2.73.0 (2026-05-14), through
  `confluent_api_key` with `managed_resource { id = "global", api_version = "global/v1",
  kind = "Global" }`. Today's constraint `~> 2.38` allows it. Raise the minimum to `~> 2.73` so an
  older cached provider can't be picked.
- Windows rules in `AGENTS.md` apply to all new subprocess and file code: `processes.exe(...)`,
  `encoding="utf-8", errors="replace"`, and `scripts/processes.py`.

## Approaches

- **A. One switch inside `terraform/core` (chosen).** Core takes an optional existing environment
  ID and the attendee's user ID. When they're set, core looks up the environment, skips the service
  accounts, and makes the keys user-owned. Output names stay the same, so the labs and scripts
  don't change, and destroy leaves the environment by default.
- **B. A second core root for existing environments.** Clearer names, but about 300 duplicated
  lines that drift apart, and deploy, destroy and both labs would have to pick which state to read.
  Rejected.
- **C. An admin pre-creates the service accounts.** Rejected: the admin can't.

For the RTCE Global key: Terraform in both flows (chosen), instead of Terraform only for existing
environments. That gives one way to make the key, and drops the CLI call and the `_key_owned_by`
check. The cost is that the default flow changes and needs its own test run.

## Design

### The attendee's flow

- Interactive `uv run deploy`, for a workshop deploy of either lab: right after the Confluent CLI
  login, deploy asks
  `Deploy into an existing environment? Paste its ID (env-xxxxx), or press Enter to create a new one:`
- `uv run deploy --environment env-xxxxx` skips the question.
- The ID is saved in `credentials.env` as `TF_VAR_existing_environment_id`. Reruns, `--testing`
  and `--automated` reuse it.
- Pressing Enter (or no flag and nothing saved) gives today's flow.
- Deploy refuses an existing environment, whether from the flag or saved, together with any of:
  - `--with-labs`
  - `--video-2`
  - `--always-on`
  - a `--testing` / `--automated` run that would deploy the full Flight Disruptions demo, that is
    one without `--workshop` (my call)

  The message says why: statements that run as a user stop after 4 hours.
- When the attendee chooses to generate a key, "Generate new Confluent Cloud API keys?" already
  makes a user-owned Cloud key (`scripts/credentials.py`), which works with EnvironmentAdmin. In
  this mode, the fallback text that says "an existing OrganizationAdmin key" should say
  EnvironmentAdmin.

### Checks before Terraform runs (existing environment only)

1. The environment exists and the signed-in user can see it (`confluent environment describe`).
   Otherwise stop: "Can't find env-xxxxx with your login. Check the ID, and that you have
   EnvironmentAdmin on it."
2. Schema Registry is enabled, and the Stream Governance package is ADVANCED. Essentials is
   untested; this is my call. Otherwise stop, and tell the attendee what to ask their admin for.
   Relax this to Essentials only if the end-to-end test passes on an Essentials environment.
3. Find the user's resource ID (`u-…`) from the CLI login. The spike read it from
   `confluent iam rbac role-binding list --current-user -o json`; the builder picks the most
   reliable source. Pass it to Terraform as `TF_VAR_user_id`. Don't save it; work it out on every
   deploy.
4. Count the user's Global keys. If they already have 2 and neither is the one in core's Terraform
   state, stop before Terraform. Tell them to delete one (Confluent Cloud → API keys).

### `terraform/core`

New variables: `existing_environment_id` (default `""`) and `user_id` (default `""`). Use
`local.existing = var.existing_environment_id != ""`.

| Today | With an existing environment |
|---|---|
| `confluent_environment.staging` | `count = 0`; a `data "confluent_environment"` looks up the ID instead. A local `env_id` picks one, and every `environment { id = … }` uses it. |
| `confluent_service_account.app-manager` and `rtce-reader`, their role bindings | `count = 0` |
| `confluent_kafka_acl.app-manager-*` | `count = 0` (EnvironmentAdmin's RBAC covers them) |
| Kafka, Schema Registry and Flink keys owned by app-manager (`ServiceAccount`) | owned by `var.user_id` (`kind = "User"`) |
| cluster, Flink pool, `random_id` | unchanged: created new |

- **New in both flows:** `confluent_api_key.rtce-global`, a Global key (`managed_resource` as in
  Constraints). It's owned by `rtce-reader` in a new environment and by `var.user_id` in an
  existing one.
  - New sensitive outputs: `rtce_global_api_key` and `rtce_global_api_secret`.
  - Terraform deletes it on destroy in both flows.
- Outputs keep their names. `app_manager_service_account_id` returns `var.user_id` in an existing
  environment. A comment explains this, so every lab's Flink statement
  (`principal { id = local.core.app_manager_service_account_id }`) runs as the attendee with no
  lab change.
- `confluent_rtce_service_account_id` returns `""` in an existing environment.
- Add an output `existing_environment` (bool) for scripts that need to know.
- Giving existing resources a `count` would make Terraform plan to replace them in current
  deployments. Add `moved` blocks (for example `confluent_environment.staging` →
  `confluent_environment.staging[0]`) for every resource that gets a `count`, so current
  deployments plan only the new Global key.
- Validation: `user_id` must be set when `existing_environment_id` is.
- Raise the Confluent provider constraint to `~> 2.73` in every root that declares it.

### Labs

No change is expected:

- Both labs read core's outputs.
- The Tableflow key (`airline-demo/main.tf:312`) owned by a `ServiceAccount` only exists when
  analytics is on, which is never in workshop mode. Give that resource a precondition that fails
  clearly if analytics is ever on together with an existing environment.

### RTCE Global key in the scripts

- `scripts/setup_rtce.ensure_global_key` stops creating keys. It reads `rtce_global_api_key` and
  `rtce_global_api_secret` from core's outputs, and saves them to `credentials.env` under the same
  names as today, so everything that reads the key there keeps working.
- Remove `_create_rtce_global_key_via_cli` and `_key_owned_by`, plus any tests that only cover
  them.
- Standalone `setup-rtce` without a deployment keeps asking for a key, as today.
- **Upgrade path:** a deployment made before this change has a CLI-made key saved in
  `credentials.env`. The next deploy creates the Terraform key, and `ensure_global_key` overwrites
  the saved one with it. The old key goes when `rtce-reader` is destroyed.
- Check `scripts/pit_decisions/card.py` and anything else that reads the key, and point them at
  the same source.

### Retry on a new cluster

- If a lab's `terraform apply` fails with Confluent's "technical difficulties" Flink error, deploy
  waits about 60 seconds and reruns the apply once.
- This applies to both flows, since a new environment can hit the same error. The builder decides
  how to match the error.

### `uv run destroy`

- Terraform never deletes a data source, so destroying core keeps the environment.
- The cluster, pool and every Terraform-made key (the Global key included) go as usual.
- Subjects and `rtce-connection` stay (see Non-goals). No extra cleanup code.
- In an existing environment, the destroy summary says "Your environment env-xxxxx is kept."
  instead of warning that it will be destroyed.
- After a full destroy, `TF_VAR_existing_environment_id` stays in `credentials.env`, so a redeploy
  goes back into the same environment.

### Switching flows

- If core's state exists and was deployed in the other flow (or into a different environment ID),
  deploy refuses and tells the attendee to run `uv run destroy` first. Changing it in place would
  make Terraform replace the environment.
- To go back to a new environment: `uv run destroy`, then delete `TF_VAR_existing_environment_id`
  from `credentials.env`. Alternatively, the builder may add `--environment new`. Decide in the
  plan, and say which in the docs.

### Docs

- A short "Deploying into an existing environment" section in both labs' setup instructions:
  - who it's for;
  - what the admin must provide: the attendee's own environment, EnvironmentAdmin on it, and
    Stream Governance ADVANCED;
  - the `--environment` flag;
  - the 2-Global-key note;
  - that destroy keeps the environment, along with its Schema Registry subjects and the RTCE
    connection.
- `docs/flight-disruptions/workshop.md:718` promises destroy removes "the Flink connection". Add a
  short note there for the existing-environment case.
- Note in `AGENTS.md` that `app_manager_service_account_id` can be a user, and that the RTCE
  Global key comes from Terraform.

## Tests

Pytest, matching the existing style:

- deploy's flag and prompt handling, including every refusal;
- the checks before Terraform (CLI calls mocked);
- `ensure_global_key` reading core's outputs;
- the destroy summary for an existing environment;
- the "switching flows" refusal;
- the retry.

Terraform: `terraform validate`, plus a plan in both modes if the test suite already plans. An
existing deployment's plan should show only the new Global key.

Manual end-to-end runs:

1. **Default flow, new environment, both labs:** deploy the workshop, turn on RTCE (the Global key
   now comes from Terraform), destroy, and confirm no keys are left over.
2. **Existing environment, both labs,** with the EnvironmentAdmin-only test login (`env-z9qmqy`):
   deploy the workshop, run the first workshop steps including RTCE, then destroy. Confirm the
   environment remains, with no cluster, pool or user API keys left in it.

## Left to the builder

- Where in `deploy.py` each step goes, and helper names.
- How to get the user ID reliably, and how to match the transient Flink error.
- Whether going back to a new environment needs `--environment new`.
- Any other script that reads `app_manager_service_account_id` or `confluent_rtce_service_account_id`
  and needs a guard (`demo_reset.py`, `generate_deployment_summary.py`, `mcp_setup.py`). Most are
  full-demo-only.
