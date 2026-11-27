# Always-on Amazon Quick Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `uv run deploy --always-on` also builds an Athena workgroup, a Quick data source, two datasets and a Quick Topic over the Tableflow tables, then a new deployment `river-air-enablement` in `us-east-1` proves it.

**Architecture:** Tableflow → Glue stays in `terraform/airline-demo`, with a 30 s IAM wait added. A new `terraform/always-on/quick.tf` reads airline-demo's state and adds Athena + Quick; the Topic is a `terraform_data` that calls the AWS CLI on create and destroy. `scripts/deploy.py` waits for the Glue tables and finds the demo users' Quick user ARNs before it applies `terraform/always-on`.

**Tech Stack:** Terraform (hashicorp/aws 6.73, hashicorp/time), AWS CLI, Python 3 with `uv`, pytest.

**Spec:** [docs/superpowers/specs/2026-11-17-always-on-quick-design.md](../specs/2026-11-17-always-on-quick-design.md)

## Global Constraints

- Never declare `aws_quicksight_account_subscription` or any account-wide Quick setting.
- `QUICK_IDENTITY_REGION = "us-east-1"`; Quick data source, datasets and Topic use `local.core.cloud_region`.
- Resource names start with `local.name` (`river-air-enablement` for the new deployment): `<name>-athena-results`, `<name>-analytics` (workgroup), `<name>-quick-athena` (role), `<name>-athena` (data source ID), `<name>-flight-impact` and `<name>-passenger-recommendations` (dataset IDs), `<name>-flight-history` (Topic ID).
- Delayed means `delay_minutes >= 15`; recovery time is `recommended_at - impacted_at` in seconds.
- With `var.quick_principal_arns = []` or `analytics_bucket = ""`, `quick.tf` creates nothing.
- No email addresses in tracked Terraform (`test_no_email_addresses_in_tracked_terraform`).
- Work on branch `always-on-quick` off `continuous-demo-on-aws` in `~/0-workshops/Enablement/quickstart-streamhouse`. Never run Terraform in `~/0-workshops/quickstart-streamhouse` (the live `river-air-demo`).
- `uv run pytest` and `uv run ruff check scripts tests` pass after every task.

## Review Focus

- Tableflow is FAILED or the Glue tables never appear → deploy warns with the topic's error and the `-replace` fix, skips Quick, and still finishes the board and generator (Task 4: `test_prepare_quick_skips_when_glue_tables_never_appear`).
- A `DEMO_USER_EMAILS` address has no Quick user → warned by name, the others still get access (Task 4: `test_quick_principals_match_emails_case_insensitively`).
- Rerun when the Topic already exists → create deletes it first, so the apply doesn't fail with `ResourceExistsException` (Task 3: `test_topic_create_is_idempotent_and_destroy_tolerates_a_missing_topic`).
- `uv run destroy` → the Topic is deleted before the datasets, and the results bucket empties (Task 3 same test; Task 2: `test_athena_results_bucket_force_destroys`).
- Upsert topics in Iceberg → Athena returns one row per key (Task 6, Step 4).

---

### Task 1: Wait for the Tableflow IAM role before enabling Tableflow

**Files:**
- Modify: `terraform/airline-demo/providers.tf` (add `time = { source = "hashicorp/time", version = ">= 0.9.0" }`)
- Modify: `terraform/airline-demo/analytics.tf` (new `time_sleep.tableflow_role`; `depends_on` of `confluent_tableflow_topic.analytics` and `confluent_catalog_integration.glue`)
- Test: `tests/test_always_on.py`

**Interfaces:**
- Produces: `time_sleep.tableflow_role[0]` (count on `local.analytics_enabled`), `create_duration = "30s"`, `depends_on` both role policy attachments.

- [ ] **Step 1: Write the failing test**

```python
def test_tableflow_waits_for_its_iam_role():
    analytics = (ROOT / "terraform/airline-demo/analytics.tf").read_text()
    sleep = _block(analytics, "time_sleep.tableflow_role")
    assert re.search(r'create_duration\s*=\s*"30s"', sleep)
    assert "aws_iam_role_policy_attachment.tableflow_s3" in sleep
    assert "aws_iam_role_policy_attachment.tableflow_glue" in sleep
    for address in ("confluent_tableflow_topic.analytics", "confluent_catalog_integration.glue"):
        assert "time_sleep.tableflow_role" in _block(analytics, address)
```

- [ ] **Step 2:** `uv run pytest tests/test_always_on.py::test_tableflow_waits_for_its_iam_role -v` → FAIL (`ValueError: substring not found`).
- [ ] **Step 3:** Add the provider and `time_sleep.tableflow_role`; add it to both `depends_on` lists. Comment: the 2026-11-12 `river-air-demo` deploy failed both topics with "Unable to assume the IAM role" and Tableflow never retries.
- [ ] **Step 4:** Test passes; `terraform -chdir=terraform/airline-demo init -backend=false && terraform -chdir=terraform/airline-demo validate` → `Success!`.
- [ ] **Step 5:** Commit `Wait for the Tableflow role to propagate before enabling Tableflow`.

### Task 2: Athena workgroup, Quick role, data source and datasets

**Files:**
- Create: `terraform/always-on/quick.tf`
- Modify: `terraform/always-on/variables.tf`, `terraform/always-on/outputs.tf`
- Test: `tests/test_always_on.py`

**Interfaces:**
- Consumes: `../airline-demo/terraform.tfstate` outputs `analytics_bucket`, `glue_database` (local backend, same as core).
- Produces: `variable "quick_principal_arns" { type = list(string), default = [] }`; `local.quick_enabled = length(var.quick_principal_arns) > 0 && local.analytics.analytics_bucket != ""`; resources `aws_quicksight_data_set.flight_impact[0]`, `aws_quicksight_data_set.passenger_recommendations[0]`; outputs `quick_datasets` (list of dataset IDs, `[]` when off) and `athena_workgroup` (`""` when off).

- [ ] **Step 1: Write the failing tests**

```python
def test_quick_never_manages_the_account_subscription():
    text = "".join(p.read_text() for p in (ROOT / "terraform").rglob("*.tf"))
    assert "aws_quicksight_account_subscription" not in text
    assert "aws_quicksight_account_settings" not in text


def test_quick_reads_athena_through_its_own_role():
    quick = (TF / "quick.tf").read_text()
    source = _block(quick, "aws_quicksight_data_source.athena")
    assert re.search(r'type\s*=\s*"ATHENA"', source)
    assert "aws_iam_role.quick_athena[0].arn" in source and "aws_athena_workgroup.analytics[0].name" in source
    assert "quicksight.amazonaws.com" in _block(quick, "aws_iam_role.quick_athena")
    for name in ("flight_impact", "passenger_recommendations"):
        dataset = _block(quick, f"aws_quicksight_data_set.{name}")
        assert re.search(r'import_mode\s*=\s*"DIRECT_QUERY"', dataset)
        assert f'"{name}"' in dataset and "local.analytics.glue_database" in dataset
    assert "{delay_minutes} >= 15" in _block(quick, "aws_quicksight_data_set.flight_impact")
    assert "dateDiff({impacted_at}, {recommended_at}" in _block(quick, "aws_quicksight_data_set.passenger_recommendations")


def test_athena_results_bucket_force_destroys():
    quick = (TF / "quick.tf").read_text()
    assert re.search(r"force_destroy\s*=\s*true", _block(quick, "aws_s3_bucket.athena_results"))
    workgroup = _block(quick, "aws_athena_workgroup.analytics")
    assert re.search(r"force_destroy\s*=\s*true", workgroup) and "aws_s3_bucket.athena_results[0]" in workgroup


def test_quick_is_skipped_without_principals():
    variables = (TF / "variables.tf").read_text()
    assert re.search(r'variable "quick_principal_arns" \{[^}]*default\s*=\s*\[\]', variables, re.S)
    assert "length(var.quick_principal_arns) > 0" in (TF / "quick.tf").read_text()
```

- [ ] **Step 2:** Run the four tests → FAIL (`quick.tf` missing).
- [ ] **Step 3:** Write `quick.tf`. Every resource has `count = local.quick_enabled ? 1 : 0`.
  - `aws_s3_bucket.athena_results` (`force_destroy = true`).
  - `aws_athena_workgroup.analytics`: `force_destroy = true`, `enforce_workgroup_configuration = true`, `output_location = "s3://${aws_s3_bucket.athena_results[0].bucket}/"`.
  - `aws_iam_role.quick_athena` trusting `quicksight.amazonaws.com`, with an inline policy granting:
    - Athena query actions on the workgroup ARN;
    - `glue:GetDatabase*`, `glue:GetTable*`, `glue:GetPartition*` on catalog, database and `table/<db>/*`;
    - `s3:GetObject` and `s3:ListBucket` on the analytics bucket;
    - `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`, `s3:GetBucketLocation` and `s3:AbortMultipartUpload` on the results bucket.
  - `aws_quicksight_data_source.athena`: `parameters.athena { work_group, role_arn }`.
  - `aws_quicksight_data_set.flight_impact`:
    - `relational_table` with catalog `AwsDataCatalog`, schema `local.analytics.glue_database`, name `flight_impact`;
    - input columns from the Glue types: `string`→`STRING`, `timestamp`→`DATETIME`, `int`→`INTEGER`, `decimal`→`DECIMAL`;
    - a `logical_table_map` that adds `is_delayed = ifelse({delay_minutes} >= 15, 'Yes', 'No')`.
  - `aws_quicksight_data_set.passenger_recommendations`: built the same way, adding `recovery_seconds = dateDiff({impacted_at}, {recommended_at}, 'SS')`.
  - `permissions` on the data source and both datasets: a `dynamic` block over `var.quick_principal_arns`, with the owner actions from the Quick docs. For a data source: Describe, DescribeDataSourcePermissions, Pass, Update, Delete, UpdateDataSourcePermissions. For a dataset: Describe, DescribeDataSetPermissions, Pass, DescribeIngestion, ListIngestions, Update, Delete, CreateIngestion, CancelIngestion, UpdateDataSetPermissions.
- [ ] **Step 4:** Tests pass; `terraform -chdir=terraform/always-on init -backend=false && terraform -chdir=terraform/always-on validate` → `Success!`.
- [ ] **Step 5:** Commit `Add Athena and Quick datasets over the Tableflow history`.

### Task 3: Quick Topic through the AWS CLI

**Files:**
- Modify: `terraform/always-on/quick.tf`, `terraform/always-on/outputs.tf`
- Test: `tests/test_always_on.py`

**Interfaces:**
- Consumes: both datasets' `arn` from Task 2.
- Produces: `terraform_data.quick_topic[0]` with `input = { topic_id, account_id, region }`; output `quick_topic_id` (`""` when off).

- [ ] **Step 1: Write the failing test**

```python
def test_topic_create_is_idempotent_and_destroy_tolerates_a_missing_topic():
    topic = _block((TF / "quick.tf").read_text(), "terraform_data.quick_topic")
    assert "aws quicksight delete-topic" in topic and "aws quicksight create-topic" in topic
    assert topic.index("delete-topic") < topic.index("create-topic")  # a rerun replaces a leftover topic
    assert "update-topic-permissions" in topic
    assert re.search(r"when\s*=\s*destroy", topic) and "|| true" in topic
    assert "aws_quicksight_data_set.flight_impact[0].arn" in topic
    assert "aws_quicksight_data_set.passenger_recommendations[0].arn" in topic
```

- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Add `terraform_data.quick_topic`.
  - `triggers_replace = sha256(local.topic_json)`. `local.topic_json = jsonencode({...})`: `Name = "River Air flight history"`, `DataSets` for both datasets, and `Columns` with `ColumnFriendlyName`, `ColumnDescription` and `ColumnSynonyms`. Synonyms: `hotel_cost` → "hotel spend", "hotel cost"; `affected_passengers` → "impacted passengers"; `recovery_seconds` → "recovery time"; `is_delayed` → "delayed".
  - Create provisioner: `aws quicksight delete-topic ... || true; aws quicksight create-topic --cli-input-json "$TOPIC_JSON" && aws quicksight update-topic-permissions --grant-permissions "$TOPIC_PERMISSIONS"`. JSON goes through `environment`, never inline in the command.
  - Destroy provisioner: `aws quicksight delete-topic --aws-account-id ${self.input.account_id} --topic-id ${self.input.topic_id} --region ${self.input.region} || true`.
- [ ] **Step 4:** Test passes; `terraform validate` → `Success!`.
- [ ] **Step 5:** Commit `Create the Quick Topic so the history answers plain-English questions`.

### Task 4: Deploy finds the Quick users and waits for the Glue tables

**Files:**
- Modify: `scripts/deploy.py`. Add constants near `ALWAYS_ON_CARD`. Call `_prepare_quick` in `_finish_always_on` after `_compact_sources`. Add `ALWAYS_ON_QUICK_TOPIC` to `_write_card`. Add `quicksight:CreateDataSource` and `athena:CreateWorkGroup` to `ALWAYS_ON_IAM_ACTIONS`.
- Test: `tests/test_always_on.py` (update `_stub_finish` and `test_always_on_finish_order`)

**Interfaces:**
- Produces:
  - `QUICK_IDENTITY_REGION = "us-east-1"`, `QUICK_TABLES = ("flight_impact", "passenger_recommendations")`.
  - `_quick_principals(account_id: str, emails: list[str]) -> list[str]`: Quick user ARNs from `aws quicksight list-users --region us-east-1`, matching emails case-insensitively. Warns `⚠ No Quick user for <email>` for each miss.
  - `_wait_for_glue_tables(region: str, database: str, tables: tuple[str, ...], timeout_s: int = 900) -> bool`: polls `aws glue get-table` every 30 s.
  - `_prepare_quick(root, infra: dict, emails: list[str]) -> None`: reads airline-demo's `glue_database`, waits for the tables, and sets `os.environ["TF_VAR_quick_principal_arns"]` to the JSON list (or `"[]"` with a warning).

- [ ] **Step 1: Write the failing tests**

```python
def test_always_on_finish_order(monkeypatch, tmp_path):
    ...  # existing body; the expected order becomes:
    assert _names(calls) == ["history", "rtce", "agent", "compact", "quick", "image", "terraform", "users", "card"]


def test_quick_principals_match_emails_case_insensitively(monkeypatch, capsys):
    users = {"UserList": [{"Email": "Me@X.io", "Arn": "arn:q:me"}, {"Email": "other@x.io", "Arn": "arn:q:o"}]}
    monkeypatch.setattr(deploy.subprocess, "run", lambda cmd, **k: _completed(cmd, json.dumps(users)))
    assert deploy._quick_principals("1", ["me@x.io", "nobody@x.io"]) == ["arn:q:me"]
    assert "No Quick user for nobody@x.io" in capsys.readouterr().out


def test_prepare_quick_skips_when_glue_tables_never_appear(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(deploy, "run_terraform_output", lambda state: {"glue_database": "lkc-1"})
    monkeypatch.setattr(deploy, "_wait_for_glue_tables", lambda *a, **k: False)
    monkeypatch.setattr(deploy, "_tableflow_errors", lambda infra: {"flight_impact": "Unable to assume the IAM role"})
    (tmp_path / "terraform/airline-demo").mkdir(parents=True)
    (tmp_path / "terraform/airline-demo/terraform.tfstate").write_text("{}")
    deploy._prepare_quick(tmp_path, INFRA, ["me@x.io"])
    out = capsys.readouterr().out
    assert deploy.os.environ["TF_VAR_quick_principal_arns"] == "[]"
    assert "Unable to assume the IAM role" in out and "-replace" in out
```

- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement the three functions, plus `_tableflow_errors(infra) -> dict[str, str]`. It reads `confluent tableflow topic list --environment --cluster -o json` and returns `topic_name → error_message` where `phase == "FAILED"`. The skip message names the `terraform -chdir=terraform/airline-demo apply -replace=...` command for each failed topic. `_stub_finish` stubs `_prepare_quick` as `calls.append("quick")`. Account ID comes from `aws sts get-caller-identity`.
- [ ] **Step 4:** `uv run pytest tests/test_always_on.py -v` and `uv run ruff check scripts tests` pass.
- [ ] **Step 5:** Commit `Share the Quick datasets with the demo users who have Quick`.

### Task 5: Docs

**Files:**
- Modify: `docs/flight-disruptions/always-on.md`:
  - "What colleagues see": Quick is for demo users with a Quick user.
  - Prerequisites: a Quick user in `us-east-1`, matched by email.
  - What deploy creates.
  - Operator check 6: ask both Appendix E questions in the Topic.
  - The card's `ALWAYS_ON_QUICK_TOPIC`.
  - Destroy removes the Quick resources but never the subscription.
- Modify: `AGENTS.md`, the always-on bullet and the `terraform/always-on/` layout line: add "Athena and an Amazon Quick Topic over the Tableflow tables".
- Modify: `docs/flight-disruptions/demo-script-screens.md` row 3-2a: in always-on, use the `River Air flight history` Topic.

- [ ] **Step 1:** Edit the three files.
- [ ] **Step 2:** `uv run pytest` passes (workshop and doc tests included).
- [ ] **Step 3:** Commit `Document Quick in the always-on demo`.

### Task 6: Deploy `river-air-enablement` in us-east-1 and verify

**Files:**
- Create (gitignored): `credentials.env` with `TF_VAR_deployment_name='river-air-enablement'` and `TF_VAR_cloud_region='us-east-1'`.

- [ ] **Step 1:** Write the two lines above. Confirm `git check-ignore credentials.env` prints `credentials.env`.
- [ ] **Step 2:** The operator runs `uv run deploy --always-on` (interactive: Confluent API key, the ambient AWS credentials, `DEMO_USER_EMAILS` = the operator's email). Expected:
  - `Deployment name: RIVER-AIR-ENABLEMENT`;
  - `✓ AWS caller ... can create the always-on roles`;
  - the card shows `ALWAYS_ON_QUICK_TOPIC=river-air-enablement-flight-history`.
- [ ] **Step 3:** `confluent tableflow topic list --environment <env> --cluster <lkc>` shows `flight_impact` and `passenger_recommendations` `RUNNING`. `aws glue get-tables --database-name <lkc>` lists both.
- [ ] **Step 4:** In the `river-air-enablement-analytics` workgroup, Athena returns the same number for `count(*)` and `count(DISTINCT key)` on both tables. It also runs Appendix E's two queries and both return non-zero answers. Appendix E's figures (1,643 flights, 22.7 s) are for the scripted seed, and the continuous stream gives its own; record them in `always-on.md`.
- [ ] **Step 5:** In Quick (`us-east-1`), open the Topic and ask both Appendix E questions. The answers match Step 4.
- [ ] **Step 6:** `aws lambda list-functions` still shows `river-air-demo-flight-board` unchanged, and `RIVER-AIR-DEMO-ENV` is untouched.
