# Tableflow Root Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Workshop deploys never download the `hashicorp/aws` provider. Tableflow moves from `terraform/airline-demo`
into a new `terraform/tableflow` root, which deploy applies only when Tableflow is on.

**Architecture:**
- The new root holds all of today's `analytics.tf`, the Tableflow API key, and the `aws` and `time` providers. It reads
  core through `terraform_remote_state`.
- `airline-demo` keeps only `confluent`.
- Deploy appends `tableflow` to its targets only for non-workshop Flights deploys that have Tableflow AWS creds.
- Destroy removes `tableflow` before `airline-demo`. `always-on` reads `tableflow`'s state.

**Tech Stack:** Terraform (confluentinc/confluent ~> 2.73, hashicorp/aws >= 5.17.0, hashicorp/time >= 0.9.0), Python
(uv, pytest, ruff).

**Spec:** `docs/superpowers/specs/2026-11-26-tableflow-root-design.md`

## Global Constraints
- Branch `always-on-quick` (PR #19). Pushes use `git push-external`. Don't merge.
- The workshop path must keep working. Nothing in `core` or `agentic-pit-decisions` changes.
- No migration of existing state (there are no deployments).
- Windows rules (AGENTS.md):
  - every CLI is called as `processes.exe(...)`;
  - text-capturing subprocess calls pass `encoding="utf-8", errors="replace"`;
  - file I/O passes `encoding="utf-8"`.
- Before each commit: `uv run pytest -q`, `uv run ruff check scripts tests`, `uv run ruff format --check scripts tests`,
  and `terraform fmt -check` on the changed roots.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus
1. **A workshop deploy of either lab, or both:** `_targets` never includes `tableflow`. Test in Task 2.
2. **`--with-labs` without Tableflow creds:** no `tableflow` target. Test in Task 2.
3. **Destroy with `tableflow` state present:** `tableflow` goes before `airline-demo`, including for
   `--lab flight-disruptions`. Test in Task 2.
4. **Always-on with Tableflow skipped or not yet deployed:** `quick.tf`'s remote-state read of `tableflow` must not fail
   when that state file is absent. Covered by `try()`, checked in Task 1's validate.
5. **Leftover references to `terraform/airline-demo` Tableflow addresses**, in the `-replace` hint and destroy's release
   constant. Tests in Task 2.

---

### Task 1: Move Tableflow into `terraform/tableflow`

**Files:**
- Create: `terraform/tableflow/providers.tf`, `terraform/tableflow/main.tf`, `terraform/tableflow/variables.tf`,
  `terraform/tableflow/outputs.tf`
- Move: `terraform/airline-demo/analytics.tf` → `terraform/tableflow/analytics.tf` (`git mv`)
- Modify:
  - `terraform/airline-demo/providers.tf`: drop `aws`, `time` and `provider "aws"`;
  - `terraform/airline-demo/main.tf`: drop `confluent_api_key.tableflow` (lines 309-337), `analytics_enabled`,
    `tableflow_topics`, and the `tableflow-api-key`/`tableflow`/`glue` labels;
  - `terraform/airline-demo/variables.tf`: drop `enable_analytics` and `tableflow_topics`;
  - `terraform/airline-demo/outputs.tf`: drop `tableflow_topics`, `analytics_bucket`, `tableflow_role_name` and
    `glue_database`.
- Modify: `terraform/always-on/quick.tf:16-27`, so `data.terraform_remote_state.airline_demo` becomes `tableflow` and
  reads `../tableflow/terraform.tfstate`. Keep the `try(..., "")` reads.
- Test: `tests/test_tableflow_root.py` (new); update `tests/test_always_on.py:522-530` to read
  `terraform/tableflow/analytics.tf`.

**Interfaces:**
- Produces outputs from `terraform/tableflow`, with the same names and meanings as today: `tableflow_topics`,
  `analytics_bucket`, `tableflow_role_name` and `glue_database`.
- Produces variables: `enable_analytics` (bool, default true), `tableflow_topics` (list, default `["flight_impact"]`)
  and `continuous` (bool, default false). Their descriptions move verbatim.
- Produces resource addresses that Task 2 relies on: `confluent_tableflow_topic.analytics["<topic>"]`,
  `confluent_catalog_integration.glue`, and `confluent_provider_integration.tableflow`. Indexed `[0]` only if the
  builder keeps `count`; Task 2 uses whatever this task produces.

- [ ] **Step 1: Write the failing tests** in `tests/test_tableflow_root.py`:

```python
TF = Path(__file__).parent.parent / "terraform"

def _declares_aws(root: str) -> bool:
    return any("hashicorp/aws" in f.read_text(encoding="utf-8") for f in (TF / root).glob("*.tf"))

def test_only_tableflow_and_always_on_declare_the_aws_provider():
    assert {r.name for r in TF.iterdir() if r.is_dir() and _declares_aws(r.name)} == {"tableflow", "always-on"}

def test_tableflow_owns_the_analytics_outputs_and_airline_demo_dropped_them():
    outputs = (TF / "tableflow/outputs.tf").read_text(encoding="utf-8")
    for name in ("tableflow_topics", "analytics_bucket", "tableflow_role_name", "glue_database"):
        assert f'output "{name}"' in outputs
        assert f'output "{name}"' not in (TF / "airline-demo/outputs.tf").read_text(encoding="utf-8")

def test_always_on_reads_tableflow_state():
    quick = (TF / "always-on/quick.tf").read_text(encoding="utf-8")
    assert "../tableflow/terraform.tfstate" in quick and "../airline-demo/terraform.tfstate" not in quick
```

- [ ] **Step 2: Run them.** Run `uv run pytest tests/test_tableflow_root.py -v`. Expected: 3 FAIL.
- [ ] **Step 3: Implement the move.**
  - `terraform/tableflow` reads core through `data "terraform_remote_state" "core"` (path `../core/terraform.tfstate`),
    copying the `local.core` and `local.name` patterns from `airline-demo/main.tf:41-58`.
  - The `aws` provider block moves verbatim, with its comment. If resources become unconditional, the `skip_*` and
    placeholder-credential logic can go. Add a precondition, for example on `aws_s3_bucket.analytics`: core must have
    Tableflow creds and must not be an existing environment.
  - `confluent_tableflow_topic.analytics` drops its `depends_on` on `airline-demo` statements, because deploy applies
    this root afterwards. Keep `time_sleep.tableflow_role` and the role binding.
  - Delete no comments that explain the IAM bootstrap order or the propagation wait. Move them.
- [ ] **Step 4: Validate.**
  - Run `terraform -chdir=terraform/tableflow init -backend=false && terraform -chdir=terraform/tableflow validate`, and
    the same for `airline-demo` and `always-on`. Expected: `Success!` for each.
  - Run `terraform -chdir=terraform/airline-demo providers` and confirm it lists no `hashicorp/aws`.
  - Run `uv run pytest -q`. Expected: all pass.
- [ ] **Step 5: Commit:** `git commit -m "Tableflow: move it into its own terraform/tableflow root"`.

### Task 2: Deploy and destroy apply `tableflow` only when Tableflow is on

**Files:**
- Modify: `scripts/deploy.py`:
  - `_targets` (line 347) and its callers (bot path around line 500, interactive path around line 620);
  - the `-replace` hint (lines 1247-1252), which uses `-chdir=terraform/tableflow` and Task 1's addresses;
  - the `CONTINUOUS` comment (line 109), which now mentions both roots.
- Modify: `scripts/destroy.py`:
  - `_envs_to_destroy` (line 160);
  - `TABLEFLOW_PROVIDER_INTEGRATION` (line 98), set to Task 1's address;
  - the destroy loop's provider-integration release, which runs for `tableflow` instead of `airline-demo`;
  - the comment at lines 93-97.
- Modify: `scripts/tfvars.py:130` (the docstring mentions `enable_analytics`) and `scripts/workshop_key_manager.py`, if
  it references `airline-demo` Tableflow (grep).
- Test: `tests/test_tableflow_root.py`; update `tests/test_destroy.py:21-65` and `tests/test_always_on.py:381-390`.

**Interfaces:**
- Consumes: Task 1's root name `tableflow` and resource addresses.
- Produces:
  - `deploy._targets(labs: list[str], tableflow: bool = False) -> list[str]`, which returns
    `["core", *lab roots, "tableflow"]` when `tableflow` is true and Flights is in `labs`;
  - `deploy._tableflow_on(labs: list[str], workshop: bool, creds: dict) -> bool`, which is
    `FLIGHT_DISRUPTIONS in labs and not workshop and bool((creds.get("TF_VAR_aws_tableflow_access_key") or "").strip()) and os.environ.get("TF_VAR_enable_analytics", "true").lower() != "false"`.

- [ ] **Step 1: Write the failing tests:**

```python
def test_workshops_and_creds_free_demos_never_init_a_root_with_the_aws_provider():
    for labs, workshop, creds in [
        (["flight-disruptions"], True, {"TF_VAR_aws_tableflow_access_key": "AKIA"}),
        (["agentic-pit-decisions"], False, {}),
        (["flight-disruptions", "agentic-pit-decisions"], True, {}),
        (["flight-disruptions"], False, {}),  # --with-labs without Tableflow creds
    ]:
        targets = deploy._targets(labs, deploy._tableflow_on(labs, workshop, creds))
        assert not any(_declares_aws(t) for t in targets), (labs, workshop)

def test_a_full_demo_with_tableflow_creds_applies_tableflow_last():
    labs = ["flight-disruptions"]
    assert deploy._targets(labs, deploy._tableflow_on(labs, False, {"TF_VAR_aws_tableflow_access_key": "AKIA"})) == [
        "core", "airline-demo", "tableflow"]

def test_destroy_takes_tableflow_down_before_airline_demo():
    assert destroy._envs_to_destroy(None) == ["tableflow", "agentic-pit-decisions", "airline-demo", "core"]
    assert destroy._envs_to_destroy("flight-disruptions") == ["tableflow", "airline-demo"]

def test_the_replace_hint_points_at_the_tableflow_root():
    src = (Path(__file__).parent.parent / "scripts/deploy.py").read_text(encoding="utf-8")
    assert "-chdir=terraform/tableflow" in src and "-chdir=terraform/airline-demo apply" not in src
```

  Always-on stays first in `_envs_to_destroy` when its state exists, as today.
- [ ] **Step 2: Run them.** Run `uv run pytest tests/test_tableflow_root.py -v`. Expected: the new tests FAIL.
- [ ] **Step 3: Implement.**
  - Pass `_tableflow_on(...)` into `_targets` on both deploy paths. Always-on's existing flow (`--with-labs` plus creds)
    then gets `tableflow` automatically.
  - Update the destroy loop, so `release_provider_integration` retries for `env == "tableflow"`. Update the tests that
    asserted it ran for `airline-demo`.
- [ ] **Step 4: Run the tests.** Run `uv run pytest -q`, `uv run ruff check scripts tests` and
  `uv run ruff format --check scripts tests`. Expected: all pass.
- [ ] **Step 5: Commit:** `git commit -m "Deploy and destroy: apply terraform/tableflow only when Tableflow is on"`.

### Task 3: Docs

**Files:**
- Modify:
  - `AGENTS.md`, `docs/flight-disruptions/always-on.md` and `docs/flight-disruptions/walkthrough.md`: every mention of
    `terraform/airline-demo/analytics.tf` or Tableflow living in `airline-demo` now says `terraform/tableflow`.
  - Any `terraform -chdir=terraform/airline-demo apply -replace=...confluent_tableflow_topic...` command in those docs.
- Steps:
  - [ ] Make the edits (grep `analytics.tf`, `airline-demo` near "Tableflow").
  - [ ] Run `uv run pytest -q`.
  - [ ] Commit: `git commit -m "Docs: Tableflow lives in terraform/tableflow"`.

### Task 4: Live check (manual, the user runs it before merge)
- [ ] A Flights workshop deploy and destroy in a fresh clone. The `terraform init` output lists no `hashicorp/aws`.
- [ ] The F1 end-to-end run (the user's planned test).
- [ ] `--with-labs` with Tableflow creds, then destroy, before demos or always-on rely on it.
