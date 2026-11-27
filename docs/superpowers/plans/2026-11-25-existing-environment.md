# Existing Environment Deploy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `uv run deploy` can put either lab's workshop into an attendee's existing Confluent Cloud
environment, where the attendee is only EnvironmentAdmin. In both flows the RTCE Global API key now
comes from Terraform.

**Architecture:**
- One switch in `terraform/core`: `existing_environment_id` plus `user_id`.
  - With the switch on, core looks up the environment instead of creating it, skips the service
    accounts and their ACLs, and makes every key user-owned.
  - Output names don't change, so the labs and most scripts are untouched.
- A new focused module, `scripts/existing_env.py`, holds the deploy-side logic: choosing the
  environment, refusals, checks before Terraform, and the current user's ID.
- `deploy.py` and `destroy.py` call into it.

**Tech Stack:** Python 3 (uv, pytest, ruff), Terraform with the `confluentinc/confluent` provider
`~> 2.73`, Confluent CLI.

**Spec:** `docs/superpowers/specs/2026-11-25-existing-environment-design.md`

## Global Constraints

- Branch: `always-on-quick`. Don't push or merge until the user asks. Pushes use
  `git push-external`.
- The default new-environment flow must keep working. An existing deployment's next plan adds only
  the Global key, and replaces nothing.
- An existing environment is allowed only for workshop deploys. It's refused with `--with-labs`,
  `--video-2`, `--always-on`, or a full Flight Disruptions deploy (a bot run without
  `--workshop`). Refusal reason: "Flink statements that run as a user stop after 4 hours."
- Saved setting: `TF_VAR_existing_environment_id` in `credentials.env`. The user ID is never
  saved; it's worked out each run and passed as `TF_VAR_user_id`.
- Confluent provider constraint `~> 2.73` in `terraform/core/versions.tf`,
  `terraform/airline-demo/providers.tf` and `terraform/agentic-pit-decisions/providers.tf`.
- Global key `managed_resource`: `id = "global"`, `api_version = "global/v1"`,
  `kind = "Global"`. A user owner uses `api_version = "iam/v2"` and `kind = "User"`.
- Stream Governance must be `ADVANCED` in an existing environment.
- Windows rules (`AGENTS.md`):
  - every CLI is called as `processes.exe("tool")`;
  - every text-capturing subprocess passes `encoding="utf-8", errors="replace"`;
  - file I/O passes `encoding="utf-8"`.
- Before each commit: `uv run pytest -q` and `uv run ruff check . && uv run ruff format --check .`
  pass.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A pasted ID with spaces, quotes or the wrong prefix** (`" env-abc12 "`, `lkc-123`): strip
   it, accept only `env-` followed by letters and digits, ask again when interactive, and exit with
   a clear message otherwise. Test in Task 4.
2. **A pre-change deployment upgrading:** the `count` added to existing resources must not plan a
   replacement. A `moved` block is needed for every resource that gets a `count`. Static test in
   Task 2.
3. **`uv run destroy` in an existing environment:** core's variables still validate (`user_id` set
   whenever `existing_environment_id` is), and the summary says the environment is kept. Test in
   Task 7.
4. **The CLI can't answer** (an expired login, an unknown JSON shape) during the Global-key count:
   don't block the deploy on a check that can't tell, and let Terraform report. Test in Task 5.
5. **Terraform output piped for the retry on Windows:** the apply still streams line by line, and
   reads as UTF-8 with `errors="replace"`. Test in Task 6.

---

### Task 1: Prove the Terraform route and record the CLI's JSON (manual, live)

The spike proved user-owned keys with the CLI only. This task proves Terraform can do the same, and
saves the exact JSON shapes that later tests use. It needs the user's go-ahead: it creates and
deletes real Confluent resources with the EnvironmentAdmin-only login (`tmm+wp2@confluent.io`,
`env-z9qmqy`).

**Files:**
- Create: `tests/fixtures/existing_env/environment_describe.json`
- Create: `tests/fixtures/existing_env/schema_registry_describe.json`
- Create: `tests/fixtures/existing_env/role_bindings_current_user.json`
- Create: `tests/fixtures/existing_env/global_keys_current_user.json`

- [ ] **Step 1:** Ask the user to confirm the live run, and to sign the CLI in as the
  EnvironmentAdmin-only login.
- [ ] **Step 2:** In a scratchpad folder, write a throwaway Terraform root that creates two things
  with a user-owned Cloud key:
  - a `confluent_api_key` with `managed_resource` Global and owner `{ id = "<u-…>",
    api_version = "iam/v2", kind = "User" }`;
  - a Flink-region key with the same owner.

  Run `terraform apply`. Expected: both keys are created. Then run `terraform destroy`. Expected:
  both keys are gone (`confluent api-key list --current-user` shows neither).
- [ ] **Step 3:** Run each command below with `-o json`, replace every ID, email and secret with a
  placeholder (`env-abc123`, `u-abc123`, `lsrc-abc123`, `KEY1`), and save it to its fixture file:
  - `confluent environment describe env-z9qmqy`
  - `confluent schema-registry cluster describe --environment env-z9qmqy`
  - `confluent iam rbac role-binding list --current-user`
  - `confluent api-key list --resource global --current-user`

  Note in the commit message which JSON field holds:
  - the governance package;
  - the Schema Registry ID;
  - the user principal;
  - the key ID.
- [ ] **Step 4:** If Step 2 fails, stop and report to the user. The design depends on it.
- [ ] **Step 5:** Commit the fixtures: `git commit -m "Record Confluent CLI JSON for existing-environment checks"`.

### Task 2: `terraform/core` switch and the Terraform Global key

**Files:**
- Modify: `terraform/core/variables.tf`, `terraform/core/main.tf`, `terraform/core/outputs.tf`
- Modify: `terraform/core/versions.tf:6`, `terraform/airline-demo/providers.tf:6`,
  `terraform/agentic-pit-decisions/providers.tf:6`
- Modify: `terraform/airline-demo/main.tf:312` (Tableflow key precondition)
- Test: `tests/test_existing_env.py` (new; static checks of the `.tf` text, like
  `tests/test_workshop.py` already does)

**Interfaces:**
- Produces these core variables: `existing_environment_id` (string, default `""`) and `user_id`
  (string, default `""`, with a validation that it's non-empty when `existing_environment_id` is
  set).
- Produces these core outputs:
  - `rtce_global_api_key` and `rtce_global_api_secret` (sensitive);
  - `existing_environment` (bool);
  - `confluent_environment_id` and `confluent_environment_display_name`, which come from the data
    source when existing;
  - `app_manager_service_account_id`, which equals `var.user_id` when existing;
  - `confluent_rtce_service_account_id`, which equals `""` when existing.

- [ ] **Step 1: Write the failing tests** in `tests/test_existing_env.py`:

```python
CORE = Path(__file__).parent.parent / "terraform" / "core"

def _tf(name): return (CORE / name).read_text(encoding="utf-8")

COUNTED = [
    "confluent_environment.staging",
    "confluent_service_account.app-manager",
    "confluent_role_binding.app-manager-kafka-cluster-admin",
    "confluent_service_account.rtce-reader",
    "confluent_role_binding.rtce-reader-developer-read",
    "confluent_role_binding.rtce-reader-schema-registry-read",
    "confluent_kafka_acl.app-manager-read-on-topic",
    "confluent_kafka_acl.app-manager-describe-on-cluster",
    "confluent_kafka_acl.app-manager-write-on-topic",
    "confluent_kafka_acl.app-manager-create-topic",
    "confluent_kafka_acl.app-manager-read-on-group",
]

def test_every_resource_the_switch_skips_has_a_moved_block():
    main = _tf("main.tf")
    for address in COUNTED:
        kind, name = address.split(".")
        block = main.split(f'resource "{kind}" "{name}" {{', 1)[1].split("\n}", 1)[0]
        assert "count = local.existing ? 0 : 1" in block, address
        assert f"from = {address}\n" in main and f"to   = {address}[0]" in main, address

def test_core_makes_the_rtce_global_key():
    main = _tf("main.tf")
    block = main.split('resource "confluent_api_key" "rtce-global" {', 1)[1].split("\n}", 1)[0]
    assert 'id          = "global"' in block and 'kind        = "Global"' in block
    assert "rtce_global_api_key" in _tf("outputs.tf") and "rtce_global_api_secret" in _tf("outputs.tf")

def test_no_resource_reads_the_created_environment_directly():
    main = _tf("main.tf")
    # Only the env_id / env_display_name locals and the counted role binding may touch the created environment.
    assert "confluent_environment.staging.id" not in main
    assert "confluent_environment.staging.resource_name" not in main
    assert "confluent_environment.staging.display_name" not in main + _tf("outputs.tf")

def test_every_root_needs_the_provider_that_makes_global_keys():
    root = CORE.parent
    for f in (root / "core/versions.tf", root / "airline-demo/providers.tf", root / "agentic-pit-decisions/providers.tf"):
        assert 'version = "~> 2.73"' in f.read_text(encoding="utf-8"), f
```

- [ ] **Step 2: Run them to verify they fail.**
  Run: `uv run pytest tests/test_existing_env.py -v`
  Expected: 4 FAIL.
- [ ] **Step 3: Implement in `terraform/core`:**
  - **Locals:** `existing = var.existing_environment_id != ""` and `env_id`. `env_id` picks
    `data.confluent_environment.existing[0].id` or `confluent_environment.staging[0].id`. Also
    `env_display_name`, plus owner locals for the three cluster keys:
    `key_owner = { id, api_version, kind }`. These hold the user (`iam/v2`, `User`) when
    existing, and app-manager otherwise.
  - **Data source:** `data "confluent_environment" "existing" { count = local.existing ? 1 : 0; id = var.existing_environment_id }`.
  - **`count = local.existing ? 0 : 1`** on every resource in `COUNTED`, with a `moved` block for
    each. Every `confluent_environment.staging.id` becomes `local.env_id`.
  - **References inside the counted resources:** the ACLs and role bindings reference
    `confluent_service_account.app-manager[0]` and `rtce-reader[0]`.
  - **The three key resources:**
    - `owner` uses `local.key_owner`;
    - `depends_on` keeps the role binding, which works when its count is 0;
    - descriptions say "owned by the deploying user or the app-manager service account".
  - **`confluent_api_key.rtce-global`:** owner `rtce-reader[0]` or the user, `managed_resource`
    Global as in Global Constraints.
  - **Comments:**
    - replace the "CLI-created Global API key" comment block above `rtce-reader`;
    - add a comment on `app_manager_service_account_id` saying it holds the attendee's user ID in
      an existing environment.
  - **Tableflow key precondition** (`airline-demo/main.tf:312`): add
    `lifecycle { precondition { condition = !try(local.core.existing_environment, false), error_message = "Tableflow isn't available when deploying into an existing environment." } }`.
  - Raise the provider constraint in all three roots.
- [ ] **Step 4: Run the tests and validate.**
  Run: `uv run pytest tests/test_existing_env.py -v`
  Expected: PASS.
  Run: `cd terraform/core && terraform init -upgrade -backend=false >/dev/null && terraform validate`
  Expected: `Success! The configuration is valid.` Do the same in `terraform/airline-demo`.
- [ ] **Step 5: Commit:** `git commit -m "Core: deploy into an existing environment; Terraform makes the RTCE Global key"`.

### Task 3: `setup_rtce` reads the Global key from core

**Files:**
- Modify: `scripts/setup_rtce.py` (`ensure_global_key` at line 236; delete
  `_create_rtce_global_key_via_cli` at line 163 and `_key_owned_by` at line 207; drop
  `_TF_RTCE_SA_ID` if it's unused)
- Test: `tests/test_existing_env.py`; delete `test_a_failed_key_check_keeps_the_stored_key`
  (`tests/test_workshop.py:208`) and `test_stale_rtce_key_is_detected`
  (`tests/test_recovery_agent.py:92`)

**Interfaces:**
- Consumes: core outputs `rtce_global_api_key` and `rtce_global_api_secret` (Task 2).
- Produces: `ensure_global_key(creds_file: Path, creds: dict) -> str`, with the same signature and
  callers (`deploy.py:696`, `deploy.py:715`, `setup_rtce.py:1135`).

- [ ] **Step 1: Write the failing tests:**

```python
def test_global_key_comes_from_core_and_replaces_a_stale_one(monkeypatch, tmp_path):
    creds_file = tmp_path / "credentials.env"
    creds_file.write_text("CONFLUENT_RTCE_API_KEY=OLD\nCONFLUENT_RTCE_API_SECRET=OLDS\n", encoding="utf-8")
    monkeypatch.setattr(setup_rtce, "_read_core_tf_outputs",
                        lambda _: {"rtce_global_api_key": "NEW", "rtce_global_api_secret": "NEWS"})
    monkeypatch.setattr(setup_rtce.subprocess, "run", lambda *a, **k: pytest.fail("no CLI call"))
    creds = {"CONFLUENT_RTCE_API_KEY": "OLD", "CONFLUENT_RTCE_API_SECRET": "OLDS"}
    assert setup_rtce.ensure_global_key(creds_file, creds) == "NEW"
    assert creds["CONFLUENT_RTCE_API_SECRET"] == "NEWS"
    assert "CONFLUENT_RTCE_API_KEY=NEW" in creds_file.read_text(encoding="utf-8")

def test_without_a_deployment_the_stored_key_is_kept(monkeypatch, tmp_path):
    monkeypatch.setattr(setup_rtce, "_read_core_tf_outputs", lambda _: {})
    assert setup_rtce.ensure_global_key(tmp_path / "credentials.env", {"CONFLUENT_RTCE_API_KEY": "MINE"}) == "MINE"
```

- [ ] **Step 2: Run them to verify they fail.**
  Run: `uv run pytest tests/test_existing_env.py -k global_key -v`
  Expected: FAIL. Today's code calls the CLI.
- [ ] **Step 3: Implement `ensure_global_key`.**
  - When core's outputs have both values, store them in `creds` and save them to `credentials.env`
    with `_save_to_env_file`, but only when they differ from what's stored.
  - Otherwise return the stored key.
  - Delete the two CLI helpers and their two old tests, and update the docstring.
- [ ] **Step 4: Run the tests to verify they pass.**
  Run: `uv run pytest -q`
  Expected: all pass.
- [ ] **Step 5: Commit:** `git commit -m "setup-rtce: take the Global key from Terraform"`.

### Task 4: Choosing the environment, refusals and the flow switch (`scripts/existing_env.py`)

**Files:**
- Create: `scripts/existing_env.py`
- Test: `tests/test_existing_env.py`

**Interfaces:**
- Produces:
  - `ENV_KEY = "TF_VAR_existing_environment_id"`
  - `ENV_ID = re.compile(r"^env-[a-z0-9]+$")`
  - `full_demo(*, with_labs: bool, video_2: bool, always_on: bool, flights: bool, workshop: bool) -> bool`.
    Returns True for `with_labs or video_2 or always_on or (flights and not workshop)`.
  - `choose(flag: str | None, creds_file: Path, creds: dict, *, interactive: bool, full: bool) -> str`.
    Returns the environment ID, or `""` for a new environment, and saves a new ID under `ENV_KEY`.
    - The flag wins, then the saved ID, then the interactive prompt.
    - The prompt text is copied verbatim from the spec:
      `Deploy into an existing environment? Paste its ID (env-xxxxx), or press Enter to create a new one: `
    - When interactive, ask again on a bad ID.
    - With a flag or saved ID that's bad, `sys.exit`.
    - When `full` and an ID is chosen, `sys.exit` with a message containing
      "Flink statements that run as a user stop after 4 hours".
    - Never prompt when `full`.
  - `check_flow_switch(env_id: str, core_outputs: dict) -> None`.
    - Exits with "Run `uv run destroy` first" when core's state is from the other flow, or from
      another environment ID.
    - A state without `existing_environment` counts as a new-environment flow.
    - Empty `core_outputs` means nothing is deployed, so it passes.

- [ ] **Step 1: Write the failing tests:**

```python
def test_a_pasted_id_is_trimmed_checked_and_saved(monkeypatch, tmp_path):
    answers = iter(["lkc-123", '  "env-abc12" '])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))
    creds_file, creds = tmp_path / "credentials.env", {}
    assert existing_env.choose(None, creds_file, creds, interactive=True, full=False) == "env-abc12"
    assert creds[existing_env.ENV_KEY] == "env-abc12"
    assert "TF_VAR_existing_environment_id=env-abc12" in creds_file.read_text(encoding="utf-8")

def test_enter_keeps_the_new_environment_flow(monkeypatch, tmp_path):
    monkeypatch.setattr("builtins.input", lambda _: "")
    assert existing_env.choose(None, tmp_path / "c.env", {}, interactive=True, full=False) == ""

def test_full_demos_never_ask_and_refuse_an_existing_environment(monkeypatch, tmp_path):
    monkeypatch.setattr("builtins.input", lambda _: pytest.fail("no prompt for a full demo"))
    assert existing_env.choose(None, tmp_path / "c.env", {}, interactive=True, full=True) == ""
    with pytest.raises(SystemExit, match="stop after 4 hours"):
        existing_env.choose(None, tmp_path / "c.env", {existing_env.ENV_KEY: "env-abc12"}, interactive=False, full=True)

def test_a_bad_flag_exits(tmp_path):
    with pytest.raises(SystemExit, match="env-"):
        existing_env.choose("lkc-123", tmp_path / "c.env", {}, interactive=False, full=False)

@pytest.mark.parametrize("flights,workshop,expected", [(True, True, False), (True, False, True), (False, False, False)])
def test_only_workshop_deploys_count_as_not_full(flights, workshop, expected):
    assert existing_env.full_demo(with_labs=False, video_2=False, always_on=False, flights=flights, workshop=workshop) is expected

@pytest.mark.parametrize("env_id,outputs", [
    ("env-abc12", {"confluent_environment_id": "env-new"}),  # pre-change or new-env state
    ("", {"existing_environment": True, "confluent_environment_id": "env-abc12"}),
    ("env-other", {"existing_environment": True, "confluent_environment_id": "env-abc12"}),
])
def test_switching_flows_needs_a_destroy_first(env_id, outputs):
    with pytest.raises(SystemExit, match="uv run destroy"):
        existing_env.check_flow_switch(env_id, outputs)

def test_same_flow_or_nothing_deployed_passes():
    existing_env.check_flow_switch("", {})
    existing_env.check_flow_switch("env-abc12", {"existing_environment": True, "confluent_environment_id": "env-abc12"})
    existing_env.check_flow_switch("", {"confluent_environment_id": "env-new"})
```

- [ ] **Step 2: Run them to verify they fail.**
  Run: `uv run pytest tests/test_existing_env.py -v`
  Expected: the new tests fail on `ModuleNotFoundError` / `ImportError`.
- [ ] **Step 3: Implement** `scripts/existing_env.py`. Save with
  `scripts.credentials_env`-style helpers if any exist; otherwise use
  `dotenv.set_key(str(creds_file), ENV_KEY, value)`, as `deploy._save_env_safe` does. Strip quotes
  and whitespace before matching.
- [ ] **Step 4: Run the tests to verify they pass.**
  Run: `uv run pytest tests/test_existing_env.py -v`
  Expected: PASS.
- [ ] **Step 5: Commit:** `git commit -m "Choose an existing environment, refuse full demos and flow switches"`.

### Task 5: Checks before Terraform and the user's ID (`scripts/existing_env.py`)

**Files:**
- Modify: `scripts/existing_env.py`
- Test: `tests/test_existing_env.py`, using the fixtures from Task 1

**Interfaces:**
- Produces:
  - `current_user_id() -> str`: the `u-…` ID from `confluent iam rbac role-binding list --current-user -o json`
    (field per Task 1), or `""`.
  - `preflight(env_id: str, core_outputs: dict) -> str`: runs spec checks 1–4 in order and returns
    the user ID. Each failed check `sys.exit`s with a clear message:
    - check 1: "Can't find {env_id} with your login. Check the ID, and that you have
      EnvironmentAdmin on it."
    - check 2: ask your admin to enable Stream Governance ADVANCED (Schema Registry missing, or
      the package isn't ADVANCED).
    - check 3: no user ID found ("Couldn't find your Confluent user ID; run `confluent login` again").
    - check 4: two Global keys already, and neither is `core_outputs.get("rtce_global_api_key")`.
      Name both key IDs and say to delete one under Confluent Cloud → API keys.

  If the key list can't be read, don't block.
- Helper: `_cli_json(*args: str) -> object`. It runs `[processes.exe("confluent"), *args, "-o", "json"]`
  with `capture_output=True, text=True, timeout=60, encoding="utf-8", errors="replace"`, and
  returns the parsed JSON, or `None` on any failure.

- [ ] **Step 1: Write the failing tests.** Use a fake `subprocess.run` that maps each command's
  first words to a fixture file (or to `returncode=1`):

```python
def test_preflight_returns_the_user_id_when_all_checks_pass(fake_cli):
    fake_cli(env="environment_describe.json", sr="schema_registry_describe.json",
             user="role_bindings_current_user.json", keys="global_keys_current_user.json")
    assert existing_env.preflight("env-abc123", {}) == "u-abc123"

@pytest.mark.parametrize("broken,message", [
    ("env", "Can't find env-abc123"),
    ("sr", "Stream Governance"),
    ("user", "confluent login"),
])
def test_each_failed_check_stops_with_its_message(fake_cli, broken, message):
    fake_cli(**{broken: None})  # None: the command fails
    with pytest.raises(SystemExit, match=message):
        existing_env.preflight("env-abc123", {})

def test_essentials_is_refused(fake_cli):
    fake_cli(env_package="ESSENTIALS")  # fixture copy with the package field changed
    with pytest.raises(SystemExit, match="ADVANCED"):
        existing_env.preflight("env-abc123", {})

def test_two_foreign_global_keys_stop_but_our_own_key_does_not(fake_cli):
    fake_cli(keys=[{"key": "A"}, {"key": "B"}])  # rows in the Task 1 shape
    with pytest.raises(SystemExit, match="A.*B"):
        existing_env.preflight("env-abc123", {})
    assert existing_env.preflight("env-abc123", {"rtce_global_api_key": "B"}) == "u-abc123"

def test_an_unreadable_key_list_does_not_block(fake_cli):
    fake_cli(keys=None)
    assert existing_env.preflight("env-abc123", {}) == "u-abc123"
```

  The `fake_cli` fixture lives in `tests/test_existing_env.py`. It loads the files under
  `tests/fixtures/existing_env/`, with defaults equal to the passing set, and patches
  `existing_env.subprocess.run`. It also asserts that every call passes `encoding="utf-8"` and
  `errors="replace"`.
- [ ] **Step 2: Run them to verify they fail.**
  Run: `uv run pytest tests/test_existing_env.py -k "preflight or check or keys" -v`
  Expected: FAIL (`AttributeError: preflight`).
- [ ] **Step 3: Implement** `_cli_json`, `current_user_id` and `preflight` in
  `scripts/existing_env.py`, reading the field names recorded in Task 1.
- [ ] **Step 4: Run the tests to verify they pass.**
  Run: `uv run pytest tests/test_existing_env.py -v`
  Expected: PASS.
- [ ] **Step 5: Commit:** `git commit -m "Check an existing environment before Terraform runs"`.

### Task 6: Retry a lab apply after Confluent's transient Flink error

**Files:**
- Modify: `scripts/terraform_runner.py:20-61` (`run_terraform`)
- Test: `tests/test_existing_env.py`

**Interfaces:**
- Produces:
  - `FLINK_HICCUP = "technical difficulties"` (matched case-insensitively);
  - `RETRY_WAIT_SECONDS = 60`;
  - `run_terraform(env_path, auto_approve=True, targets=())` keeps its signature and `bool`
    return.

- [ ] **Step 1: Write the failing tests:**

```python
def test_apply_retries_once_after_the_flink_hiccup(monkeypatch, tmp_path):
    outputs = iter([(1, "Error: ... we are experiencing technical difficulties ..."), (0, "Apply complete!")])
    monkeypatch.setattr(terraform_runner, "_apply", lambda cmd, cwd: next(outputs))
    monkeypatch.setattr(terraform_runner.subprocess, "run", lambda *a, **k: None)  # init
    slept = []
    monkeypatch.setattr(terraform_runner.time, "sleep", slept.append)
    assert terraform_runner.run_terraform(tmp_path / "airline-demo")
    assert slept == [terraform_runner.RETRY_WAIT_SECONDS]

def test_other_failures_and_a_second_hiccup_do_not_retry_forever(monkeypatch, tmp_path):
    monkeypatch.setattr(terraform_runner, "_apply", lambda cmd, cwd: (1, "Technical difficulties"))
    monkeypatch.setattr(terraform_runner.subprocess, "run", lambda *a, **k: None)
    monkeypatch.setattr(terraform_runner.time, "sleep", lambda s: None)
    assert not terraform_runner.run_terraform(tmp_path / "airline-demo")

def test_apply_streams_utf8_output(monkeypatch, tmp_path):
    seen = {}
    class P:
        stdout = iter(["line ✓\n"])
        def __init__(self, cmd, **kw): seen.update(kw)
        def wait(self): return 0
    monkeypatch.setattr(terraform_runner.subprocess, "Popen", P)
    assert terraform_runner._apply(["terraform", "apply"], tmp_path) == (0, "line ✓\n")
    assert seen["encoding"] == "utf-8" and seen["errors"] == "replace" and seen["stderr"] == terraform_runner.subprocess.STDOUT
```

- [ ] **Step 2: Run them to verify they fail.**
  Run: `uv run pytest tests/test_existing_env.py -k "apply" -v`
  Expected: FAIL (`AttributeError: _apply`).
- [ ] **Step 3: Implement** `_apply(cmd: list[str], cwd: Path) -> tuple[int, str]` with
  `subprocess.Popen` and `stdout=PIPE, stderr=STDOUT, text=True, encoding="utf-8",
  errors="replace", env=processes.terraform_env()`.
  - It prints each line as it arrives (`print(line, end="", flush=True)`) and returns the exit code
    and the full text.
  - `run_terraform` calls `_apply`.
  - On a non-zero exit whose text contains `FLINK_HICCUP`, and only on the first attempt, print
    "Confluent's Flink service hiccuped on the new cluster; retrying in 60 seconds…", sleep, and
    apply again.
  - Keep the existing `CalledProcessError` / `FileNotFoundError` handling.
- [ ] **Step 4: Run the tests to verify they pass.**
  Run: `uv run pytest -q`
  Expected: all pass.
- [ ] **Step 5: Commit:** `git commit -m "Retry a Terraform apply once after Flink's transient error"`.

### Task 7: Wire it into deploy and destroy

**Files:**
- Modify: `scripts/deploy.py`:
  - the argparse block (around line 410);
  - the bot path (around line 450);
  - the interactive path after the CLI login (around line 497);
  - the key-generation fallback text (line 530).
- Modify: `scripts/credentials.py:48` (docstring: OrganizationAdmin, or EnvironmentAdmin in an
  existing environment).
- Modify: `scripts/destroy.py`, the summary at line 249 and before Terraform runs.
- Test: `tests/test_existing_env.py`

**Interfaces:**
- Consumes: `existing_env.choose`, `full_demo`, `check_flow_switch`, `preflight` and
  `current_user_id` (Tasks 4–5), and `setup_rtce._read_core_tf_outputs(creds_file)`.
- Produces:
  - `deploy._existing_environment(args, creds_file, creds: dict, labs: list[str], workshop: bool, *, interactive: bool) -> str`.
    - It calls `choose`. When an ID comes back, it calls `check_flow_switch` and `preflight`,
      sets `os.environ["TF_VAR_user_id"]` and `os.environ["TF_VAR_existing_environment_id"]`,
      and returns the ID.
    - With no ID, it still calls `check_flow_switch("", outputs)`.
  - `destroy._kept_environment(root: Path) -> str`: the environment ID when core's state says
    `existing_environment`, else `""`. Read core's state with `scripts.terraform.run_terraform_output`,
    imported into `destroy` as `run_terraform_output`.
  - `destroy._user_for_core(creds: dict) -> None`: when `TF_VAR_existing_environment_id` is set in
    `creds`, set `os.environ["TF_VAR_user_id"] = existing_env.current_user_id()`.

- [ ] **Step 1: Write the failing tests:**

```python
def test_deploy_passes_the_user_to_terraform_and_refuses_bots_running_the_full_demo(monkeypatch, tmp_path):
    monkeypatch.setattr(deploy.setup_rtce, "_read_core_tf_outputs", lambda _: {})
    monkeypatch.setattr(deploy.existing_env, "preflight", lambda env_id, outputs: "u-abc123")
    monkeypatch.delenv("TF_VAR_user_id", raising=False)
    args = deploy._parse_args(["--environment", "env-abc12", "--workshop"])
    creds = {}
    assert deploy._existing_environment(args, tmp_path / "c.env", creds, ["flight-disruptions"], True, interactive=False) == "env-abc12"
    assert os.environ["TF_VAR_user_id"] == "u-abc123"
    with pytest.raises(SystemExit, match="4 hours"):
        deploy._existing_environment(deploy._parse_args(["--testing"]), tmp_path / "c.env", creds,
                                     ["flight-disruptions"], False, interactive=False)

def test_destroy_says_the_environment_is_kept(monkeypatch, tmp_path):
    (tmp_path / "terraform" / "core").mkdir(parents=True)
    (tmp_path / "terraform" / "core" / "terraform.tfstate").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(destroy, "run_terraform_output",
                        lambda _: {"existing_environment": True, "confluent_environment_id": "env-abc12"})
    assert destroy._kept_environment(tmp_path) == "env-abc12"

def test_destroy_gives_core_the_user_so_its_variables_validate(monkeypatch):
    monkeypatch.setattr(destroy.existing_env, "current_user_id", lambda: "u-abc123")
    monkeypatch.delenv("TF_VAR_user_id", raising=False)
    destroy._user_for_core({})
    assert "TF_VAR_user_id" not in os.environ
    destroy._user_for_core({"TF_VAR_existing_environment_id": "env-abc12"})
    assert os.environ["TF_VAR_user_id"] == "u-abc123"
```

  If `deploy.py` has no `_parse_args` yet, the first test's Step 3 extracts the existing parser
  into `_parse_args(argv: list[str] | None = None) -> argparse.Namespace` and moves the existing
  `parser.error` checks into it.
- [ ] **Step 2: Run them to verify they fail.**
  Run: `uv run pytest tests/test_existing_env.py -k "deploy or destroy" -v`
  Expected: FAIL.
- [ ] **Step 3: Implement in `deploy.py`:**
  - `--environment` (`metavar="ENV_ID"`), help: "Deploy a workshop into this existing
    environment (saved for later runs)".
  - In both paths, call `_existing_environment` after the labs and workshop mode are known and the
    CLI is logged in, before `write_tfvars_for_deployment`.
  - Pass the result's flag into the summary line `Environment: existing env-… (kept on destroy)`,
    or `new`.
  - At line 530, say EnvironmentAdmin instead of OrganizationAdmin when an ID was chosen.
- [ ] **Step 4: Implement in `destroy.py`:**
  - Call `_user_for_core(creds)` in both interactive and `--testing` mode, before any Terraform
    runs, so core's validation passes.
  - In the interactive summary, when `_kept_environment(root)` returns an ID, replace the warning
    with `Your environment {env_id} is kept; the cluster, compute pool and keys are deleted.`
- [ ] **Step 5: Run the tests to verify they pass.**
  Run: `uv run pytest -q`
  Expected: all pass.
- [ ] **Step 6: Commit:** `git commit -m "deploy --environment: deploy a workshop into an existing environment"`.

### Task 8: Docs

**Files:**
- Modify: `README.md`, after the `uv run deploy` block (line 95). Add a short section,
  "Deploying into an existing environment", covering:
  - who it's for;
  - what the admin provides: the attendee's own environment, EnvironmentAdmin on it, and Stream
    Governance ADVANCED;
  - `uv run deploy --environment env-xxxxx`, or paste the ID at the prompt;
  - workshops only;
  - the 2-Global-key note;
  - that destroy keeps the environment, its Schema Registry subjects and `rtce-connection`;
  - how to go back to a new environment: `uv run destroy`, then delete
    `TF_VAR_existing_environment_id` from `credentials.env`.
- Modify: `docs/flight-disruptions/workshop.md:141` and `docs/agentic-pit-decisions/walkthrough.md:109`.
  Add one line under `uv run deploy`: "Given an existing environment by your admin? Paste its ID
  when deploy asks; see the README."
- Modify: `docs/flight-disruptions/workshop.md:718`. Add: "In an environment your admin gave you,
  destroy keeps the environment, its Schema Registry subjects and the Flink connection."
- Modify: `AGENTS.md`. Note that `app_manager_service_account_id` can be the attendee's user ID,
  and that the RTCE Global key comes from Terraform (`rtce_global_api_key`).

- [ ] **Step 1:** Make the edits.
- [ ] **Step 2:** Run `uv run pytest -q`. Expected: all pass. `tests/test_workshop.py` checks the
  guide's SQL, which these edits don't touch.
- [ ] **Step 3: Commit:** `git commit -m "Docs: deploying into an existing environment"`.

### Task 9: End-to-end runs (manual, live)

These need the user's go-ahead and their CLI logins.

- [ ] **Step 1: Default flow.** Signed in as an OrganizationAdmin:
  1. `uv run deploy --workshop --lab flight-disruptions`.
  2. Expected: core's plan creates `confluent_api_key.rtce-global`, and `credentials.env` gets its
     key.
  3. Run the workshop steps through `uv run setup-rtce --agent-connection`. Expected: the RTCE
     check passes.
  4. `uv run destroy`. Expected: no keys are left (`confluent api-key list --resource global` has
     no `rtce-reader` keys).
  5. Repeat for `--lab agentic-pit-decisions` through its credential card.
- [ ] **Step 2: Existing environment.** Signed in as the EnvironmentAdmin-only login:
  1. `uv run deploy --environment env-z9qmqy`, for each lab.
  2. Expected: the checks pass, and the deploy finishes. A single "technical difficulties" retry
     is fine.
  3. Run the first workshop steps, including RTCE.
  4. `uv run destroy`.
  5. Expected: the summary says the environment is kept. Afterwards
     `confluent kafka cluster list --environment env-z9qmqy`, `confluent flink compute-pool list`
     and `confluent api-key list --current-user` are empty.
- [ ] **Step 3: Refusals.** `uv run deploy --environment env-z9qmqy --with-labs` exits with the
  4-hour message before anything is created.
- [ ] **Step 4:** Report the results to the user, with any output from failures.
