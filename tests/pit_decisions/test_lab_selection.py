"""The two labs share one environment: deploy picks which to build, destroy and reset touch only their own."""

from pathlib import Path

import pytest
from dotenv import dotenv_values

from scripts import deploy, destroy
from scripts.pit_decisions.reset import is_lab_statement

ROOT = Path(__file__).resolve().parents[2]


def _select(
    tmp_path, monkeypatch, requested=None, saved="", workshop=False, interactive=False, answer=None, deployed=()
):
    creds_file = tmp_path / "credentials.env"
    creds_file.write_text(f"DEPLOY_LABS={saved}\n" if saved else "", encoding="utf-8")
    for lab in deployed:
        state = tmp_path / "terraform" / deploy.LABS[lab][1] / "terraform.tfstate"
        state.parent.mkdir(parents=True, exist_ok=True)
        state.write_text('{"resources": [{"mode": "managed", "instances": [{}]}]}', encoding="utf-8")
    if answer:
        monkeypatch.setattr(deploy, "prompt_choice", lambda question, options: answer)
    labs = deploy._select_labs(requested, creds_file, dotenv_values(creds_file), workshop, interactive)
    return labs, dotenv_values(creds_file).get("DEPLOY_LABS")


@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        ("flight-disruptions", ["flight-disruptions"]),
        ("agentic-pit-decisions", ["agentic-pit-decisions"]),
        ("both", ["flight-disruptions", "agentic-pit-decisions"]),
    ],
)
def test_lab_flag_wins_and_is_saved(tmp_path, monkeypatch, requested, expected):
    labs, saved = _select(tmp_path, monkeypatch, requested=requested, saved="flight-disruptions")
    assert labs == expected
    assert saved == ",".join(expected)


def test_prompt_answer_maps_to_the_lab(tmp_path, monkeypatch):
    labs, saved = _select(tmp_path, monkeypatch, interactive=True, answer="Agentic Pit Decisions")
    assert labs == ["agentic-pit-decisions"]
    assert saved == "agentic-pit-decisions"


def test_bots_reuse_the_saved_choice_or_default_to_flight_disruptions(tmp_path, monkeypatch):
    assert _select(tmp_path, monkeypatch, saved="agentic-pit-decisions")[0] == ["agentic-pit-decisions"]
    assert _select(tmp_path, monkeypatch)[0] == ["flight-disruptions"]


def test_workshop_is_flight_disruptions_only(tmp_path, monkeypatch):
    assert _select(tmp_path, monkeypatch, workshop=True, saved="agentic-pit-decisions")[0] == ["flight-disruptions"]
    with pytest.raises(SystemExit):
        _select(tmp_path, monkeypatch, requested="agentic-pit-decisions", workshop=True)
    both = ["flight-disruptions", "agentic-pit-decisions"]
    assert _select(tmp_path, monkeypatch, workshop=True, saved=",".join(both))[0] == both


def _menu(tmp_path, monkeypatch, deployed=(), answer_index=1, workshop=False):
    """Run the lab question and return (labs, the options it showed)."""
    shown = []

    def choose(question, options):
        shown.extend(options)
        return options[answer_index]

    for lab in deployed:
        state = tmp_path / "terraform" / deploy.LABS[lab][1] / "terraform.tfstate"
        state.parent.mkdir(parents=True, exist_ok=True)
        state.write_text('{"resources": [{"mode": "managed", "instances": [{}]}]}', encoding="utf-8")
    creds_file = tmp_path / "credentials.env"
    creds_file.write_text("DEPLOY_LABS=flight-disruptions\n" if workshop else "", encoding="utf-8")
    monkeypatch.setattr(deploy, "prompt_choice", choose)
    labs = deploy._select_labs(None, creds_file, dotenv_values(creds_file), workshop, True)
    return labs, shown


def test_deploy_always_asks_and_marks_a_deployed_lab_without_forcing_it(tmp_path, monkeypatch):
    """A saved workshop used to skip the question; now it asks and marks what's deployed, and deploys only the pick."""
    labs, shown = _menu(tmp_path, monkeypatch, deployed=["agentic-pit-decisions"], answer_index=0)
    assert shown == ["Agentic Airline Operations", "Agentic Pit Decisions (already running)", "Both"]
    assert labs == ["flight-disruptions"]


def test_nothing_deployed_means_no_marks(tmp_path, monkeypatch):
    labs, shown = _menu(tmp_path, monkeypatch, answer_index=1)
    assert shown == ["Agentic Airline Operations", "Agentic Pit Decisions", "Both"]
    assert labs == ["agentic-pit-decisions"]


def test_a_failed_deploy_with_an_empty_state_is_not_running(tmp_path):
    """A failed first apply leaves a state file with no resources: that lab isn't deployed."""
    state = tmp_path / "terraform" / "agentic-pit-decisions" / "terraform.tfstate"
    state.parent.mkdir(parents=True)
    state.write_text('{"resources": [{"mode": "data", "instances": [{}]}]}', encoding="utf-8")
    assert deploy._deployed_labs(tmp_path) == []
    assert not deploy._has_resources(tmp_path / "terraform" / "core" / "terraform.tfstate")  # no file at all


def test_targets_always_start_with_core():
    assert deploy._targets(["agentic-pit-decisions"]) == ["core", "agentic-pit-decisions"]
    assert deploy._targets(list(deploy.LABS)) == ["core", "airline-demo", "agentic-pit-decisions"]


def test_destroy_one_lab_keeps_core_and_the_other_lab():
    assert destroy._envs_to_destroy("agentic-pit-decisions") == ["agentic-pit-decisions"]
    assert destroy._envs_to_destroy("flight-disruptions") == ["tableflow", "airline-demo"]
    assert destroy._envs_to_destroy(None)[-1] == "core"


def test_every_lab_root_exists():
    for _, env in deploy.LABS.values():
        assert (ROOT / "terraform" / env / "main.tf").exists()


@pytest.mark.parametrize(
    "path", sorted((ROOT / "terraform" / "airline-demo" / "sql").glob("*.sql")), ids=lambda p: p.name
)
def test_pit_decisions_reset_leaves_flight_disruptions_statements_alone(path):
    assert not is_lab_statement(path.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "path", sorted((ROOT / "docs" / "agentic-pit-decisions" / "sql").glob("*.sql")), ids=lambda p: p.name
)
def test_pit_decisions_reset_stops_its_own_statements(path):
    assert is_lab_statement(path.read_text(encoding="utf-8"))


def test_a_leftover_workshop_setting_doesnt_block_picking_pit_decisions(tmp_path, monkeypatch):
    """Picked Airline, said no at Ready (or destroyed that lab), then picked Pit Decisions: no flag error."""
    labs, _ = _menu(tmp_path, monkeypatch, answer_index=1, workshop=True)
    assert labs == ["agentic-pit-decisions"]


def test_a_saved_workshop_counts_only_while_the_airline_lab_is_deployed(tmp_path):
    creds_file = tmp_path / "credentials.env"
    creds_file.write_text("TF_VAR_workshop=true\n", encoding="utf-8")
    creds = dotenv_values(creds_file)
    assert deploy._workshop_mode(False, False, creds_file, creds, default=False) is False  # leftover: dropped
    assert "TF_VAR_workshop" not in dotenv_values(creds_file)


def test_destroying_the_airline_lab_forgets_its_workshop_setting(tmp_path):
    creds_file = tmp_path / "credentials.env"
    creds_file.write_text(
        "TF_VAR_workshop=true\nDEPLOY_LABS=flight-disruptions,agentic-pit-decisions\n", encoding="utf-8"
    )
    destroy._forget_lab(creds_file, "flight-disruptions")
    saved = dotenv_values(creds_file)
    assert "TF_VAR_workshop" not in saved
    assert saved["DEPLOY_LABS"] == "agentic-pit-decisions"
