"""The two labs share one environment: deploy picks which to build, destroy and reset touch only their own."""

from pathlib import Path

import pytest
from dotenv import dotenv_values

from scripts import deploy, destroy
from scripts.pit_decisions.reset import is_lab_statement

ROOT = Path(__file__).resolve().parents[2]


def _select(tmp_path, monkeypatch, requested=None, saved="", workshop=False, interactive=False, answer=None):
    creds_file = tmp_path / "credentials.env"
    creds_file.write_text(f"DEPLOY_LABS={saved}\n" if saved else "")
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


def test_targets_always_start_with_core():
    assert deploy._targets(["agentic-pit-decisions"]) == ["core", "agentic-pit-decisions"]
    assert deploy._targets(list(deploy.LABS)) == ["core", "airline-demo", "agentic-pit-decisions"]


def test_destroy_one_lab_keeps_core_and_the_other_lab():
    assert destroy._envs_to_destroy("agentic-pit-decisions") == ["agentic-pit-decisions"]
    assert destroy._envs_to_destroy("flight-disruptions") == ["airline-demo"]
    assert destroy._envs_to_destroy(None)[-1] == "core"


def test_every_lab_root_exists():
    for _, env in deploy.LABS.values():
        assert (ROOT / "terraform" / env / "main.tf").exists()


@pytest.mark.parametrize(
    "path", sorted((ROOT / "terraform" / "airline-demo" / "sql").glob("*.sql")), ids=lambda p: p.name
)
def test_pit_decisions_reset_leaves_flight_disruptions_statements_alone(path):
    assert not is_lab_statement(path.read_text())


@pytest.mark.parametrize(
    "path", sorted((ROOT / "docs" / "agentic-pit-decisions" / "sql").glob("*.sql")), ids=lambda p: p.name
)
def test_pit_decisions_reset_stops_its_own_statements(path):
    assert is_lab_statement(path.read_text())
