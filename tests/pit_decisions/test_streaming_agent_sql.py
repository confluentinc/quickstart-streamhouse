from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SQL = "docs/agentic-pit-decisions/sql"
WALKTHROUGH = "docs/agentic-pit-decisions/walkthrough.md"
# (source, marker): each statement the walkthrough has attendees paste.
SQL_SOURCES = (
    (f"{SQL}/enrichment_anomaly.sql", "CREATE TABLE `car_state`"),
    (f"{SQL}/streaming_agent_create_agent.sql", "CREATE AGENT `pit_strategy_agent`"),
    (f"{SQL}/streaming_agent_pit_decisions.sql", "CREATE TABLE `pit_decisions`"),
)


def _statement(relative_path: str, marker: str) -> str:
    source = (ROOT / relative_path).read_text()
    return source[source.index(marker) :].strip()


def _dedent(text: str) -> str:
    # The walkthrough embeds these statements inside a numbered list, so every line is
    # indented to stay in the list item. The File Sync Rule is about the SQL content,
    # not the markdown indentation, so compare with leading whitespace stripped.
    return "\n".join(line.lstrip() for line in text.splitlines())


def _normalize(text: str) -> str:
    return " ".join(text.split())


def test_attendee_walkthrough_sql_matches_the_canonical_sources():
    walkthrough = _dedent((ROOT / WALKTHROUGH).read_text())
    for source_path, marker in SQL_SOURCES:
        statement = _dedent(_statement(source_path, marker))
        assert statement in walkthrough, f"{marker} from {source_path} missing in {WALKTHROUGH}"


def test_pit_decisions_delegates_the_suggestion_to_the_agent():
    # The suggestion now comes from the agent's own answer (parsed out of the response
    # with REGEXP_EXTRACT), not computed by a SQL CASE and not injected back into the
    # prompt. This is the core of the streaming-agent design: the model decides.
    decision_sql = (ROOT / SQL / "streaming_agent_pit_decisions.sql").read_text()
    assert "Suggestion:" in decision_sql, "suggestion is no longer parsed from the agent response"
    assert "AS suggestion" in decision_sql
    assert "END AS suggestion" not in decision_sql, "suggestion must not be a SQL CASE"
    assert "REQUIRED SUGGESTION" not in decision_sql, "the answer must not be injected into the prompt"


def test_agent_prompt_reserves_pit_now_for_the_anomaly_signal():
    # The demo narrative — PIT NOW only when the lap-24 anomaly fires — is now a prompt
    # guardrail rather than a SQL CASE. The agent is told to reserve PIT NOW for the ML
    # anomaly signal, and that tire age / race context alone must not produce it.
    agent_sql = _normalize((ROOT / SQL / "streaming_agent_create_agent.sql").read_text())
    assert "PIT NOW is reserved for this signal being true" in agent_sql
    assert "should produce PIT NOW; those inform PIT SOON or STAY OUT instead" in agent_sql
