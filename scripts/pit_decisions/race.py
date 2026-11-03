"""``f1-race`` — run the race simulator locally from a credential card.

Maps the ``F1_*`` credential-card variables to the simulator's expected env
(``race_simulator/config.py``) and runs it against your Confluent cluster.

  uv run f1-race                     # pacing from this deployment's config
  uv run f1-race --20                # shorthand for --seconds-per-lap 20
  uv run f1-race --creds <card>.env --seconds-per-lap 60 --once
"""

from __future__ import annotations

import argparse
import os
import sys

from scripts.pit_decisions.card import (
    DEFAULT_SECONDS_PER_LAP,
    MIN_SECONDS_PER_LAP,
    load_card,
    validate_seconds_per_lap,
)


def _strip_scheme(bootstrap: str) -> str:
    """confluent-kafka wants host:port; the card bootstrap may carry a scheme."""
    return bootstrap.split("://", 1)[-1] if "://" in bootstrap else bootstrap


def _expand_numeric_flags(argv: list[str]) -> list[str]:
    """Rewrite the ``--<N>`` shorthand to ``--seconds-per-lap <N>``.

    Restores the ergonomics of the old ``scripts/start_race.py``: ``--20`` reads
    naturally at a demo and is far less to type. argparse cannot express it (a
    numeric option name is not a valid dest), so it is preprocessed here.
    """
    out: list[str] = []
    for arg in argv:
        if arg.startswith("--") and arg[2:].isdigit():
            out += ["--seconds-per-lap", arg[2:]]
        else:
            out.append(arg)
    return out


def _resolve_seconds_per_lap(explicit: int | None, card: dict) -> int:
    """--seconds-per-lap, else the pacing deploy recorded on the card, else the default."""
    if explicit is not None:
        raw, source = explicit, "--seconds-per-lap"
    elif card.get("F1_SECONDS_PER_LAP"):
        raw, source = card["F1_SECONDS_PER_LAP"], "the credential card"
    else:
        raw, source = DEFAULT_SECONDS_PER_LAP, "default"
    value, problem = validate_seconds_per_lap(raw)
    if problem:
        # readings_per_lap is SECONDS_PER_LAP // 2, so a tiny pace produces no telemetry at all.
        sys.exit(f"Error: {problem}")
    print(f"Pacing: {value}s/lap (~{value}-minute race, from {source})")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the F1 race simulator locally from a credential card",
        epilog="Shorthand: --20 is the same as --seconds-per-lap 20.",
    )
    parser.add_argument(
        "--creds",
        help="Path to a credential card (default: the one `uv run deploy` wrote)",
    )
    parser.add_argument(
        "--seconds-per-lap",
        type=int,
        default=None,
        metavar="N",
        help=(
            f"Wall-clock seconds per simulated lap (minimum {MIN_SECONDS_PER_LAP}). "
            "Defaults to the pacing recorded for this deployment, else 20."
        ),
    )
    parser.add_argument("--once", action="store_true", help="Run a single race instead of looping continuously")
    args = parser.parse_args(_expand_numeric_flags(sys.argv[1:]))

    path, card = load_card(args.creds)
    print(f"Using credential card: {path}")
    seconds_per_lap = _resolve_seconds_per_lap(args.seconds_per_lap, card)

    def need(key: str) -> str:
        v = card.get(key)
        if not v:
            sys.exit(f"Credential card is missing {key}. Regenerate it with `uv run deploy`.")
        return v

    # Map card F1_* vars to the simulator's expected env (race_simulator/config.py).
    os.environ["KAFKA_BOOTSTRAP"] = _strip_scheme(need("F1_KAFKA_BOOTSTRAP"))
    os.environ["KAFKA_API_KEY"] = need("F1_KAFKA_API_KEY")
    os.environ["KAFKA_API_SECRET"] = need("F1_KAFKA_API_SECRET")
    os.environ["SR_URL"] = need("F1_SCHEMA_REGISTRY_URL")
    os.environ["SR_API_KEY"] = need("F1_SR_API_KEY")
    os.environ["SR_API_SECRET"] = need("F1_SR_API_SECRET")
    os.environ["SECONDS_PER_LAP"] = str(seconds_per_lap)
    os.environ["RACE_LOOP"] = "false" if args.once else "true"

    # Skip the pre-race warmup laps (~80s at 20s/lap). They produce four
    # telemetry windows at lap=0 and **no** race_standings, and LAB 3's first CTE
    # is an *inner* temporal join against race_standings — so on a cold start
    # those rows have no version to join against and never reach the anomaly
    # function at all. Even if they did, 4 windows against a context size of 12
    # changes nothing: real race data supplies 12 windows (1/lap) by lap 12, long
    # before the lap-24 anomaly.
    #
    # setdefault, so `PRE_RACE_WARMUP_LAPS=4 uv run f1-race` still works.
    os.environ.setdefault("PRE_RACE_WARMUP_LAPS", "0")

    # Import after setting env — scripts.pit_decisions.race_simulator.config reads os.environ at import time.
    from scripts.pit_decisions.race_simulator import simulator

    simulator.main()


if __name__ == "__main__":
    main()
