# Windows workshop test results

Agentic Pit Decisions reached the lap-24 `PIT NOW` milestone on Windows. Flight Disruptions booked an agent-generated offer and closed its alternate. The run used `fix/destroy-rtce-mcp` at `e05319b`, Windows Server 2025 x64 on EC2, Windows PowerShell 5.1, Git 2.56.1, uv 0.13.10, Terraform 1.17.2, and Confluent CLI 4.83.0. Each lab used a fresh clone and a separate Confluent Cloud environment.

## Attendee results

| Step | Observed result |
| --- | --- |
| Windows prerequisites | The four documented `winget` commands and all four version checks passed under the desktop user account. The documented clone command worked. I then switched each clone to the branch under test. |
| F1 deploy and race | The interactive `uv run deploy` with choice `2` seeded and verified 198 history rows. `uv run f1-race` and `uv run f1-pitwall` ran in separate PowerShell sessions. The Windows dashboard returned HTTP 200 and streamed live data on its WebSocket. |
| F1 anomaly and pit strategy | The walkthrough SQL returned nominal readings, `PIT SOON` on laps 19–23, and `PIT NOW` for lap 24 at P8. The 144.48°C reading triggered `MEDIUM` for 36 laps. The original Pit Wall unlocked both panels without a restart and received the decision while its leaderboard was on lap 26. |
| F1 Lightning and RTCE | All three Lightning Tables became active. The PowerShell command from `uv run setup-rtce --lightning car_telemetry` returned ten rows. The challenge query returned the lap-24 compound and reasoning. `uv run setup-rtce` registered Claude Code, listed three topics, and `claude mcp list` showed Connected. Direct RTCE reads confirmed the pit decisions and the lap-30 front-right temperature of 98.7°C. |
| F1 teardown | `uv run destroy` exited successfully. The Windows checkout's Terraform state, F1 credential card, saved credentials, and Claude Code RTCE registration were gone afterward. |
| Flight Disruptions deploy and Labs 1–2 | `uv run deploy` with choice `1` brought up the starter environment. `uv run airport-datagen --skip-history` published 14,384 initial records. The workshop queries showed an OK connection and a 10-minute RA417 delay with zero passengers affected initially. |
| Flight Disruptions Labs 3–4 | All seven Lightning Tables became active. The exact hotel query returned the three hotels and their current prices. `uv run airport-app` served 180 flights, RA417 telemetry, 170 passengers, and a 182-seat map. `uv run setup-rtce --agent-connection` worked. The recovery agent's insert ran and `uv run workshop-status` passed every check. |
| Flight Disruptions Lab 5 | The first 12 HIGH-risk passengers received 24 offers. Seat 10E booked RA606 for November 26 with the Grand Hyatt at SFO for $329. Both the dashboard and the guide's Flink query showed `BOOKED` for that offer and `CLOSED` for the Marriott alternate on RA621. |
| Flight Disruptions teardown | `uv run destroy` stopped the Windows generator and exited successfully. The app and generator processes, saved credentials, and Terraform state were gone afterward. |

The remote desktop stopped updating during the lab. I submitted the guide SQL from Windows against Flink, including the two documented UTC statement properties, and enabled Lightning Tables through the Confluent CLI. I checked the Windows apps through their localhost HTTP and WebSocket interfaces and used the dashboard's Select and Book endpoints. One reused HTTP connection dropped after Select. A fresh read confirmed the selection, then Book and both status checks passed. The Console click sequence and the dashboards' visual interactions still need a desktop run. Claude Code had no model login on the test VM, so the F1 natural-language questions were checked through direct RTCE reads. The Flight Disruptions optional coding-agent challenge was outside this run.

## Changes and recommendations

- **Fixed a Windows test failure.** `test_quick_never_manages_the_account_subscription` used the default CP1252 decoder on UTF-8 Terraform files. It now reads them explicitly as UTF-8. Keep the Windows CI setting `PYTHONUTF8=0` so tests catch this class of failure. [Test](../tests/test_always_on.py) · [CI workflow](../.github/workflows/tests.yml)
- **Clarified F1 timing.** The Pit Wall took about four minutes to discover the newly created decisions topic. The walkthrough now gives attendees a restart option and distinguishes the decision's lap from the live leaderboard's lap. A follow-up test could measure how fast an already-open Pit Wall discovers a topic created after startup. [F1 walkthrough](agentic-pit-decisions/walkthrough.md#4-review-the-results)
- **Make login failures actionable.** In the SSH-hosted PowerShell, the second password entry surfaced as a generic login failure. The underlying Confluent CLI reported Windows credential encryption errors. The deploy succeeded from the desktop user's session with the authorized login values in `credentials.env`. Show the redacted CLI error when login fails so a credential-store error points the attendee to local PowerShell. The separate Systems Manager service shell also lacked the user's `winget` alias. [Login checks](../scripts/login_checks.py)

## Verification

| Command | Result |
| --- | --- |
| `uv run pytest -q` on Windows before the fix | 675 passed, 1 skipped, 1 failed out of 677 collected. |
| `uv run ruff check scripts tests` on Windows and Mac | All checks passed. |
| `uv run pytest -o addopts= tests/test_always_on.py::test_quick_never_manages_the_account_subscription` | Passed on Mac and on Windows with `PYTHONUTF8=0` after the fix. |
| `uv run pytest -o addopts= -q tests/test_workshop.py tests/pit_decisions/test_streaming_agent_sql.py` on Mac | 21 passed. |
