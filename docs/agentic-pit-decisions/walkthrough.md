# F1 Agentic Pit Decisions walkthrough

![Agentic Pit Decisions architecture](../images/agentic-pit-decisions/architecture.png)

In this ~45-minute lab, River Racing's car #88 streams live telemetry from Silverstone. Flink uses `ML_DETECT_ANOMALIES` to flag a front-left tire anomaly, and a streaming agent running on Confluent-hosted Claude recommends `STAY OUT`, `PIT SOON`, or `PIT NOW` on every lap. You'll create your own Confluent Cloud environment with `uv run deploy` and run the race simulator locally.

Along the way, you build a small **Streamhouse**: an architecture that continuously captures, processes, and serves the current state of the business. Here the business is a race, and the agent's every call is only as good as how current its view of the car is.

## Set up Confluent Cloud

#### Step 1 — Create your Confluent Cloud account

1. Go to the **[Confluent Cloud sign-up page](https://cnfl.io/learnwithcflt)**.
2. Enter your **name**, **work email**, **company**, and a **password**, then accept the terms
   and click **Start Free**.
3. On first sign-in, an **onboarding wizard** asks a few questions, then offers to create your
   first cluster.

> [!WARNING]
> **Don't create a cluster.** Deploy creates one for you. Click the Confluent logo (top-left) to exit the wizard.

#### Step 2 — Add a payment method (credit card)

Deploy needs a payment method to create your cluster. New sign-ups get **$400 in free credit**, so the workshop won't charge your card.

1. Go directly to **[Billing & payment](https://confluent.cloud/settings/billing/payment)**.
2. Open the **Payment details and contacts** tab.
3. Click **Add credit card / bank account**, enter your **card details**, then **Save**.

   <img src="../images/add-payment-method.png" alt="Payment details and contacts — Add credit card / bank account" width="500">

## Prerequisites

> [!TIP]
>
> **Using a host-provided workstation?** Skip to [Deploy to Confluent Cloud](#deploy-to-confluent-cloud).

<details>
<summary>for Mac</summary>

No Homebrew yet? Install it from [brew.sh](https://brew.sh) first.

```bash
brew install git uv
brew tap hashicorp/tap
brew install hashicorp/tap/terraform
brew install --cask confluent-cli
```

</details>

<details>
<summary>for Windows</summary>

1. Open **PowerShell**: Start menu → type **PowerShell** → **Windows PowerShell**.
2. Install the tools. If winget asks you to accept its terms, enter `Y`.

    ```powershell
    winget install --id Git.Git -e --scope user --source winget
    winget install --id astral-sh.uv -e --source winget
    winget install --id Hashicorp.Terraform -e --source winget
    winget install --id ConfluentInc.Confluent-CLI -e --source winget
    ```

3. Close PowerShell and open a new window.
4. When you clone the repo later, do it in your user folder (where PowerShell opens), not OneDrive or Documents. Long paths break the install.

**Problems?**

- **`winget` is not recognized:** install [App Installer](https://apps.microsoft.com/detail/9NBLGGH4NNS1) from the Microsoft Store, then open a new PowerShell window.
- **winget is blocked on your laptop:** install [Git](https://git-scm.com/downloads/win), [uv](https://docs.astral.sh/uv/getting-started/installation/), [Terraform](https://developer.hashicorp.com/terraform/install), and the [Confluent CLI](https://docs.confluent.io/confluent-cli/current/install.html) from their download pages. Unzip Terraform and the Confluent CLI into one folder, add it to your `Path` (Start menu → **Edit environment variables for your account**), then open a new PowerShell window.
- **Windows on Arm** (Surface Pro X, Copilot+ PCs): after you `cd quickstart-streamhouse`, run `uv venv --python cpython-3.12-windows-x86_64-none` once.

</details>

Check that each tool prints a version:

```bash
git --version
uv --version
terraform -version
confluent version
```

## Deploy to Confluent Cloud

1. Clone the repo and go into it:

    ```bash
    git clone https://github.com/confluentinc/quickstart-streamhouse.git
    cd quickstart-streamhouse
    ```

2. **Only if you signed up to Confluent Cloud with SSO:** sign in to the Confluent CLI first. Skip this if you signed up with email and password.

    ```bash
    confluent login
    ```

3. Deploy your environment:

    ```bash
    uv run deploy
    ```

    <img src="../images/agentic-pit-decisions/selfservice-up.png" alt="uv run deploy for Agentic Pit Decisions: lab choice, login, new environment, API key created, summary, and the ready prompt" width="600">

    Deploy asks for the following, in this order:

    | Prompt | Answer |
    |---|---|
    | `Which lab do you want to deploy?` | Enter `2` for **Agentic Pit Decisions**. |
    | `Email (Enter to skip)` and `Password` | Your Confluent Cloud email and password. Already signed in? Press **Enter**. |
    | `Deploy into an existing environment? Paste its ID (env-xxxxx), or press Enter to create a new one:` | Press **Enter** for a new environment.<br>Or paste an environment ID to deploy into it. |
    | `Ready to deploy? (y/n)` | Enter `y`. |

    When it finishes, deploy prints your environment (catalog) and cluster (database) names, which you need in Section 1.

> [!NOTE]
>
> Deploy takes about 10 minutes.

## 1. Enhance the data with built-in ML functions and start the race

Deploy created three tables: the car's sensor readings, the live race positions, and past race results. Now you combine the sensor readings and race positions into one live table that spots tire problems on its own, then start the race.

1. Open the Flink SQL workspace:

    1. Go to the [Flink UI](https://confluent.cloud/go/flink) and select your **environment**.

        <img src="../images/select-flink-environment.png" alt="The Navigate to your Flink compute pools dialog with the streamhouse environment selected" width="450">

    2. Open a **SQL workspace**.

        <img src="../images/open-sql-workspace.png" alt="The streamhouse compute pool card with its SQL Workspace button" width="400">

    3. Set **Catalog** to your environment and **Database** to your cluster: the names `uv run deploy` printed.

        <img src="../images/set-catalog-database.png" alt="The SQL workspace with Use catalog set to the streamhouse environment and Use database set to its cluster" width="600">

    <details>
    <summary>Optional: Explore the pre-deployed source tables and models</summary>

    ```sql
    SHOW TABLES;
    ```

    | Table | Source | Format |
    |-------|--------|--------|
    | `car_telemetry` | Race simulator — car #88 sensors, every 2s | Avro |
    | `race_standings` | Race simulator — all 22 cars, keyed by `car_number` (upsert) | Avro |
    | `driver_race_history` | 198 historical race rows seeded during setup | JSON |

    ```sql
    SELECT COUNT(*) FROM driver_race_history;
    ```

    ```sql
    SHOW MODELS;        -- llm_textgen_model, Claude hosted in Confluent
    ```

    </details>

2. Create `car_state`. It groups the car's sensor readings into 20-second windows and averages each window into one row. It adds the car's race position and flags an abnormal front-left tire temperature with `ML_DETECT_ANOMALIES`. Run this:

    ```sql
    CREATE TABLE `car_state` WITH ('changelog.mode' = 'append', 'kafka.consumer.isolation-level' = 'read-uncommitted') AS
    WITH enriched AS (
      SELECT
        t.car_number, t.event_time, t.lap,
        t.tire_temp_fl_c, t.tire_temp_fr_c, t.tire_temp_rl_c, t.tire_temp_rr_c,
        t.tire_pressure_fl_psi, t.tire_pressure_fr_psi, t.tire_pressure_rl_psi, t.tire_pressure_rr_psi,
        t.engine_temp_c, t.brake_temp_fl_c, t.brake_temp_fr_c, t.battery_charge_pct, t.fuel_remaining_kg,
        r.`position`, r.gap_to_ahead_sec, r.gap_to_leader_sec, r.pit_stops, r.tire_compound, r.tire_age_laps
      FROM `car_telemetry` t
      JOIN `race_standings` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ FOR SYSTEM_TIME AS OF t.event_time AS r ON t.car_number = r.car_number
    ),
    windowed AS (
      SELECT
        window_start, window_end, window_time, car_number, MAX(lap) AS lap,
        AVG(tire_temp_fl_c) AS tire_temp_fl_c, AVG(tire_temp_fr_c) AS tire_temp_fr_c,
        AVG(tire_temp_rl_c) AS tire_temp_rl_c, AVG(tire_temp_rr_c) AS tire_temp_rr_c,
        AVG(tire_pressure_fl_psi) AS tire_pressure_fl_psi, AVG(tire_pressure_fr_psi) AS tire_pressure_fr_psi,
        AVG(tire_pressure_rl_psi) AS tire_pressure_rl_psi, AVG(tire_pressure_rr_psi) AS tire_pressure_rr_psi,
        AVG(engine_temp_c) AS engine_temp_c, AVG(brake_temp_fl_c) AS brake_temp_fl_c, AVG(brake_temp_fr_c) AS brake_temp_fr_c,
        AVG(battery_charge_pct) AS battery_charge_pct, AVG(fuel_remaining_kg) AS fuel_remaining_kg,
        MAX(`position`) AS `position`, MAX(gap_to_ahead_sec) AS gap_to_ahead_sec, MAX(gap_to_leader_sec) AS gap_to_leader_sec,
        MAX(pit_stops) AS pit_stops, MAX(tire_compound) AS tire_compound, MAX(tire_age_laps) AS tire_age_laps
      FROM TABLE(TUMBLE(TABLE enriched, DESCRIPTOR(event_time), INTERVAL '20' SECOND))
      GROUP BY window_start, window_end, window_time, car_number
    ),
    anomaly AS (
      SELECT *,
        ML_DETECT_ANOMALIES(tire_temp_fl_c, window_time,
          JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 50, 'confidencePercentage' VALUE 99.99, 'enableStl' VALUE FALSE))
          OVER (PARTITION BY car_number ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS anomaly_tire_temp_fl_result
      FROM windowed
    )
    SELECT
      car_number, lap,
      tire_temp_fl_c, tire_temp_fr_c, tire_temp_rl_c, tire_temp_rr_c,
      tire_pressure_fl_psi, tire_pressure_fr_psi, tire_pressure_rl_psi, tire_pressure_rr_psi,
      engine_temp_c, brake_temp_fl_c, brake_temp_fr_c, battery_charge_pct, fuel_remaining_kg,
      CASE
        WHEN anomaly_tire_temp_fl_result.is_anomaly AND anomaly_tire_temp_fl_result.actual_value > anomaly_tire_temp_fl_result.upper_bound THEN true
        ELSE false
      END AS anomaly_tire_temp_fl,
      `position`, gap_to_ahead_sec, gap_to_leader_sec, pit_stops, tire_compound, tire_age_laps
    FROM anomaly
    WHERE lap > 0;
    ```

> [!NOTE]
>
> This statement runs continuously. Don't stop or delete it.

3. In your terminal, start the race:

    ```bash
    uv run f1-race
    ```

> [!WARNING]
>
> **Leave it running for the rest of the lab.**

4. Start the Pit Wall dashboard in a **new terminal** in the `quickstart-streamhouse` folder. It opens [http://localhost:8001](http://localhost:8001) in your browser:

    ```bash
    uv run f1-pitwall
    ```

    It shows the Silverstone track map, the live leaderboard, and car #88's tire and fuel gauges. The **Anomaly Detection** panel unlocks as `car_state` emits rows, and the **AI Pit Strategist** panel unlocks once you create `pit_decisions` in Section 3.

    ![Pit Wall dashboard showing a nominal front-left tire temperature](../images/agentic-pit-decisions/pitwall-nominal.png)

> [!WARNING]
>
> **Leave it running for the rest of the lab.**

5. Back in the Flink SQL workspace, verify the stream in a new cell:

    ```sql
    SELECT car_number, lap, `position`, tire_compound, tire_age_laps, anomaly_tire_temp_fl, tire_temp_fl_c
    FROM `car_state`;
    ```

    A new row appears every 20 seconds.

> [!NOTE]
>
> When you're done with a `SELECT`, click **Stop** in its cell to free up your compute pool.

## 2. Create the agent to provide pit decision guidance

Create `pit_strategy_agent`. On every lap, it checks the car and the race and recommends `STAY OUT`, `PIT SOON`, or `PIT NOW`.

1. In a new cell, run the `CREATE AGENT` statement:

    ```sql
    CREATE AGENT `pit_strategy_agent`
    USING MODEL `llm_textgen_model`
    USING PROMPT 'OUTPUT FORMAT — respond with exactly these 7 labeled lines in this order. Plain text only, no markdown or bold.

    Suggestion: [PIT NOW | PIT SOON | STAY OUT]
    Condition Summary: [one sentence describing current car condition]
    Race Context: [one sentence on race situation]
    Recommended Compound: [SOFT | MEDIUM | HARD | N/A if STAY OUT]
    Recommended Stint Laps: [integer expected laps on new tires | N/A if STAY OUT]
    Recommended Reason: [one sentence explaining compound choice | N/A if STAY OUT]
    Reasoning: [2-4 sentences full explanation of your decision]

    Correct STAY OUT example:
    Suggestion: STAY OUT
    Condition Summary: Front-left tire temperature is nominal at 107C with 18 laps of age on SOFT compound.
    Race Context: Currently P3. Leader is 8.2s ahead.
    Recommended Compound: N/A
    Recommended Stint Laps: N/A
    Recommended Reason: N/A
    Reasoning: No anomaly has been detected, so tire temps and pressures are within normal operating windows for this stage of the stint. Track position P3 is strong and pitting now would surrender 4-6 seconds.

    Correct PIT NOW example:
    Suggestion: PIT NOW
    Condition Summary: Front-left tire temperature anomaly at 145C, 20C above expected upper bound — failure risk imminent.
    Race Context: Currently P8 with 36 laps remaining.
    Recommended Compound: MEDIUM
    Recommended Stint Laps: 36
    Recommended Reason: Mediums will carry John to the flag across the remaining 36 laps and give him the pace to recover positions lost during the stop.
    Reasoning: The FL anomaly flag indicates the SOFT has gone past its operating limit with blowout risk. Pitting now onto mediums avoids tire failure. Based on historical data, John averages +2.75 positions on SOFT-MEDIUM — this is his strongest strategy.

    ---

    You are the AI pit wall strategist for River Racing at the 2026 British Grand Prix (Silverstone, 60 laps). Driver: John Doe, Car #88.

    DECISION FRAMEWORK — weigh these in priority order:
    1. Anomaly signal: anomaly_tire_temp_fl comes from a trained statistical anomaly detector (ML_DETECT_ANOMALIES) that evaluates tire_temp_fl_c against expected bounds. PIT NOW is reserved for this signal being true — treat it as validated evidence of a real tire problem requiring an immediate stop. When it is false, a raw temperature reading that merely looks high is still within expected bounds for the stint. Nothing else in this input — not tire age, not race context, not competitor behavior — should produce PIT NOW; those inform PIT SOON or STAY OUT instead.
    2. Pit history: a car that has already pitted this race should usually stay out and run its current tires to the end, absent a new anomaly.
    3. Tire age and compound: a SOFT tire past ~20 laps enters its strategic pit window. Call PIT SOON (never PIT NOW) throughout this window when no anomaly is flagged.
    4. Otherwise: default to STAY OUT.

    STRICT OUTPUT VALUES — the Suggestion line must be the exact literal string PIT NOW, PIT SOON, or STAY OUT with no extra text. For STAY OUT, write N/A for Recommended Compound, Recommended Stint Laps, and Recommended Reason.

    TIRE STRATEGY at Silverstone (60-lap race):
    - SOFT: Optimal laps 1-19; degrades in laps 20-22 (PIT SOON window), no failure risk unless the anomaly sensor fires.
    - MEDIUM: Best for a 30-40 lap second stint after SOFT (clean 1-stop).
    - HARD: Durable but slow; only for 40+ lap stints.
    - John Doe historical best: SOFT -> MEDIUM 1-stop averages +2.75 positions over 4 prior races.'
    WITH ('max_iterations' = '10');
    ```

2. Confirm the agent exists:

    ```sql
    SHOW AGENTS;
    ```

## 3. Create the pit decisions table

`pit_decisions` runs `pit_strategy_agent` on every row of `car_state` with [`AI_RUN_AGENT`](https://docs.confluent.io/cloud/current/flink/reference/functions/model-inference-functions.html#ai-run-agent) and splits each answer into columns. It also covers laps that already happened.

1. In a new cell, create `pit_decisions`:

    ```sql
    CREATE TABLE `pit_decisions` WITH ('changelog.mode' = 'append', 'kafka.consumer.isolation-level' = 'read-uncommitted') AS
    SELECT
      cs.car_number, cs.lap, cs.`position`,
      cs.tire_compound AS tire_compound_current, cs.tire_age_laps, cs.anomaly_tire_temp_fl,
      TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Suggestion:\*{0,2}\s*([^\n]+)', 1)) AS suggestion,
      TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Condition Summary:\*{0,2}\s*([^\n]+)', 1)) AS condition_summary,
      TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Race Context:\*{0,2}\s*([^\n]+)', 1)) AS race_context,
      NULLIF(TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Recommended Compound:\*{0,2}\s*([^\n]+)', 1)), 'N/A') AS recommended_tire_compound,
      CAST(NULLIF(TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Recommended Stint Laps:\*{0,2}\s*([^\n]+)', 1)), 'N/A') AS INT) AS recommended_stint_laps,
      NULLIF(TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Recommended Reason:\*{0,2}\s*([^\n]+)', 1)), 'N/A') AS recommended_reason,
      TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Reasoning:\*{0,2}\s*([\s\S]+?)$', 1)) AS reasoning,
      CAST(response AS STRING) AS raw_response
    FROM `car_state` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ cs,
    LATERAL TABLE(AI_RUN_AGENT(
      `pit_strategy_agent`,
      CONCAT(
        'CAR STATE — Lap ', CAST(cs.lap AS STRING), ' of 60 | Silverstone British Grand Prix\n',
        'Driver: John Doe (#', CAST(cs.car_number AS STRING), ') | Current Position: P', CAST(cs.`position` AS STRING), '\n',
        '\nTIRE DATA:\n',
        '  Compound: ', cs.tire_compound, ' | Age: ', CAST(cs.tire_age_laps AS STRING), ' laps\n',
        '  FL Temp: ', CAST(ROUND(cs.tire_temp_fl_c, 1) AS STRING), 'C  FR: ', CAST(ROUND(cs.tire_temp_fr_c, 1) AS STRING), 'C  RL: ', CAST(ROUND(cs.tire_temp_rl_c, 1) AS STRING), 'C  RR: ', CAST(ROUND(cs.tire_temp_rr_c, 1) AS STRING), 'C\n',
        '  FL Pressure: ', CAST(ROUND(cs.tire_pressure_fl_psi, 1) AS STRING), 'psi  FR: ', CAST(ROUND(cs.tire_pressure_fr_psi, 1) AS STRING), 'psi  RL: ', CAST(ROUND(cs.tire_pressure_rl_psi, 1) AS STRING), 'psi  RR: ', CAST(ROUND(cs.tire_pressure_rr_psi, 1) AS STRING), 'psi\n',
        '  FL Tire Anomaly Detected: ', CAST(cs.anomaly_tire_temp_fl AS STRING), '\n',
        '\nCAR SYSTEMS:\n',
        '  Engine Temp: ', CAST(ROUND(cs.engine_temp_c, 1) AS STRING), 'C  Brake FL: ', CAST(ROUND(cs.brake_temp_fl_c, 1) AS STRING), 'C  Brake FR: ', CAST(ROUND(cs.brake_temp_fr_c, 1) AS STRING), 'C\n',
        '  Battery: ', CAST(ROUND(cs.battery_charge_pct, 1) AS STRING), '%  Fuel Remaining: ', CAST(ROUND(cs.fuel_remaining_kg, 1) AS STRING), 'kg\n',
        '\nRACE CONTEXT:\n',
        '  Gap to Leader: ', CAST(ROUND(cs.gap_to_leader_sec, 2) AS STRING), 's  Gap to Car Ahead: ', CAST(ROUND(cs.gap_to_ahead_sec, 2) AS STRING), 's\n',
        '  Pit Stops Taken: ', CAST(cs.pit_stops AS STRING), '\n',
        '  Laps Remaining: ', CAST(60 - cs.lap AS STRING)
      ),
      MAP['debug', 'true']
    ));
    ```

> [!NOTE]
>
> This statement runs continuously. Don't stop or delete it.

## 4. Review the results

1. Query `pit_decisions` in your SQL workspace to see the streaming recommendations:

    ```sql
    SELECT lap, `position`, suggestion, condition_summary, reasoning
    FROM `pit_decisions`;
    ```

    ![Flink SQL workspace showing pit_decisions results progressing from PIT SOON to PIT NOW](../images/agentic-pit-decisions/pit-decisions-query.png)

2. Filter for the tire temperature anomaly to inspect the lap 24 pit call:

    ```sql
    SELECT lap, `position`, tire_compound_current, tire_age_laps, anomaly_tire_temp_fl,
           suggestion, recommended_tire_compound, recommended_stint_laps, reasoning
    FROM `pit_decisions`
    WHERE anomaly_tire_temp_fl = true;
    ```

    ![Flink SQL workspace showing the lap 24 PIT NOW row with the agent's full reasoning](../images/agentic-pit-decisions/pit-decisions-anomaly-query.png)

    No rows yet? The anomaly appears at lap 24. Rerun the query once the race passes it.

> [!NOTE]
>
> | Lap | Position | Suggestion | What's happening |
> |---|---|---|---|
> | 1–18 | P3 → P1 | `STAY OUT` | Competitive, nominal tire temps |
> | ~19–23 | P1 → P8 | `PIT SOON` | Aging SOFTs enter the strategic pit window |
> | **24** | **P8** | **`PIT NOW`** | **Front-left anomaly (~145°C) triggers an immediate stop** |
> | 25–60 | P14 → P1–P2 | `STAY OUT` | Recovery stint to the flag on fresh MEDIUMs |
>
> The `PIT NOW` row is for lap 24, the lap where the anomaly appears. The live leaderboard can advance while the Flink window closes and the agent responds.

3. Switch to the Pit Wall dashboard. Its **Anomaly Detection** and **AI Pit Strategist** panels are now unlocked:

    ![Pit Wall dashboard with both Anomaly Detection and AI Pit Strategist panels unlocked, showing the PIT NOW call at lap 24](../images/agentic-pit-decisions/pitwall-pit-now.png)

    The Pit Wall may take a few minutes to discover `pit_decisions` if it was open before you created the table. If the SQL query shows decisions and the panel is still locked, restart only `uv run f1-pitwall` and leave `f1-race` running.

## 5. Enable Lightning Tables on the race topics

Lightning Tables let apps query a topic's live data, and power the Real-Time Context Engine (RTCE), which lets any AI agent query it over MCP.

1. Open **[Topics](https://confluent.cloud/go/topics)** and select your **environment** and **cluster**. Enable Lightning Tables on `car_telemetry`, `car_state`, and `pit_decisions`. For each one:

    1. Click **+ Enable** in the **Lightning Tables** column.

        <img src="../images/agentic-pit-decisions/lightning-tables-enable.png" alt="The cluster's Topics list with + Enable in the Lightning Tables column for car_state and car_telemetry" width="750">

    2. Click **Enable**.

        <img src="../images/agentic-pit-decisions/lightning-tables-confirm.png" alt="The Enable a Lightning Table for car_telemetry? dialog with its Enable button" width="750">

    3. Wait a couple of minutes while it sets up.

        <img src="../images/agentic-pit-decisions/lightning-tables-progress.png" alt="The Lightning Table dialog showing Enabled Lightning Table, Query engine is ready, and Finalizing infrastructure" width="750">

    4. Click **Done** when it says **Lightning Table is enabled**.

        <img src="../images/agentic-pit-decisions/lightning-tables-enabled.png" alt="The Lightning Table enabled for car_telemetry dialog with Lightning Table is enabled and a Done button" width="750">

## 🧩 Challenge #1 — Analyze race data with Lightning Queries

Query the Lightning Tables you enabled in Section 5 directly with Lightning Queries.

Print a ready-to-run query in a **new terminal** in the `quickstart-streamhouse` folder:

```bash
uv run setup-rtce --lightning car_telemetry
```

Copy the printed command (`curl` on Mac, PowerShell on Windows) and run it. It returns 10 telemetry rows.

**Your challenge:** edit the command's `query` to find out what tire compound the agent recommended on lap 24, and why.

<details>
<summary>Hint</summary>

```sql
SELECT lap, <column>, <column> FROM pit_decisions WHERE lap = 24 LIMIT 1
```

Fill in the first `<column>` with the recommended tire compound, and the second with the agent's reasoning for that call.

</details>

**Expected result:**

<img src="../images/agentic-pit-decisions/lightning-query-lap-24.png" alt="The Lightning Query curl command and its JSON result for lap 24: MEDIUM, with the agent's reasoning" width="750">

## 6. (Optional) Ask your coding agent about the race with Real-Time Context Engine

<details>
<summary>Install Claude Code</summary>

On Mac:

```bash
brew install --cask claude-code
```

On Windows:

```powershell
winget install --id Anthropic.ClaudeCode -e --source winget
```

Then open a new PowerShell window in the `quickstart-streamhouse` folder.

</details>

1. Back in your terminal, register RTCE with your coding agent:

    ```bash
    uv run setup-rtce
    ```

    Enter `1` for Claude Code or `2` for Codex. Already have it open? Restart it.

> [!NOTE]
>
> The connection can take a minute or two. In Claude Code, run `/mcp` and check that `confluent-streamhouse-rtce` shows as connected.

2. Start your coding agent in the `quickstart-streamhouse` folder (for example, `claude`) and ask:

    ```text
    What topics do I have access to in the Real-Time Context Engine?
    ```

    ![Claude listing the topics enabled in the Real-Time Context Engine](../images/agentic-pit-decisions/rtce-listtopics.png)

    ```text
    How has the engine temperature been trending over the last few laps?
    ```

    ![Claude answering how the engine temperature has trended over the last few laps by calling the Real-Time Context Engine](../images/agentic-pit-decisions/rtce-engine-trend.png)

    ```text
    What's the front-left tire temperature on car 88 right now?
    ```

    ![Claude answering the front-left tire temperature question by calling the Real-Time Context Engine](../images/agentic-pit-decisions/rtce-cartemp.png)

    ```text
    At what lap in the race does the engine temperature peak?
    ```

    ```text
    What is the front right tire temperature at lap 30?
    ```

## 🧩 (Optional) Challenge #2 — Social Media Agent (Claude + Real-Time Context Engine)

Take on the role of River Racing's social media team. Using live race data from RTCE, have your AI agent draft an engaging social media post:

1. Connect your coding agent to RTCE first ([Section 6](#6-optional-ask-your-coding-agent-about-the-race-with-real-time-context-engine)).
2. In your coding agent, give it a role, for example: "You're River Racing's social media manager. Use only live race data."
3. Ask it for a post (prompts below).

Prompts:
- "We just made a big move — write a post celebrating it."
- "The pit wall just made a call. Draft a post about our strategy."
- "Write a 3-tweet recap thread of John's race so far."

**Success looks like:** a post with the real lap, position, and pit call from your live race. No data? Check that `f1-race` is running.

## Run the lab again or tear it down

Stop `f1-race` and `f1-pitwall`: press **Ctrl-C** in their terminals.

**Run the lab again:** reset, then start over from [Section 1](#1-enhance-the-data-with-built-in-ml-functions-and-start-the-race).

```bash
uv run f1-reset
```

Tear everything down. Enter `y` to confirm:

```bash
uv run destroy
```

<details>
<summary>Remove only this lab</summary>

Keeps the environment, cluster, and the Agentic Airline Operations lab:

```bash
uv run destroy --lab agentic-pit-decisions
```

</details>

## Troubleshooting

<details>
<summary>Common issues and fixes</summary>

- **`car_state` is empty at first.** `car_state` emits one row per 20-second window, and `f1-race` aligns lap 1 to the next 20-second wall-clock boundary before starting. Make sure `car_state` is **Running** before starting `uv run f1-race`.
- **No anomaly around lap 24.** `ML_DETECT_ANOMALIES` needs 12 windows of context (`minTrainingSize = 12`) before lap 24. If `car_state` was started mid-race after lap 12, reset and start `car_state` before `uv run f1-race`.
- **Agent fields are null but `raw_response` has text.** Inspect `SELECT lap, suggestion, raw_response FROM pit_decisions LIMIT 5;`. If `raw_response` is empty across the board, check that your org can use Confluent-hosted models: `DESCRIBE MODEL llm_textgen_model;` should show `'provider' = 'confluent'`.
- **`No Agentic Pit Decisions credential card`.** Run `uv run deploy` again.

</details>

---

**← Back to Overview**: [Main README](../../README.md)
