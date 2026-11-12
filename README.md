# Streamhouse Quickstart

*Your business, always current.*

A **Streamhouse** is a data architecture that continuously captures, processes, governs, and serves the current state of the business to applications, AI agents, and analytics, so they all work from the same fresh, trusted source of truth. It sits alongside your warehouse or lakehouse, not in place of it.

- **Always current:** data updates as business events happen, not on a batch schedule.
- **Mission critical:** built to production service levels, because the business depends on it.
- **Decentralized:** the same trusted context, in whatever shape each consumer needs, wherever it's needed.

Build one on Confluent Cloud in either of these hands-on labs:

<table>
<tr>
<th width="25%">Lab</th>
<th width="75%">Description</th>
</tr>
<tr>
<td><a href="./docs/flight-disruptions/workshop.md"><strong>Agentic Airline Operations</strong></a></td>
<td>Rebook passengers in real time when a delay breaks their connections. A streaming agent recovers three delayed flights (RA417, RA183, and RA185) while hotel rooms sell out. <a href="https://www.youtube.com/watch?v=4pzIwuhAG2s">Watch the demo video</a>.<br><br><img src="./docs/images/flight-disruptions/architecture.png" alt="Flight Disruptions architecture diagram"></td>
</tr>
<tr>
<td><a href="./docs/agentic-pit-decisions/walkthrough.md"><strong>F1 Agentic Pit Decisions</strong></a></td>
<td>Call every F1 pit stop with anomaly detection and a streaming agent.<br><br><img src="./docs/images/agentic-pit-decisions/architecture.png" alt="Agentic Pit Decisions architecture diagram"></td>
</tr>
</table>

## Prerequisites

1. A [Confluent Cloud](https://cnfl.io/learnwithcflt) account with a payment method, so Terraform can create your cluster. Add one under **[Billing & payment](https://confluent.cloud/settings/billing/payment)** → **Payment details and contacts** → **Add credit card / bank account**. Free credit is used first.
2. [Git](https://git-scm.com), [uv](https://github.com/astral-sh/uv), [Terraform](https://github.com/hashicorp/terraform), and the [Confluent CLI](https://docs.confluent.io/confluent-cli/current/overview.html).

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

    In PowerShell:

    ```powershell
    winget install --id Git.Git -e
    winget install --id astral-sh.uv -e
    winget install --id Hashicorp.Terraform -e
    ```

    Then download the latest `confluent_*_windows_amd64.zip` from the [Confluent CLI releases](https://github.com/confluentinc/cli/releases/latest), unzip it, add the folder that contains `confluent.exe` to your `PATH`, and reopen the terminal.

    </details>

## Quick start

```bash
git clone https://github.com/confluentinc/quickstart-streamhouse.git
cd quickstart-streamhouse
```

**SSO users only:** sign in to the Confluent CLI.

> [!TIP]
>
> **Signed up with an email and password?** Skip this step. Deploy logs you in to the CLI with them.

Deploy can't log in with single sign-on (SSO), so SSO users sign in to the Confluent CLI first. Enter your email when it asks, then finish signing in in the browser window it opens:

```bash
confluent login
```

Deploy, and pick a lab (or both) when it asks:

```bash
uv run deploy
```

Then follow its guide: [Agentic Airline Operations](./docs/flight-disruptions/workshop.md) or [F1 Agentic Pit Decisions](./docs/agentic-pit-decisions/walkthrough.md).

> [!TIP]
>
> **Presenting a demo?** Run `uv run deploy --with-labs` to get everything prebuilt.

## Clean up

Tear down everything deploy created (the environment, cluster, Flink pool, and both labs) and answer `y`:

```bash
uv run destroy
```

<details>
<summary>Remove only one lab</summary>

These keep the environment and the other lab.

| To remove | Run |
|---|---|
| Only Flight Disruptions | `uv run destroy --lab flight-disruptions` |
| Only Agentic Pit Decisions | `uv run destroy --lab agentic-pit-decisions` |

</details>
