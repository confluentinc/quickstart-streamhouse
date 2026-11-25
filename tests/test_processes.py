"""Process, executable, and encoding helpers that the labs rely on, on macOS, Linux, and Windows alike."""

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from scripts import local_port, mcp_setup, processes, setup_rtce
from scripts.pit_decisions import reset

ROOT = Path(__file__).parent.parent
SLEEPER = [sys.executable, "-c", "import time; time.sleep(60)", "airport-datagen-test-marker"]


@pytest.fixture
def sleeper():
    child = subprocess.Popen(SLEEPER)
    yield child
    if child.poll() is None:
        child.kill()
    child.wait()


def _wait_for_exit(child, seconds=10):
    deadline = time.monotonic() + seconds
    while child.poll() is None and time.monotonic() < deadline:
        time.sleep(0.1)
    return child.poll() is not None


def test_a_running_process_is_alive_and_a_finished_one_is_not(sleeper):
    assert processes.pid_alive(sleeper.pid)
    sleeper.kill()
    sleeper.wait()
    assert not processes.pid_alive(sleeper.pid)
    assert not processes.pid_alive(0)


def test_checking_a_process_leaves_it_running(sleeper):
    """os.kill(pid, 0) is not a liveness check on Windows (it sends a console Ctrl+C event); pid_alive is."""
    for _ in range(3):
        assert processes.pid_alive(sleeper.pid)
    time.sleep(0.5)
    assert sleeper.poll() is None


def test_command_line_names_the_program(sleeper):
    deadline = time.monotonic() + 10
    command = processes.command_line(sleeper.pid)
    while not command and time.monotonic() < deadline:  # ps may not see a process that just started
        time.sleep(0.2)
        command = processes.command_line(sleeper.pid)
    assert command is not None and "airport-datagen-test-marker" in command


def test_stop_ends_only_a_process_whose_command_matches(sleeper):
    assert not processes.stop(sleeper.pid, "f1-race")  # a PID reused by some other program is left alone
    assert sleeper.poll() is None
    assert processes.stop(sleeper.pid, "airport-datagen")
    assert _wait_for_exit(sleeper)


def test_stop_never_ends_this_process():
    assert not processes.stop(os.getpid(), "python", "pytest")


def test_stop_previous_stream_stops_the_stream_in_the_pid_file(tmp_path, sleeper):
    from scripts import airport_datagen

    pid_file = tmp_path / "tmp" / "datagen.pid"
    pid_file.parent.mkdir()
    pid_file.write_text(str(sleeper.pid), encoding="utf-8")
    airport_datagen.stop_previous_stream(tmp_path)
    assert _wait_for_exit(sleeper)
    assert not pid_file.exists()


def test_detached_start_has_its_own_session_or_console():
    kwargs = processes.detached()
    if sys.platform == "win32":
        flags = kwargs["creationflags"]
        assert flags & subprocess.CREATE_NEW_PROCESS_GROUP and flags & subprocess.CREATE_NO_WINDOW
        assert not flags & subprocess.DETACHED_PROCESS  # would open a window for each console child
    else:
        assert kwargs == {"start_new_session": True}


def test_detached_child_can_still_write_its_log(tmp_path):
    log = tmp_path / "datagen.log"
    with open(log, "a", encoding="utf-8") as out:
        child = subprocess.Popen(
            [sys.executable, "-c", "import scripts; print('\\u2713 streaming')"],
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
            env={k: v for k, v in os.environ.items() if k not in ("PYTHONIOENCODING", "PYTHONUTF8")},
            **processes.detached(),
        )
        assert child.wait(timeout=60) == 0
    assert "✓ streaming" in log.read_text(encoding="utf-8")


def test_redirected_output_is_utf8_without_pythonioencoding(tmp_path):
    """Windows writes a redirected stream in cp1252, which has no ✓; importing scripts switches it to UTF-8."""
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONIOENCODING", "PYTHONUTF8")}
    result = subprocess.run(
        [sys.executable, "-c", "import scripts, sys; print('\\u2713 \\u2192 done'); print('\\u26a0', file=sys.stderr)"],
        cwd=ROOT,
        capture_output=True,
        env=env,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.decode("utf-8").strip() == "✓ → done"
    assert result.stderr.decode("utf-8").strip() == "⚠"


def test_exe_keeps_bare_names_for_real_programs(monkeypatch):
    monkeypatch.setattr(processes.shutil, "which", lambda name: rf"C:\tools\{name}.EXE")
    monkeypatch.setattr(processes, "WINDOWS", True)
    assert processes.exe("terraform") == "terraform"


def test_exe_gives_a_cmd_shim_its_full_path_on_windows(monkeypatch):
    monkeypatch.setattr(processes.shutil, "which", lambda name: rf"C:\Program Files\nodejs\{name}.cmd")
    monkeypatch.setattr(processes, "WINDOWS", True)
    assert processes.exe("npx") == r"C:\Program Files\nodejs\npx.cmd"
    monkeypatch.setattr(processes, "WINDOWS", False)
    assert processes.exe("npx") == "npx"


def test_exe_keeps_the_name_of_a_missing_program(monkeypatch):
    monkeypatch.setattr(processes.shutil, "which", lambda name: None)
    monkeypatch.setattr(processes, "WINDOWS", True)
    assert processes.exe("claude") == "claude"  # so the caller still gets FileNotFoundError and its fallback


@pytest.mark.skipif(sys.platform != "win32", reason="CreateProcess can't start a .cmd by name only on Windows")
def test_a_cmd_shim_starts_through_exe(tmp_path, monkeypatch):
    shim = tmp_path / "lab-shim-test.cmd"
    shim.write_text("@echo shim ran %1\r\n", encoding="utf-8")
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    with pytest.raises(FileNotFoundError):
        subprocess.run(["lab-shim-test", "ok"], capture_output=True)
    result = subprocess.run([processes.exe("lab-shim-test"), "ok"], capture_output=True, encoding="utf-8")
    assert result.stdout.strip() == "shim ran ok"


@pytest.mark.parametrize(
    "command",
    [
        "/repo/.venv/bin/python /repo/.venv/bin/f1-race --laps 5",
        r'"C:\Users\me\repo\.venv\Scripts\python.exe" "C:\Users\me\repo\.venv\Scripts\f1-race.exe" --laps 5',
        r'"C:\Users\me\AppData\Local\Microsoft\WinGet\Packages\uv\uv.exe" run f1-race',
        "python -m scripts.pit_decisions.race",
    ],
)
def test_a_local_race_is_found_whatever_the_path_style(command):
    assert reset.is_local_race(command)


@pytest.mark.parametrize("command", [r"C:\repo\.venv\Scripts\f1-pitwall.exe", "/usr/bin/vim f1-race-notes.txt"])
def test_other_programs_are_not_a_local_race(command):
    assert not reset.is_local_race(command)


def test_stop_command_suits_the_shell(monkeypatch):
    monkeypatch.setattr(processes, "WINDOWS", True)
    assert processes.stop_command([4242]) == "taskkill /F /PID 4242"
    assert processes.stop_command([1, 2]) == "taskkill /F /PID 1 /PID 2"
    monkeypatch.setattr(processes, "WINDOWS", False)
    assert processes.stop_command([1, 2]) == "kill 1 2"


def test_netstat_listeners_are_read_without_the_state_column(monkeypatch):
    """The LISTENING column is translated on non-English Windows; a listener's remote end of 0.0.0.0:0 isn't."""
    output = "\n".join(
        [
            "  Proto  Local Address          Foreign Address        State           PID",
            "  TCP    127.0.0.1:8000         0.0.0.0:0              ABHÖREN         4242",
            "  TCP    127.0.0.1:8000         127.0.0.1:50123        HERGESTELLT     4242",
            "  TCP    [::]:8001              [::]:0                 ABHÖREN         77",
            "  TCP    127.0.0.1:18000        0.0.0.0:0              ABHÖREN         9",
        ]
    )
    monkeypatch.setattr(processes.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0, output, ""))
    assert processes._windows_listening_pids(8000) == [4242]
    assert processes._windows_listening_pids(8001) == [77]


def test_a_taken_port_is_detected_on_this_computer():
    """Windows' SO_REUSEADDR lets a probe share a listening port; the check must still see it taken."""
    import socket

    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        port = server.getsockname()[1]
        with pytest.raises(SystemExit, match=f"Port {port} is already in use"):
            local_port.ensure_port_free("127.0.0.1", port, "airport-app")


def test_claude_project_key_uses_forward_slashes():
    assert mcp_setup.claude_project_key(Path("repo") / "sub") == "repo/sub"


def test_mcp_server_starts_npx_through_cmd_on_windows(monkeypatch):
    monkeypatch.setattr(processes, "WINDOWS", True)
    assert mcp_setup._mcp_command("npx") == ("cmd", ["/c", "npx", "-y", "@confluentinc/mcp-confluent"])
    monkeypatch.setattr(processes, "WINDOWS", False)
    assert mcp_setup._mcp_command("/opt/node/bin/npx") == ("/opt/node/bin/npx", ["-y", "@confluentinc/mcp-confluent"])


def test_node_beside_npx_has_the_platforms_name(monkeypatch):
    monkeypatch.setattr(processes, "WINDOWS", True)
    assert mcp_setup._node_beside(r"C:\nodejs\npx.cmd").endswith("node.exe")
    monkeypatch.setattr(processes, "WINDOWS", False)
    assert mcp_setup._node_beside("/opt/node/bin/npx") == str(Path("/opt/node/bin/node"))


def test_lightning_command_for_powershell_quotes_the_sql(monkeypatch):
    monkeypatch.setattr(setup_rtce.sys, "platform", "win32")
    infra = {"region": "us-east-1", "env_id": "env-1", "cluster_id": "lkc-1", "org_id": "o"}
    command = setup_rtce.lightning_command(infra, "key", "secret", "passenger_state", key="P-1")
    assert command.startswith("Invoke-RestMethod -Method Post -Uri 'https://")
    assert "curl" not in command
    assert "'P-1''" in command  # the SQL's 'P-1' inside a single-quoted PowerShell string
    assert command.endswith("| ConvertTo-Json -Depth 10")


def test_text_files_are_checked_out_with_lf():
    """.gitattributes pins LF, which Terraform's file() sends to Flink byte for byte."""
    for path in [*(ROOT / "terraform" / "airline-demo" / "sql").glob("*.sql"), ROOT / "terraform/airline-demo/main.tf"]:
        assert b"\r\n" not in path.read_bytes(), path


def test_claude_md_is_a_file_that_imports_agents_md():
    claude = ROOT / "CLAUDE.md"
    assert not claude.is_symlink()
    assert claude.read_text(encoding="utf-8").strip() == "@AGENTS.md"


def test_setup_mcp_accepts_only_node_versions_with_a_prebuilt_kafka_client():
    """mcp-confluent needs Node >= 22.23.4, and kafka-javascript has prebuilt binaries through Node 24 (ABI 137)."""
    assert mcp_setup._KAFKA_JS_PREBUILT_ABIS == {127, 131, 137}  # Node 22, 23, 24
    assert 147 not in mcp_setup._KAFKA_JS_PREBUILT_ABIS  # Node 26, winget's OpenJS.NodeJS.LTS


def test_windows_node_search_includes_program_files(monkeypatch, tmp_path):
    monkeypatch.setattr(processes, "WINDOWS", True)
    monkeypatch.setenv("ProgramFiles", str(tmp_path))
    monkeypatch.delenv("NVM_SYMLINK", raising=False)
    monkeypatch.setenv("APPDATA", str(tmp_path / "appdata"))
    assert mcp_setup._candidate_npx_paths()[0] == tmp_path / "nodejs" / "npx.cmd"


def test_terraform_env_restores_tf_var_names_on_windows(monkeypatch):
    monkeypatch.setattr(processes, "WINDOWS", True)
    env = processes.terraform_env({"TF_VAR_WORKSHOP": "true", "TF_VAR_ENABLE_RECOVERY_AGENT": "true", "PATH": "p"})
    assert env == {"TF_VAR_workshop": "true", "TF_VAR_enable_recovery_agent": "true", "PATH": "p"}
    monkeypatch.setattr(processes, "WINDOWS", False)
    assert processes.terraform_env({"TF_VAR_workshop": "true"}) == {"TF_VAR_workshop": "true"}


def test_every_terraform_variable_is_lower_case():
    """terraform_env lower-cases TF_VAR_ names on Windows, which is right only while every declared name is."""
    import re

    names = {
        m for p in ROOT.glob("terraform/*/*.tf") for m in re.findall(r'^variable "([^"]+)"', p.read_text("utf-8"), re.M)
    }
    assert names and all(name == name.lower() for name in names)


@pytest.mark.skipif(not processes.shutil.which("terraform"), reason="needs terraform on PATH")
def test_a_variable_set_from_python_reaches_terraform(tmp_path, monkeypatch):
    """deploy sets TF_VAR_workshop and TF_VAR_enable_recovery_agent in os.environ, then runs Terraform."""
    (tmp_path / "main.tf").write_text(
        'variable "workshop" {\n  type    = bool\n  default = false\n}\n'
        'output "workshop" {\n  value = var.workshop\n}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("TF_VAR_workshop", "true")
    monkeypatch.setenv("TF_IN_AUTOMATION", "1")
    from scripts.terraform_runner import run_terraform

    monkeypatch.setattr("scripts.terraform_runner._generate_deployment_summary", lambda env_path: None)
    assert run_terraform(tmp_path)
    shown = subprocess.run(
        ["terraform", "output", "-raw", "workshop"], cwd=tmp_path, capture_output=True, encoding="utf-8"
    )
    assert shown.stdout == "true"
