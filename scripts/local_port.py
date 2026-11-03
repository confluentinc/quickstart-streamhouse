"""Fail fast when a lab server's port is taken.

A server left running from an earlier deployment keeps answering on the port with
stale data, so a new `uv run airport-app` or `uv run f1-pitwall` would lose the bind
while the browser still shows the old page. Checking first names the old process.
"""

from __future__ import annotations

import errno
import socket
import subprocess


def _listeners(port: int) -> list[tuple[str, str]]:
    """(PID, command line) of each process listening on the TCP port, when lsof can tell."""

    def run(*command: str) -> str:
        return subprocess.run(command, capture_output=True, text=True, timeout=5).stdout

    try:
        pids = dict.fromkeys(run("lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t").split())
        return [(pid, run("ps", "-o", "command=", "-p", pid).strip()) for pid in pids]
    except (OSError, subprocess.SubprocessError):
        return []


def ensure_port_free(host: str, port: int, server: str) -> None:
    """Exit with the PID and command holding host:port, before the server starts in front of it."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # as uvicorn does, so TIME_WAIT doesn't count
        try:
            sock.bind((host, port))
            return
        except OSError as exc:
            if exc.errno != errno.EADDRINUSE:
                return  # let the server report anything other than a taken port
    lines = [f"Port {port} is already in use."]
    holders = _listeners(port)
    for pid, command in holders:
        lines.append(f"  PID {pid}: {command or '(command unknown)'}")
    if any(server in command for _, command in holders):
        lines.append(f"That is a {server} left running from an earlier run; it serves stale data.")
    if holders:
        lines.append(f"Stop it with `kill {' '.join(pid for pid, _ in holders)}`, then run this again.")
    lines.append("Or choose another port with --port.")
    raise SystemExit("\n".join(lines))
