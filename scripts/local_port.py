"""Fail fast when a lab server's port is taken.

A server left running from an earlier deployment keeps answering on the port with
stale data, so a new `uv run airport-app` or `uv run f1-pitwall` would lose the bind
while the browser still shows the old page. Checking first names the old process.
"""

from __future__ import annotations

import errno
import socket

from scripts import processes


def _listeners(port: int) -> list[tuple[str, str]]:
    """(PID, command line) of each process listening on the TCP port, when this computer can tell."""
    pids = processes.listening_pids(port) or []
    return [(str(pid), processes.command_line(pid) or "") for pid in pids]


def ensure_port_free(host: str, port: int, server: str) -> None:
    """Exit with the PID and command holding host:port, before the server starts in front of it."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        if processes.WINDOWS:
            # Windows' SO_REUSEADDR lets a socket share a port another one listens on, and Windows refuses the
            # probe with WSAEACCES instead of EADDRINUSE. An exclusive bind fails with EADDRINUSE as it should.
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:
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
        stop = processes.stop_command([int(pid) for pid, _ in holders])
        lines.append(f"Stop it with `{stop}`, then run this again.")
    lines.append("Or choose another port with --port.")
    raise SystemExit("\n".join(lines))
