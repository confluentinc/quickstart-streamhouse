import socket

import pytest

from scripts import local_port


def _listening_socket() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    return sock


def test_free_port_passes():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    local_port.ensure_port_free("127.0.0.1", port, "airport-app")


def test_taken_port_names_the_old_server_and_how_to_stop_it(monkeypatch):
    command = "/repo/.venv/bin/python /repo/.venv/bin/f1-pitwall"
    monkeypatch.setattr(local_port, "_listeners", lambda port: [("4242", command)])
    with _listening_socket() as old:
        with pytest.raises(SystemExit) as error:
            local_port.ensure_port_free("127.0.0.1", old.getsockname()[1], "f1-pitwall")
    message = str(error.value)
    assert f"PID 4242: {command}" in message
    assert "left running from an earlier run" in message
    assert "kill 4242" in message


def test_listeners_finds_this_process():
    with _listening_socket() as sock:
        holders = local_port._listeners(sock.getsockname()[1])
    if not holders:
        pytest.skip("lsof is not available")
    assert holders[0][1]
