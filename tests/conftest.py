import pytest

from scripts import processes


@pytest.fixture(autouse=True)
def bare_program_names(request, monkeypatch):
    """Tests compare argv with bare names ("aws", "claude"); a .cmd shim on the test machine must not change them.

    test_processes.py checks the real lookup.
    """
    if request.module.__name__.rsplit(".", 1)[-1] != "test_processes":
        monkeypatch.setattr(processes, "exe", lambda name: name)
