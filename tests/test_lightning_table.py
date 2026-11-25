import io
import json

from scripts import lightning_table

REPLY = {
    "result": {
        "schema": {"columns": [{"name": "LAP"}, {"name": "tire_compound"}]},
        "data": [[23, "SOFT"], [24, "MEDIUM"]],
    }
}


def _run(monkeypatch, capsys, text: str, width: int = 120) -> str:
    monkeypatch.setattr(lightning_table.sys, "stdin", io.StringIO(text))
    monkeypatch.setattr(lightning_table.shutil, "get_terminal_size", lambda *a: (width, 24))
    lightning_table.main()
    return capsys.readouterr().out


def test_rows_print_as_an_aligned_table(monkeypatch, capsys):
    out = _run(monkeypatch, capsys, json.dumps(REPLY)).splitlines()
    assert out[0].split() == ["lap", "tire_compound"]
    assert set(out[1]) <= {"-", " "}
    assert out[2].split() == ["23", "SOFT"] and out[3].split() == ["24", "MEDIUM"]
    assert out[-1] == "2 row(s)"
    assert out[2].index("SOFT") == out[0].index("tire_compound")  # columns line up


def test_a_table_wider_than_the_terminal_prints_one_field_per_line(monkeypatch, capsys):
    reply = {"result": {"schema": {"columns": [{"name": "lap"}, {"name": "reasoning"}]}, "data": [[24, "x" * 200]]}}
    out = _run(monkeypatch, capsys, json.dumps(reply), width=80)
    assert "lap       : 24" in out and "reasoning : " + "x" * 200 in out  # nothing is cut off


def test_powershell_json_with_a_byte_order_mark_still_parses(monkeypatch, capsys):
    out = _run(monkeypatch, capsys, "﻿" + json.dumps(REPLY))
    assert "MEDIUM" in out


def test_a_reply_that_isnt_rows_is_printed_as_is(monkeypatch, capsys):
    error = '{"error_code": 404, "message": "Table not found"}'
    assert "Table not found" in _run(monkeypatch, capsys, error)
    assert "not json" in _run(monkeypatch, capsys, "not json")


def test_no_rows(monkeypatch, capsys):
    reply = {"result": {"schema": {"columns": [{"name": "lap"}]}, "data": []}}
    assert _run(monkeypatch, capsys, json.dumps(reply)).strip().endswith("0 row(s)")
