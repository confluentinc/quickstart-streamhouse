"""Catalog color updates must preserve definitions and verify persistence."""

from unittest.mock import Mock

import pytest

from scripts import deploy, topic_tags


def response(data, status=200):
    return Mock(ok=status < 400, status_code=status, json=Mock(return_value=data))


def test_update_preserves_definition_and_retries_readback(monkeypatch):
    monkeypatch.setattr(topic_tags.time, "sleep", lambda _: None)
    tag = {"name": "PII", "description": "Synthetic", "entityTypes": ["cf_entity"],
           "attributeDefs": [], "superTypes": [], "color": "DEFAULT_COLOR"}
    session = Mock()
    session.request.side_effect = [response(tag), response([{"name": "PII"}]),
                                   response(tag), response({**tag, "color": "MAGENTA_LIGHT"})]
    assert topic_tags.update_tag_color(session, "https://catalog", "PII", "MAGENTA_LIGHT")
    payload = session.request.call_args_list[1].kwargs["json"][0]
    assert payload == {**tag, "color": "MAGENTA_LIGHT"}


def test_matching_color_does_not_write():
    session = Mock()
    session.request.return_value = response({"color": "GREEN_LIGHT"})
    assert not topic_tags.update_tag_color(session, "https://catalog", "DATA_PRODUCT", "GREEN_LIGHT")
    assert session.request.call_count == 1


def test_bulk_error_with_http_200_is_not_success():
    session = Mock()
    session.request.side_effect = [response({"name": "PII"}), response([{"error": {"code": 400}}])]
    with pytest.raises(RuntimeError, match="rejected"):
        topic_tags.update_tag_color(session, "https://catalog", "PII", "MAGENTA_LIGHT")


def test_color_that_does_not_persist_fails(monkeypatch):
    monkeypatch.setattr(topic_tags.time, "sleep", lambda _: None)
    session = Mock()
    session.request.side_effect = [response({"name": "PII"}), response([{}])] + [response({})] * 5
    with pytest.raises(RuntimeError, match="verification failed"):
        topic_tags.update_tag_color(session, "https://catalog", "PII", "MAGENTA_LIGHT")


def test_rate_limit_is_retried(monkeypatch):
    monkeypatch.setattr(topic_tags.time, "sleep", lambda _: None)
    session = Mock()
    session.request.side_effect = [response({}, 429), response({"color": "GREEN_LIGHT"})]
    assert not topic_tags.update_tag_color(session, "https://catalog", "DATA_PRODUCT", "GREEN_LIGHT")


def test_deploy_applies_colors_after_agent(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(deploy, "_run_datagen", lambda *a, **kw: calls.append("data"))
    monkeypatch.setattr(deploy.setup_rtce, "main", lambda _: calls.append("rtce"))
    monkeypatch.setattr(deploy, "_start_agent", lambda _: calls.append("agent"))
    monkeypatch.setattr(deploy, "apply_tag_colors", lambda _: calls.append("colors"))
    monkeypatch.setattr(deploy, "_print_env_name", lambda _: None)
    deploy._finish(tmp_path, "codex")
    assert calls == ["data", "rtce", "agent", "colors"]
