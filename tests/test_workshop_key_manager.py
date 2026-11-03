"""The workshop AWS key's IAM policy covers the names terraform gives the Tableflow resources."""

from fnmatch import fnmatchcase

from scripts import workshop_key_manager as wkm


def _resources(policy: dict, sid: str) -> str:
    return next(s["Resource"] for s in policy["Statement"] if s["Sid"] == sid)


def _covers(tmp_path, creds: str, bucket: str, role: str) -> bool:
    (tmp_path / "credentials.env").write_text(creds)
    policy = wkm.get_demo_policy("123", "us-east-1", wkm.get_resource_scope(tmp_path))
    return (
        fnmatchcase(f"arn:aws:s3:::{bucket}", _resources(policy, "TableflowBucket"))
        and fnmatchcase(f"arn:aws:iam::123:role/{role}", _resources(policy, "TableflowIamRole"))
        and fnmatchcase(f"arn:aws:iam::123:policy/{role}-s3-access", _resources(policy, "TableflowIamPolicy"))
    )


def test_fixed_deployment_name_is_scoped_exactly(tmp_path):
    creds = "TF_VAR_resource_prefix=flight-recovery\nTF_VAR_deployment_name=RIVER-AIR-PROD\n"
    assert _covers(tmp_path, creds, "river-air-prod-analytics", "river-air-prod-tableflow-glue")
    assert not _covers(tmp_path, creds, "flight-recovery-1a2b3c4d-analytics", "flight-recovery-1a2b3c4d-tableflow-glue")


def test_random_suffix_is_a_wildcard(tmp_path):
    creds = "TF_VAR_resource_prefix=flight-recovery\n"
    assert _covers(tmp_path, creds, "flight-recovery-1a2b3c4d-analytics", "flight-recovery-1a2b3c4d-tableflow-glue")
    assert not _covers(tmp_path, creds, "river-air-prod-analytics", "river-air-prod-tableflow-glue")
