# Always-on demo: Amazon Quick over the Tableflow history

## Goal

`uv run deploy --always-on` leaves the operator able to open Amazon Quick and ask scene 3's questions in plain English
(Appendix E of [demo-script-screens.md](../../flight-disruptions/demo-script-screens.md)), with no console setup.

This extends [the always-on design](./2026-11-12-continuous-demo-on-aws-design.md), which left Athena workgroups and
Quick setup out of scope.

## What deploy builds

Already built today (unchanged): Tableflow on `flight_impact` and `passenger_recommendations` to Iceberg in
`<name>-analytics`, synced to the Glue database named after the Kafka cluster ID (`terraform/airline-demo/analytics.tf`).

New, in `terraform/always-on/quick.tf`:

- an S3 bucket `<name>-athena-results` and an Athena workgroup `<name>-analytics` that writes query results there;
- an IAM role `<name>-quick-athena` that Quick assumes (set as the data source's own `role_arn`, so the account's Quick
  service role is never changed): Athena in that workgroup, read on Glue and the analytics bucket, read and write on
  the results bucket;
- a Quick Athena data source and two direct-query datasets, `flight_impact` (with an `is_delayed` field,
  `delay_minutes >= 15`) and `passenger_recommendations` (with `recovery_seconds`, `recommended_at - impacted_at`);
- a Quick Topic over both datasets with friendly names and synonyms, created with the AWS CLI because the AWS provider
  has no Topic resource;
- everything shared with the Quick users whose email is in `DEMO_USER_EMAILS`. With none, Quick is skipped and the rest
  of the always-on deploy is unchanged.

Fix in `terraform/airline-demo/analytics.tf`: wait 30 s after the Tableflow IAM role and its policies exist before
enabling Tableflow and the Glue integration. The 2026-11-12 deployment (`river-air-demo`) failed both topics with
"Unable to assume the IAM role" and never retried, though the role was correct.

## Constraints

- Never create, change, or destroy the Quick account subscription: it is shared by the whole AWS account.
- Quick users live in the identity region `us-east-1` (`QUICK_IDENTITY_REGION`). Quick resources live in core's region.
- Lake Formation in this account is IAM-only (`IAM_ALLOWED_PRINCIPALS`), so IAM policies are enough.
- Deploy waits for both Glue tables before creating the Quick datasets, and says why when Tableflow failed.
- A new deployment must use its own `TF_VAR_deployment_name`; the first one is `river-air-enablement` in `us-east-1`.
  The existing `river-air-demo` deployment (another checkout) is not touched.

## Out of scope

- Quick users for colleagues (each is a paid seat; add their email to `DEMO_USER_EMAILS` once they have one).
- SPICE, refresh schedules, dashboards.
- Quick in the scripted demo (`--with-labs`).
