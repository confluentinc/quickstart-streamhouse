# Change Log

## Unreleased (`dev`)

### Features and Fixes

- Initial demo: Terraform for Confluent Cloud, Flink SQL pipeline, seeded airline data generator, RTCE/Lightning Tables setup, operations + passenger app, and Tableflow walkthrough.
- Self-service workshop ([docs/workshop.md](./docs/workshop.md)): `uv run deploy --workshop` deploys only the source feeds, and attendees build `passenger_state`, `flight_impact`, and a recovery agent on a Confluent-hosted model, turning on Lightning Tables and RTCE by hand. `uv run setup-rtce` adds `--agent-connection` and `--lightning … --run`. The generator names a passenger to follow, prints when RA417's passengers turn HIGH and when the Grand Hyatt sells out, and picks a stopped stream back up with `--resume`. The app keeps the result of Select or Book, such as the sold-out swap, in its status line for 10 seconds.
- The recovery agent runs on Claude hosted in Confluent (`sql/32`) instead of Amazon Bedrock, so `uv run deploy` starts it without AWS credentials, which now serve only Tableflow. `uv run reset-demo-2` drops the agent's model with the rest of Demo 2 and leaves a workshop checkout alone. A deployment from before keeps its Bedrock model until `uv run reset-demo-2 --full`.
