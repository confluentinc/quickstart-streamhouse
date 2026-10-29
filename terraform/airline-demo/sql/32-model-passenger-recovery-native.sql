-- Claude hosted in Confluent (Flink Native Inference), the model the recovery agent in sql/30 runs
-- on, with no connection or key to manage. IF NOT EXISTS keeps an existing model, so a deployment
-- from before the agent left Bedrock keeps that model under this name until `uv run reset-demo-2`
-- drops it.
CREATE MODEL IF NOT EXISTS passenger_recovery_model
INPUT (prompt STRING)
OUTPUT (response STRING)
WITH (
  'provider' = 'confluent',
  'task' = 'text_generation',
  'confluent.model' = 'anthropic.claude-sonnet-4-6',
  'confluent.input_format' = 'confluent-chat',
  'confluent.params.max_tokens' = '1024'
);
