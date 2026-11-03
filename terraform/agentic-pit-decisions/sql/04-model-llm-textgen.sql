-- Claude hosted in Confluent (Flink Native Inference), the model pit_strategy_agent runs on,
-- with no connection or key to manage.
CREATE MODEL IF NOT EXISTS llm_textgen_model
INPUT (prompt STRING)
OUTPUT (response STRING)
WITH (
  'provider' = 'confluent',
  'task' = 'text_generation',
  'confluent.model' = 'anthropic.claude-sonnet-4-6',
  'confluent.params.max_tokens' = '50000'
);
