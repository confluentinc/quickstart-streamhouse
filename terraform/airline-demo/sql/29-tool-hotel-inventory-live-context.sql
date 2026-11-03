-- The agent's live context: the Real-Time Context Engine MCP server. `uv run deploy`
-- creates 'rtce-connection' with the Confluent CLI (streamable HTTP + the Global key), so
-- no secret appears in this statement.
CREATE TOOL IF NOT EXISTS hotel_inventory_live_context
USING CONNECTION `rtce-connection`
WITH (
  'type' = 'mcp',
  'allowed_tools' = 'getMetadata,queryData',
  'request_timeout' = '30'
);
