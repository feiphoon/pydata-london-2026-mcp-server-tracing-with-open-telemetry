# 8: Wrap Up & Q&A

## What we did today

In ~90 minutes we went from *"Tom Nook is charging 12,000,000+ Bells
and the dashboard says HTTP 200"* to a fully-observable agent stack:

| Artefact                                                  | Where it lives                                        | What it answers                                      |
|-----------------------------------------------------------|-------------------------------------------------------|------------------------------------------------------|
| **`nook-mcp`** — FastMCP server with Kafka + TANUKI pricing tools | `mcp/src/nook_mcp/`                            | Every Kafka read/write Tom Nook can make             |
| Three-file split (server / tools / kafka_client)          | `server.py`, `tools.py`, `kafka_client.py`            | Each file → its own span layer; clean separation     |
| **`telemetry.py`** — global TracerProvider, OTLP HTTP      | `mcp/src/nook_mcp/telemetry.py` (~75 lines)           | Where every span exits `nook-mcp`           |
| Manual spans following **GenAI + MCP + messaging semconv** | `tools.py`, `kafka_client.py`                         | The four-layer waterfall, in three dialects          |
| **Hybrid sampling + redaction** in the Collector           | `observability/otel-collector-config.yaml`            | Production-shape pipeline (PII out, expensive in)    |
| **OAuth bearer-token middleware**                 | `mcp/src/nook_mcp/auth.py`                            | The `enduser.*` column on every span                 |
| Four AC-themed Kafka topics seeded continuously            | `seed/topics.py`, `seed/producer.py`                  | Realistic asymmetric volumes — incl. PII-bearing one |

- Everything you wrote is in the workshop repo and licensed for use at work

----

## The Animal Crossing answer

Did we solve the case? The question Isabelle needs is:

```text
COUNT of  tool quote_upgrade  WHERE  nook.quote.bells > 12000000
GROUP BY enduser.id
```

That `>` plus `GROUP BY` is an **aggregation**, so it runs in the
metrics/query layer. It is **not** a Jaeger query: Jaeger's
Tags search is exact-equality only (no ranges, no `GROUP BY`). Jaeger is
where you then **drill into** a single offender — filter
`enduser.id=tom_nook@nookinc.island` and read one runaway trace end to
end.

*Tom Nook's own account is the `enduser.id`
behind ~95% of the expensive quotes; `nook.quote.abd_multiplier` is the
dominant driver; and each expensive quote burns only ~£0.00004 of
self-hosted Qwen tokens.*

----

## Five takeaways for Monday morning

1. **Distributed tracing is the primary observability signal for
   agentic AI.** Metrics aggregate, logs grep, but only traces
   reconstruct *what this one agent did in this one request*
2. **The semantic conventions are the contract.** Pin to
   `gen_ai.*`, `mcp.*`, `messaging.*` and your dashboards survive
   every vendor, framework and model swap
3. **The Collector is where production lives.** Redaction,
   sampling, dimensional metrics, multi-backend fanout — all in one YAML file
5. **The traces *are* the audit trail.** EU AI Act Article 19,
   GDPR Articles 5/17/22, SOC 2 — all satisfied by a redacted,
   immutably-stored, `enduser.*`-attributed span tree

----

## Next steps and further reading

- [OpenTelemetry: AI Agent Observability](https://opentelemetry.io/blog/2025/ai-agent-observability/) —
  the post that anchored the workshop premise
- [OTel semantic conventions for MCP](https://opentelemetry.io/docs/specs/semconv/gen-ai/mcp/) —
  every span attribute we used, plus the ones we did not
- [OTel semantic conventions for GenAI](https://opentelemetry.io/docs/specs/semconv/gen-ai/) —
  the `gen_ai.*` reference
- [FastMCP telemetry docs](https://gofastmcp.com/servers/telemetry) —
  the framework side of what we wired in §4
- **Auto-instrumentation**: [OpenLLMetry](https://github.com/traceloop/openllmetry),
  [OpenInference](https://github.com/Arize-ai/openinference),
  [OpenLIT](https://github.com/openlit/openlit) — the §6 quick-survey
- **Workshop repo** (all code, slides, exercises, reference cards):
  `https://github.com/<org>/pydata-mcp-otel-workshop`

## Q&A

Common questions answered:

- *"Do I need Lenses HQ for the OAuth bit?"* No — any OAuth 2.1
  issuer with a token-introspection endpoint works. The workshop
  uses Lenses HQ because the CE base already runs it
- *"Does this work for `stdio` MCP servers?"* Yes — the
  `telemetry.py` setup is transport-agnostic. The auto-instrumented
  `tools/call` spans appear identically; only the auth layer differs
- *"Can I export to <vendor>?"* Yes. Swap the Collector's
  `exporters.otlp/jaeger` for `otlp/yourvendor`. Nothing in
  `nook-mcp` changes

----

## Handout A — Span Attribute Reference Card

The complete list of attributes on a `nook-mcp` span tree, by
layer. **Print this and stick it on your monitor.**

### Layer 1: MCP protocol (FastMCP auto-instrumentation)

| Attribute               | Type   | Example                                          |
|-------------------------|--------|--------------------------------------------------|
| `mcp.method.name`       | string | `tools/call`, `tools/list`, `initialize`         |
| `mcp.session.id`        | string | session UUID                                     |
| `fastmcp.component.type`| string | `tool`, `resource`, `prompt`                     |
| `fastmcp.component.key` | string | `quote_upgrade`, `peek_topic`, `publish_event`   |
| `rpc.system`            | string | `mcp`                                            |
| `rpc.service`           | string | `nook-mcp`                                       |
| `rpc.method`            | string | `tools/call`                                     |
| `error.type`            | string | exception class name (on failure)                |

### Layer 2: Application (`tools.py`)

Set by the `_tool_attrs` helper on every `tool <name>` span; the
business attributes are set by the tool body itself.

| Attribute                              | Type   | Set by tool          | Example         |
|----------------------------------------|--------|----------------------|-----------------|
| `mcp.method.name`                      | string | every tool           | `tools/call`    |
| `gen_ai.tool.name`                     | string | every tool           | `quote_upgrade` |
| `nook.topic`                           | string | peek/query/publish   | `abd-balance`   |
| `nook.peek.limit` / `.records_returned`| int    | `peek_topic`         | `10` / `10`     |
| `nook.query.limit` / `.records_returned`| int   | `query_topic`        | `50` / `42`     |
| `nook.query.key_filter` / `.since`     | string | `query_topic`        | `player`        |
| `nook.publish.partition` / `.offset`   | int    | `publish_event`      | `2` / `48211`   |
| `nook.room`                            | string | `quote_upgrade`      | `second-floor`  |
| `nook.quote.net_worth_base`            | int    | `quote_upgrade`      | `1_750_000`     |
| `nook.quote.abd_balance` / `.catch_total` | int | `quote_upgrade`      | `1_500_000` / `250_000` |
| `nook.quote.room_base`                 | int    | `quote_upgrade`      | `1_248_000`     |
| `nook.quote.bells`                     | int    | `quote_upgrade`      | `14_203_456`    |
| `nook.quote.abd_multiplier`            | float  | `quote_upgrade`      | `3.0`           |
| `nook.quote.catch_multiplier`          | float  | `quote_upgrade`      | `1.5`           |
| `nook.quote.luck_multiplier`           | float  | `quote_upgrade`      | `2.0`           |
| `nook.quote.plea_multiplier`           | float  | `quote_upgrade`      | `0.9`           |
| `nook.tree.type`                       | string | `shake_tree`         | `cedar`         |
| `nook.tree.drop` / `.drop_count`       | str/int| `shake_tree`         | `coconut` / `2` |

### Layer 3a: Messaging (`kafka_client.py`)

| Attribute                              | Type   | Example                         |
|----------------------------------------|--------|---------------------------------|
| `messaging.system`                     | string | `kafka`                         |
| `messaging.operation.name`             | string | `receive`, `publish`            |
| `messaging.destination.name`           | string | `abd-balance`                   |
| `messaging.batch.message_count`        | int    | `20`                            |
| `messaging.kafka.destination.partition`| int    | `2` (on publish)                |
| `messaging.kafka.message.offset`       | int    | `48211` (on publish)            |
| `messaging.kafka.message.key`          | string | hashed visitor name (post-§6)   |

### Layer 3b: GenAI (`_llm_reasoning`)

Example values are the workshop's self-hosted Qwen on Ollama; the
attribute *names* are vendor-neutral, so swapping in Anthropic/OpenAI
only changes the values (`gen_ai.provider.name=anthropic`,
`gen_ai.request.model=claude-sonnet-4-…`).

| Attribute                       | Type   | Example                          |
|---------------------------------|--------|----------------------------------|
| `gen_ai.provider.name`          | string | `ollama`                         |
| `gen_ai.operation.name`         | string | `chat`                           |
| `gen_ai.request.model`          | string | `qwen3.5:0.8b`                   |
| `gen_ai.response.model`         | string | `qwen3.5:0.8b`                   |
| `gen_ai.request.max_tokens`     | int    | `256`                            |
| `gen_ai.usage.input_tokens`     | int    | `320`                            |
| `gen_ai.usage.output_tokens`    | int    | `92`                             |

### Layer X: Auth (added in §7, on every layer-1+2 span)

Set automatically by FastMCP's `server_span` from the introspected
token (via `auth.py`'s `LensesHQTokenVerifier`) — no `tools.py` changes.

| Attribute               | Type   | Example                                |
|-------------------------|--------|----------------------------------------|
| `enduser.id`            | string | `tom_nook@nookinc.island` (hashed: SHA-256, 12 hex) |
| `enduser.scope`         | string | `read:topics write:home-upgrade-quotes`|

----

## References

### OpenTelemetry & semantic conventions

- [OpenTelemetry: AI Agent Observability](https://opentelemetry.io/blog/2025/ai-agent-observability/)
- [OTel semantic conventions for MCP](https://opentelemetry.io/docs/specs/semconv/gen-ai/mcp/)
- [OTel semantic conventions for GenAI](https://opentelemetry.io/docs/specs/semconv/gen-ai/)
- [OTel semantic conventions for AWS Bedrock](https://opentelemetry.io/docs/specs/semconv/gen-ai/aws-bedrock/)
- [OTel semantic conventions for messaging](https://opentelemetry.io/docs/specs/semconv/messaging/kafka/)

### FastMCP & MCP

- [FastMCP: OpenTelemetry instrumentation](https://gofastmcp.com/servers/telemetry)
- [Model Context Protocol specification](https://modelcontextprotocol.io/)
- [MCP authorization spec (OAuth 2.1)](https://modelcontextprotocol.io/specification/2025-06-18/basic/authorization)

### Production case studies

- [AWS: Bedrock AgentCore Observability](https://aws.amazon.com/blogs/machine-learning/build-trustworthy-ai-agents-with-amazon-bedrock-agentcore-observability/)
- [Grafana: Observing agentic AI workflows with OpenTelemetry and the OpenAI Agents SDK](https://grafana.com/blog/observing-agentic-ai-workflows-with-grafana-cloud-opentelemetry-and-the-openai-agents-sdk/)
- [Grafana: Monitor AI agents on Amazon Bedrock AgentCore with Grafana Cloud](https://grafana.com/blog/how-to-monitor-ai-agent-applications-on-amazon-bedrock-agentcore-with-grafana-cloud/)
- [Anthropic: Claude Code monitoring (OpenTelemetry)](https://docs.anthropic.com/en/docs/claude-code/monitoring-usage)

### Auto-instrumentation projects

- [OpenLLMetry (Traceloop)](https://github.com/traceloop/openllmetry)
- [OpenInference (Arize)](https://github.com/Arize-ai/openinference)
- [OpenLIT](https://github.com/openlit/openlit)

### OAuth & MCP authorization

- [Stytch: Building MCP with OAuth CIMD](https://stytch.com/blog/oauth-client-id-metadata-mcp/)
- [WorkOS: Dynamic client registration in MCP](https://workos.com/blog/dynamic-client-registration-dcr-mcp-oauth)
- [FastMCP authentication docs](https://gofastmcp.com/servers/auth/authentication)

### And finally

> *"All right, all right, all right. Now I, Mr. Resetti, am the one
> who's got something to say to YOU. Save your traces!"*
