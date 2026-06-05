# 3: OpenTelemetry for Agentic AI

## Why Tom Nook's server-room dashboard is useless

What we want to know about his MCP agent vs what a traditional dashboard
can actually tell us:

| What we need to know about Tom Nook's agent                       | Traditional tooling | Why it falls short |
|------------------------------------------------------------------|--------------------|--------------------|
| How many tokens did the LLM burn pricing one quote?               | No equivalent concept | Metrics/APM tools don't track tokens |
| What is the cost in Bells (and pounds) per `quote_upgrade` call? | No equivalent concept | No per-request cost attribution |
| Which topic did he reach into to justify the bill?                | Request logs | Every Kafka read hides under one HTTP 200 on his MCP endpoint |
| Did Tom Nook quietly swap models overnight?                       | Not captured | GenAI-specific metadata is not in standard span attributes |
| If `quote_upgrade` had to retry after a Mr. Resetti reset | Not captured | Traditional tracing assumes one service processes one request |

----

## The three pillars, but one matters most

- **Metrics**: aggregate counters and histograms (request rate, error rate,
  latency percentiles) — answers *how much* and *how often*
- **Logs**: timestamped text records — answers *what happened* (good for
  grep, bad for structure)
- **Traces**: hierarchical records of causally related operations across
  services — answers *what happened during this specific quote*

For agentic AI, traces are the **primary** signal because the architecture is *interaction-centric*: a single `quote_upgrade` call produces a *tree* of nested operations across MCP, Kafka and the LLM provider

Metrics and logs are supporting cast. Useful, but they cannot reconstruct Tom Nook's reasoning path

----

## Distributed tracing: the 60-second version

- A **span** is a single unit of work (e.g. *"tool quote_upgrade"*, or
  *"abd-balance receive"*)
- Each span has: a **name**, a **start/end time**, a bag of
  **attributes** (key-value metadata) and a **parent** span
- A **trace** is a tree of spans sharing a single trace ID
- **Context propagation** passes the trace ID across process boundaries.
  HTTP services use the `traceparent` header; MCP uses
  `params._meta.traceparent` on every JSON-RPC payload
- The result is the **waterfall view** we previewed in §1: exactly what
  the agent did, how long each step took and where failures occurred —
  per `quote_upgrade` call, not aggregated

----

## OpenTelemetry: the standard

- OpenTelemetry officially graduated at CNCF on May 21 at the Observability Summit in Minneapolis.
- The second most active CNCF project after Kubernetes
- 70,000+ contributors from 14,000+ organisations
- Native support from 30+ observability vendors (Grafana, Datadog,
  Honeycomb, Elastic, Splunk, etc)
- Vendor-neutral: instrument once, export to any backend. Today our
  backend is Jaeger; at work in production it can be whichever
  vendor your platform team already pays for
- The GenAI Special Interest Group published purpose-built semantic
  conventions for **LLM calls**, **agent orchestration** and **MCP**

----

## The OTel GenAI semantic conventions

These are the attributes every LLM span in our `quote_upgrade` trace
carries (and every other agent in the wild speaks the same dialect):

| Attribute                       | What it records              | Example from `nook-mcp`         |
|---------------------------------|------------------------------|---------------------------------|
| `gen_ai.provider.name`          | LLM provider                 | `ollama`                        |
| `gen_ai.operation.name`         | Operation type               | `chat`                          |
| `gen_ai.request.model`          | Model requested              | `qwen3.5:0.8b`                  |
| `gen_ai.response.model`         | Model that actually responded| `qwen3.5:0.8b`                  |
| `gen_ai.request.max_tokens`     | Token cap on the response    | `256`                           |
| `gen_ai.usage.input_tokens`     | Tokens sent in the prompt    | `~320` |
| `gen_ai.usage.output_tokens`    | Tokens in the completion     | `~92` |

- They are vendor-neutral: we already call a real model over HTTP; swap the local Ollama client for an Anthropic or OpenAI one and the attribute names do not change

----

## The OTel MCP semantic conventions

Every MCP protocol message that hits `nook-mcp` becomes a span. The
attributes split across two layers: FastMCP 3.2.3's `server_span`
helper stamps a fixed set on the **layer-1 protocol span**
automatically; the workshop's own `_tool_attrs` helper (introduced next) stamps the cross-framework GenAI attributes on the **layer-2 tool
body span** so they sit on the same span as the business attributes:

| Attribute               | Emitted by                              | What it records                         | Example                          |
|-------------------------|-----------------------------------------|-----------------------------------------|----------------------------------|
| `mcp.method.name`       | FastMCP (layer 1) + `_tool_attrs` (layer 2) | MCP protocol method                 | `tools/call`, `tools/list`, `initialize` |
| `mcp.session.id`        | FastMCP (layer 1)                       | Session identifier                      | `sess_7f3a9b2c`                  |
| `mcp.resource.uri`      | FastMCP (layer 1, on resource reads)    | Resource URI                            | n/a — `nook-mcp` exposes tools only |
| `rpc.system`            | FastMCP (layer 1)                       | Always `mcp`                            | `mcp`                            |
| `rpc.service`           | FastMCP (layer 1)                       | Server name                             | `nook-mcp`                       |
| `rpc.method`            | FastMCP (layer 1)                       | MCP method (same as `mcp.method.name`)  | `tools/call`                     |
| `fastmcp.component.key` | FastMCP (layer 1)                       | The tool/resource the call targets      | `quote_upgrade`                  |
| `gen_ai.tool.name`      | `_tool_attrs` (layer 2)                 | The tool the agent called               | `quote_upgrade`                  |

- `gen_ai.tool.name` lives in the **GenAI** semconv registry, not MCP —
  the MCP namespace deliberately stops at the protocol layer and
  reuses GenAI's tool vocabulary for the tool itself. FastMCP's auto
  layer uses its own `fastmcp.component.key` for the same value; we
  re-stamp `gen_ai.tool.name` on the layer-2 span so the standard
  vendor-neutral key sits next to the business attributes
  (`nook.quote.bells`, etc) and dashboard queries read one place.
- These attributes are what you filter on in Jaeger to ask *"show me
  every `quote_upgrade` call in the last hour"* without writing a
  regex.

----

## The OTel messaging semantic conventions

`quote_upgrade` reads three Kafka topics; when `kafka_client.py` is instrumented, those reads will each open a child span tagged with
the OTel **messaging** semconv (the spans below do not exist yet — we
add them next:

| Attribute                                       | What it records                  | Example                          |
|------------------------------------------------|----------------------------------|----------------------------------|
| `messaging.system`                              | Broker family                    | `kafka`                          |
| `messaging.operation.name`                      | What we did                      | `receive`, `publish`             |
| `messaging.destination.name`                    | Topic name                       | `abd-balance`                    |
| `messaging.batch.message_count`                 | Records in this read             | `20`                             |
| `messaging.kafka.destination.partition`         | Partition we landed on (publish) | `0`                              |
| `messaging.kafka.message.offset`                | Offset of the produced record    | `48211`                          |

----

## The trace hierarchy for a `quote_upgrade` call

This is the tree FastMCP 3.2.3 + `nook-mcp`'s instrumentation produces
end-to-end, with four distinct semantic-convention dialects in one trace:

```text
Level 0: Agent conversation                                        (lives in Tom Nook's chat UI)
│
├── Level 1: MCP protocol                                          (FastMCP 3.2.3 auto-instrumentation)
│   ├── initialize                          (CLIENT → nook-mcp)    mcp.method.name=initialize
│   ├── tools/list                          (CLIENT → nook-mcp)    mcp.method.name=tools/list
│   └── tools/call quote_upgrade            (CLIENT → nook-mcp)    mcp.method.name=tools/call
│       │
│       └── Level 2: Tool body                                     (nook_mcp.tools)
│           └── tool quote_upgrade                                  gen_ai.tool.name=quote_upgrade
│               │
│               ├── Level 3a: Messaging                            (nook_mcp.kafka_client)
│               │   ├── abd-balance receive                         messaging.system=kafka
│               │   ├── catch-log receive                           messaging.operation.name=receive
│               │   └── island-visitors receive                     messaging.destination.name=...
│               │
│               └── Level 3b: GenAI                                (nook_mcp.tools._llm_reasoning, or _simulate_llm_reasoning when offline)
│                   └── chat qwen3.5:0.8b                           gen_ai.provider.name=ollama
```

- **Level 1** tells you: *which MCP method was called and how long the
  round-trip took*
- **Level 2** tells you: *what the tool did internally and which
  arguments it received*
- **Level 3a** tells you: *which topics, partitions and offsets the agent
  actually touched in Kafka*
- **Level 3b** tells you: *which model, how many tokens, how much it
  cost in pounds*

One trace, four observability dialects.

- Simplified for clarity: in the real trace each Kafka read sits under
  the multiplier sub-tool that triggers it (`get_abd_balance`,
  `get_todays_catch`/`apply_catch_value_multiplier`, …), so Level 2 is
  a little deeper than shown — but the four dialects and their
  attributes are exactly as above
- Not every tool produces all four layers. A pure-compute tool like
  `shake_tree` (no Kafka, no LLM) is just **Level 1 + Level 2** — a
  `tools/call shake_tree` protocol span over a single `tool shake_tree`
  application span carrying `nook.tree.*` attributes. Same dialects,
  fewer layers

----

## Real-world adoption

This is not a toy. The pattern in front of us is the same pattern in
production. If you can read Tom Nook's trace, you can read traces from these vendors:

- **Amazon** launched Bedrock AgentCore Observability at AWS Summit
  NYC 2025, built entirely on OTel and the GenAI semantic conventions
- **Grafana Labs** demonstrated production tracing of the OpenAI Agents SDK and AWS Bedrock AgentCore in Grafana Cloud
- **Honeycomb** ships a dedicated GenAI attributes view that reads the
  same OTel `gen_ai.*` semantic conventions — decorating
  agentic spans and surfacing LLM, MCP and tool-call telemetry
- **Anthropic's Claude Code** exports OTel metrics and events natively

The trajectory: OTel is becoming the standard telemetry backbone for AI agents in the same way it already is for microservices

## Go to Section-4
