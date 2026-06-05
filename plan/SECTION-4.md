# 4: Instrument `nook-mcp`

## FastMCP's zero-config instrumentation

- **FastMCP uses the OpenTelemetry API natively** — not a third-party
  wrapper, not a plugin. Spans are always created
- Without an SDK configured, every `get_tracer()` call returns a
  **no-op tracer**. Zero overhead, zero output. This is the default
  shape of every FastMCP server in the wild — including Tom Nook's
- The instant a `TracerProvider` is installed *before* `fastmcp` is
  imported, the same call sites start emitting real spans. **No code
  changes** in `server.py`, `tools.py` or anywhere else

----

## `telemetry.py`

```python
def configure_telemetry(service_name: str | None = None) -> None:
    """Install the global TracerProvider. Idempotent."""
    if _INITIALISED:
        return

    resource = Resource.create({
        "service.name":    service_name or os.environ.get("OTEL_SERVICE_NAME", "nook-mcp"),
        "service.version": "0.1.0",
    })
    provider = TracerProvider(resource=resource)

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318")
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=f"{endpoint.rstrip('/')}/v1/traces"),
            schedule_delay_millis=1_000,   # 1s, not the default 5s
        )
    )

    trace.set_tracer_provider(provider)
```

- `Resource` carries `service.name` — what shows up in Jaeger's
  Service dropdown as `nook-mcp`
- `OTLPSpanExporter` over HTTP/protobuf to the collector at port 4318
  (gRPC works too on 4317; HTTP is friendlier in the workshop)
- `BatchSpanProcessor` with `schedule_delay_millis=1_000` so we can
  see spans in Jaeger ~1 second after they fire, not the default 5 seconds

----

## The order-of-operations gotcha (`server.py`)

```python
# Initialise the OpenTelemetry SDK before anything else imports
# the OTel API, so every tracer obtained downstream is the real,
# exporting one.
from . import telemetry
telemetry.configure_telemetry()

from fastmcp import FastMCP   # noqa: E402   ← AFTER configure_telemetry()

from . import tools           # noqa: E402

mcp = FastMCP("nook-mcp")
for tool_func in (
    tools.list_topics, tools.peek_topic, tools.query_topic,
    tools.publish_event, tools.quote_upgrade,
    # … plus the TANUKI pricing tools and red herrings; see server.py
):
    mcp.tool(tool_func)
```

- **The SDK must be installed before the framework you want auto-instrumented imports the OTel API**
- Get this wrong (configure the SDK after FastMCP imports) and layer 1
  silently disappears: no `tools/call` spans. Easy to miss because the rest of the trace still works

----

## What FastMCP 3.2.3 gives us automatically

Once `configure_telemetry()` runs first, every inbound MCP message
becomes a span — with zero changes to `server.py`. FastMCP's
`server_span` helper (in `fastmcp/server/telemetry.py`) emits a
`SERVER` span for each protocol method with a fixed set of
framework-stamped attributes:

| Span name (example)        | When it fires                | Key attributes                                                                                              |
|----------------------------|------------------------------|-------------------------------------------------------------------------------------------------------------|
| `initialize`               | Session handshake            | `mcp.method.name=initialize`, `rpc.system=mcp`                                                              |
| `tools/list`               | Client discovers tools       | `mcp.method.name=tools/list`, `mcp.session.id`                                                              |
| `tools/call quote_upgrade` | Client invokes a tool        | `mcp.method.name=tools/call`, `fastmcp.component.type=tool`, `fastmcp.component.key=quote_upgrade`          |
| `tools/call publish_event` | Client invokes any other tool| Same shape; `fastmcp.component.key` differs                                                                 |

- This is **layer 1** of the `nook-mcp` waterfall
- No instrumentation code in `server.py`, `tools.py` or anywhere else
  is required for this layer. Purely a side-effect of having an SDK
  installed before `fastmcp` is imported
- The tool name lives in `fastmcp.component.key` here, not
  `gen_ai.tool.name` — that's a FastMCP-specific attribute. In §4
  Step 4 we re-stamp the cross-framework `gen_ai.tool.name` onto our
  own layer-2 tool body span so dashboards (§5) can group by a
  vendor-neutral key alongside the business attributes
- FastMCP also records exceptions on these spans

----

## What we add by hand — the three lower layers

FastMCP gives us layer 1. We add three more layers explicitly because
that is where the *story of a quote* lives:

| Layer | File                                | Span name pattern              | Semantic-convention dialect                                    |
|-------|-------------------------------------|--------------------------------|----------------------------------------------------------------|
| 2     | `nook_mcp.tools`                    | `tool <name>`                  | MCP + GenAI (`mcp.method.name`, `gen_ai.tool.name`) + `nook.*` business attrs |
| 3a    | `nook_mcp.kafka_client`             | `<topic> receive`, `<topic> publish` | Messaging (`messaging.system=kafka`, `messaging.operation.name`, `messaging.destination.name`, …) |
| 3b    | `nook_mcp.tools._llm_reasoning` (live) / `_simulate_llm_reasoning` (fallback) | `chat qwen3.5:0.8b` | GenAI (`gen_ai.provider.name`, `gen_ai.request.model`, `gen_ai.usage.input_tokens`, …) |

- Each layer answers a *different* question, in a *different* dialect
- The dialects do not collide — every attribute name is namespaced
- The whole thing is ~150 lines spread across two files

----

## The pattern for a per-tool span (`tools.py`)

Every tool in `nook_mcp.tools` follows the same three-step shape:

```python
_tracer = trace.get_tracer(__name__)


async def peek_topic(topic: str, limit: int = 10) -> dict:
    """Return the most recent messages from a Kafka topic, newest first."""
    limit = max(1, min(int(limit), 200))
    with _tracer.start_as_current_span(
        "tool peek_topic",
        attributes=_tool_attrs(
            "peek_topic",
            **{"nook.topic": topic, "nook.peek.limit": limit},
        ),
    ) as span:
        records = await kafka_client.peek_topic_messages(topic, limit=limit)
        span.set_attribute("nook.peek.records_returned", len(records))
        return {"topic": topic, "count": len(records), "records": records}
```

- One `_tracer = trace.get_tracer(__name__)` at module load. The
  *named* tracer is what links the span to the file in Jaeger's UI
- One `start_as_current_span("tool <name>", attributes=...)` per tool
- `_tool_attrs(...)` is a four-line helper that sets the common
  `mcp.method.name=tools/call` and `gen_ai.tool.name=<name>` attributes
- **Business attributes** (`nook.topic`, `nook.peek.records_returned`,
  later `nook.quote.bells`) ride on the same span, captured at the
  point in the code where we *know* the values

----

## The pattern for a Kafka span (`kafka_client.py`)

```python
async def peek_topic_messages(topic: str, limit: int = 10) -> list[dict]:
    with _tracer.start_as_current_span(
        f"{topic} receive",
        attributes={
            "messaging.system":            "kafka",
            "messaging.operation.name":    "receive",
            "messaging.destination.name":  topic,
        },
    ) as span:
        # ... open consumer, seek, drain ...
        span.set_attribute("messaging.batch.message_count", len(messages))
        return messages
```

- Span **name** follows the OTel messaging convention:
  `<destination> <operation>` — e.g. `abd-balance receive`.
  Jaeger groups by name; consistent naming is how you find every
  read of a single topic
- `messaging.system=kafka` is the tag every vendor's Kafka view keys
  off — Grafana, Honeycomb and Datadog all surface a Kafka tab when
  they see it
- Errors recorded via `span.record_exception(exc)` and
  `span.set_status(Status(StatusCode.ERROR, ...))` — see
  `query_topic_messages` in the same file for the full pattern

----

## The pattern for a GenAI span (`_llm_reasoning`)

```python
with _tracer.start_as_current_span(
    f"chat {_OLLAMA_MODEL}",
    attributes={
        "gen_ai.provider.name":       "ollama",
        "gen_ai.operation.name":      "chat",
        "gen_ai.request.model":       "qwen3.5:0.8b",
        "gen_ai.response.model":      "qwen3.5:0.8b",
        "gen_ai.request.max_tokens":  256,
        "gen_ai.usage.input_tokens":  input_tokens,
        "gen_ai.usage.output_tokens": output_tokens,
    },
):
    await asyncio.sleep(0.6 + random.random() * 0.6)
```

- `_llm_reasoning` makes a real Ollama HTTP call and emits the same span; swap it for an Anthropic / OpenAI client and **the attribute names do not change** — only the provider, model and token numbers do
- **The GenAI semantic
  conventions are vendor-neutral on purpose**. The dashboard you build
  against the stub works against production with no code changes

----

## Error handling: most of it is automatic

For the `tools/call <name>` span (layer 1), **FastMCP 3.2.3 records
exceptions for free**:

```python
async def publish_event(topic: str, payload: dict, ...) -> dict:
    ...
    # If json.dumps(payload) raises here, the tools/call publish_event
    # span automatically gets:
    #   - status            = ERROR
    #   - event "exception" with the full stack trace
    #   - error.type        = "TypeError"
```

For **child** spans (layers 2/3a/3b), the standard OTel pattern is
three lines:

```python
try:
    ...
except Exception as exc:
    span = trace.get_current_span()
    if span.is_recording():
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, str(exc)))
    raise   # ← important: still propagate
```

This is the exact pattern in `kafka_client.query_topic_messages`. The
exception still propagates to the caller; the span records it on the
way out.

- `shake_tree` is the cleanest example of this on a layer-2 span:
  shake an unknown tree and it calls
  `span.set_status(Status(StatusCode.ERROR, ...))` then raises a
  `ValueError`. No Kafka, no LLM — a self-contained red `tool
  shake_tree` span you can produce in one call

----

## Exercise: Trace `nook-mcp` end-to-end

### Objective

**Add the tool-body span to the `quote_upgrade` tool** yourself,
trigger every layer of the `nook-mcp` instrumentation, read the
resulting waterfall in Jaeger, and **see a real error trace**.

### Steps

1. Confirm the OTel SDK is wired and the collector is up:

```bash
docker compose ps otel-collector jaeger nook-mcp
# All three: STATUS = up/running
```

2. **Check out the four files that produce the waterfall.** Open these and
   spend ~2 minutes each reading top-to-bottom:

| File                                        | What to look for                                                                                                              |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `mcp/src/nook_mcp/telemetry.py`             | `TracerProvider`, `OTLPSpanExporter` pointing at `otel-collector:4318`, `BatchSpanProcessor` with `schedule_delay_millis=1000` |
| `mcp/src/nook_mcp/server.py`                | `telemetry.configure_telemetry()` runs **before** `from fastmcp import FastMCP` |
| `mcp/src/nook_mcp/tools.py`                 | One `_tracer.start_as_current_span("tool <name>", ...)` per tool; `nook.*` business attributes; the GenAI `chat` span inside `_llm_reasoning` / `_simulate_llm_reasoning`. Note the one exception: `quote_upgrade` has **no** layer-2 span yet — that's the one you add in Step 4 |
| `mcp/src/nook_mcp/kafka_client.py`          | One messaging span per public helper; `messaging.system=kafka`; `record_exception` on error in `query_topic_messages`         |

3. **Wire up the tracer and the `_tool_attrs` helper in `tools.py`.**
   The span you're about to add needs a module-level tracer and a small
   attribute helper. At the top of `mcp/src/nook_mcp/tools.py`, import
   the OpenTelemetry trace API (and `Any` for the helper's type hints)
   alongside the existing imports:

```python
from typing import Any

from opentelemetry import trace
```

   Then, just below the imports, declare the named tracer once at
   module load. The *named* tracer (`__name__`) is what links every
   span to this file in Jaeger's UI:

```python
_tracer = trace.get_tracer(__name__)
```

   Finally, add the `_tool_attrs(...)` helper used by every tool span
   below the `def _knowledge_dir() function`. It stamps the common
   `mcp.method.name=tools/call` and the cross-framework
   `gen_ai.tool.name=<name>` attributes, then merges in any extra
   business attributes the caller passes:

```python
def _tool_attrs(name: str, **extra: Any) -> dict[str, Any]:
    """Construct the standard MCP tool span attributes."""
    attrs: dict[str, Any] = {
        "mcp.method.name": "tools/call",
        "gen_ai.tool.name": name,
    }
    attrs.update(extra)
    return attrs
```

4. **Add the layer-2 span to `quote_upgrade`.** Find the
   `quote_upgrade` tool in the same file — the one tool whose body is
   still bare. Wrap it in a `tool quote_upgrade` span and stamp the
   `nook.quote.*` business attributes at the point where each value is
   known:

```python
    async def quote_upgrade(room: str, justification: str) -> dict:
        """Compute Tom Nook's inflated quote (in Bells) for a home upgrade."""
        with _tracer.start_as_current_span(
            "tool quote_upgrade",
            attributes=_tool_attrs("quote_upgrade", **{"nook.room": room}),
        ) as span:
            knowledge = _knowledge_dir()
            # ... existing quote calculation unchanged: room_base, the
            #     asyncio.gather fan-out, multipliers, bells ...
            span.set_attribute("nook.quote.net_worth_base", net_worth_base)
            span.set_attribute("nook.quote.abd_balance", abd_balance)
            span.set_attribute("nook.quote.catch_total", catch_mult["total"])
            span.set_attribute("nook.quote.room_base", room_base)
            span.set_attribute("nook.quote.bells", bells)
            span.set_attribute("nook.quote.abd_multiplier", multipliers["abd"])
            span.set_attribute("nook.quote.catch_multiplier", multipliers["catch"])
            span.set_attribute("nook.quote.luck_multiplier", multipliers["luck"])
            span.set_attribute("nook.quote.plea_multiplier", multipliers["plea"])

            reasoning = await _generate_reasoning(
                room=room, bells=bells, multipliers=multipliers
            )
            return {...}  # unchanged
```

   Same two-piece pattern as every other tool: one
   `start_as_current_span("tool quote_upgrade", attributes=...)` around
   the body, plus the `nook.quote.*` business attributes set the moment
   each value is computed. Rebuild:

```bash
docker compose up -d --build nook-mcp
```

5. **Generate traffic.** Reuse your `status_test.py` from §2 and
   add a `quote_upgrade` call to `main()` so all four layers fire on
   one request:

```python
        quote = await client.call_tool(
            "quote_upgrade",
            {"room": "second-floor",
             "justification": "I pull weeds every day and never miss an ABD check-in!"},
        )
        print("quote:", quote.data)
```

```bash
uv run --with fastmcp==3.2.3 python status_test.py
```

6. Open Jaeger at <http://localhost:16686>. Pick **Service** =
   `nook-mcp`, **Operation** = `tools/call quote_upgrade`, click
   **Find Traces**. Click into the most recent trace.

### What you see now: just two spans

The **only** application code you instrumented is the `quote_upgrade`
tool body, so the trace has exactly **two spans** — not the full
waterfall:

```text
- tools/call quote_upgrade        ~2.4 s   ← MCP protocol  (FastMCP auto, layer 1)
  - tool quote_upgrade            ~2.4 s   ← your span      (tools.py, layer 2)
```

That is the correct, expected result for this step. The Kafka reads
and the LLM call still happen inside `quote_upgrade` — they just emit
no spans, because the plain `tools.py` imports the **uninstrumented**
`kafka_client` and calls a `_generate_reasoning` that opens no `chat`
span. No child spans means there is nothing yet to nest under your
`tool quote_upgrade` span.

### What to find on each span (Tags panel)

| Span                              | Find these attributes                                                                                                                          |
|-----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `tools/call quote_upgrade`        | `mcp.method.name=tools/call`, `mcp.session.id`                                                                  |
| `tool quote_upgrade`              | `gen_ai.tool.name=quote_upgrade`, `nook.room`, `nook.quote.net_worth_base`, `nook.quote.abd_balance`, `nook.quote.catch_total`, `nook.quote.room_base`, `nook.quote.bells`, `nook.quote.abd_multiplier`, `nook.quote.catch_multiplier`, `nook.quote.luck_multiplier`, `nook.quote.plea_multiplier` |

7. **Switch to the fully-instrumented module to get the whole
   waterfall.** Hand-instrumenting *every* tool, the Kafka client and
   the GenAI call is the ~150 lines this section is about — and it is
   already written for you in `tools_with_otel.py` (which in turn
   imports `kafka_client_with_otel`). Flip the import in
   `mcp/src/nook_mcp/server.py`:

```python
# Enable/disable OpenTelemetry instrumentation for the MCP tools
from . import tools_with_otel as tools  # noqa: E402
# from . import tools  # noqa: E402
```

   Rebuild and re-run:

```bash
docker compose up -d --build nook-mcp
uv run --with fastmcp==3.2.3 python status_test.py
```

   Refresh Jaeger and open the newest `tools/call quote_upgrade` trace.

### Expected waterfall

Now you should see the full shape:

```text
- tools/call quote_upgrade                    ← MCP protocol  (FastMCP auto)
  - tool quote_upgrade                         ← Application   (tools.py)
    - tool get_todays_catch                    ← sub-tool      (tools.py)
      - catch-log receive                      ← Kafka         (kafka_client_with_otel.py)
    - tool get_plea_fee_multiplier             ← sub-tool      (no I/O)
    - tool get_abd_balance                     ← sub-tool      (tools.py)
      - abd-balance receive                    ← Kafka         (kafka_client_with_otel.py)
    - tool apply_catch_value_multiplier        ← sub-tool      (tools.py)
      - island-visitors receive                ← Kafka         (kafka_client_with_otel.py)
    - tool get_luck_multiplier                 ← sub-tool      (no I/O)
    - tool apply_wealth_potential_multiplier   ← sub-tool      (no I/O)
    - chat qwen3.5:0.8b                         ← GenAI        (_llm_reasoning / _simulate_llm_reasoning)
```

Twelve spans in total. Wall-clock numbers vary wildly — the first call
after a rebuild is cold, so Kafka and the LLM can each take several
seconds — but the **shape** does not. The six layer-2 sub-tool spans
fan out concurrently via `asyncio.gather`, so their order in the
waterfall shifts between runs. Each Kafka `* receive` sits under the
sub-tool that triggered it; the three tools that take their data as
arguments (`get_plea_fee_multiplier`, `get_luck_multiplier`,
`apply_wealth_potential_multiplier`) do no I/O and so have no children.

### What the extra spans carry (Tags panel)

| Span                              | Find these attributes                                                                                                                          |
|-----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `tool <sub-tool>` (×6)            | `mcp.method.name=tools/call`, `gen_ai.tool.name=<sub-tool>`, plus any `nook.*` business attrs that sub-tool sets                               |
| `<topic> receive` (×3)            | `messaging.system=kafka`, `messaging.operation.name=receive`, `messaging.destination.name=<topic>`, `messaging.batch.message_count`            |
| `chat qwen3.5:0.8b`               | `gen_ai.provider.name=ollama`, `gen_ai.request.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`                            |

Everything below `tool quote_upgrade` is **inherited** — you wrote
none of it in this trace. The six `tool <sub-tool>` spans come from the
other instrumented tools in `tools_with_otel.py`; the three
`* receive` spans come from `kafka_client_with_otel.peek_topic_messages`
/ `query_topic_messages`; the `chat qwen3.5:0.8b` span comes from
`_generate_reasoning`. The `tool quote_upgrade` span — now produced by
the fully-instrumented module, identical to the one you wrote in
Step 4 — is what gives that whole subtree a layer-2 parent to hang
from, and the only place the `nook.quote.*` business attributes live.
*That* is the layered-instrumentation pay-off, and the reason
`kafka_client.py` was kept FastMCP-free in §2.

### Step 8: See a real error trace

Force a true error by passing an invalid `since` timestamp to
`query_topic`. With the fully-instrumented module from Step 7 in
effect, the bad string reaches `kafka_client_with_otel.py`, where
`datetime.fromisoformat(...)` raises `ValueError` — and our
`record_exception` + `set_status(ERROR)` block stamps it on the
`home-upgrade-quotes receive` span before re-raising. (The plain
`kafka_client.py` has no tracer, so this only produces a red span once
Step 7's `tools_with_otel` import is active.)

Add this block to your `status_test.py` `main()`, then re-run it with
`uv run --with fastmcp==3.2.3 python status_test.py`:

```python
        try:
            await asyncio.wait_for(
                client.call_tool(
                    "query_topic",
                    {"topic": "home-upgrade-quotes", "since": "not-a-timestamp"},
                ),
                timeout=5.0,
            )
            print("Unexpected: query_topic did not error")
        except asyncio.TimeoutError:
            print("Expected: client deadlocked on the error response "
                  "(FastMCP 3.2.3 quirk); the red trace is in Jaeger anyway.")
        except Exception as e:
            print(f"Expected error: {type(e).__name__}: {e}")
```

In Jaeger, filter **Operation** = `tools/call query_topic` and open
the most recent trace. Expect:

- The child `home-upgrade-quotes receive` span is **red** (ERROR status)
- Its **Tags** panel shows `otel.status_code = ERROR` and
  `error.type` (e.g. `ValueError`)
- Its **Logs** panel has an `exception` event with the full stack
  trace, including the exact line in `kafka_client_with_otel.py`. **No
  print statements, no `docker logs`** — the answer is on the span

### Checkpoint

- You can name **where each of the four span layers is produced** in
  the `nook-mcp` codebase
- The `quote_upgrade` tool now has a layer-2 span you wrote, carrying
  the `nook.quote.*` business attributes, and you can point at it in
  Jaeger with its inherited Kafka and GenAI children
- You can find an error trace, expand its event log, and read the
  stack trace without leaving the browser
- You understand why initialising `TracerProvider` **before** the
  `fastmcp` import is non-negotiable
- You can spot, in the waterfall, whether time was spent in Kafka or
  in the LLM — i.e. *which side of the agent is slow today*

## Return to the slides
