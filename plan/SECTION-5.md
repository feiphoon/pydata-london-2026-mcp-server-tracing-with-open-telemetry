# 5: From Traces to Dashboards — Investigating Tom Nook

## One trace tells a story. Tom Nook tells thousands

We now have questions:

1. *Which tool is the most expensive in pounds?*
2. *What is the p99 latency of `quote_upgrade` over the last hour?*
3. *How many tokens did Tom Nook burn last week, by tool?*
4. *Is one Kafka topic dragging the whole agent down?*

- **Individual traces** answer *"what happened in this one request?"*
- **Aggregations** — counts, percentiles, rolled-up token sums — answer
  *"what is true across thousands of requests?"*

----

## From spans to metrics, the production pattern

```bash
   spans (raw)                   span metrics (rolled up)
   ──────────                    ────────────────────────
   one row per                   one row per (tool, minute)
   tool call                     with count, p50, p95, p99,
                                 sum(tokens), sum(bells)

   nook-mcp ─► otel-collector ────────────┬────────► Jaeger        (traces)
                                          └────────► Prometheus    (metrics)
                                                     Grafana       (dashboards)
```

- The **OTel Collector** can fan out one OTLP stream
  to many backends — Jaeger for traces, Prometheus or a vendor for metrics — using the **`spanmetrics` connector**
- `spanmetrics` reads every span and emits histograms keyed on
  `span.name` + selected attributes. No code changes in `nook-mcp`
- This is how Amazon Bedrock AgentCore, Grafana Cloud and other
  platforms all do it under the hood
- Workshop scope: we **stay in Jaeger** for §5 and run the
  aggregations by hand

----

## The two MCP histogram metrics (and why their *gap* matters)

The OTel MCP semconv defines two histogram metrics. The OTel Collector
can derive both from our `nook-mcp` spans with three lines of YAML:

| Metric                            | What it measures                                   | Where it is computed              |
|-----------------------------------|----------------------------------------------------|-----------------------------------|
| `mcp.client.operation.duration`   | Time from client `send` to client `receive`        | The agent / FastMCP `Client`      |
| `mcp.server.operation.duration`   | Time the server spends processing                  | `nook-mcp` itself                 |
| **`client - server`**             | **Network + JSON-RPC serialisation overhead**      | Derived in the dashboard          |

- Both are broken down by `mcp.method.name` and `gen_ai.tool.name`
- If the **gap** is consistently large (say >100 ms for a tool that
  takes 200 ms server-side), you are **drowning in serialisation
  overhead**. Common cause: returning a `list[dict]` that FastMCP has
  to re-wrap as anonymous Pydantic `Root` models

----

## Cost analysis: from `gen_ai.usage.*_tokens` to pounds (and Bells)

Every `chat <model>` span in `nook-mcp` already carries the two
attributes you need for cost:

```text
gen_ai.request.model        = qwen3.5:0.8b
gen_ai.usage.input_tokens   = 320
gen_ai.usage.output_tokens  = 92
```

Multiply by the pricing card and sum:

```text
cost_gbp = (input_tokens  × input_price_per_M  / 1_000_000)
         + (output_tokens × output_price_per_M / 1_000_000)
```

`nook-mcp` runs a **self-hosted** model (Qwen on Ollama), so there is no
per-token invoice from a vendor — but it is not free. We attribute a
**nominal £0.10 per 1M tokens** to amortised hardware and electricity,
and apply it to both input and output:

| Model (deployment)          | Input (/1M tokens) | Output (/1M tokens) |
|-----------------------------|--------------------|---------------------|
| Qwen 3.5 0.8b (self-hosted) | £0.10              | £0.10               |

- For `quote_upgrade` at ~320 input + ~92 output tokens that's
  **~£0.00004 per quote** — about **£41 per million quotes**
- Sounds trivial. At Tom Nook's volume (every islander who has ever
  asked for an upgrade, plus the agent's exploratory calls), it
  compounds
- Aggregate by `gen_ai.tool.name` or by hour to answer *"where is the money going?"*

----

## Tom Nook's pricing in Bells, from the same span

The fun part: `quote_upgrade` already writes the **final Bells figure
back onto the parent span** as `nook.quote.bells`. The four TANUKI multipliers break
the quote down:

| Attribute on `tool quote_upgrade` span | What it captures                                            |
|----------------------------------------|-------------------------------------------------------------|
| `nook.quote.bells`                     | Final Bells figure Tom Nook would charge                     |
| `nook.quote.abd_multiplier`            | Wealth-tier multiplier from the player's ABD balance         |
| `nook.quote.catch_multiplier`          | Visitor bonus on today's catches (Flick/CJ ⇒ 1.5×)          |
| `nook.quote.luck_multiplier`           | Bonus for rare/high-value catches today                      |
| `nook.quote.plea_multiplier`           | "Plea Processing Fee" from the justification token count     |

- `nook.quote.bells` rides on every `tool quote_upgrade` span, so the
  threshold **`nook.quote.bells > 12000000`** surfaces every runaway
  quote. That `>` is a **dashboard** query (Grafana + Prometheus via
  the `spanmetrics` connector) — Jaeger's own **Tags** search matches
  *exact equality* only (`nook.quote.bells=13371337`), so in Jaeger you
  read the value off each span and reserve the range filter for the
  dashboard
- A wealth-driven cut like `nook.quote.abd_multiplier > 2.0` is the
  same one-line dashboard threshold — exactly the question the island's
  fintech team needed and did not have

----

## Tool-selection telemetry: what does Tom Nook's agent *actually do* with the toolbox?

Every `tools/call` span carries **`gen_ai.tool.name`** (and FastMCP
mirrors it as `gen_ai.tool.name` for cross-framework dashboards).
That single attribute turns tracing from *"what just happened?"* into
*"what does our agent **actually do** with the tools we gave it?"* —
which is one of the four signals §1 called out as invisible to
traditional dashboards:

| Question                                                       | Jaeger filter / `GROUP BY`                                     |
|----------------------------------------------------------------|----------------------------------------------------------------|
| Which of his tools does Tom Nook reach for the most?           | `GROUP BY gen_ai.tool.name` over the last hour                    |
| Is the agent re-discovering tools every call?                  | `mcp.method.name=tools/list` rate vs `tools/call` rate         |
| Did Tom Nook pick `query_topic` when `peek_topic` would have done? | `gen_ai.tool.name=query_topic` ∧ no `nook.query.since` set    |
| Which tool fails most often?                                    | `gen_ai.tool.name` × `status=ERROR`                               |
| Did a prompt change the *shape* of tool selection overnight?   | Day-over-day delta of `count(*) GROUP BY gen_ai.tool.name`        |

- **A multi-tool agent's behaviour is its tool-selection histogram.**
  Watch the long tail: a tool nobody ever calls is either dead code
  or a hint that the prompt is steering away from it. The inverse is
  just as telling — if `shake_tree` is suddenly topping Tom Nook's
  histogram, his agent is off shaking trees instead of pricing upgrades
- This is the dashboard a platform team builds first when a new
  agent goes live — a single bad prompt can move the histogram by
  10× overnight, and tracing is the fastest way to see it

----

## Performance optimisation from the waterfall

The waterfall doesn't just tell you *how long* — it tells you *where*
your time was spent. Five recurring patterns, four of which apply to
`nook-mcp` today:

| What the trace shows                                                  | Optimisation                                                                       |
|-----------------------------------------------------------------------|------------------------------------------------------------------------------------|
| LLM `chat` span is the longest bar                                    | Faster/smaller model, shorter prompt, enable streaming                             |
| Two child spans are sequential **but independent**                    | Run them concurrently with `asyncio.gather`                                        |
| One external call (HTTP/DB/Kafka) consistently slow                   | Add caching, move closer, set tighter timeouts                                     |
| The same tool is called repeatedly with the same arguments            | Memoise at the MCP level                                                           |
| `tools/list` fires before every `tools/call`                          | The client is re-discovering on every request — fix the client, not the server     |

- `quote_upgrade` is a worked example of **box two, already solved**:
  its independent topic reads are wrapped in `asyncio.gather`, so in
  the waterfall the `* receive` spans **overlap** instead of running
  back-to-back — concurrency you can read straight off the trace,
  alongside the genuine data dependency that forces the second gather
  to wait for the first

----

## What dashboards will tell us about Tom Nook

Once the spans are flowing, four dashboards build themselves. Each
maps directly to a span attribute we instrumented in §4:

| Dashboard / panel                          | Reads which span attribute                                            | Answers                                          |
|--------------------------------------------|-----------------------------------------------------------------------|--------------------------------------------------|
| **Cost per tool (GBP/hr)**                 | `gen_ai.usage.*_tokens` + `gen_ai.request.model`                      | *Where is the money going?*                       |
| **p50 / p95 / p99 latency by tool**        | parent `tool <name>` span duration                                    | *Which tool is slow on the long tail?*           |
| **Bells distribution per `quote_upgrade`** | `nook.quote.bells` + multipliers                                      | *Which rooms and signals drive Tom Nook's quotes up?* |
| **Topic read fanout per call**             | `messaging.destination.name` + `messaging.batch.message_count`        | *Which topics is the agent leaning on hardest?*  |
| **Tool-selection histogram**               | `gen_ai.tool.name` (every span)                                          | *What does Tom Nook's agent actually do with the toolbox?* |

- **Same data, five investigations.** That is the dashboard pay-off
- All four work against the workshop's stub LLM today and against a
  real Anthropic / OpenAI / Ollama client unchanged tomorrow

----

## Exercise: Debugging and Cost from Traces

Two parts plus a stretch, all driven from the same Jaeger UI you
already have open. Each part reuses traces you generated in §4.

### Part A: Debug a real failure

**Objective.** Find the root cause of the `query_topic` `ValueError`
from §4 *using only Jaeger* — no `docker logs`, no `print`.

1. In Jaeger, **Service** = `nook-mcp`, click **Find Traces**, then
   **Tags** = `error=true`. You should see exactly the trace from §4
   Step 8
2. Click the trace. The `tools/call query_topic` span tells you the
   request failed; the red `home-upgrade-quotes receive` child span
   tells you *where in the code* it failed
3. In the span detail panel, expand **Logs** → the **`exception`**
   event. Read:
   - **What** exception was raised (`error.type` — a `ValueError`)?
   - **At what line** in `kafka_client_with_otel.py` did it occur
     (inside `_iso_to_ms`)?
   - **What was the input that caused it** (look for `nook.query.since`
     on the parent span — the bad `not-a-timestamp` string)?

**Teaching point.** Without tracing, the client sees `Tool execution
failed.` and a session ID. With tracing, you get: *which tool*, *what
input*, *what error*, *at what line*, *and how far through the
execution it got*.

### Part B: Cost the quote

**Objective.** Calculate the pound cost of one `quote_upgrade`, find
the most expensive room, and discover whether the LLM or Kafka is
the real cost centre.

1. Replace the **entire contents** of `status_test.py` with the
   five-quote loop below. Note the `ROOMS` / `JUSTIFICATION` constants
   live at **module level** (not inside `main()`), so the loop can see
   them:

```python
import asyncio
from fastmcp import Client

ROOMS = ["back-room", "left-room", "right-room",
         "second-floor", "basement"]
JUSTIFICATION = "I pull weeds every day and never miss an ABD check-in!"


async def main() -> None:
    async with Client("http://localhost:8000/mcp") as client:
        for room in ROOMS:
            quote = await client.call_tool(
                "quote_upgrade",
                {"room": room, "justification": JUSTIFICATION},
            )
            print(f"{room:>12}: {quote.data['bells']:>10,} Bells")


asyncio.run(main())
```

```bash
uv run --with fastmcp==3.2.3 python status_test.py
```

2. In Jaeger, filter **Operation** = `tools/call quote_upgrade` and
   open **one** trace to see where the two cost inputs live (you'll
   automate the other four in step 3):
   - The `chat qwen3.5:0.8b` span's
     `gen_ai.usage.input_tokens` and `gen_ai.usage.output_tokens`
   - The parent `tool quote_upgrade` span's `nook.quote.bells`

3. **Automate the cost read with `cost_report.py`.** Eyeballing five
   traces by hand doesn't scale. This script pulls the same two
   attributes off each trace via Jaeger's HTTP API and applies the
   pricing card — the slide's formula, in code:

```python
# cost_report.py — GBP cost per quote, read straight from Jaeger
import json
import os
import sys
from urllib.parse import quote, urlencode
from urllib.request import urlopen

JAEGER_URL = os.environ.get("JAEGER_URL", "http://localhost:16686")

# Self-hosted Qwen: nominal £0.10 per 1M tokens, input and output alike.
QWEN_IN_GBP_PER_MTOK = 0.10
QWEN_OUT_GBP_PER_MTOK = 0.10


def fetch_recent_quote_traces(limit=5, lookback="10m"):
    params = urlencode({
        "service": "nook-mcp",
        "operation": "tools/call quote_upgrade",
        "limit": str(limit),
        "lookback": lookback,
    }, quote_via=quote)
    with urlopen(f"{JAEGER_URL}/api/traces?{params}", timeout=5) as resp:
        return json.load(resp).get("data", [])


def extract_row(trace):
    spans = trace["spans"]
    tool = next((s for s in spans if s["operationName"] == "tool quote_upgrade"), None)
    chat = next((s for s in spans if s["operationName"].startswith("chat ")), None)
    if not tool or not chat:
        return None
    tool_tags = {t["key"]: t["value"] for t in tool["tags"]}
    chat_tags = {t["key"]: t["value"] for t in chat["tags"]}
    in_tok = int(chat_tags.get("gen_ai.usage.input_tokens", 0))
    out_tok = int(chat_tags.get("gen_ai.usage.output_tokens", 0))
    return {
        "started_at_us": tool["startTime"],
        "room": tool_tags.get("nook.room", "?"),
        "bells": int(tool_tags.get("nook.quote.bells", 0)),
        "input_tokens": in_tok,
        "output_tokens": out_tok,
        "gbp": (in_tok * QWEN_IN_GBP_PER_MTOK
                + out_tok * QWEN_OUT_GBP_PER_MTOK) / 1_000_000,
    }


def main():
    rows = [r for r in (extract_row(t) for t in fetch_recent_quote_traces()) if r]
    if not rows:
        sys.exit(f"No quote_upgrade traces at {JAEGER_URL}. Run the loop first.")
    rows.sort(key=lambda r: r["started_at_us"])

    header = f"{'room':<14}{'bells':>14}{'in_tok':>9}{'out_tok':>9}{'GBP':>12}"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(f"{r['room']:<14}{r['bells']:>14,}{r['input_tokens']:>9}"
              f"{r['output_tokens']:>9}{r['gbp']:>12.7f}")

    max_bells = max(rows, key=lambda r: r["bells"])
    max_gbp = max(rows, key=lambda r: r["gbp"])
    print(f"\nMost expensive in Bells: {max_bells['room']} ({max_bells['bells']:,})")
    print(f"Most expensive in GBP:   {max_gbp['room']} (£{max_gbp['gbp']:.7f})")


if __name__ == "__main__":
    main()
```

```bash
uv run python cost_report.py    # after the five-quote loop has run
```

   What each piece does:

   - **`fetch_recent_quote_traces`** hits Jaeger's `/api/traces` HTTP
     endpoint — the *same data the UI shows*, as JSON. No new infra:
     Jaeger is already storing every span you exported
   - **`extract_row`** finds the two spans that matter in each trace —
     the parent `tool quote_upgrade` (carries `nook.room` and
     `nook.quote.bells`) and its `chat …` child (carries
     `gen_ai.usage.input_tokens` / `output_tokens`) — then applies the
     Qwen pricing card to turn tokens into pounds
   - **`main`** orders the quotes by start time, prints one row each,
     and flags the most expensive run in **Bells** versus in **GBP**

4. Read the two cost columns. The **Bells** figure (user-facing) swings
   ~3× across rooms; the **GBP** figure (finance-facing) is nearly flat
   (~2%), because the token count barely moves with the room. *The most
   expensive room in Bells is usually **not** the most expensive in
   pounds* — and the script prints both, so you can prove it.

**Teaching point.** Pound cost (LLM tokens) and Bells cost (the
multiplier maths in `tools.py`) are **two different things on the
same span**, and `cost_report.py` reads both straight off the trace.
In production, the pound number is what your finance team cares about;
the Bells number is what your *users* care about. This script is the
by-hand version of what the OTel Collector's `spanmetrics` connector
does in a real Grafana deployment: turn `gen_ai.usage.*_tokens` into a
histogram, multiplied by a pricing constant in PromQL — five lines of
YAML.

### Part C: Tom Nook hands off to Isabelle

**Objective.** See two MCP clients — in our story, two cooperating
agents — appear as a **single trace** via OpenTelemetry's W3C
`traceparent` context propagation. This is the mechanism that turns
N independent agents into one investigable workflow, and it is the
fourth signal §1 called out as invisible to traditional dashboards:
**multi-agent handoffs**.

The scenario: Tom Nook's *pricing agent* issues a `quote_upgrade`
and accepts the result by publishing it to `home-upgrade-quotes`.
Isabelle's *audit agent* — a separate `Client` connection — then verifies the quote by reading the topic back with
`peek_topic`. **Two MCP sessions, one investigation, one trace.**

1. Create `multi_agent_handoff.py` at the repo root:

```python
"""Two MCP sessions (two 'agents') under one parent trace."""
import asyncio
import os

from fastmcp import Client
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Same shape as nook_mcp.telemetry.configure_telemetry, inlined so
# this script is self-contained on the host.
_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
_provider = TracerProvider(resource=Resource.create({"service.name": "island-investigator"}))
_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{_endpoint}/v1/traces"))
)
trace.set_tracer_provider(_provider)
_tracer = trace.get_tracer(__name__)


async def main() -> None:
    # Parent span scopes BOTH MCP sessions into a single trace tree.
    with _tracer.start_as_current_span(
        "multi-agent: tom-nook → isabelle",
        attributes={"scenario": "price + publish, then audit"},
    ):
        # Agent 1 — Tom Nook's pricing agent: quote then publish.
        # In §7 this session carries enduser.id=tom_nook@nookinc.island
        # with scope write:home-upgrade-quotes.
        async with Client("http://localhost:8000/mcp") as nook:
            quote = await nook.call_tool(
                "quote_upgrade",
                {"room": "second-floor",
                 "justification": "I pull weeds every day and never miss an ABD check-in!"},
            )
            print(f"Tom Nook priced: {quote.data['bells']:>10,} Bells")
            await nook.call_tool(
                "publish_event",
                {
                    "topic":   "home-upgrade-quotes",
                    "key":     "Tom_Nook",
                    "payload": quote.data,
                },
            )

        # Agent 2 — Isabelle's auditor: a different MCP session, but
        # the SAME parent trace because we are still inside the
        # with-block. In §7 this session carries
        # enduser.id=isabelle@town-hall.island with scope read:topics —
        # so it can ONLY audit, never publish.
        async with Client("http://localhost:8000/mcp") as isabelle:
            audit = await isabelle.call_tool(
                "peek_topic",
                {"topic": "home-upgrade-quotes", "limit": 1},
            )
            latest = audit.data["records"][0]
            print(
                f"Isabelle audited: offset={latest['offset']}, "
                f"key={latest['key']!r}"
            )


asyncio.run(main())
```

2. Run it from the host (the collector is reachable on
   `localhost:4318` from the laptop, *not* `otel-collector:4318`):

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
uv run --with fastmcp==3.2.3 \
       --with opentelemetry-sdk \
       --with opentelemetry-exporter-otlp-proto-http \
       python multi_agent_handoff.py
```

3. In Jaeger, set **Service** = `island-investigator` and click
   **Find Traces**. The most recent trace should be a single tree
   with two MCP sub-trees underneath the client-side parent:

```
- multi-agent: tom-nook → isabelle                  ~3.6 s    ← client-side parent
  - tools/call quote_upgrade                         ~2.4 s   ← Tom Nook's session (mcp.session.id = A)
    - tool quote_upgrade …                           ~2.4 s
      - (the §4 quote_upgrade subtree: six sub-tool spans, their
         Kafka `* receive` children, and chat qwen3.5:0.8b)
  - tools/call publish_event                         ~80 ms   ← still session A (Tom Nook accepts)
    - tool publish_event …
      - home-upgrade-quotes publish                  ~70 ms
  - tools/call peek_topic                            ~110 ms  ← Isabelle's session (mcp.session.id = B)
    - tool peek_topic …
      - home-upgrade-quotes receive                  ~100 ms
```

   - **Two distinct `mcp.session.id` values, one trace ID.** That is
     the OTel definition of a multi-agent handoff
   - Filter the trace's spans by `mcp.session.id` to isolate each
     agent's work; the parent span keeps them connected
   - In a real production multi-agent system (LangGraph, Bedrock
     AgentCore) the parent span lives in the orchestrator and each
     tool call lives in a sub-agent. **The shape is identical.**

**Teaching point.** Multi-agent handoffs were the fourth signal §1
called out as invisible to traditional dashboards. The OTel mechanism
is just *one parent span across N MCP sessions*. Get this right and
the rest of the multi-agent observability story (per-agent latency,
per-agent cost, who-handed-off-to-whom) becomes a `GROUP BY
mcp.session.id` over attributes you already have on every span.

## Checkpoint

You have now exercised the four signals §1 promised would be
invisible to traditional dashboards:

- **Token usage** — found `gen_ai.usage.input_tokens` /
  `output_tokens` on the `chat` span and aggregated across five
  quotes (Part B)
- **Cost per invocation** — derived pound cost from tokens and
  Bells cost from `nook.quote.bells`, *on the same span* (Part B)
- **Tool selection** — read your client's tool-selection histogram
  off `gen_ai.tool.name` directly in Jaeger (Part A, Step 4)
- **Multi-agent handoffs** — *(stretch)* wired two MCP sessions into
  one trace via a client-side parent span (Stretch)

## Return to the slides anytime
