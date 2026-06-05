# 6: Production Patterns — Sampling, Redaction, Compliance

## Three problems with `nook-mcp`'s beautiful traces

| Problem (from §5)                                                          | Production control                          |
|----------------------------------------------------------------------------|---------------------------------------------|
| Spans carry **visitor names** from `island-visitors` records → PII risk    | Redaction (app hash + Collector OTTL)       |
| At Tom Nook's volume every span costs money to export → budget blowout     | **Tail-based** sampling                     |
| §1's fintech team sampled errors only → missed an 11-hour silent loop      | Smarter sampling (cost, tools, latency)     |

- Same `nook-mcp` code, same traces — the only thing that changes is
  the **Collector pipeline**
- This is where the stack stops being a great local demo and starts
  being a system you could actually ship

> **You won't hand-instrument the LLM SDK.** When you don't own the
> framework (Anthropic, OpenAI, LangChain, LangGraph, etc), OTel-native
> auto-instrumentors — **OpenLLMetry**, **OpenInference**, **OpenLIT** —
> add tracing to 40+ AI libraries and emit the *same* `gen_ai.*`
> attributes you put on `_llm_reasoning` by hand. Same dialect, any
> backend.

----

## Redaction: keep the shape, drop the identity

The `island-visitors` topic carries records like:

```json
{"name": "Flick", "date": "2026-06-01"}
```

If a tool peeks at this topic, the visitor's name lands in the
`peek_topic` span as part of `records[*].value`. We want the *shape* of
the data — so §5's "who is driving the catch bonus?" dashboards keep
working — but not the identity. Two layers do the job:

**1. Hash at the source** (`kafka_client.py`) — the primary fix:

```python
import hashlib

def _hash_name(n: str | None) -> str | None:
    return hashlib.sha256(n.encode()).hexdigest()[:12] if n else None
```

Hash the `name` field (and any record key) before it lands on a span.
Same cardinality, same `GROUP BY` dashboards, no PII.

**2. Collector OTTL backstop** — a vendor-side fence that catches
anything layer 1 missed (third-party spans, a future tool author who
forgets). One `replace_pattern` rule, shown in the pipeline below.

- This is **GDPR Article 5 "data minimisation"** in two lines of Python
  and one line of YAML.

----

## Sampling: head vs tail

At Tom Nook's volume, exporting every span costs real money. Two
strategies matter:

| Strategy        | Decision made…                    | The catch                                                                       |
|-----------------|-----------------------------------|---------------------------------------------------------------------------------|
| **Head-based**  | At trace creation (SDK)           | Cheap and stateless, but a flat 10% sample has a **90% chance of dropping your $12 runaway trace** |
| **Tail-based**  | After the trace completes (Collector) | Keep 100% of errors / slow / expensive + 5% of the boring — but needs a `decision_wait` buffer |

- **`decision_wait` is the parameter that breaks people.** Web requests
  finish in milliseconds; **agents run for minutes.** Rule of thumb:
  `decision_wait ≈ 1.5 × p99(agent_duration)` — ~5 s for `nook-mcp`
  today, 30-60 s for a real multi-agent chain
- The production pattern is **hybrid**: a coarse head sample at the SDK
  to cut volume, plus tail-sampling at the Collector to keep the
  interesting tail

----

## The pipeline: `observability/otel-collector-config.yaml`

**Redact, then sample, then batch** — three blocks on top of the §1
file:

```yaml
processors:
  batch:
    timeout: 1s

  # Redaction backstop: scrub anything that looks like an island name
  transform/redact:
    error_mode: ignore
    trace_statements:
      - context: span
        statements:
          - replace_pattern(attributes["messaging.kafka.message.key"],
                            "Tom_Nook|Isabelle|K\\.K\\.|Resetti", "REDACTED")

  # Tail sampling: keep 100% of errors, expensive quotes and token-heavy
  # runs; 5% of everything else
  tail_sampling:
    decision_wait: 5s
    policies:
      - name: errors-always
        type: status_code
        status_code: { status_codes: [ERROR] }
      - name: expensive-quotes          # the runaway quotes Isabelle wanted in §5
        type: numeric_attribute
        numeric_attribute: { key: nook.quote.bells, min_value: 12000000 }
      - name: high-token-cost           # catches the §1 silent loop (no errors!)
        type: numeric_attribute
        numeric_attribute: { key: gen_ai.usage.input_tokens, min_value: 5000 }
      - name: baseline-5pct
        type: probabilistic
        probabilistic: { sampling_percentage: 5 }

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [transform/redact, tail_sampling, batch]   # order matters!
      exporters: [otlp/jaeger, debug]
```

- **Order matters:** redact *before* sampling, or you persist PII in the
  spans you keep; `batch` last
- **`high-token-cost` is the policy the §1 fintech team lacked.** An
  11-hour silent loop has *no errors* — only a token-cost policy catches
  it
- `expensive-quotes` is Isabelle's §5 ask: every quote over twelve
  million Bells goes through, unsampled
- **This is the production default, not bespoke.** AWS Bedrock AgentCore
  Observability and Grafana Cloud ship this exact redact-then-sample
  Collector — different scale, different vendor, same dialect

----

## Compliance: the trace is the audit trail

> Redacted, long-retention traces aren't just for debugging — they are
> **evidence**. Every `tools/call` is a span, so the trace tree *is* the
> automatically-generated log the **EU AI Act (Article 19)** requires;
> content-capture-off plus hashed keys is **GDPR (Article 5)
> minimisation**; and indexing by a *hashed* `enduser.id` (§7) gives you
> GDPR Article 17 erasure-by-hash.

- The audit trail still needs the **who**, not just the *what* — that is
  the column §7 adds.

----

## Stretch: turn on the production pipeline

1. In `observability/otel-collector-config.yaml`, replace the
   `processors:` and `service.pipelines.traces.processors:` sections
   with the YAML above.

2. Restart only the collector — the rest of the stack stays up:

```bash
docker compose restart otel-collector
docker compose logs --tail=20 otel-collector
# Look for: "Everything is ready. Begin running and processing data."
```

3. Re-run the five-room `status_test.py` loop from §5. Roughly
   two-thirds of the traces are now **silently dropped** in the
   Collector; the expensive ones still surface in Jaeger:

```bash
curl -s "http://localhost:16686/api/traces?service=nook-mcp&limit=100" \
  | jq '.data | length'
```

4. Hash the visitor `name` (and any record key) in
   `kafka_client.py::_decode_record` with `_hash_name`, rebuild
   `nook-mcp`, and re-peek `island-visitors` — visitor names now appear
   only as twelve-character hashes.

## Checkpoint

- You can name the three production problems with raw OTel exports
  (PII, cost, blind spots) and **the Collector-side fix for each**
- You understand why **processor order** matters: redact → sample →
  batch
- You can write a tail-sampling policy that keeps every
  `nook.quote.bells > 12_000_000` trace and 5% of the rest
- You can explain why a **token-cost** policy catches failures an
  **errors-only** policy misses

## Return to the slides anytime
