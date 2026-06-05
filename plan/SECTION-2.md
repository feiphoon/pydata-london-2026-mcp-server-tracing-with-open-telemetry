# 2: Read Tom Nook's MCP Server

## Tom Nook talks tools, not topics

- Tom Nook's agent does **not** open a Kafka consumer. It does not know
  what a partition is. It has never seen a broker
- It talks to an **MCP server** that exposes Kafka through a small,
  schema-described set of **tools**
- That server — `nook-mcp` — is what we are going to read and instrument with OpenTelemetry

```bash
  ┌──────────────┐  JSON-RPC 2.0   ┌─────────────┐   aiokafka    ┌───────────┐
  │ Tom Nook's   │ ──────────────▶ │  nook-mcp   │ ────────────▶ │ demo-kafka│
  │  agent (LLM) │ ◀────────────── │  (FastMCP)  │ ◀──────────── │  (broker) │
  └──────────────┘   tools/call    └─────────────┘  records/ack  └───────────┘
```

----

## Model Context Protocol (MCP)

- **MCP**: an open standard for connecting LLMs and
  AI agents to your tools and data
- Open-sourced by Anthropic in late 2024; adopted by major IDEs (Cursor,
  VS Code), AI assistants (Claude, ChatGPT) and enterprise platforms
  inside 12 months
- Think of it as **"USB-C for AI"**: one standard interface between
  agents and the long tail of tools they need to act on the world.
  Without MCP, every agent-to-tool integration is bespoke
- Wire format: **JSON-RPC 2.0** over STDIO (for local tools) or
  **Streamable HTTP** (for remote servers). `nook-mcp` runs over HTTP
- Because the wire format is open and the schemas are introspectable,
  the same MCP server can be driven by Claude, Cursor, OpenAI's agents,
  a Python client

----

## What happens when Tom Nook prices one quote

The full JSON-RPC conversation for a single `quote_upgrade` call. Five messages, every one of them tagged later by OpenTelemetry as a span:

| Step | Method                        | Direction         | Purpose                                                          |
|------|-------------------------------|-------------------|------------------------------------------------------------------|
| 1    | `initialize`                  | client → server   | Exchange protocol version and capabilities                       |
| 2    | `notifications/initialized`   | client → server   | "I'm ready"                                                      |
| 3    | `tools/list`                  | client → server   | Discover available tools and their JSON Schemas                  |
| 4    | `tools/call quote_upgrade`    | client → server   | Invoke the tool with `{room, justification}`                     |
| 5    | result                        | server → client   | Bells figure + multipliers + LLM reasoning                       |

- The LLM uses the **tool descriptions** and **schemas** returned in
  step 3 to decide *which* tool to call and *with what arguments* in
  step 4. It never sees Kafka, never sees Python
- Each of these messages becomes a span in a trace

----

## FastMCP turns Python into MCP

- **FastMCP** is the de facto standard framework for MCP servers in
  Python (MIT-licensed). We're pinning **`fastmcp==3.2.3`**
- Four pieces of Python become four pieces of protocol:

| In Python                          | Becomes in MCP                              |
| ---------------------------------- | ------------------------------------------- |
| `@mcp.tool` on an `async def`      | A tool advertised by `tools/list`           |
| Type-annotated parameters          | A JSON Schema the LLM uses to fill in args  |
| The docstring (`"""..."""`)        | The tool description the LLM **reads to decide whether to call it** |
| `raise SomeError(...)`             | An `isError: true` result with the message  |

- The docstring is not just documentation for humans. It is **context to the LLM**. Treat it accordingly
- FastMCP also handles all the underlying machinery: JSON-RPC framing,
  argument validation, error formatting, content-type negotiation,
  session IDs — and as we will discover in §4, **OpenTelemetry
  auto-instrumentation** for every `tools/call` it serves

----

## Anatomy of `nook-mcp`

The server is split across three files. Each file owns one concern

| File                                  | What it owns                                                                         | Will produce span layer |
| ------------------------------------- | ------------------------------------------------------------------------------------ | ----------------------- |
| `mcp/src/nook_mcp/server.py`          | FastMCP wiring (~30 lines). Registers the tools, runs over HTTP on port 8000         | Layer 1 (auto, via FastMCP) |
| `mcp/src/nook_mcp/tools.py`           | The tool functions and their docstrings                                              | Layer 2 + layer 3b      |
| `mcp/src/nook_mcp/kafka_client.py`    | Thin async helpers around `aiokafka`. **No FastMCP code in here**                    | Layer 3a                |

- Layered on purpose: the protocol layer, the application layer and
  the transport layer are physically separated in three files
- That separation is what makes a trace read cleanly — you can see exactly which file produced each span

----

## The flagship tool: `quote_upgrade`

Tom Nook actually uses this to set the quote:

```python
async def quote_upgrade(room: str, justification: str) -> dict:
    """Compute Tom Nook's inflated quote (in Bells) for a home upgrade."""
    # reads abd-balance, catch-log and island-visitors via a chain of
    # sub-tools (get_abd_balance, get_todays_catch, apply_catch_value_
    # multiplier, get_luck_multiplier, apply_wealth_potential_multiplier,
    # get_plea_fee_multiplier), then calls the LLM and eturns the Bells figure
```

- **Three topic reads + a chain of multiplier sub-tools + one
  LLM call + a bit of Python.** That composite is exactly
  the shape that turns into the multi-layer span waterfall we will be reading for the rest of the workshop

----

## Exercise: Use Tom Nook's MCP Server

### Objective

Tom Nook has already built the server. Your job in this exercise is
to prove you can use his server and read his code. By the end you
should be able to point at every byte of every JSON-RPC message that
crosses between client and server.

### Steps

1. Confirm `nook-mcp` is still up from the §1 setup check:

```bash
docker compose ps nook-mcp
# Expect: STATUS = up/running, PORTS = 0.0.0.0:8000->8000/tcp
```

2. Open three files in your editor side-by-side and skim over them:

| File                                    | What to notice                                                            |
| --------------------------------------- | ------------------------------------------------------------------------- |
| `mcp/src/nook_mcp/server.py`            | One `FastMCP("nook-mcp")` instance, a tuple of `mcp.tool(func)` registrations. ~30 lines total |
| `mcp/src/nook_mcp/tools.py`             | The `async def` tool functions, type-annotated signatures, rich docstrings (these are the LLM-facing prompts) |
| `mcp/src/nook_mcp/kafka_client.py`      | Pure `aiokafka`. No FastMCP imports anywhere — the file would still compile if MCP did not exist |

3. Use the server with a minimal client. Create
   `status_test.py` at the repo root:

```python
import asyncio
from fastmcp import Client


async def main() -> None:
    async with Client("http://localhost:8000/mcp") as client:
        # tools/list — discover the menu
        tools = await client.list_tools()
        print(f"Tom Nook exposes {len(tools)} tools:")
        for t in tools:
            first_line = t.description.splitlines()[0]
            print(f"  - {t.name}: {first_line}")

        # tools/call — list_topics (no arguments)
        print()
        topics = await client.call_tool("list_topics", {})
        print(f"He is watching {len(topics.data)} topics: {topics.data}")

        # tools/call — peek_topic with arguments
        print()
        peek = await client.call_tool(
            "peek_topic",
            {"topic": "home-upgrade-quotes", "limit": 3},
        )
        print(f"Recent quotes (count={peek.data['count']}):")
        for r in peek.data["records"]:
            print(f"  {r['key']!r:<14} -> {r['value']}")


asyncio.run(main())
```

Run it:

```bash
uv run --with fastmcp==3.2.3 python status_test.py
```

You should see every tool name with its first-line docstring, the four
AC topics, and zero or more recent quote records. Those first-line
docstrings are exactly what Tom Nook's LLM sees when it picks a tool.

### Expected output (abridged)

```text
Tom Nook exposes 16 tools:
  - list_topics: List all Kafka topics on Tom Nook's cluster.
  - peek_topic: Return the most recent messages from a Kafka topic, newest first.
  - query_topic: Filter messages on a topic by key and/or a since-timestamp.
  - publish_event: Produce a single JSON event to a Kafka topic.
  - quote_upgrade: Compute Tom Nook's inflated quote (in Bells) for a home upgrade.
  - get_abd_balance: Read the player's current ABD (Automated Bell Dispenser) balance.
  - get_todays_catch: Return the player's catches from today with their names, types and sell values.
  - shake_tree: Shake a tree and see what falls out.
  - ... (multiplier and red-herring tools) ...
  - latest_quote: Return Tom Nook's most recent home-upgrade quote, or None if none exist.

He is watching 4 topics: ['abd-balance', 'catch-log', 'home-upgrade-quotes',
                          'island-visitors']

Recent quotes (count=2):
  'Tom_Nook' -> {'room': 'second-floor', 'bells': 14203456, ...}
  ...

Latest quote: {'topic': 'home-upgrade-quotes', 'partition': 0, 'offset': 17, ...}

Quote: 14,203,456 Bells
  multipliers: {'abd': 3.0, 'catch': 1.5, 'luck': 2.0, 'plea': 0.9}
```

### What to look for

- The **docstring** of `latest_quote` is what an LLM client would
  read in `tools/list` to decide when to call it. Treat docstrings as
  prompts, not just documentation
- The signature `() -> dict | None` is enough for FastMCP to generate
  the JSON Schema for parameters (empty object) and the return shape
- You did **not** touch `kafka_client.py`. The split between
  protocol-level code (FastMCP), tool logic (`tools.py`) and
  transport-level code (`kafka_client.py`) is preserved

### Checkpoint

- The stack is still up; the shipped `nook-mcp` registers **16** tools
- You can articulate why Tom Nook's server is currently opaque, and
  what kind of signal would make it transparent

## Return to the slides
