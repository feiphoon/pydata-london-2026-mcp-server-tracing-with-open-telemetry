# 1: Introduction & Setup Check

## TANUKI

**Tom Nook** 🦝 is a mysterious monopolist who seems to be omnipresent on all Animal Crossing Islands and in all financial dealings. He runs Nook Inc, Resident Services & Infrastructure at the Town Hall, and what is effectively the island bank (the currency is **Bells** 🔔💰), and his unsettling twin sons run the island's main shop, Nook's Cranny. It just seems to be very much *all in the family*.

Whenever a player seeks a home expansion, Nook has always offered a dream deal: built in 24 hours, pay later, no interest, no installments, no deadline, and - crucially - standardised pricing.

But no good deed goes unpunished. A recent smear campaign painting him as a manipulative thug and ruthless loan shark has broken him. He's done being reasonable.

He's invited us to be part of his villain origin story: Project TANUKI — officially **Tom's Amazing Nest Upgrade Konsultation Interface**, but actually a greedy dynamic pricing engine that sizes up each player's net worth before quoting them a number... Behind closed doors, it's actually **Tom's Avaricious Nest Upgrade Kost Inflator**.

Now we've learned that TANUKI is actually an MCP-powered AI agent backed by a private Kafka cluster that streams real-time events from across the island: catches, perceived luck, visitor activity, economic signals.

Unfortunately TANUKI's overzealous fleecing is starting to anger players, and we will need
to find out where it all goes wrong. It even makes you *write a plea explaining why you deserve the upgrade* before it prices you — and somehow that makes the bill go *up*?!

Over 90 minutes we'll stand up the full stack, build the MCP server, instrument it with OpenTelemetry, and use the traces to watch TANUKI work.

----

### Why this matters beyond Animal Crossing

- AI agents are moving into production in 2026
- When something goes wrong — a tool call fails silently, an LLM takes
  13 seconds to respond, token costs spike overnight — teams struggle to
  diagnose issues across multi-step agentic workflows
- One fintech company's agent ran in a loop for 11 hours accumulating
  **$47,000** in costs before anyone noticed. Their monitoring showed
  nothing wrong: HTTP 200s, low CPU, no exceptions
- Tom Nook's quotes are the same shape of problem. The bill is real and
  growing; the traditional dashboard is silent

----

### The problem is structural

- AI agents are *interaction-centric* systems, not *computation-centric*
- 98% of wall-clock time lives in LLM API calls and tool executions, not
  in your Python code
- Traditional APM tools, profilers and log aggregators were built for
  the computation-centric world. They cannot answer:
  - Which tool did Tom Nook's agent pick to price the upgrade?
  - How many tokens did the LLM burn rationalising the number?
  - Which of the six Kafka topics is driving the quote up?
  - Where in a five-step chain did the markup get added?
- Distributed tracing answers all of these. It is the primary
  observability signal for agentic AI. Metrics and logs are supporting
  cast

----

### What we will do today

- Check out a FastMCP server (`nook-mcp`) exposing a toolbox of **generic Kafka tools** (`list_topics`, `peek_topic`, `query_topic`, `publish_event`)
  plus Tom Nook's **TANUKI pricing pipeline** — the multiplier tools
  and the flagship `quote_upgrade` composite — over an AC-themed Kafka
  cluster
- OpenTelemetry instrumentation that captures every MCP protocol
  message, every Kafka read and write, and every LLM call automatically
- A local **Jaeger** instance where we visualise the traces and use
  them to investigate the quote
- The skills to debug failures, analyse costs and optimise performance
  from traces — applied to the AC scenario in the room, transferable to
  real production agents on Monday

----

### The tools attendees will be tracing

#### Generic Kafka tools

| Tool            | What Tom Nook uses it for                                     |
| --------------- | ------------------------------------------------------------- |
| `list_topics`   | Inventories everything streaming on the island                |
| `peek_topic`    | Reads the latest N records from a single topic                |
| `query_topic`   | Filters records by key and/or since-time                      |
| `publish_event` | Writes back to `home-upgrade-quotes` once a quote is accepted |

#### TANUKI pricing pipeline

| Tool                                                    | Role in the quote                                              |
| ------------------------------------------------------- | -------------------------------------------------------------- |
| `get_abd_balance` / `apply_wealth_potential_multiplier` | Wealth-tier multiplier from the player's bank balance          |
| `get_todays_catch` / `apply_catch_value_multiplier`     | Visitor bonus (Flick/CJ) on today's catches                    |
| `get_luck_multiplier`                                   | Bonus for rare/high-value catches                              |
| `get_plea_fee_multiplier`                               | "Plea Processing Fee" charged on the justification text        |
| `quote_upgrade`                                         | The flagship composite that chains all of the above, then runs a LLM reasoning step |

#### Island-action tools

| Tool         | What it does                                                       |
| ------------ | ----------------------------------------------------------------- |
| `shake_tree` | Shake a tree for fruit, Bells or a wasp nest — pure compute, no Kafka or LLM, so it produces the **simplest** two-span trace (and a clean error trace on an unknown tree) |

- `quote_upgrade` is the trace you will be staring at most of today —
  it reads three topics (`abd-balance`, `catch-log`, `island-visitors`)
  via the pipeline above and is the one Tom Nook uses to set the bill
- Every other tool produces a simpler trace shape, which is exactly
  why we keep them in the toolbox — `shake_tree` is the minimal case,
  a single application span under the MCP protocol span

----

### Setup check (hands-on)

Confirm the platform Tom Nook (and we) need is up by following `SETUP.md`
