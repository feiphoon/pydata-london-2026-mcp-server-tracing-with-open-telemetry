# Observing Agentic AI in Production: MCP Server Tracing with OpenTelemetry and Animal Crossing

Workshop materials for **"Observing Agentic AI in Production: MCP Server
Tracing with OpenTelemetry and Animal Crossing"** at PyData London 2026.

**Tom Nook** 🦝 is a mysterious monopolist who seems to be omnipresent on all Animal Crossing Islands and in all financial dealings. He runs Nook Inc, Resident Services & Infrastructure at the Town Hall, and what is effectively the island bank (the currency is **Bells** 🔔💰), and his unsettling twin sons run the island's main shop, Nook's Cranny. It just seems to be very much *all in the family*.

Whenever a player seeks a home expansion, Nook has always offered a dream deal: built in 24 hours, pay later, no interest, no installments, no deadline, and - crucially - standardised pricing.

But no good deed goes unpunished. A recent smear campaign painting him as a manipulative thug and ruthless loan shark has broken him. He's done being reasonable.

He's invited us to be part of his villain origin story: Project TANUKI — officially **Tom's Amazing Nest Upgrade Konsultation Interface**, but actually a greedy dynamic pricing engine that sizes up each player's net worth before quoting them a number... Behind closed doors, it's actually **Tom's Avaricious Nest Upgrade Kost Inflator**.

To power it, we're building him an MCP-powered AI agent backed by a private Kafka cluster streaming real-time events from across the island: catches, perceived luck, visitor activity, economic signals. Tom sees everything.

Unfortunately TANUKI's overzealous fleecing is starting to anger players, and we will need
to find out where it all goes wrong. It even makes you *write a plea explaining why you deserve the upgrade* before it prices you — and somehow that makes the bill go *up*?!

Over 90 minutes we'll stand up the full stack, instrument Nook MCP with OpenTelemetry, and use the traces to watch TANUKI work.

| Path                              | Purpose                                                       |
| --------------------------------- | ------------------------------------------------------------- |
| `docker-compose.yml`              | Lenses Community Edition base (Lenses HQ, Apache Kafka, Postgres).   |
| `docker-compose.override.yml`     | Adds `nook-mcp`, `nook-seeder`, OTel Collector and Jaeger.    |
| `mcp/`                            | Nook MCP server (`fastmcp==3.2.3`, `aiokafka`).             |
| `mcp/src/nook_mcp/server.py`      | FastMCP wiring; registers the tools.                          |
| `mcp/src/nook_mcp/tools.py`       | The five tool functions exposed over MCP.                     |
| `mcp/src/nook_mcp/kafka_client.py`| Async helpers around aiokafka.           |
| `seed/`                           | Continuous producer of Animal Crossing themed Kafka records.  |
| `observability/`                  | OTel Collector configuration (Jaeger + debug exporter).       |
| `workshop/`                       | Section walkthroughs of the workshop, including the presentation slides.    |

Note: The default Lenses MCP server `lensesio/mcp:6.2` service is overridden by Nook MCP and is gated behind a compose profile so it can still be run for reference:
`ACCEPT_EULA=true docker compose --profile with-lenses-mcp up`.
