# Set Up Your Environment to Interact with TANUKI

You will be setting up three important components, over a two-part installation process.

1. Installation part 1: Our TANUKI observability stack (Lenses, Kafka, Nook MCP server, OTel collector and Jaeger) via Docker Compose.
2. Installation part 2: Access to an LLM, served locally via Ollama (we will use the Qwen3.5 model).
3. Installation part 2: OpenCode as the chat/prompt terminal client, with the Nook MCP server connected.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation part 1](#installation-part-1)
  - [Bring the TANUKI stack up](#bring-the-tanuki-stack-up)
  - [Smoke test](#smoke-test)
- [Installation part 2](#installation-part-2)
  - [Set Up Ollama](#set-up-ollama)
  - [Set Up OpenCode](#set-up-opencode)
    - [Chat with OpenCode](#chat-with-opencode)
- [After this workshop](#after-this-workshop)

## Prerequisites

- **Have roughly 4GB of RAM and disk space free on your machine, for the containers.**
- A code editor (VS Code, Cursor, Kiro or similar)

## Installation part 1

1. EITHER [Docker](https://docs.docker.com/get-started/get-docker/) or [OrbStack](https://orbstack.dev/download), to run Docker Compose
2. [uv](https://docs.astral.sh/uv/getting-started/installation/#installation-methods) for Python package management

### Bring the TANUKI stack up

```bash
ACCEPT_EULA=true docker compose up --build
```

The first run takes a few minutes while images pull and the two local
images (`nook-mcp`, `nook-seeder`) build.

Expected services:

| Service         | URL                         | Purpose                                             |
| --------------- | --------------------------- | --------------------------------------------------- |
| Lenses HQ       | <http://localhost:9991>     | `admin`/`admin` — OAuth issuer. |
| nook-mcp        | <http://localhost:8000/mcp> | The MCP server that we will instrument.                |
| Jaeger UI       | <http://localhost:16686>    | Trace visualisation.                                |
| OTel Collector  | OTLP on `:4317`/`:4318`     | Receives spans from `nook-mcp`.                     |
| Kafka (host)    | `localhost:9092`            | For host-side tools (e.g. `kcat`).                  |
| Schema Registry | <http://localhost:8081>     | Available, not used in the workshop.              |

### Smoke test

Once `docker compose up` reaches a steady state, three things should be
true.

**1. The seeder has created and is filling the topics.** Run this from the
broker container to list available topics:

```bash
docker compose exec demo-kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
```

You should see at least these topics:

```text
abd-balance
catch-log
island-visitors
```

Now, sample some records from the balance topic:

```bash
docker compose exec demo-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic abd-balance --max-messages 3 --from-beginning
```

**2. `nook-mcp` is responding and can reach Kafka.** Call its tools
through a minimal `fastmcp` client from any Python 3.12+ environment with
`fastmcp==3.2.3` installed (e.g. the repo root):

See `smoke_test.py` in the repo root.

Run it:

```bash
uv run --with fastmcp==3.2.3 python smoke_test.py
```

Expected (abridged):

```text
tools: ['get_current_month', 'get_abd_balance_multiplier', ..., 'quote_upgrade']
topics: ['abd-balance', 'catch-log', 'home-upgrade-quotes', 'island-visitors']
quote: {'room': 'second-floor', 'base_bells': 2000000, 'bells': 3241800, ...}
```

**3. Traces are flowing into Jaeger.** Open <http://localhost:16686>,
pick `nook-mcp` from the **Service** dropdown, optionally filter by
**Operation** `tools/call list_topics`, and click **Find Traces**.

## Installation part 2

3. Local Ollama for LLM access
4. OpenCode, to talk to the MCP server

### Set Up Ollama

Download from <https://ollama.com/download>
OR `brew install ollama` if you're on MacOS.

> If you're on MacOS and you later find that Ollama stalls on run, re-download using `curl` rather than Homebrew.

Open your terminal and run:

```bash
# Pull and download ~1GB
ollama pull qwen3.5:0.8b

# Confirm it's downloaded
ollama list

# Run the model
ollama run qwen3.5:0.8b "Hello, what model are you? Don't overthink it."

# Confirm it's running
ollama ps

# You should see something like this
# NAME         ID              SIZE      PROCESSOR    CONTEXT    UNTIL
# qwen3.5:0.8b    f3817196d142    2.8 GB    100% GPU     32768      4 minutes from now
```

### Set Up OpenCode

Download OpenCode Terminal from <https://opencode.ai/download>
OR `brew install opencode` if you're on MacOS.

Open your terminal and run:

```bash
ollama launch opencode --config
```

In config menu, choose `qwen3.5:0.8b` (right at the bottom), **but do not launch OpenCode yet**. We want to connect the Nook MCP server first.

```bash
# List MCP servers
opencode mcp list

# Start steps to add Nook MCP. Choose:
# Location: Current project
# Server name: Nook MCP
# Server type: Remote
# Server URL: http://localhost:8000/mcp
# Does this server require OAuth authentication? Yes
# Do you have a pre-registered client ID? No
opencode mcp add

# Manually authenticate Nook MCP server.
# You will get a browser popup dialog to authenticate with Lenses.
# Log in with username:password of admin:admin
# If already authenticated, you will see an authorisation consent dialogue. Check to allow all and hit the Authorise button.
opencode mcp auth "Nook MCP"
```

#### Chat with OpenCode

```bash
opencode
```

Inside the OpenCode session, type the following prompt to verify you're connecting to Nook MCP:

```bash
List my topics
```

You should see the topics from earlier.

## After this workshop

[See TEARDOWN](TEARDOWN.md).
