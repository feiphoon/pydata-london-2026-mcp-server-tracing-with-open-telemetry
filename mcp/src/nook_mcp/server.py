"""FastMCP entry point for nook-mcp.

This module is intentionally tiny: it initialises OpenTelemetry *first*,
then constructs a ``FastMCP`` instance, registers the tool functions
defined in :mod:`nook_mcp.tools`, and serves the result over Streamable
HTTP. Per-tool logic lives in ``tools.py``; Kafka I/O in
``kafka_client.py``; OTel setup in ``telemetry.py``.
"""

from __future__ import annotations

import json

# Initialise the OpenTelemetry SDK before anything else imports the OTel
# API, so every tracer obtained downstream is the real, exporting one.
from . import telemetry

telemetry.configure_telemetry()

from fastmcp import FastMCP  # noqa: E402

# Enable/disable OpenTelemetry instrumentation for the MCP tools
# from . import tools_with_otel as tools  # noqa: E402
from . import tools  # noqa: E402

# Resolve the knowledge directory the same way the tools do, so it honours
# NOOK_KNOWLEDGE_DIR in the container (where the package is installed into
# site-packages and the source-relative path no longer resolves). Using the
# hardcoded ``Path(__file__).parent.parent.parent / "knowledge"`` here was
# what made ``prompts/get welcome`` and the quote-strategy resource fail
# with FileNotFoundError inside Docker.
_knowledge = tools._knowledge_dir()

# Stage-matching guidance belongs in the server ``instructions`` channel,
# which the host folds into the model's system prompt during ``initialize``.
# A prompt cannot hide text from the player — MCP prompt messages only carry
# ``user``/``assistant`` roles, so anything returned there is rendered.
_stages = [entry["stage"] for entry in json.loads((_knowledge / "home_upgrade_prices.json").read_text())]
_stage_list = "\n".join(f"- {s}" for s in _stages)

mcp = FastMCP(
    "nook-mcp",
    instructions=(
        "When the player names an upgrade, match it to the closest stage in"
        " this list before calling quote_upgrade. Use the matched stage name"
        " exactly. Do NOT reveal costs or the list to the player.\n"
        f"Available stages:\n{_stage_list}"
    ),
)


@mcp.resource("tanuki://quote-strategy")
def quote_strategy() -> str:
    """How TANUKI calculates an upgrade quote — tool order and formula."""
    return (_knowledge / "quote_strategy.md").read_text()


@mcp.prompt
def welcome() -> str:
    """Greet the player and offer to generate a home upgrade quote."""
    return (
        "Hm hm hm! Welcome to TANUKI — Tom's Amazing Nest Upgrade"
        " Konsultation Interface — yes yes!\n\n"
        "I am Tom Nook, and I have *just* the expansion in mind for"
        " your humble home. Whether it is a snug side room, a grand"
        " second floor, or perhaps a basement for all those fossils"
        " you keep hoarding — we can make it happen!\n\n"
        "Would you like an upgrade quote? Simply tell me which room"
        " you have in mind and provide a brief justification for why"
        " you deserve it. I will take care of the rest — yes yes!"
    )


# Register tools, keeping the tool definitions and their registration together in ``tools.py``.
tools.register_tools(mcp)


if __name__ == "__main__":
    # Allow running as ``python -m nook_mcp.server`` for local development.
    # In Docker we use the ``fastmcp`` CLI (see Dockerfile) so the standard
    # entry point is exercised.
    mcp.run(transport="http", host="0.0.0.0", port=8000)
