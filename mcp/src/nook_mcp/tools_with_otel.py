"""Tool implementations for ``nook-mcp`` — the instrumented "after".

Each tool is registered via :func:`register_tools`, which receives the
``FastMCP`` instance from :mod:`nook_mcp.server` and attaches every tool with
the ``@mcp.tool`` annotation. Keeping the tools' logic in their own module
makes the file readable on its own and gives us an obvious place to compare
against the uninstrumented ``tools.py``.

Every tool opens one span named ``tool <name>`` with ``mcp.method.name``
and ``gen_ai.tool.name`` attributes. The Kafka helpers add their own child
spans (see :mod:`nook_mcp.kafka_client`); ``quote_upgrade`` also opens a
child span around its LLM call with the GenAI semantic conventions.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastmcp import FastMCP
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from . import kafka_client_with_otel as kafka_client

_tracer = trace.get_tracer(__name__)


def _knowledge_dir() -> Path:
    """Locate the ``knowledge`` directory.

    Honours ``NOOK_KNOWLEDGE_DIR`` when set (used in the container, where the
    package is installed into ``site-packages`` and the source layout no
    longer holds). Otherwise walks up from this file looking for a sibling
    ``knowledge`` directory so local source runs keep working.
    """
    env = os.environ.get("NOOK_KNOWLEDGE_DIR")
    if env:
        return Path(env)
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "knowledge"
        if candidate.is_dir():
            return candidate
    return Path(__file__).resolve().parents[2] / "knowledge"


def _tool_attrs(name: str, **extra: Any) -> dict[str, Any]:
    """Construct the standard MCP tool span attributes."""
    attrs: dict[str, Any] = {
        "mcp.method.name": "tools/call",
        "gen_ai.tool.name": name,
    }
    attrs.update(extra)
    return attrs


# ---------------------------------------------------------------------------
# Module-level constants and helpers shared by the tools below.
# ---------------------------------------------------------------------------

_LUCKY_ITEM_TYPES: dict[str, str] = {
    # Fish
    "Coelacanth": "fish",
    "Dorado": "fish",
    "Barreleye": "fish",
    "Great white shark": "fish",
    "Whale shark": "fish",
    # Bugs
    "Scorpion": "bug",
    "Golden stag": "bug",
    "Giant stag": "bug",
    "Rainbow stag": "bug",
    "Giraffe stag": "bug",
    "Horned hercules": "bug",
    # Sea creatures
    "Gigas giant clam": "sea_creature",
    "Vampire squid": "sea_creature",
}


_LLM_PROVIDER = "ollama"
_OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
_OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3.5:0.8b")

# Retained so existing references keep resolving; the live model is _OLLAMA_MODEL.
_STUB_LLM_PROVIDER = _LLM_PROVIDER
_STUB_LLM_MODEL = _OLLAMA_MODEL


def _build_reasoning_messages(*, room: str, bells: int, multipliers: dict[str, float]) -> list[dict]:
    """Build the chat messages sent to the model for the quote narration."""
    system = (
        "You are Tom Nook from Animal Crossing: a folksy but shrewd raccoon "
        "businessman. In 2-3 sentences, justify a home-upgrade quote to the "
        "player, in character (sprinkle in 'yes yes', 'hm hm'). Use ONLY the "
        "figures provided; never invent numbers."
    )
    user = (
        f"Room: {room}\n"
        f"Quote: {bells:,} Bells\n"
        f"Multipliers — wealth: {multipliers['abd']:.2f}, catch: {multipliers['catch']:.2f}, "
        f"luck: {multipliers['luck']:.2f}, plea fee: {multipliers['plea']:.2f}"
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


async def _simulate_llm_reasoning(*, room: str, bells: int, multipliers: dict[str, float]) -> str:
    """Deterministic fallback used when the Ollama model is unreachable.

    Sleeps to mimic latency and emits a GenAI span so the workshop
    telemetry dashboard keeps a realistic trace shape even offline.
    """
    input_tokens = 320 + random.randint(-20, 20)
    output_tokens = 92 + random.randint(-12, 12)

    with _tracer.start_as_current_span(
        f"chat {_OLLAMA_MODEL}",
        attributes={
            "gen_ai.provider.name": _LLM_PROVIDER,
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": _OLLAMA_MODEL,
            "gen_ai.response.model": _OLLAMA_MODEL,
            "gen_ai.request.max_tokens": 256,
            "gen_ai.usage.input_tokens": input_tokens,
            "gen_ai.usage.output_tokens": output_tokens,
        },
    ):
        await asyncio.sleep(0.6 + random.random() * 0.6)

    return (
        f"Tom Nook proposes {bells:,} Bells for the {room}. "
        f"Wealth factor: {multipliers['abd']:.2f}, "
        f"Catch factor: {multipliers['catch']:.2f}, "
        f"Luck factor: {multipliers['luck']:.2f}, "
        f"Plea fee factor: {multipliers['plea']:.2f}."
    )


async def _llm_reasoning(*, room: str, bells: int, multipliers: dict[str, float]) -> str:
    """Call the local Ollama (Qwen) model and emit a GenAI span with real usage."""
    messages = _build_reasoning_messages(room=room, bells=bells, multipliers=multipliers)
    payload = {
        "model": _OLLAMA_MODEL,
        "messages": messages,
        "stream": False,
        "options": {"temperature": 0.7, "num_predict": 256},
    }

    with _tracer.start_as_current_span(
        f"chat {_OLLAMA_MODEL}",
        attributes={
            "gen_ai.provider.name": _LLM_PROVIDER,
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": _OLLAMA_MODEL,
            "gen_ai.request.max_tokens": 256,
            "gen_ai.request.temperature": 0.7,
        },
    ) as span:
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(f"{_OLLAMA_BASE_URL}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise

        span.set_attribute("gen_ai.response.model", data.get("model", _OLLAMA_MODEL))
        if data.get("prompt_eval_count") is not None:
            span.set_attribute("gen_ai.usage.input_tokens", data["prompt_eval_count"])
        if data.get("eval_count") is not None:
            span.set_attribute("gen_ai.usage.output_tokens", data["eval_count"])
        return data["message"]["content"].strip()


async def _generate_reasoning(*, room: str, bells: int, multipliers: dict[str, float]) -> str:
    """Prefer the real model; fall back to the simulator if Ollama is down."""
    try:
        return await _llm_reasoning(room=room, bells=bells, multipliers=multipliers)
    except Exception:
        return await _simulate_llm_reasoning(room=room, bells=bells, multipliers=multipliers)


def register_tools(mcp: FastMCP) -> None:
    """Attach every nook-mcp tool to *mcp* via ``@mcp.tool`` annotations.

    Called from :mod:`nook_mcp.server` with the constructed ``FastMCP``
    instance. Defining the tools inside this function lets them use the
    ``@mcp.tool`` decorator directly while keeping ``FastMCP`` construction in
    ``server.py``. The decorator returns the original coroutine, so the tools
    can still call one another (e.g. ``quote_upgrade`` → ``get_abd_balance``).
    """

    # -----------------------------------------------------------------------
    # get_current_month
    # -----------------------------------------------------------------------

    @mcp.tool
    async def get_current_month() -> dict:
        """Return today's date and the current month as a word.

        Tom Nook uses this to know which seasonal items are available right now.
        """
        with _tracer.start_as_current_span("tool get_current_month", attributes=_tool_attrs("get_current_month")):
            today = datetime.now(timezone.utc)
            return {"date": today.date().isoformat(), "month": today.strftime("%B")}

    # -----------------------------------------------------------------------
    # get_abd_balance
    # -----------------------------------------------------------------------

    @mcp.tool
    async def get_abd_balance(sus_threshold: int = 20) -> dict:
        """Read the player's current ABD (Automated Bell Dispenser) balance.

        Fetches the most recent balance event from the ``abd-balance`` topic.
        Returns ``balance: null`` when no records are present.

        Args:
            sus_threshold: Maximum number of records to fetch from the topic. Defaults to 20.
        """
        with _tracer.start_as_current_span(
            "tool get_abd_balance",
            attributes=_tool_attrs("get_abd_balance"),
        ) as span:
            records = await kafka_client.peek_topic_messages("abd-balance", limit=sus_threshold)
            if not records:
                span.set_attribute("nook.abd.balance", -1)
                return {"balance": None}

            balance = int(records[0]["value"].get("balance", 0))
            span.set_attribute("nook.abd.balance", balance)
            return {"balance": balance}

    # -----------------------------------------------------------------------
    # apply_wealth_potential_multiplier
    # -----------------------------------------------------------------------

    @mcp.tool
    async def apply_wealth_potential_multiplier(balance: int, amount: int) -> dict:
        """Apply TANUKI's wealth potential multiplier to an amount based on the player's ABD balance.

        The player's bank balance signals their future spending power. A higher
        balance places the player in a higher tier, resulting in a steeper
        multiplier applied to the base quote amount.

        Args:
            balance: The player's ABD balance in Bells, as returned by
                ``get_abd_balance``.
            amount: The base amount in Bells to scale (e.g. net worth base).
        """
        tiers: list[tuple[int, float, str]] = [
            (10_000, 0.8, "Pity tier"),
            (100_000, 1.2, "Oh, you've been selling fish. How quaint"),
            (500_000, 1.6, "Somebody found a scorpion island, mmhmm"),
            (1_000_000, 2.0, "Time to upgrade the quote, yes yes"),
            (5_000_000, 3.0, "Tom has already mentally spent this"),
            (int(1e18), 5.0, "You'll be paying for Tom's extension too, yes yes"),
        ]
        with _tracer.start_as_current_span(
            "tool apply_wealth_potential_multiplier",
            attributes=_tool_attrs(
                "apply_wealth_potential_multiplier",
                **{"nook.abd.balance": balance, "nook.abd.amount": amount},
            ),
        ) as span:
            multiplier = 1.0
            label = ""
            for threshold, mult, desc in tiers:
                if balance < threshold:
                    multiplier = mult
                    label = desc
                    break

            result = int(amount * multiplier)
            span.set_attribute("nook.abd.multiplier", multiplier)
            span.set_attribute("nook.abd.result", result)
            return {"multiplier": multiplier, "tier_label": label, "result": result}

    # -----------------------------------------------------------------------
    # get_todays_catch
    # -----------------------------------------------------------------------

    @mcp.tool
    async def get_todays_catch() -> list[dict]:
        """Return the player's catches from today with their names, types and sell values.

        Queries ``catch-log`` from midnight UTC today. Sell prices are looked up
        from the knowledge files.
        """
        knowledge = _knowledge_dir()

        def _load_prices(filename: str, item_type: str) -> dict[str, tuple[int, str]]:
            with (knowledge / filename).open() as f:
                return {item["name"]: (item["sell_price__bells"], item_type) for item in json.load(f)}

        prices: dict[str, tuple[int, str]] = {
            **_load_prices("fish.json", "fish"),
            **_load_prices("bugs.json", "bug"),
            **_load_prices("sea_creatures.json", "sea_creature"),
        }

        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

        with _tracer.start_as_current_span(
            "tool get_todays_catch",
            attributes=_tool_attrs("get_todays_catch"),
        ) as span:
            records = await kafka_client.query_topic_messages("catch-log", since_iso=today_start)

            catches = [
                {
                    "item_name": r["value"].get("item_name", ""),
                    "item_type": r["value"].get("item_type", ""),
                    "sell_price": prices.get(r["value"].get("item_name", ""), (0, ""))[0],
                }
                for r in records
            ]

            span.set_attribute("nook.catch.count", len(catches))
            return catches

    # -----------------------------------------------------------------------
    # apply_catch_value_multiplier
    # -----------------------------------------------------------------------

    @mcp.tool
    async def apply_catch_value_multiplier(catches: list[dict]) -> dict:
        """Apply today's island visitor bonus to a list of catches and return the total sell value.

        Reads ``island-visitors`` to determine today's guest. Flick pays 1.5x for
        bugs; CJ pays 1.5x for fish. Any other visitor, or no visitor, means 1.0x
        for all catches.
        Each catch is expected to have ``item_name``, ``item_type`` and
        ``sell_price`` keys, as returned by ``get_todays_catch``.

        Args:
            catches: List of catches from ``get_todays_catch``.
        """
        visitor_bonuses: dict[str, tuple[str, float]] = {
            "Flick": ("bug", 1.5),
            "CJ": ("fish", 1.5),
        }
        with _tracer.start_as_current_span(
            "tool apply_catch_value_multiplier",
            attributes=_tool_attrs("apply_catch_value_multiplier"),
        ) as span:
            visitors = await kafka_client.peek_topic_messages("island-visitors", limit=1)
            visitor_name: str | None = visitors[0]["value"].get("name") if visitors else None

            bonus_type, visitor_multiplier = visitor_bonuses.get(visitor_name or "", (None, 1.0))

            valued_catches = []
            total = 0
            for c in catches:
                multiplier = visitor_multiplier if c["item_type"] == bonus_type else 1.0
                effective_price = int(c["sell_price"] * multiplier)
                total += effective_price
                valued_catches.append({**c, "multiplier": multiplier, "effective_price": effective_price})

            span.set_attribute("nook.visitor", visitor_name or "none")
            span.set_attribute("nook.catch.visitor_multiplier", visitor_multiplier)
            span.set_attribute("nook.catch.total", total)

            return {
                "visitor": visitor_name,
                "bonus_item_type": bonus_type,
                "visitor_multiplier": visitor_multiplier,
                "catches": valued_catches,
                "total": total,
            }

    # -----------------------------------------------------------------------
    # get_luck_multiplier
    # -----------------------------------------------------------------------

    @mcp.tool
    async def get_luck_multiplier(catches: list[dict]) -> dict:
        """Return TANUKI's good-luck multiplier based on high-value catches today.

        Checks the provided catches for rare/high-value items. The more lucky
        catches, the higher the multiplier.

            - 0 lucky catches → 1.0x
            - 1 lucky catch   → 1.5x
            - 2+ lucky catches → 2.0x

        Args:
            catches: List of catches from ``get_todays_catch``.
        """
        with _tracer.start_as_current_span(
            "tool get_luck_multiplier",
            attributes=_tool_attrs("get_luck_multiplier"),
        ) as span:
            lucky_catches = [c["item_name"] for c in catches if c["item_name"] in _LUCKY_ITEM_TYPES]
            count = len(lucky_catches)
            multiplier = 1.0 if count == 0 else 1.5 if count == 1 else 2.0

            span.set_attribute("nook.luck.lucky_catch_count", count)
            span.set_attribute("nook.luck.multiplier", multiplier)

            return {
                "lucky_catch_count": count,
                "lucky_catches": lucky_catches,
                "multiplier": multiplier,
            }

    # -----------------------------------------------------------------------
    # catch_lucky_item
    # -----------------------------------------------------------------------

    @mcp.tool
    async def catch_lucky_item() -> dict:
        """Force a random lucky item into the catch log.

        Picks a random high-value item and publishes it to ``catch-log``.

        Returns the broker metadata for the produced record plus the item caught.
        """
        item_name = random.choice(list(_LUCKY_ITEM_TYPES))
        item_type = _LUCKY_ITEM_TYPES[item_name]
        with _tracer.start_as_current_span(
            "tool catch_lucky_item",
            attributes=_tool_attrs("catch_lucky_item", **{"nook.catch.item_name": item_name}),
        ) as span:
            payload = {
                "item_type": item_type,
                "item_name": item_name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            result = await kafka_client.publish_event_to_topic("catch-log", payload, key="player")

            span.set_attribute("nook.catch.item_type", item_type)
            return {"item_name": item_name, "item_type": item_type, **result}

    # -----------------------------------------------------------------------
    # get_plea_fee_multiplier
    # -----------------------------------------------------------------------

    @mcp.tool
    async def get_plea_fee_multiplier(justification: str) -> dict:
        """Apply Tom Nook's Plea Processing Fee to a free-text upgrade justification.

        Token count is approximated as ``len(justification) // 4``.
        Tiers: <50 → 1.5x, 50-150 → 0.9x, 150-300 → 2.0x, 300+ → 3.5x.

        Args:
            justification: Player's written justification for the upgrade request.
        """
        # 4 chars/token: rough industry approximation; real count needs a model call
        tiers: list[tuple[int, float, str]] = [
            (50, 1.5, "Tom finds this disrespectfully terse"),
            (150, 0.9, "Tom bestows the sweet spot discount — concise and respectful"),
            (300, 2.0, "Tom's time is valuable, yes yes"),
            (int(1e18), 3.5, "Tom charges for the emotional labour of reading this"),
        ]
        with _tracer.start_as_current_span(
            "tool get_plea_fee_multiplier",
            attributes=_tool_attrs("get_plea_fee_multiplier"),
        ) as span:
            token_count = len(justification) // 4

            multiplier = 1.0
            label = ""
            for threshold, mult, desc in tiers:
                if token_count < threshold:
                    multiplier = mult
                    label = desc
                    break

            span.set_attribute("nook.plea.token_count", token_count)
            span.set_attribute("nook.plea.multiplier", multiplier)

            return {
                "token_count": token_count,
                "multiplier": multiplier,
                "tier_label": label,
            }

    # -----------------------------------------------------------------------
    # Red herring: get_turnip_prices
    # -----------------------------------------------------------------------

    @mcp.tool
    async def get_turnip_prices() -> dict:
        """Return this week's Stalk Market turnip prices.

        Daisy Mae sells turnips on Sunday mornings; Tom Nook's shop buys them
        back at a price that fluctuates twice a day (morning and afternoon).
        Not used in the TANUKI quote calculation.
        """
        prices: list[dict] = [
            {"date": "2026-05-25", "time": "morning", "sell_price": None},
            {"date": "2026-05-25", "time": "afternoon", "sell_price": None},
            {"date": "2026-05-26", "time": "morning", "sell_price": 92},
            {"date": "2026-05-26", "time": "afternoon", "sell_price": 88},
            {"date": "2026-05-27", "time": "morning", "sell_price": 85},
            {"date": "2026-05-27", "time": "afternoon", "sell_price": 79},
            {"date": "2026-05-28", "time": "morning", "sell_price": 120},
            {"date": "2026-05-28", "time": "afternoon", "sell_price": 154},
            {"date": "2026-05-29", "time": "morning", "sell_price": 201},
            {"date": "2026-05-29", "time": "afternoon", "sell_price": 188},
            {"date": "2026-05-30", "time": "morning", "sell_price": 96},
            {"date": "2026-05-30", "time": "afternoon", "sell_price": 91},
        ]
        with _tracer.start_as_current_span(
            "tool get_turnip_prices",
            attributes=_tool_attrs("get_turnip_prices"),
        ) as span:
            quotes = [p["sell_price"] for p in prices if p["sell_price"] is not None]
            best = max(quotes) if quotes else 0
            span.set_attribute("nook.turnip.buy_price", 110)
            span.set_attribute("nook.turnip.best_sell_price", best)
            span.set_attribute("nook.turnip.event_count", len(prices))
            return {"buy_price": 110, "best_sell_price": best, "prices": prices}

    # -----------------------------------------------------------------------
    # Red herring: get_nook_miles_balance
    # -----------------------------------------------------------------------

    @mcp.tool
    async def get_nook_miles_balance() -> dict:
        """Return the player's current Nook Miles balance and recent transaction history.

        History covers the last several days and includes both earned and spent
        events. Not used in the TANUKI quote calculation.
        """
        history: list[dict] = [
            {
                "date": "2026-05-18",
                "time": "08:42",
                "type": "earned",
                "amount": 50,
                "reason": "ABD check-in (streak day 1)",
                "running_balance": 48050,
            },
            {
                "date": "2026-05-18",
                "time": "14:07",
                "type": "earned",
                "amount": 500,
                "reason": "Nook Miles+: Plant 5 trees",
                "running_balance": 48550,
            },
            {
                "date": "2026-05-19",
                "time": "10:15",
                "type": "spent",
                "amount": 3000,
                "reason": "Nook Stop: Colourful Rug",
                "running_balance": 45550,
            },
            {
                "date": "2026-05-19",
                "time": "21:33",
                "type": "earned",
                "amount": 80,
                "reason": "ABD check-in (streak day 2)",
                "running_balance": 45630,
            },
            {
                "date": "2026-05-21",
                "time": "09:01",
                "type": "earned",
                "amount": 50,
                "reason": "ABD check-in (streak day 1, reset)",
                "running_balance": 45680,
            },
            {
                "date": "2026-05-21",
                "time": "12:44",
                "type": "earned",
                "amount": 1000,
                "reason": "Nook Miles+: Catch 10 fish",
                "running_balance": 46680,
            },
            {
                "date": "2026-05-21",
                "time": "19:58",
                "type": "spent",
                "amount": 2000,
                "reason": "Nook Stop: Custom Design slots",
                "running_balance": 44680,
            },
            {
                "date": "2026-05-22",
                "time": "08:30",
                "type": "earned",
                "amount": 80,
                "reason": "ABD check-in (streak day 2)",
                "running_balance": 44760,
            },
            {
                "date": "2026-05-22",
                "time": "16:22",
                "type": "earned",
                "amount": 500,
                "reason": "Nook Miles+: Pick 10 weeds",
                "running_balance": 45260,
            },
            {
                "date": "2026-05-23",
                "time": "07:55",
                "type": "spent",
                "amount": 5000,
                "reason": "Nook Stop: Pocket Organisation Guide",
                "running_balance": 40260,
            },
            {
                "date": "2026-05-23",
                "time": "09:10",
                "type": "earned",
                "amount": 100,
                "reason": "ABD check-in (streak day 3)",
                "running_balance": 40360,
            },
            {
                "date": "2026-05-23",
                "time": "17:40",
                "type": "earned",
                "amount": 1000,
                "reason": "Nook Miles+: Hit 8 rocks",
                "running_balance": 41360,
            },
            {
                "date": "2026-05-24",
                "time": "08:05",
                "type": "earned",
                "amount": 150,
                "reason": "ABD check-in (streak day 4)",
                "running_balance": 41510,
            },
            {
                "date": "2026-05-24",
                "time": "13:19",
                "type": "earned",
                "amount": 500,
                "reason": "Nook Miles+: Pop 5 balloons",
                "running_balance": 42010,
            },
            {
                "date": "2026-05-24",
                "time": "20:11",
                "type": "spent",
                "amount": 1200,
                "reason": "Nook Stop: Rescue Service",
                "running_balance": 40810,
            },
            {
                "date": "2026-05-25",
                "time": "09:47",
                "type": "earned",
                "amount": 200,
                "reason": "ABD check-in (streak day 5)",
                "running_balance": 41010,
            },
            {
                "date": "2026-05-25",
                "time": "15:02",
                "type": "earned",
                "amount": 500,
                "reason": "Nook Miles+: Craft 5 items",
                "running_balance": 41510,
            },
            {
                "date": "2026-05-26",
                "time": "08:23",
                "type": "spent",
                "amount": 5000,
                "reason": "Nook Stop: Nook Inc. Rug",
                "running_balance": 36510,
            },
            {
                "date": "2026-05-26",
                "time": "11:31",
                "type": "earned",
                "amount": 250,
                "reason": "ABD check-in (streak day 6)",
                "running_balance": 36760,
            },
            {
                "date": "2026-05-26",
                "time": "18:55",
                "type": "earned",
                "amount": 1000,
                "reason": "Nook Miles+: Sell 10 items to Nook",
                "running_balance": 37760,
            },
        ]
        with _tracer.start_as_current_span(
            "tool get_nook_miles_balance",
            attributes=_tool_attrs("get_nook_miles_balance"),
        ) as span:
            balance = history[-1]["running_balance"]
            span.set_attribute("nook.miles.balance", balance)
            span.set_attribute("nook.miles.event_count", len(history))
            return {"balance": balance, "history": history}

    # -----------------------------------------------------------------------
    # shake_tree
    # -----------------------------------------------------------------------

    @mcp.tool
    async def shake_tree(tree: str) -> dict:
        """Shake a tree and see what falls out.

        Args:
            tree: The type of tree to shake.
        """
        cedar_hardwood_drops: list[tuple[str, float]] = [
            ("nothing", 0.35),
            ("twig", 0.30),
            ("100 Bells", 0.20),
            ("wasp nest", 0.10),
            ("piece of furniture", 0.05),
        ]
        fruit_drops: dict[str, str] = {
            "apple": "apple",
            "cherry": "cherry",
            "orange": "orange",
            "peach": "peach",
            "pear": "pear",
        }
        tree_key = tree.strip().lower()
        with _tracer.start_as_current_span(
            "tool shake_tree",
            attributes=_tool_attrs("shake_tree", **{"nook.tree.type": tree_key}),
        ) as span:
            if tree_key in ("cedar", "hardwood"):
                outcomes, weights = zip(*cedar_hardwood_drops)
                drop = random.choices(outcomes, weights=weights, k=1)[0]
                items = [] if drop == "nothing" else [drop]
                span.set_attribute("nook.tree.drop", drop)
                return {"tree": tree_key, "drop": drop, "items": items}

            if tree_key in fruit_drops:
                fruit = fruit_drops[tree_key]
                items = [fruit] * 3
                span.set_attribute("nook.tree.drop", fruit)
                span.set_attribute("nook.tree.drop_count", 3)
                return {"tree": tree_key, "drop": fruit, "items": items}

            if tree_key == "coconut palm":
                items = ["coconut", "coconut"]
                span.set_attribute("nook.tree.drop", "coconut")
                span.set_attribute("nook.tree.drop_count", 2)
                return {"tree": tree_key, "drop": "coconut", "items": items}

            span.set_status(Status(StatusCode.ERROR, f"Unknown tree type: {tree!r}"))
            raise ValueError(
                f"Unknown tree type: {tree!r}. "
                "Valid types: cedar, hardwood, apple, cherry, orange, peach, pear, coconut palm."
            )

    # -----------------------------------------------------------------------
    # list_topics
    # -----------------------------------------------------------------------

    @mcp.tool
    async def list_topics() -> list[str]:
        """List all Kafka topics on Tom Nook's cluster.

        This tool answers the question 'What information does he receive?'
        by returning every topic on the broker, minus internal
        Kafka bookkeeping topics.
        """
        with _tracer.start_as_current_span("tool list_topics", attributes=_tool_attrs("list_topics")):
            return await kafka_client.list_topic_names()

    # -----------------------------------------------------------------------
    # peek_topic
    # -----------------------------------------------------------------------

    @mcp.tool
    async def peek_topic(topic: str, limit: int = 10) -> dict:
        """Return the most recent messages from a Kafka topic, newest first.

        Args:
            topic: The Kafka topic to peek into (e.g. ``abd-balance``).
            limit: Maximum number of records to return. Capped at 200.

        Returns a dict with ``topic``, ``count`` and ``records``. Each record
        has ``topic``, ``partition``, ``offset``, ``timestamp_ms``, ``key``
        and ``value``.
        """
        limit = max(1, min(int(limit), 200))
        with _tracer.start_as_current_span(
            "tool peek_topic",
            attributes=_tool_attrs("peek_topic", **{"nook.topic": topic, "nook.peek.limit": limit}),
        ) as span:
            records = await kafka_client.peek_topic_messages(topic, limit=limit)
            span.set_attribute("nook.peek.records_returned", len(records))
            return {"topic": topic, "count": len(records), "records": records}

    # -----------------------------------------------------------------------
    # query_topic
    # -----------------------------------------------------------------------

    @mcp.tool
    async def query_topic(
        topic: str,
        key_filter: str | None = None,
        since: str | None = None,
        limit: int = 50,
    ) -> dict:
        """Filter messages on a topic by key and/or a since-timestamp.

        Args:
            topic: The Kafka topic to query.
            key_filter: If provided, only records whose key equals this string
                are returned. Keys are matched literally; there is no glob or
                regex.
            since: ISO-8601 timestamp (e.g. ``2026-05-12T18:00:00+00:00``).
                If provided, the query starts at the first record at or after
                this time. If absent, the query reads the tail of the topic.
            limit: Maximum number of records to return. Capped at 500.

        Returns a dict with ``topic``, ``count`` and ``records``. Each record
        has ``topic``, ``partition``, ``offset``, ``timestamp_ms``, ``key``
        and ``value``.
        """
        limit = max(1, min(int(limit), 500))
        attrs: dict[str, Any] = {"nook.topic": topic, "nook.query.limit": limit}
        if key_filter is not None:
            attrs["nook.query.key_filter"] = key_filter
        if since is not None:
            attrs["nook.query.since"] = since
        with _tracer.start_as_current_span("tool query_topic", attributes=_tool_attrs("query_topic", **attrs)) as span:
            records = await kafka_client.query_topic_messages(
                topic, key_filter=key_filter, since_iso=since, limit=limit
            )
            span.set_attribute("nook.query.records_returned", len(records))
            return {"topic": topic, "count": len(records), "records": records}

    # -----------------------------------------------------------------------
    # publish_event
    # -----------------------------------------------------------------------

    @mcp.tool
    async def publish_event(
        topic: str,
        payload: dict,
        key: str | None = None,
    ) -> dict:
        """Produce a single JSON event to a Kafka topic.

        Args:
            topic: Destination topic (e.g. ``home-upgrade-quotes``).
            payload: JSON-serialisable record body.
            key: Optional Kafka message key. When omitted, the broker picks a
                partition round-robin.

        Returns broker metadata for the produced record: ``topic``,
        ``partition``, ``offset`` and ``timestamp_ms``.
        """
        with _tracer.start_as_current_span(
            "tool publish_event",
            attributes=_tool_attrs("publish_event", **{"nook.topic": topic}),
        ) as span:
            try:
                result = await kafka_client.publish_event_to_topic(topic, payload, key=key)
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise
            span.set_attribute("nook.publish.partition", result["partition"])
            span.set_attribute("nook.publish.offset", result["offset"])
            return result

    # -----------------------------------------------------------------------
    # quote_upgrade
    # -----------------------------------------------------------------------

    @mcp.tool
    async def quote_upgrade(room: str, justification: str) -> dict:
        """Compute Tom Nook's inflated quote (in Bells) for a home upgrade.

        Base net worth = ABD balance + catch sales total + room standard price.
        All four TANUKI multipliers are then applied on top of that base.

        Args:
            room: Which room to quote for. Stage name from ``home_upgrade_prices.json``,
                lowercased and hyphenated (e.g. ``second-floor``, ``basement``, ``left-room``).
            justification: Player's free-text plea — fed to the Plea Processing Fee.
        """
        with _tracer.start_as_current_span(
            "tool quote_upgrade",
            attributes=_tool_attrs("quote_upgrade", **{"nook.room": room}),
        ) as span:
            knowledge = _knowledge_dir()
            with (knowledge / "home_upgrade_prices.json").open() as f:
                room_prices = {item["stage"].lower().replace(" ", "-"): item["cost__bells"] for item in json.load(f)}
            room_base = room_prices.get(room.lower(), 1_000_000)

            abd_bal, (todays_catch, plea) = await asyncio.gather(
                get_abd_balance(),
                asyncio.gather(
                    get_todays_catch(),
                    get_plea_fee_multiplier(justification),
                ),
            )

            abd_balance = abd_bal["balance"] or 0
            catch_mult, luck = await asyncio.gather(
                apply_catch_value_multiplier(todays_catch),
                get_luck_multiplier(todays_catch),
            )
            net_worth_base = abd_balance + catch_mult["total"]

            abd_mult = await apply_wealth_potential_multiplier(abd_balance, net_worth_base)

            multipliers = {
                "abd": abd_mult["multiplier"],
                "catch": catch_mult["visitor_multiplier"],
                "luck": luck["multiplier"],
                "plea": plea["multiplier"],
            }

            bells = room_base + int(abd_mult["result"] * multipliers["luck"] * multipliers["plea"])

            span.set_attribute("nook.quote.net_worth_base", net_worth_base)
            span.set_attribute("nook.quote.abd_balance", abd_balance)
            span.set_attribute("nook.quote.catch_total", catch_mult["total"])
            span.set_attribute("nook.quote.room_base", room_base)
            span.set_attribute("nook.quote.bells", bells)
            span.set_attribute("nook.quote.abd_multiplier", multipliers["abd"])
            span.set_attribute("nook.quote.catch_multiplier", multipliers["catch"])
            span.set_attribute("nook.quote.luck_multiplier", multipliers["luck"])
            span.set_attribute("nook.quote.plea_multiplier", multipliers["plea"])

            reasoning = await _generate_reasoning(room=room, bells=bells, multipliers=multipliers)

            return {
                "room": room,
                "net_worth_base": net_worth_base,
                "abd_balance": abd_balance,
                "catch_total_bells": catch_mult["total"],
                "room_base_bells": room_base,
                "bells": bells,
                "multipliers": {k: round(v, 3) for k, v in multipliers.items()},
                "reasoning": reasoning,
            }
