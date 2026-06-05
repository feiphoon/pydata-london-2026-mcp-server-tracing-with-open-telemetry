# 7: Connecting Auth and Observability — Who Burned the Tokens?

## The missing column

§6 left us with redacted, intelligently-sampled, compliance-grade
traces. Every span knows:

- *what* happened — `mcp.method.name`, `gen_ai.tool.name`
- *how much* it cost — `gen_ai.usage.*_tokens`, `nook.quote.bells`
- *where* the time went — the waterfall and its `messaging.*` children
- *when* it happened — span timestamps + `mcp.session.id`

What every span is **still** missing is a single attribute:

> *who*

- Tom Nook and Isabelle hit the same `http://nook-mcp:8000/mcp`
  endpoint with the same (lack of) permissions
- The trace can tell us a 14-million-Bell `quote_upgrade` happened.
  It cannot tell us **which agent operator triggered it**
- Without that column, every "GROUP BY user" dashboard from §5 is
  unanswerable, the "enterprise 100%, free tier 5%" sampling policy
  from §6 is unimplementable, and the GDPR Article 17 "right to
  erasure" promise has nothing to erase by
- Section 7 adds that column

----

## `nook-mcp`'s OAuth model

The workshop repo already runs **Lenses HQ** on
`http://lenses-hq:9991` as part of the Community Edition base. Two
of Lenses HQ's endpoints are everything we need:

```text
   Tom Nook's agent                   nook-mcp                       lenses-hq
   ────────────────                   ────────                       ─────────
        │   Authorization: Bearer <jwt>   │                                │
        │ ──────────────────────────────▶ │                                │
        │                                 │     POST /oauth2/introspect    │
        │                                 │ ─────────────────────────────▶ │
        │                                 │ ◀───────────────────────────── │
        │                                 │     { active, sub, scope }     │
        │ ◀────────────────────────────── │                                │
        │   tool result + 200 OK           │                                │
```

- `nook-mcp` is an **OAuth 2.1 Resource Server**. It never sees the
  password — only a bearer token issued elsewhere
- For *every* incoming MCP request, `mcp/src/nook_mcp/auth.py` calls
  the introspect endpoint and decodes the user and scope claims

----

## `auth.py` — the whole resource server

The entire file is one verifier subclass plus a builder. FastMCP does
the rest:

```python
# mcp/src/nook_mcp/auth.py
import os

from fastmcp.server.auth import (
    AccessToken, AuthProvider, RemoteAuthProvider, TokenVerifier,
)
from fastmcp.server.auth.providers.introspection import IntrospectionTokenVerifier


class LensesHQTokenVerifier(IntrospectionTokenVerifier):
    """Promote the *human* identifier into ``AccessToken.client_id`` so
    FastMCP writes the user — not the OAuth app — to ``enduser.id``."""

    async def verify_token(self, token: str) -> AccessToken | None:
        result = await super().verify_token(token)
        if result is None:
            return result
        claims = result.claims or {}
        human_id = claims.get("username") or claims.get("sub") or result.client_id
        return result.model_copy(update={"client_id": str(human_id)})


def is_auth_enabled() -> bool:
    # Auth is opt-in: active only when the resource-server creds exist,
    # so §1-§6 keep running un-authenticated with zero code changes.
    return bool(os.environ.get("NOOK_AUTH_CLIENT_ID")
                and os.environ.get("NOOK_AUTH_CLIENT_SECRET"))


def build_auth_provider() -> AuthProvider | None:
    if not is_auth_enabled():
        return None

    verifier: TokenVerifier = LensesHQTokenVerifier(
        introspection_url=os.environ.get(
            "NOOK_AUTH_INTROSPECTION_URL",
            "http://lenses-hq:9991/oauth2/introspect"),
        client_id=os.environ["NOOK_AUTH_CLIENT_ID"],
        client_secret=os.environ["NOOK_AUTH_CLIENT_SECRET"],
        base_url=os.environ.get("NOOK_AUTH_BASE_URL", "http://localhost:8000"),
        cache_ttl_seconds=60,   # don't introspect on every span of a chatty call
    )
    return RemoteAuthProvider(
        token_verifier=verifier,
        authorization_servers=[
            os.environ.get("NOOK_AUTH_AUTHORIZATION_SERVER", "http://localhost:9991")
        ],
        base_url=os.environ.get("NOOK_AUTH_BASE_URL", "http://localhost:8000"),
        scopes_supported=["read", "write", "delete"],
        resource_name="nook-mcp",
    )
```

- **`IntrospectionTokenVerifier`** does the RFC 7662 call to Lenses HQ
  on every (cached) request. `cache_ttl_seconds=60` stops us
  introspecting once per span in a chatty `quote_upgrade`
- **`RemoteAuthProvider`** publishes the RFC 9728
  `/.well-known/oauth-protected-resource` document so MCP clients
  discover Lenses HQ on their own — no client-side config
- **`LensesHQTokenVerifier`** is the one custom touch: Lenses HQ's
  introspection response carries `client_id` (the OAuth *app*), `sub`
  (stable user id) and `username` (human login). The stock verifier
  would put the *app* id on `enduser.id`; we promote
  `username → sub → client_id` so the span carries the human who
  placed the call — the whole point of "who burned the tokens?"

----

## Wiring it in: `server.py`, compose, and a one-time registration

**1. `server.py` — opt-in, so §1-§6 are unchanged:**

```python
from . import auth, tools  # noqa: E402

# Activates only when NOOK_AUTH_CLIENT_ID/SECRET are present. Once `auth=`
# is set, FastMCP stamps enduser.id / enduser.scope on every server span.
_auth_provider = auth.build_auth_provider()
mcp = FastMCP("nook-mcp", auth=_auth_provider) if _auth_provider else FastMCP("nook-mcp")
```

**2. `docker-compose.override.yml` — the env that flips auth on:**

```yaml
services:
  nook-mcp:
    environment:
      # ... existing KAFKA_BOOTSTRAP / OTEL_* vars ...
      NOOK_AUTH_INTROSPECTION_URL: http://lenses-hq:9991/oauth2/introspect
      # base_url and authorization_server must be reachable from the
      # *browser*, not just the container — hence localhost.
      NOOK_AUTH_BASE_URL: http://localhost:8000
      NOOK_AUTH_AUTHORIZATION_SERVER: http://localhost:9991
      # Filled in by the registration step below. Unset ⇒ un-authenticated.
      NOOK_AUTH_CLIENT_ID: ${NOOK_AUTH_CLIENT_ID:-}
      NOOK_AUTH_CLIENT_SECRET: ${NOOK_AUTH_CLIENT_SECRET:-}
```

**3. One-time: register `nook-mcp` as a resource-server client.** This
is how `nook-mcp` gets the `client_id`/`client_secret` it uses for
HTTP Basic auth on the introspection call (separate from the tokens MCP
*clients* obtain). Run once during preflight:

```bash
curl -sS -X POST http://localhost:9991/oauth2/register \
  -H "Content-Type: application/json" \
  -d '{
    "client_name": "nook-mcp-resource-server",
    "redirect_uris": ["http://localhost/unused"],
    "grant_types": ["authorization_code"],
    "response_types": ["code"],
    "token_endpoint_auth_method": "client_secret_basic"
  }'
# → returns { "client_id": "...", "client_secret": "..." }

export NOOK_AUTH_CLIENT_ID="<client_id>"
export NOOK_AUTH_CLIENT_SECRET="<client_secret>"
docker compose up -d --build nook-mcp
```

----

## What auth gives every span — for free

The moment `nook-mcp` runs FastMCP's auth middleware, **every span
the server emits automatically carries two new attributes**:

| Attribute        | Source                                                   | Example for Tom Nook                    | Example for Isabelle               |
|------------------|----------------------------------------------------------|-----------------------------------------|------------------------------------|
| `enduser.id`     | Human identifier (`username` → `sub` → `client_id`) promoted by `LensesHQTokenVerifier` | `tom_nook@nookinc.island`               | `isabelle@town-hall.island`        |
| `enduser.scope`  | `scope` claim from the introspected access token         | `read:topics write:home-upgrade-quotes` | `read:topics audit:abd-balance` |

- Set by FastMCP's `server_span` context manager — **zero
  instrumentation code in `tools.py`** to make it happen
- Because it is framework-level, it lands on **every** tool's span —
  even a trivial `shake_tree` call (read-scope only) carries the same
  `enduser.id` and `enduser.scope` as a flagship `quote_upgrade`

----

## Five investigations §5 could not answer, now unblocked

Every §5 dashboard gains a free `GROUP BY enduser.id` axis — same span
attributes, same Collector pipeline, *whose* is the new dimension:

| Query (PromQL / TraceQL)                               | Now answers                                                  |
|--------------------------------------------------------|--------------------------------------------------------------|
| `SUM(gen_ai.usage.*_tokens) GROUP BY enduser.id`       | *Which user burned the most tokens this month?*              |
| `enduser.scope =~ ".*write.*"`                         | *Who is calling write-scope tools? (security audit)*         |
| `SUM(nook.quote.bells) BY enduser.id` per day          | *Who is driving Tom Nook's bill — and is it Tom Nook?*       |

- The last row is the punchline: with auth on, *the trace stream itself
  proves who is running up the bill*
- **These are aggregation queries — they need the metrics/query layer,
  not Jaeger.** Jaeger's Tags search is *exact-equality only* (no
  `SUM`, no `GROUP BY`, no regex, no ranges), so in Jaeger you can only
  do the *filter* half — e.g. `enduser.id=tom_nook@nookinc.island`
  pulls every trace for one user to eyeball. The roll-ups above run in
  §5's `spanmetrics` → Prometheus/Grafana (PromQL) with `enduser.id`
  added to the dimensions list, or via TraceQL in Grafana Tempo /
  Honeycomb

----

## Compliance: the audit trail completes itself

§6 made the trace stream the compliance log; auth supplies the column
it was missing. A span already carried the *what* (`gen_ai.tool.name`),
*when* (timestamps), *input* (`nook.*`) and *outcome* (status +
`nook.quote.bells`) — now it also carries the **who** (`enduser.id`) and
**with what permissions** (`enduser.scope`).

- That `enduser.id` is what GDPR Article 17 erasure keys on (hash it in
  `auth.py` and you delete by hash); the EU AI Act Article 19 log §6
  promised is now genuinely *attributable*, not anonymous

----

## Demo: turn auth on, watch `enduser.id` appear

**Prerequisite:** the resource-server registration above has run and
`nook-mcp` was rebuilt with `NOOK_AUTH_CLIENT_ID`/`NOOK_AUTH_CLIENT_SECRET`
set in the environment.

1. **Client-side OAuth.** An MCP client doesn't carry a hand-minted
   token — FastMCP's `OAuth` helper discovers Lenses HQ from `nook-mcp`'s
   protected-resource metadata, self-registers via Dynamic Client
   Registration, and opens a browser for PKCE login. Lenses HQ returns
   `401` to FastMCP's pre-flight `GET` (its SPA renders the login screen
   in-browser), so we subclass `OAuth` to skip the pre-flight and open
   the browser directly:

```python
# oauth_test.py
import asyncio
import webbrowser

from fastmcp import Client
from fastmcp.client.auth import OAuth


class LensesHQOAuth(OAuth):
    async def redirect_handler(self, authorization_url: str) -> None:
        print(f"[OAuth] open this URL if it didn't pop:\n  {authorization_url}")
        webbrowser.open(authorization_url)


async def main() -> None:
    auth = LensesHQOAuth(
        mcp_url="http://localhost:8000/mcp",
        scopes="read",
        client_name="nook-mcp-workshop-client",
        callback_port=7777,   # fixed, so the redirect_uri is stable across runs
    )
    async with Client("http://localhost:8000/mcp", auth=auth) as client:
        await client.call_tool("peek_topic", {"topic": "abd-balance", "limit": 5})
        await client.call_tool(
            "quote_upgrade",
            {"room": "second-floor",
             "justification": "I pull weeds every day and never miss an ABD check-in!"},
        )


asyncio.run(main())
```

   Run it and log in at the browser prompt (Lenses HQ default
   `admin`/`admin`):

```bash
uv run --with fastmcp==3.2.3 python oauth_test.py
```

2. In Jaeger, filter **Tags** = `enduser.id=admin` → every span
   that call produced now carries it.

3. **Scope test.** A `write`-scope user calling `publish_event` for
   `home-upgrade-quotes` succeeds; a `read`-only user calling the same
   tool gets **403**. Both attempts land in the trace stream with full
   attribution — the failed one with `status=ERROR` and an
   `enduser.scope` tag that **proves** the caller lacked the
   permission, not an HTTP 403 buried in a log file three hours later

## Checkpoint

- You can name the **two** span attributes (`enduser.id`,
  `enduser.scope`) that auth adds, and the **one** OAuth endpoint
  on Lenses HQ that produces them (`/oauth2/introspect`)
- You can describe a Jaeger filter (`enduser.id=...`) that pulls
  every call made by a single agent operator

## Return to the slides anytime
