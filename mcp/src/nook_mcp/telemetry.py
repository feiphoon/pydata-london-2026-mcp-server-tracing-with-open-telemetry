"""OpenTelemetry SDK setup for ``nook-mcp``.

Initialised once at process startup by :mod:`nook_mcp.server` before
FastMCP is imported. The choice of OTLP HTTP / protobuf matches the env
vars set in ``docker-compose.override.yml``:

    OTEL_SERVICE_NAME=nook-mcp
    OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
    OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf

This module is also the home of the workshop's *named* tracer: every
other module obtains its spans via ``tracer = trace.get_tracer(__name__)``
which becomes meaningful once the provider is configured here.
"""

from __future__ import annotations

import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)


_INITIALISED = False


def configure_telemetry(service_name: str | None = None) -> None:
    """Install the global :class:`TracerProvider`.

    Idempotent: a second call is a no-op.
    """
    global _INITIALISED
    if _INITIALISED:
        return

    resource = Resource.create(
        {
            "service.name": service_name or os.environ.get("OTEL_SERVICE_NAME", "nook-mcp"),
            "service.version": "0.1.0",
        }
    )
    provider = TracerProvider(resource=resource)

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318")
    otlp_exporter = OTLPSpanExporter(
        endpoint=f"{endpoint.rstrip('/')}/v1/traces",
    )
    provider.add_span_processor(
        BatchSpanProcessor(
            otlp_exporter,
            # Short export interval so we can see spans in Jaeger
            # within a couple of seconds, not the default five.
            schedule_delay_millis=1_000,
        )
    )

    if os.environ.get("OTEL_DEBUG_CONSOLE", "").lower() in {"1", "true"}:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)
    _INITIALISED = True
