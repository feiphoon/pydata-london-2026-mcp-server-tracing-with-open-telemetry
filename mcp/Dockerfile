FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN pip install uv

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
COPY knowledge ./knowledge

RUN uv pip install --system .

# The package is installed into site-packages, so the source-relative
# knowledge lookup no longer resolves. Point the tools at the copied dir.
ENV NOOK_KNOWLEDGE_DIR=/app/knowledge

EXPOSE 8000

# Run as a proper module so the package's relative imports resolve. This
# also gives us a single entry point to initialise the OpenTelemetry SDK
# before FastMCP imports anything, which we will need from section 3
# onwards.
CMD ["python", "-m", "nook_mcp.server"]
