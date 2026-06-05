FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN pip install "aiokafka>=0.11.0,<0.13"

COPY seed/producer.py seed/topics.py ./
COPY mcp/knowledge/ ./knowledge/

CMD ["python", "-u", "producer.py"]
