FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY . .
RUN uv sync --frozen --no-dev && uv pip install -e .
# trades/ (paper_trades.csv) and logs/ live on the mounted volume /data
RUN mkdir -p /data/trades /data/logs && ln -s /data/trades trades && ln -s /data/logs logs
ENV PYTHONUNBUFFERED=1
# Secrets come from Fly: SOLANA_NODE_RPC_ENDPOINT, SOLANA_NODE_WSS_ENDPOINT, SOLANA_PRIVATE_KEY
CMD ["sh", "-c", "mkdir -p /data/trades /data/logs && exec uv run --no-sync pump_bot"]
