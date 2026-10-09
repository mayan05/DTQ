FROM python:3.12-alpine
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
COPY . .
RUN uv sync --frozen --no-cache || uv sync

ENV PATH="/app/.venv/bin:$PATH"
CMD ["python", "-m", "app.worker"]
