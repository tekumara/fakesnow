FROM cgr.dev/chainguard/python:latest-dev AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

COPY pyproject.toml uv.lock README.md LICENSE ./
COPY fakesnow/ ./fakesnow/

RUN uv sync --locked --no-dev --extra server --no-editable

FROM cgr.dev/chainguard/python:latest

COPY --from=builder /app/.venv /app/.venv

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 64616
ENTRYPOINT []
CMD ["fakesnow", "-s", "--host", "0.0.0.0", "--port", "64616"]
