# One image for the always-on demo on AWS (terraform/always-on): App Runner runs the
# default command (the flight board on port 8000) and ECS runs
# `airport-datagen --continuous`. Settings and credentials arrive as environment
# variables from Secrets Manager at run time; none are baked into any layer.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies first, so code changes reuse the cached dependency layer.
COPY pyproject.toml uv.lock .python-version README.md LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project

COPY scripts/ ./scripts/
RUN uv sync --frozen --no-dev

EXPOSE 8000
ENTRYPOINT ["uv", "run", "--no-sync"]
CMD ["airport-app", "--host", "0.0.0.0", "--port", "8000"]
