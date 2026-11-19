# One image for the always-on demo on AWS (terraform/always-on): Lambda runs the
# flight board on port 8000 (through the Lambda Web Adapter below) and ECS runs
# `airport-datagen --continuous`. Settings and credentials come from Secrets Manager
# at run time (ECS injects them; on Lambda the app reads the secret itself); none are
# baked into any layer.
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

# The Lambda Web Adapter turns Lambda invocations into HTTP requests to port 8000.
# It is a Lambda extension, so it does nothing when ECS or `docker run` starts the image.
COPY --from=public.ecr.aws/awsguru/aws-lambda-adapter:0.9.1 /lambda-adapter /opt/extensions/lambda-adapter

EXPOSE 8000
ENTRYPOINT ["uv", "run", "--no-sync"]
CMD ["airport-app", "--host", "0.0.0.0", "--port", "8000"]
