# The tickdb exporter (FR-18 to FR-21 in shared-capture mode): one small Python process that
# Prometheus scrapes. Built from the repo by `make up` (docker compose build exporter).
FROM ghcr.io/astral-sh/uv:0.12.23 AS uv

FROM python:3.12.16-slim-bookworm
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/tickdb \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH=/opt/tickdb/bin:$PATH
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
COPY config ./config
RUN uv sync --locked --no-dev --no-editable && rm -rf /root/.cache
USER 1000:1000
EXPOSE 9108
ENTRYPOINT ["tickdb", "exporter"]
