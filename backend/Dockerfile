FROM python:3.12-slim

ENV LANG=C.UTF-8

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN mkdir -p /app

COPY pyproject.toml uv.lock* /app/

RUN cd /app && uv sync --frozen

COPY . /app/

EXPOSE 8000

WORKDIR /app

ENTRYPOINT ["/bin/bash", "docker_entrypoint.sh"]
