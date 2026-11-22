# Build from the wrapper checkout with: docker build -f Dockerfile source/mcp
ARG ALPINE_IMAGE=docker.io/library/alpine:latest
FROM ${ALPINE_IMAGE}

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# The upstream mirror search passes --include to grep.
RUN apk upgrade --no-cache \
    && apk add --no-cache ca-certificates grep py3-pip python3 rsync \
    && python3 -m venv /opt/venv

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir . \
    && pip check \
    && mkdir /mirror \
    && chown nobody:nogroup /mirror

ENV RFC_MIRROR=/mirror \
    RFC_TRANSPORT=http \
    HOST=0.0.0.0 \
    PORT=8080

EXPOSE 8080
USER nobody
CMD ["mcp-server-rfc"]
