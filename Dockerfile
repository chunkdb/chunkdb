# syntax=docker/dockerfile:1.7

FROM ubuntu:24.04 AS build

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ca-certificates \
        pkg-config \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

ARG CHUNKDB_WITH_TLS=OFF
RUN cmake -S . -B build \
      -DCHUNKDB_BUILD_TESTS=ON \
      -DCHUNKDB_WITH_TLS=${CHUNKDB_WITH_TLS} \
    && cmake --build build --parallel

FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

# No default CHUNKDB_TOKEN is baked into this image on purpose. A shipped default
# would be a publicly known credential, and because the env var outranks --token
# during resolution it would also silently override an operator's explicit flag.
# With no token set the server refuses to start until one is supplied via
# CHUNKDB_TOKEN, --token-file, or --no-auth.

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libstdc++6 \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system chunkdb \
    && useradd --system --gid chunkdb --home-dir /var/lib/chunkdb --create-home chunkdb

WORKDIR /var/lib/chunkdb
COPY --from=build /src/build/chunkdb_server /usr/local/bin/chunkdb_server
RUN mkdir -p /var/lib/chunkdb/data \
    && chown -R chunkdb:chunkdb /var/lib/chunkdb

USER chunkdb

EXPOSE 4242
VOLUME ["/var/lib/chunkdb/data"]

# PING is answered before the auth gate, so the probe needs no credential.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD printf 'PING\r\nQUIT\r\n' | nc -w 2 127.0.0.1 4242 | grep -q '+PONG'

ENTRYPOINT ["/usr/local/bin/chunkdb_server"]
CMD ["--listen-uri", "chunk://0.0.0.0:4242/", "--data-dir", "/var/lib/chunkdb/data", "--durability", "relaxed", "--workers", "4"]
