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
ENV CHUNKDB_TOKEN=dev-token

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

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD printf 'AUTH %s\r\nPING\r\nQUIT\r\n' "${CHUNKDB_TOKEN:-dev-token}" | nc -w 2 127.0.0.1 4242 | grep -q '+PONG'

ENTRYPOINT ["/usr/local/bin/chunkdb_server"]
CMD ["--listen-uri", "chunk://dev-token@0.0.0.0:4242/", "--data-dir", "/var/lib/chunkdb/data", "--durability", "relaxed", "--workers", "4"]
