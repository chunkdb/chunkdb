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

ARG CHUNKDB_WITH_TLS=ON
ARG CHUNKDB_BUILD_TESTS=OFF
RUN nice -n 10 cmake -S . -B build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCHUNKDB_WERROR=ON \
      -DCHUNKDB_BUILD_TESTS=${CHUNKDB_BUILD_TESTS} \
      -DCHUNKDB_WITH_TLS=${CHUNKDB_WITH_TLS} \
    && nice -n 10 cmake --build build --parallel 3

FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libstdc++6 \
        libssl3t64 \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system chunkdb \
    && useradd --system --gid chunkdb --home-dir /var/lib/chunkdb --create-home chunkdb

WORKDIR /var/lib/chunkdb
COPY --from=build /src/build/chunkdb_server /usr/local/bin/chunkdb_server
COPY --from=build /src/build/chunkdb_verify /usr/local/bin/chunkdb_verify
COPY --from=build /src/build/chunkdb_restore /usr/local/bin/chunkdb_restore
RUN mkdir -p /var/lib/chunkdb/data /var/lib/chunkdb/backups \
    && chown -R chunkdb:chunkdb /var/lib/chunkdb

COPY scripts/docker/entrypoint.sh /usr/local/bin/chunkdb-entrypoint

USER chunkdb

EXPOSE 4242
VOLUME ["/var/lib/chunkdb"]

# The probe needs no credential: HELLO 3 is answered with the server's map
# (`%...`), or with AUTH_REQUIRED when logins need a user.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD printf 'HELLO 3\r\n' | nc -w 2 127.0.0.1 4242 | grep -q -e '^%' -e '^-ERR AUTH_REQUIRED'

ENTRYPOINT ["/usr/local/bin/chunkdb-entrypoint"]
CMD ["--listen-uri", "chunk://0.0.0.0:4242/", "--data-dir", "/var/lib/chunkdb/data", "--backup-dir", "/var/lib/chunkdb/backups", "--durability", "relaxed", "--workers", "4"]
