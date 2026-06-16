# syntax=docker/dockerfile:1

FROM rust:1.95-bookworm AS builder

WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        clang \
        cmake \
        libclang-dev \
        libssl-dev \
        pkg-config \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release --bin fdc_server

FROM debian:bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libclang1 \
        libgcc-s1 \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /usr/sbin/nologin fdc \
    && mkdir -p /app/var/fdc-market-data \
    && chown -R fdc:fdc /app

COPY --from=builder /workspace/target/release/fdc_server /usr/local/bin/fdc_server

USER fdc

ENV FDC_SERVER_ADDR=0.0.0.0:18080 \
    FDC_SERVER_ENV=production

EXPOSE 18080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:18080/health >/dev/null || exit 1

CMD ["/usr/local/bin/fdc_server"]
