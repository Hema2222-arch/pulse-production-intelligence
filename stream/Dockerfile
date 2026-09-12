FROM rust:1.88-bookworm AS build

WORKDIR /app

RUN apt-get update \
    && apt-get install -y \
        pkg-config \
        libssl-dev \
        libsasl2-dev \
        cmake \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml .
COPY Cargo.lock* ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y \
        ca-certificates \
        libssl3 \
        libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/release/pulse-stream /usr/local/bin/pulse-stream

CMD ["pulse-stream"]