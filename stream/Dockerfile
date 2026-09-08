FROM rust:1.88-bookworm AS build

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --reinstall binutils libbinutils \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml .
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/release/pulse-stream /usr/local/bin/pulse-stream

CMD ["pulse-stream"]
WORKDIR /app
COPY Cargo.toml .
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/pulse-stream /usr/local/bin/pulse-stream
CMD ["pulse-stream"]
