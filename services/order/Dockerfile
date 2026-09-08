FROM rust:1.88-bookworm AS build
WORKDIR /app
COPY Cargo.toml .
COPY src ./src
RUN cargo build --release
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/pulse-order /usr/local/bin/order
CMD ["order"]
