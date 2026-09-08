FROM rust:1.88-bookworm AS build
WORKDIR /app
COPY Cargo.toml .
COPY src ./src
RUN cargo build --release
FROM debian:bookworm-slim
COPY --from=build /app/target/release/pulse-inventory /usr/local/bin/inventory
CMD ["inventory"]
