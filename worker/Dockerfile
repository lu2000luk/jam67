FROM rust:1.75-alpine AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/target/release/jam67-ws-server .

EXPOSE 8080

CMD ["./jam67-ws-server"]
