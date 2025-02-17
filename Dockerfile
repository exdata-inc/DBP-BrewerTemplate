FROM rust:latest AS builder

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y protobuf-compiler build-essential

RUN cargo build --release

FROM rust:latest

WORKDIR /app

COPY --from=builder /app/target/release/dbp-brewer-template /usr/local/bin/dbp-brewer-template

ENTRYPOINT ["dbp-brewer-template"]
