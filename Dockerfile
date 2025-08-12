FROM rust:slim-bullseye AS buildstage
WORKDIR /build
ENV PROTOC_NO_VENDOR 1
RUN rustup component add rustfmt && \
    apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libssl-dev  && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
COPY . /build/
RUN cargo build --release

FROM rust:slim-bullseye
RUN useradd -m bbs
USER bbs
COPY --from=buildstage /build/target/release/bbs /usr/bin/
CMD ["bbs", "--db-url $DB_URL", "--pds $PDS"]
