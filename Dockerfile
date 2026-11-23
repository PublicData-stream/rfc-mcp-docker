# Alpine image indexes, including the linux/amd64 and linux/arm64 manifests.
FROM docker.io/library/rust:1.99.1-alpine3.24@sha256:44819b27be4c97b0e4d7203bd2e02c4820f97700211b6a37fb4c246a2de59383 AS build
WORKDIR /src
ENV CARGO_TARGET_DIR=/tmp/rust/target
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY apps apps
COPY crates crates
COPY fixtures fixtures
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked cargo fetch --locked
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    cargo build --release --workspace --bins --frozen \
    && mkdir /out \
    && cp /tmp/rust/target/release/rfc-mcp-docker-serve \
          /tmp/rust/target/release/rfc-mcp-docker-sync \
          /tmp/rust/target/release/rfc-mcp-docker-feed /out/

# The fixture driver is excluded from service images.
FROM build AS phase1-build
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    cargo build --release --bin rfc-phase1-fixture --features phase1-fixtures --frozen \
    && cp /tmp/rust/target/release/rfc-phase1-fixture /out/

FROM build AS phase2-build
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    cargo build --release --bin rfc-phase2-fixture --features phase2-fixtures --frozen \
    && cp /tmp/rust/target/release/rfc-phase2-fixture /out/

FROM build AS phase3-build
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    cargo build --release --bin rfc-phase3-fixture --features phase3-fixtures --frozen \
    && cp /tmp/rust/target/release/rfc-phase3-fixture /out/

FROM build AS phase4-build
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    cargo build --release --bin rfc-phase4-fixture --features phase4-fixtures --frozen \
    && cp /tmp/rust/target/release/rfc-phase4-fixture /out/

FROM docker.io/library/rust:1.99.1-alpine3.24@sha256:44819b27be4c97b0e4d7203bd2e02c4820f97700211b6a37fb4c246a2de59383 AS ripgrep
WORKDIR /ripgrep
RUN apk add --no-cache make=4.4.1-r4
RUN set -eu; \
    commit=e89fff89ac9af12e8d4ce9d5fd07beb408ca730f; \
    wget -q -O /tmp/rg.tar.gz "https://codeload.github.com/BurntSushi/ripgrep/tar.gz/$commit"; \
    echo 'af40d0f4cb7a55994906c009a7ba865047f0d487c95bceb8db59ffc4f2173d20  /tmp/rg.tar.gz' | sha256sum -c -; \
    tar -xzf /tmp/rg.tar.gz --strip-components=1; \
    rm /tmp/rg.tar.gz
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked cargo fetch --locked
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    CARGO_PROFILE_RELEASE_STRIP=debuginfo RUSTFLAGS="-C target-feature=+crt-static" \
      cargo build --target-dir /tmp/rust/target/rg \
      --release --bin rg --no-default-features --frozen \
    && cp /tmp/rust/target/rg/release/rg /tmp/rg

FROM docker.io/library/alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS runtime

FROM runtime AS serve
COPY --from=build /out/rfc-mcp-docker-serve /usr/local/bin/
COPY --from=ripgrep /tmp/rg /usr/local/bin/rg
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/rfc-mcp-docker-serve"]

FROM runtime AS sync
RUN apk add --no-cache rsync=3.5.2-r1
COPY --from=build /out/rfc-mcp-docker-sync /usr/local/bin/
USER 10002:10002
ENTRYPOINT ["/usr/local/bin/rfc-mcp-docker-sync"]

# Disposable upstream fixture tools used only by the integration harness.
FROM sync AS phase0-fixtures
USER root
RUN apk add --no-cache openssl python3
USER 10004:10004

FROM runtime AS feed
COPY --from=build /out/rfc-mcp-docker-feed /usr/local/bin/
USER 10003:10003
ENTRYPOINT ["/usr/local/bin/rfc-mcp-docker-feed"]

FROM runtime AS phase1-fixture
COPY --from=phase1-build /out/rfc-phase1-fixture /rfc-phase1-fixture

FROM runtime AS phase2-fixture
COPY --from=phase2-build /out/rfc-phase2-fixture /rfc-phase2-fixture

FROM runtime AS phase3-fixture
COPY --from=phase3-build /out/rfc-phase3-fixture /rfc-phase3-fixture

FROM runtime AS phase4-fixture
COPY --from=phase4-build /out/rfc-phase4-fixture /rfc-phase4-fixture

# Phase 6 exercises daemon entrypoints with synthetic upstreams and crash hooks.
FROM build AS phase6-build
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    cargo build --release --bin rfc-mcp-docker-sync --bin rfc-mcp-docker-feed \
        --features rfc-mcp-docker/phase6-fixtures,rfc-mcp-docker-sync/phase6-fixtures --frozen \
    && cp /tmp/rust/target/release/rfc-mcp-docker-sync /out/rfc-phase6-sync \
    && cp /tmp/rust/target/release/rfc-mcp-docker-feed /out/rfc-phase6-feed

FROM runtime AS phase6-fixture
COPY --from=phase6-build /out/rfc-phase6-sync /out/rfc-phase6-feed /
