#!/bin/sh
set -eu

export CARGO_TARGET_DIR=/tmp/rust/target
export TMPDIR=$(mktemp -d /tmp/rfc-mcp-rust.XXXXXXXX)
trap 'rmdir "$TMPDIR" 2>/dev/null || true' EXIT

cargo fmt --all -- --check
cargo build --workspace --frozen
cargo clippy --workspace --all-targets --frozen -- -D warnings
cargo test --workspace --all-targets --frozen
python3 scripts/phase0/mcp-wire.py --binary "$CARGO_TARGET_DIR/debug/rfc-mcp-docker-serve"
cargo test --workspace --doc --frozen
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --frozen
