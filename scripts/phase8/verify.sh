#!/bin/sh
set -eu

cd "$(dirname "$0")/../.."
export CARGO_TARGET_DIR=/tmp/rust/target TMPDIR=/tmp PYTHONDONTWRITEBYTECODE=1
context=${RFC_PHASE8_CONTEXT:-rootless}
prefix=${RFC_PHASE8_IMAGE_PREFIX:-"rfc-phase8-$$"}
evidence=$(mktemp -d /tmp/rfc-phase8-evidence.XXXXXXXX)
printf 'Evidence directory: %s\n' "$evidence"

failed=0
check() {
    name=$1
    shift
    printf '\n%s: %s\n' "$name" "$*"
    if "$@" >"$evidence/$name.log" 2>&1; then
        printf '%s PASS\n' "$name" | tee -a "$evidence/results.txt"
    else
        printf '%s FAIL\n' "$name" | tee -a "$evidence/results.txt"
        tail -n 25 "$evidence/$name.log"
        failed=1
    fi
}

check revision git rev-parse HEAD
check host uname -srmo
check rust-version rustc --version --verbose
check docker-version docker --context "$context" version
check compose-version docker --context "$context" compose version
check rust scripts/phase0/check-rust.sh
check clippy-all cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings
check test-all cargo test --workspace --all-features --frozen
check fuzz-http python3 -B scripts/phase8/http-smoke.py
check workflows actionlint
check compose docker --context "$context" compose --env-file deploy/compose.env.example config --quiet
check maintenance-image docker --context "$context" build --target maintenance -t "$prefix-maintenance" .
check phase0 python3 -B scripts/phase0/integration.py --context "$context" --project "$prefix"

for phase in 1 2 3 4 5 6 7; do
    check "phase$phase" python3 -B "scripts/phase$phase/integration.py" \
        --context "$context" --image-prefix "$prefix"
done
check auto-upgrade python3 -B scripts/https-discovery/automatic_upgrade.py --context "$context" --image-prefix "$prefix"
check recovery python3 -B scripts/phase8/integration.py --context "$context" \
    --image-prefix "$prefix" --evidence "$evidence"
check capacity python3 -B scripts/phase8/capacity.py --context "$context" \
    --image-prefix "$prefix" --evidence "$evidence"

exit "$failed"
