#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
export CARGO_TARGET_DIR=/tmp/rust/target TMPDIR=/tmp
context=${RFC_PHASE8_CONTEXT:-rootless}
prefix=${RFC_PHASE8_IMAGE_PREFIX:-rfc-phase8-qualified}
out=$(realpath -m "${1:?Pass a new evidence directory beneath /tmp}")
[[ $out == /tmp/* ]]
[[ $(uname -m) == x86_64 ]]
umask 077
mkdir "$out"
work=$(mktemp -d /tmp/rfc-phase8-audit.XXXXXXXX)
trap 'rm -rf -- "$work"' EXIT

scanner_version=0.76.2
archive=trivy_${scanner_version}_Linux-64bit.tar.gz
checksum=bc0fc726e017b8927a62b2d3531d7a84ee01769daf5c99ed866dced54089fdbf
if [[ -n ${RFC_PHASE8_TRIVY_ARCHIVE:-} ]]; then
    cp -- "$RFC_PHASE8_TRIVY_ARCHIVE" "$work/$archive"
else
    curl --fail --show-error --silent --location --retry 2 \
        --output "$work/$archive" \
        "https://github.com/aquasecurity/trivy/releases/download/v$scanner_version/$archive"
fi
(cd "$work" && printf '%s  %s\n' "$checksum" "$archive" | sha256sum --check --strict)
tar -xzf "$work/$archive" -C "$work" trivy
docker_host=$(docker context inspect "$context" --format '{{.Endpoints.docker.Host}}')
scan() {
    DOCKER_HOST="$docker_host" TRIVY_SKIP_VERSION_CHECK=true \
        "$work/trivy" --config '' --cache-dir "$work/cache" "$@"
}

git rev-parse HEAD > "$out/git-revision.txt"
docker --context "$context" build --target audit-inputs --output "type=local,dest=$work/rust" .
for package in application ripgrep; do
    LC_ALL=C sort -u "$work/rust/$package/packages.txt" > "$out/$package-packages.txt"
    (cd "$work/rust/$package" && sha256sum Cargo.lock) > "$out/$package-lock.sha256"
done

postgres=$(sed -n 's/^    image: \(docker\.io\/library\/postgres:.*\)$/\1/p' compose.yml)
[[ $postgres == docker.io/library/postgres:*@sha256:* ]]
docker --context "$context" pull "$postgres"
scan image --download-db-only
scan version --format json > "$out/scanner.json"

mkdir "$work/source"
git ls-files --cached --others --exclude-standard -z |
    tar -cf - --null -T - |
    tar -xf - -C "$work/source"
scan fs --scanners vuln,license --license-full --skip-db-update --format json \
    --output "$out/source-review.json" "$work/source"
scan fs --scanners vuln --skip-db-update --format json \
    --output "$out/ripgrep-advisories.json" "$work/rust/ripgrep"
scan fs --scanners secret --format json --output "$work/source-secrets.json" "$work/source"
python3 -B scripts/phase8/secret-summary.py "$work/source-secrets.json" > "$out/source-secrets.json"

for role in serve sync feed postgres; do
    image=$prefix-$role
    if [[ $role == postgres ]]; then
        image=$postgres
    fi
    id=$(docker --context "$context" image inspect --format '{{.Id}}' "$image")
    printf '%s %s %s\n' "$role" "$image" "$id" >> "$out/images.txt"
    scan image --image-src docker --format cyclonedx --output "$out/$role.cdx.json" "$id"
    scan image --image-src docker --scanners vuln,license --skip-db-update --format json \
        --output "$out/$role-review.json" "$id"
    scan image --image-src docker --scanners secret --format json \
        --output "$work/$role-secrets.json" "$id"
    python3 -B scripts/phase8/secret-summary.py "$work/$role-secrets.json" > "$out/$role-secrets.json"
done

printf 'Review the resulting findings in %s\n' "$out"
