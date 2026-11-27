#!/usr/bin/env bash
set -euo pipefail

context=${1:?Docker context required}
kind=${2:?Release channel required}
evidence=${3:?Evidence directory required}
[[ $evidence == /tmp/* ]]
work=$(mktemp -d /tmp/rfc-release-trivy.XXXXXXXX)
trap 'rm -rf -- "$work"' EXIT

scanner_version=0.76.2
case "$(uname -m)" in
    x86_64)
        platform=64bit
        checksum=bc0fc726e017b8927a62b2d3531d7a84ee01769daf5c99ed866dced54089fdbf
        ;;
    aarch64)
        platform=ARM64
        checksum=ab9d51f86e6a5fb936f605faff6b593ac51bc378fff4d3e8b9d58358b9c367c3
        ;;
    *) exit 1 ;;
esac
archive=trivy_${scanner_version}_Linux-${platform}.tar.gz
if [[ -n ${RFC_RELEASE_TRIVY_ARCHIVE:-} ]]; then
    cp -- "$RFC_RELEASE_TRIVY_ARCHIVE" "$work/$archive"
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

scan image --download-db-only
scan version --format json > "$evidence/scanner.json"
reports=()
for package in application ripgrep; do
    report=$evidence/$package-advisories.json
    scan fs --scanners vuln --skip-db-update --format json --output "$report" "$evidence/rust/$package"
    reports+=("$report")
done
for role in serve sync feed maintenance; do
    id=$(jq -er --arg role "$role" '.images[$role].image_id' "$evidence/image-identities.json")
    scan image --image-src docker --skip-db-update --format cyclonedx --output "$evidence/$role.cdx.json" "$id"
    report=$evidence/$role-advisories.json
    scan image --image-src docker --scanners vuln --skip-db-update --format json --output "$report" "$id"
    reports+=("$report")
done

python3 -B "$(dirname "$0")/release-audit-policy.py" "$kind" "${reports[@]}"
