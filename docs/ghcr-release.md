# Publish the RFC MCP service images

The [publish workflow](../.github/workflows/publish-images.yml) publishes a fixed version tag to each package under `ghcr.io/publicdata-stream/`.

| Package | Compose variable |
| --- | --- |
| `rfc-mcp-docker-serve` | `RFC_SERVE_IMAGE` |
| `rfc-mcp-docker-sync` | `RFC_SYNC_IMAGE` |
| `rfc-mcp-docker-feed` | `RFC_FEED_IMAGE` |

Each tag identifies a two-platform OCI index for `linux/amd64/v3` and `linux/arm64`. The AMD64 binaries require an x86-64-v3 host CPU or guest CPU configuration. Use the three index digests in the workflow summary, or in the `ghcr-release-digests` artifact, to pin the matching Compose inputs. The artifact also records the individual platform digests and source revision. [Compose deployment](deployment.md)

Starting with the first stable release that runs the updated workflow, each package also has a mutable `:latest` tag. The workflow sets it to the stable release's exact index digest after both platforms of all three versioned indexes pass anonymous pull verification. Beta and build releases receive version tags only.

## Trigger publication

1. Start from a commit on `main` whose most recent exact-commit branch-push CI run passed. Choose a new version, check the remote tag name, and create a GPG-signed annotated tag with `git tag -s`. Verify it locally and make sure GitHub can verify the signing key.
2. Push exactly that tag ref. A new `X.Y.Z-build.<sha8>` tag starts publication. Use the first eight lowercase hexadecimal characters of the commit SHA. For `X.Y.Z` and `X.Y.Z-beta.N`, publish a GitHub Release for the existing tag. Mark beta releases as prereleases.
3. Check the release run. Both native build jobs must pass the Phase 0 probes and the image audit before the publish job writes any version tag. Review critical vulnerabilities for all channels and high vulnerabilities with a fix for stable and beta. The audit artifacts include the Trivy reports, service SBOMs, and Cargo dependency inventories for both architectures. Follow the [Compose release checklist](release-readiness.md) for redistribution review, PostgreSQL qualification, and deployment.

## Verify and recover

The publisher links each package to this repository through the OCI source label. GitHub creates new packages as private. A package admin must make the three packages public in their GitHub package settings. Rerun the failed anonymous verification job after setting visibility. A successful check downloads both platform images for every package without credentials and compares them with the digest receipt. [GitHub package visibility](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility#configuring-visibility-of-packages-for-an-organization)

If publication fails partway through, rerun failed jobs within the 14-day archive retention window to reuse the qualified image archives. The publisher accepts an existing tag only when its digest matches and fills in missing tags. If a tag points to different bytes, investigate the conflict and use a new release version for changed content. Treat every pushed Git release tag and GHCR version tag as fixed.

GHCR updates each `:latest` tag individually. If promotion fails, the three aliases can temporarily point to different releases. Rerun the failed promotion for the highest verified stable release to finish updating the aliases. The job skips aliases that already match and skips the entire promotion if any alias points to a higher stable version. Keep using the three digests from a single receipt for a coordinated Compose deployment.
