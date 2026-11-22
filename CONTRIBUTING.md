# Contributing

Use the Rust toolchain in `rust-toolchain.toml`. Fetch locked dependencies once with `cargo fetch --locked`, then run `scripts/phase0/check-rust.sh`. Run the [Compose probes](docs/deployment-prerequisites.md) for changes to runtime dependencies, images, or isolation.

Keep commits focused, GPG-sign each commit, and add the `Assisted-by: Codex <codex@openai.com>` trailer to Codex-assisted commits. Treat registry publication and deployment as separate operations. Third-party RFC fixtures carry their own notices and provenance.
