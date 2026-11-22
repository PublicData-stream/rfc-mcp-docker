# 0006 — Ripgrep worker confinement

Run the pinned static musl ripgrep in an unprivileged Alpine serve image. Close inherited descriptors, set no-new-privileges and resource limits, and apply Landlock plus a seccomp syscall allowlist before exec. Pass only approved source paths and arguments to a worker with a clean environment. Fail closed if any restriction cannot be installed.

Use a read-only runtime root, dropped capabilities, Docker's default seccomp profile, and bounded scratch. The disposable probe tests the boundary using synthetic files and a local socket. Phase 07 adds generation-aware file selection, cancellation, process-group cleanup, and scan and output limits.

[Landlock](https://www.kernel.org/doc/html/latest/userspace-api/landlock.html) · [Seccomp](https://docs.kernel.org/userspace-api/seccomp_filter.html)
