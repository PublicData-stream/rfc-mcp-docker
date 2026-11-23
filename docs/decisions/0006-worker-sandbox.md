# 0006 — Ripgrep worker confinement

Build pinned static musl ripgrep without PCRE2 in an unprivileged Alpine serve image. Close inherited descriptors, set no-new-privileges and resource limits, and apply Landlock plus a seccomp syscall allowlist before exec. Send an approved original over stdin and give the worker no pathname access to the mirror. Fail closed if any restriction cannot be installed.

Use a read-only runtime root, dropped capabilities, Docker's default seccomp profile, and bounded scratch. The disposable probe tests the same confinement using synthetic files and a local socket.

[Landlock](https://www.kernel.org/doc/html/latest/userspace-api/landlock.html) · [Seccomp](https://docs.kernel.org/userspace-api/seccomp_filter.html)
