# Security Policy

## Supported versions

`nkv` is pre-release (`v0.x`) with a non-finalized API. Only the latest tagged
release receives security fixes; there are no backports to earlier `v0.x` tags.
Fixes ship in a new release rather than as patches to older ones.

| Version        | Supported |
| -------------- | --------- |
| latest `v0.x`  | ✅        |
| older `v0.x`   | ❌        |

This policy will be revisited when `v1.0.0` is released.

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues, pull
requests, or discussions.**

Report privately using GitHub's private vulnerability reporting:

1. Go to the [Security tab](../../security) of this repository.
2. Click **Report a vulnerability**.
3. Fill in the details.

This opens a private advisory visible only to you and the maintainers.

### What to include

- The type of issue (e.g. auth bypass, data exposure, denial of service).
- Affected version or commit.
- Step-by-step reproduction, ideally a minimal Go program.
- Any relevant bucket/stream configuration or NATS server version.
- Impact — what an attacker can achieve.

### What to expect

- **Acknowledgement:** we aim to confirm receipt within a few business days.
- **Assessment:** we will confirm the issue and determine severity.
- **Fix:** developed privately, then released alongside a published GitHub Security
  Advisory.
- **Credit:** we are happy to credit you in the advisory unless you prefer otherwise.

Please give us a reasonable opportunity to release a fix before public disclosure.

## Scope

`nkv` is a client library for NATS JetStream Key-Value buckets. In scope:

- Flaws in this library that allow bypassing bucket/key access constraints, corrupting
  bucket data, leaking data across buckets or keys, or causing unbounded resource use in
  a consuming application.
- Unsafe handling of untrusted key names, subjects, headers, or encoded values.

Out of scope:

- Vulnerabilities in `nats-server` or `nats.go` — report those to the
  [NATS project](https://github.com/nats-io/nats-server/security/policy).
- Insecure deployment or configuration of NATS itself (open ports, missing TLS, missing
  authentication).
- Issues that require an already-compromised NATS server or credentials.
