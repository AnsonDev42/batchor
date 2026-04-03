# Versioning Policy

`batchor` follows a `0.x` SemVer-style policy.

Current guarantees:

- patch releases should preserve the documented public behavior of the Python API and CLI
- minor releases may include breaking changes before `1.0`
- the README, docs, and released package metadata define the supported public surface

Support window:

- only the latest released `0.x` minor is supported
- older minors should be treated as frozen and unsupported once a newer minor is released

Compatibility notes:

- public Python API changes should be documented in release notes
- CLI command behavior is part of the public surface and should follow the same policy
- storage schema changes must be documented clearly before release
