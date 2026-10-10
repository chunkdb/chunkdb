# Release policy for 2.x

Release operators must validate the [compatibility promise](../COMPATIBILITY.md), [durability contract](../DURABILITY_CONTRACT.md) and documented platform boundary before publishing.
Within 2.x, protocol and storage changes are additive; breaking changes require 3.0.
Clients version independently and must be validated against protocol 3.

Use the [release checklist](RELEASE_CHECKLIST.md) for tests, archives and artifact checks.
Publish only artifacts whose target architecture, runtime dependencies and supported platform behavior have been verified.
Native Windows TLS claims cover MSYS2 MinGW64 and its OpenSSL package.
Document known limitations and compatibility changes before release rather than weakening those claims after publication.
Historical release descriptions are in [CHANGELOG_HISTORY.md](CHANGELOG_HISTORY.md).
