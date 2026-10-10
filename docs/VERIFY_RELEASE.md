# Verify a downloaded archive

Download the platform archive and its matching `.sha256` sidecar from the same release.
GitHub's source archives are source snapshots, rather than executable packages.
Compare the downloaded bytes with the first hash field in the sidecar before extracting.
A matching checksum detects file mismatch/corruption; it does not authenticate a signer or establish how the archive was built.

## macOS

Set `archive` to the downloaded filename, then run:

```sh
archive=chunkdb-v2.0.0-macos-arm64.tar.gz
actual=$(shasum -a 256 "$archive" | awk '{print $1}')
expected=$(awk '{print $1}' "$archive.sha256")
test "$actual" = "$expected" && echo OK || echo MISMATCH
```

## Linux

```sh
archive=chunkdb-v2.0.0-linux-x86_64.tar.gz
actual=$(sha256sum "$archive" | awk '{print $1}')
expected=$(awk '{print $1}' "$archive.sha256")
test "$actual" = "$expected" && echo OK || echo MISMATCH
```

## Windows PowerShell

```powershell
$archive = 'chunkdb-v2.0.0-windows-x86_64.zip'
$actual = (Get-FileHash $archive -Algorithm SHA256).Hash.ToLower()
$expected = ((Get-Content "$archive.sha256") -split '\s+')[0].ToLower()
if ($actual -eq $expected) { 'OK' } else { 'MISMATCH' }
```

The filenames illustrate platform/version selection; use the exact filename attached to your selected release.
MISMATCH means the archive should not be used as a verified download; obtain the matching archive and sidecar again.
After extraction, follow [quick start](QUICK_START.md) with the included server and tools.
Release operators use the separate [release checklist](releases/RELEASE_CHECKLIST.md).
