# Windows Native (No Docker)

This guide is for running `chunkdb` directly on Windows without Docker.

Stable support boundary:

- Windows native core path: supported.
- Windows native TLS path: not yet guaranteed as fully supported.

Important shell context:

- Open **MSYS2 MinGW64** shell.
- Do **not** run these commands in PowerShell or `cmd.exe`.

Quick shell check:

```bash
echo $MSYSTEM
```

Expected output example:

```text
MINGW64
```

## 1) Install toolchain packages

```bash
pacman -Syu --noconfirm
# If MSYS2 asks you to restart the shell, close it and reopen "MSYS2 MinGW64",
# then run:
pacman -S --needed --noconfirm \
  mingw-w64-x86_64-toolchain \
  mingw-w64-x86_64-cmake \
  mingw-w64-x86_64-ninja \
  git
```

Expected output example:

```text
:: Synchronizing package databases...
:: Starting full system upgrade...
```

## 2) Configure and build

```bash
cd /c/Users/<your-user>/chunkdb
cmake -S . -B build -G Ninja -DCHUNKDB_BUILD_TESTS=ON -DCHUNKDB_WITH_TLS=OFF
cmake --build build
```

Expected output example:

```text
-- Build files have been written to: /c/Users/<your-user>/chunkdb/build
[100%] Built target chunkdb_server
```

## 3) Run smoke tests

```bash
ctest --test-dir build -L smoke --output-on-failure
```

Expected output example:

```text
100% tests passed, 0 tests failed
Label Time Summary: smoke = ...
```

## 4) Run server

```bash
printf 'chunk-token\n' > ./chunkdb.token
./build/chunkdb_server \
  --listen-uri chunk://127.0.0.1:4242/ \
  --token-file ./chunkdb.token \
  --data-dir ./data \
  --durability relaxed \
  --workers 4
```

Expected output example:

```text
2026-03-16 10:12:22.001 INFO server pid=1234 ready to accept connections protocol=tcp host=127.0.0.1 port=4242 tls=off workers=4
```

## 5) Benchmark quick start

Benchmark binaries:
- `./build/chunkdb_server_bench` (protocol benchmark, primary)
- `./build/chunkdb_bench` (direct storage benchmark, internal)

Discover flags:

```bash
./build/chunkdb_server_bench --help
./build/chunkdb_bench --help
```

First benchmark command:

```bash
./build/chunkdb_server_bench \
  --uri chunk://chunk-token@127.0.0.1:4242/ \
  --tests ping,info,set,get \
  --requests 5000 --clients 50 --pipeline 1
```

Token-in-URI benchmark commands are development-only. For server startup, prefer
`--token-file` or `CHUNKDB_TOKEN`.

Expected output example:

```text
chunkdb protocol benchmark
[set]
Throughput (req/s): ...
```

## Benchmark Cleanup Status

Historical local run:
- one Windows-native run printed valid benchmark metrics and then failed during temp-dir cleanup due to `writer.lock` still being in use.
- raw log: [bench/artifacts/manual-runs/server-20260315-windows-native.txt](../bench/artifacts/manual-runs/server-20260315-windows-native.txt)
- reproduction command:
  - `build\\chunkdb_server_bench.exe --server-mode spawn --requests 5000 --port 4242`

Post-fix CI run:
- after teardown hardening, Windows benchmark cleanup completed without the previous failure.
- raw log: [bench/artifacts/manual-runs/server-20260315-windows-ci-70023dd-serial-mutex.txt](../bench/artifacts/manual-runs/server-20260315-windows-ci-70023dd-serial-mutex.txt)

## Troubleshooting

### Wrong shell

Symptom:

- `pacman` is not found, or build tools are missing even after install.

Fix:

- Start **MSYS2 MinGW64** shell explicitly from Start Menu.
- Run `echo $MSYSTEM` and confirm it prints `MINGW64`.

### Missing `ninja`, `cmake`, or `g++`

Symptom:

- CMake reports `CMAKE_MAKE_PROGRAM` not found, or compiler not found.

Fix:

- Re-run package install command from step 1 in **MSYS2 MinGW64** shell.
- Verify tools:

```bash
cmake --version
ninja --version
g++ --version
```

### Path format confusion (`C:\...` vs `/c/...`)

Symptom:

- `cd` fails, or CMake cannot find source/build directories.

Fix:

- In MSYS2, use Unix-style paths:
  - `C:\Users\Alice\chunkdb` -> `/c/Users/Alice/chunkdb`
- Keep `cmake -S . -B build` from the repository root to avoid path mix-ups.
