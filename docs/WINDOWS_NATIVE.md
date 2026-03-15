# Windows Native (No Docker)

This guide is for running `chunkdb` directly on Windows without Docker.

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
100% tests passed, 0 tests failed out of 15
Label Time Summary: smoke = ...
```

## 4) Run server

```bash
./build/chunkdb_server \
  --listen-uri chunk://dev-token@127.0.0.1:4242/ \
  --data-dir ./data \
  --durability relaxed \
  --workers 4
```

Expected output example:

```text
chunkdb listening on 127.0.0.1:4242 workers=4 durability=relaxed
```

## Known Benchmark Caveat

In one Windows-native benchmark run, `chunkdb_server_bench` printed valid results and then failed while deleting its temp directory because the lock file was still in use.

Reproduction command:

```text
build\chunkdb_server_bench.exe --ops 5000 --port 4242
```

Raw log:
- [bench/artifacts/manual-runs/server-20260315-windows-native.txt](../bench/artifacts/manual-runs/server-20260315-windows-native.txt)

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
