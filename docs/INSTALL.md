# Install the server

You can run a downloaded server or build one from source.

## Download

Choose your platform's latest archive from [Releases](https://github.com/chunkdb/chunkdb/releases/latest).
Follow [Verify a downloaded archive](VERIFY_RELEASE.md) before extracting it.
The archive contains `chunkdb_server`, `chunkdb_verify`, `chunkdb_restore`, and `chunkdb_admin`.
Windows executables have the `.exe` suffix.

## Start

Run the server from the extracted directory:

```sh
export CHUNKDB_ADMIN_USER=admin
export CHUNKDB_ADMIN_PASSWORD='choose-a-long-private-password'
./chunkdb_server --listen-uri chunk://127.0.0.1:4242/ --data-dir ./data --backup-dir ./backups
```

Keep the process running.
Follow [Quick start](QUICK_START.md#2-connect-the-shell) with your chosen password instead of the Docker password export.
Set `CHUNKDB_PASSWORD` to the same password when connecting the CLI.
Bootstrap settings apply only before users exist.

## Build from source

You need a C++20 compiler and CMake 3.20 or later.
OpenSSL enables TLS; see [Windows native](WINDOWS_NATIVE.md) for the Windows toolchain.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCHUNKDB_WITH_TLS=ON
cmake --build build --parallel 3 --target chunkdb_server chunkdb_verify chunkdb_restore chunkdb_admin
```

Use `./build/chunkdb_server` in the start command.
If CMake cannot find OpenSSL, it warns and builds without TLS.
See [Server flags](SERVER_FLAGS.md) for paths, connection limits, and TLS settings.
