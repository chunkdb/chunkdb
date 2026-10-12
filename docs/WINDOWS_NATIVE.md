# Windows native

Use the MSYS2 UCRT64 toolchain for native Windows builds, including TLS with its OpenSSL package.
You need Windows 10 or later, which includes the Universal C Runtime.
MSVC and other Windows OpenSSL distributions are untested.

Run the commands below in the **MSYS2 UCRT64** shell.
Check that `echo "$MSYSTEM"` prints UCRT64.

## Install and build

```sh
pacman -Syu --noconfirm
pacman -S --needed --noconfirm mingw-w64-ucrt-x86_64-toolchain mingw-w64-ucrt-x86_64-cmake mingw-w64-ucrt-x86_64-ninja mingw-w64-ucrt-x86_64-openssl git
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCHUNKDB_WITH_TLS=ON
cmake --build build --parallel 3
```

If MSYS2 requests a shell restart during upgrade, reopen UCRT64 before installing the remaining packages.
Run CMake from the repository root.
In this shell, `C:\Users\Alice` is `/c/Users/Alice`.

CMake must find OpenSSL for TLS: otherwise it warns and produces a server without TLS support.
For a plain-only build set `-DCHUNKDB_WITH_TLS=OFF`.

## Start and connect

```sh
printf 'choose-a-long-private-password\n' > admin.password
MSYS2_ARG_CONV_EXCL="*" ./build/chunkdb_server.exe --listen-uri chunk://127.0.0.1:4242/ --data-dir ./data --backup-dir ./backups --admin-user admin --admin-password-file ./admin.password --workers 4
```

Keep the server running and install chunk-cli using the first command block in [Connect the shell](QUICK_START.md#2-connect-the-shell).
In another UCRT64 shell, connect with your chosen password:

```sh
export CHUNKDB_PASSWORD='choose-a-long-private-password'
export CHUNKDB_URI='chunk://admin@127.0.0.1:4242/'
MSYS2_ARG_CONV_EXCL="*" chunk-cli --uri "$CHUNKDB_URI" shell
```

Continue with [Write and read blocks](QUICK_START.md#3-write-and-read-blocks).
MSYS2 argument conversion can reinterpret connection URIs and `/CN=...` arguments.
Use `MSYS2_ARG_CONV_EXCL="*"` for commands containing them.
Native Windows paths work for directory and file flags.

BACKUP destination names use `/` separators on every platform.

## TLS

Supply a PEM certificate and key with a `chunks://` listen URI:

```sh
MSYS2_ARG_CONV_EXCL="*" ./build/chunkdb_server.exe --listen-uri chunks://127.0.0.1:4242/ --tls-cert cert.pem --tls-key key.pem --data-dir ./data --backup-dir ./backups
```

Use a client with the certificate's trusted CA and matching server name.
See [Quick start](QUICK_START.md#2-connect-the-shell) and [server flags](SERVER_FLAGS.md).
You can omit bootstrap settings after users exist.
They do not reset passwords.

## Filesystem requirements and troubleshooting

The server requires writer ownership and the atomic publication/sync operations described by the [durability contract](DURABILITY_CONTRACT.md).
Required directory-sync capability failures are errors, including snapshot bookkeeping in relaxed mode.
Keep all required manifests, clocks and initialized markers with a consistent [backup](BACKUP.md).

If pacman or build tools are missing, confirm UCRT64 and the installed packages before reconfiguring.

If TLS startup is unavailable, inspect CMake's OpenSSL discovery and use the UCRT64 package rather than mixing toolchains.
Close your server before offline verification, restore, or password reset.
Filesystem sharing does not authorize a second writer.
