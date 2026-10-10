# Windows native in 2.0

Use the MSYS2 MinGW64 toolchain for native Windows builds, including TLS with its OpenSSL package.
MSVC and other Windows OpenSSL distributions are untested.
Run the commands below in the **MSYS2 MinGW64** shell; `echo "$MSYSTEM"` should print MINGW64.

## Install and build

```sh
pacman -Syu --noconfirm
pacman -S --needed --noconfirm mingw-w64-x86_64-toolchain mingw-w64-x86_64-cmake mingw-w64-x86_64-ninja mingw-w64-x86_64-openssl git
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCHUNKDB_WITH_TLS=ON
cmake --build build --parallel 3
```

If MSYS2 requests a shell restart during upgrade, reopen MinGW64 before installing the remaining packages.
Run CMake from the repository root; in this shell `C:\Users\Alice` is `/c/Users/Alice`.
CMake must find OpenSSL for TLS: otherwise it warns and produces a server without TLS support.
For a plain-only build set `-DCHUNKDB_WITH_TLS=OFF`.

## Start and connect

```sh
printf 'change-me\n' > admin.password
MSYS2_ARG_CONV_EXCL="*" ./build/chunkdb_server.exe --listen-uri chunk://127.0.0.1:4242/ --data-dir ./data --backup-dir ./backups --admin-user admin --admin-password-file ./admin.password --workers 4
```

Keep the server running and follow the CLI commands in [quick start](QUICK_START.md) with that password.
MSYS2 argument conversion can reinterpret connection URIs and `/CN=...` arguments; `MSYS2_ARG_CONV_EXCL="*"` prevents conversion for commands that contain them.
Native Windows paths remain usable for directory/file flags; BACKUP destination names use `/` separators on every platform.

## TLS

Supply a PEM certificate and key with a `chunks://` listen URI:

```sh
MSYS2_ARG_CONV_EXCL="*" ./build/chunkdb_server.exe --listen-uri chunks://127.0.0.1:4242/ --tls-cert cert.pem --tls-key key.pem --data-dir ./data --backup-dir ./backups
```

Use a client with the certificate's trusted CA and matching server name.
See [client connection guides](QUICK_START.md#a-small-world-in-each-client) and [server flags](SERVER_FLAGS.md).
Bootstrap settings can be omitted after users exist; they do not reset passwords.

## Filesystem requirements and troubleshooting

The server requires writer ownership and the atomic publication/sync operations described by the [durability contract](DURABILITY_CONTRACT.md).
Required directory-sync capability failures are errors, including snapshot bookkeeping in relaxed mode.
Keep all required manifests, clocks and initialized markers with a consistent [backup](BACKUP.md).
If pacman or build tools are missing, confirm MinGW64 and the installed packages before reconfiguring.
If TLS startup is unavailable, inspect CMake's OpenSSL discovery and use the MinGW64 package rather than mixing toolchains.
Close your own server before offline verification/restore or password reset; filesystem sharing does not authorize a second writer.
