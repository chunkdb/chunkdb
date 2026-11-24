#!/bin/sh
set -eu

# The image's health probe. With --metrics-listen among the server's
# arguments it asks for GET /metrics, which the server renders from its
# tables without a login; otherwise it sends HELLO 3 to port 4242, answered
# with the server's map (`%...`) or with AUTH_REQUIRED when logins need a user.
address=$(tr '\0' '\n' < /proc/1/cmdline | sed -n '/^--metrics-listen$/{n;p;}' | tail -n 1)

if [ -z "$address" ]; then
    printf 'HELLO 3\r\n' | nc -w 2 127.0.0.1 4242 | grep -q -e '^%' -e '^-ERR AUTH_REQUIRED'
    exit
fi

port=${address##*:}
host=${address%:*}
host=${host#[}
host=${host%]}
case "$host" in
    0.0.0.0) host=127.0.0.1 ;;
    ::) host=::1 ;;
esac
printf 'GET /metrics HTTP/1.0\r\n\r\n' | nc -w 2 "$host" "$port" | head -n 1 | grep -q '^HTTP/1.1 200 '
