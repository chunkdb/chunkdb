#!/bin/sh
set -eu

# Mirror bootstrap-related server options without consuming its arguments.
data_dir=/var/lib/chunkdb/data
auth=scram
admin_user=${CHUNKDB_ADMIN_USER:-admin}
password_file=${CHUNKDB_ADMIN_PASSWORD_FILE:-}
previous=
for argument do
    case "$previous" in
        --data-dir) data_dir=$argument ;;
        --auth) auth=$argument ;;
        --admin-user) admin_user=$argument ;;
        --admin-password-file) password_file=$argument ;;
    esac
    case "$argument" in
        --help|--version|-h) exec chunkdb_server "$@" ;;
    esac
    previous=$argument
done

if [ "$auth" = scram ]; then
    if [ -n "$password_file" ]; then
        export CHUNKDB_ADMIN_USER="$admin_user"
        set -- "$@" --admin-password-file "$password_file"
    elif [ -n "${CHUNKDB_ADMIN_PASSWORD:-}" ]; then
        export CHUNKDB_ADMIN_USER="$admin_user"
    elif [ ! -e "$data_dir/chunkdb.users" ]; then
        CHUNKDB_ADMIN_PASSWORD=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
        export CHUNKDB_ADMIN_USER="$admin_user" CHUNKDB_ADMIN_PASSWORD
        printf 'First administrator: %s\nGenerated password: %s\n' "$admin_user" "$CHUNKDB_ADMIN_PASSWORD"
        printf 'Change it with chunk-cli --uri chunk://%s@127.0.0.1:4242/ "ALTER USER %s PASSWORD"\n' "$admin_user" "$admin_user"
    fi
fi
exec chunkdb_server "$@"
