#!/usr/bin/env bash
# Run the named shell fences in QUICK_START and PASSWORD_RESET against an owned
# Docker fixture. chunkdb_docs_check extracts and rewrites the page commands
# (unique container, volume, freshly built image, ephemeral loopback port) and
# redacts commands.log. Every exit path captures the container logs and removes
# this run's container and volume. Needs bash 4+, docker and GNU timeout.
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: docker-quickstart.sh --tool PATH --cli PATH [--page FILE] [--reset-page FILE]
                            [--image IMAGE] [--logs DIR]
  --tool        built chunkdb_docs_check
  --cli         built chunk-cli
  --page        default docs/QUICK_START.md
  --reset-page  default docs/PASSWORD_RESET.md
  --image       default chunkdb:quickstart-ci
  --logs        default docker-quickstart-logs (commands.log, checks.json)
EOF
  exit 2
}

tool=""
cli=""
page="docs/QUICK_START.md"
reset_page="docs/PASSWORD_RESET.md"
image="chunkdb:quickstart-ci"
logs="docker-quickstart-logs"
while (($#)); do
  case "$1" in
    --tool | --cli | --page | --reset-page | --image | --logs)
      (($# >= 2)) || usage
      name="${1#--}"
      printf -v "${name//-/_}" '%s' "$2"
      shift 2
      ;;
    *) usage ;;
  esac
done
if [[ -z "$tool" || ! -x "$tool" ]]; then
  echo "docker-quickstart: --tool must name the built chunkdb_docs_check executable" >&2
  usage
fi
if [[ -z "$cli" || ! -f "$cli" ]]; then
  echo "docker-quickstart: --cli must name the built chunk-cli executable" >&2
  usage
fi
if command -v timeout >/dev/null 2>&1; then
  timeout_command=timeout
elif command -v gtimeout >/dev/null 2>&1; then
  timeout_command=gtimeout
else
  echo "docker-quickstart: GNU timeout is required" >&2
  exit 1
fi

mkdir -p "$logs"
log="$logs/commands.log"
work="$(mktemp -d "${TMPDIR:-/tmp}/chunkdb-page-XXXXXX")"
container="chunkdb-page-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
volume="$container-data"
port=""
secret_files=()
passed=()
error_message=""
cleanup_error=""
fixture_started=0
watch_pid=""
watch_open=0
watch_exit=0
run_out="$work/output"
run_status=0

export CHUNKDB_URI='chunk://admin@127.0.0.1:4242/'
PATH="$(cd "$(dirname "$cli")" && pwd -P):$PATH"
export PATH

fail() {
  error_message="$1"
  exit 1
}

trim() {
  local text="$1"
  text="${text#"${text%%[![:space:]]*}"}"
  printf '%s' "${text%"${text##*[![:space:]]}"}"
}

redact() {
  local args=() file
  for file in ${secret_files[@]+"${secret_files[@]}"}; do args+=(--secret "$file"); done
  "$tool" docker-quickstart-commands --redact ${args[@]+"${args[@]}"}
}

# Appends TEXT and a newline to commands.log with secrets redacted.
record() {
  printf '%s\n' "$1" | redact >>"$log"
}

record_file() {
  redact <"$1" >>"$log"
  printf '\n' >>"$log"
}

add_secret() {
  local file="$work/secret.${#secret_files[@]}"
  printf '%s' "$1" >"$file"
  secret_files+=("$file")
}

# Prints one extracted page command; see chunkdb_docs_check docker-quickstart-commands.
query() {
  "$tool" docker-quickstart-commands --page "$page" --reset-page "$reset_page" \
    --container "$container" --image "$image" ${port:+--port "$port"} "$@"
}

# query_into VARIABLE ARGS...: sets VARIABLE to the exact output, trailing newlines included.
query_into() {
  local _query_target="$1" _query_value
  shift
  _query_value="$(query "$@" && printf x)" || fail "cannot extract page command: $*"
  printf -v "$_query_target" '%s' "${_query_value%x}"
}

# run -c COMMAND | run ARGV...: records the command and its output, sets
# run_status and leaves the output in $run_out. Status 124 is a timeout.
run() {
  if [[ "$1" == -c ]]; then
    record "\$ $2"
    set +e
    "$timeout_command" -k 5 60 /bin/bash -c "$2" >"$run_out" 2>&1
    run_status=$?
    set -e
  else
    record "\$ $*"
    set +e
    "$timeout_command" -k 5 60 "$@" >"$run_out" 2>&1
    run_status=$?
    set -e
  fi
  if ((run_status == 124)); then
    return
  fi
  record_file "$run_out"
}

run_checked() {
  run "$@"
  if ((run_status == 124)); then
    fail "command timed out after 60 seconds: ${*: -1}"
  fi
  if ((run_status != 0)); then
    fail "command failed (see commands.log): $run_status"
  fi
}

block() {
  local name="$1" suffix="${2-}" code
  record "PAGE BLOCK $name"
  query_into code --block "$name"
  run_checked -c "$code$suffix"
}

expect_output() {
  local needle
  for needle in "$@"; do
    grep -qF -- "$needle" "$run_out" || fail "missing expected output: $needle"
  done
}

health() {
  local deadline=$((SECONDS + 90)) state address
  while true; do
    block health
    state="$(trim "$(<"$run_out")")"
    if [[ "$state" == healthy ]]; then
      break
    fi
    if [[ "$state" != starting && "$state" != unhealthy ]] || ((SECONDS >= deadline)); then
      fail "container did not become healthy: $state"
    fi
    sleep 0.5 # Poll the documented Docker health state, not protocol timing.
  done
  run_checked docker port "$container" 4242/tcp
  address="$(trim "$(<"$run_out")")"
  [[ "$address" =~ ^127\.0\.0\.1:[0-9]+$ ]] || fail "unexpected loopback binding: $address"
  port="${address##*:}"
  export CHUNKDB_URI="chunk://admin@127.0.0.1:$port/"
}

# read_file VARIABLE FILE: sets VARIABLE to the exact file content.
read_file() {
  local _read_target="$1" _read_value
  _read_value="$(cat "$2" && printf x)" || fail "cannot read $2"
  printf -v "$_read_target" '%s' "${_read_value%x}"
}

login() {
  local name="$1" password="$work/login-password"
  block "$name" $'\n'"printf %s \"\$CHUNKDB_PASSWORD\" > $(printf %q "$password")"
  read_file CHUNKDB_PASSWORD "$password"
  export CHUNKDB_PASSWORD
  rm -f "$password"
  [[ -n "$CHUNKDB_PASSWORD" ]] || fail "page did not export a login password: $name"
  add_secret "$CHUNKDB_PASSWORD"
  expect_output PONG
}

# Discovers the documented secret without logging its literal export.
exported_password() {
  local name="$1" line password="$work/export-password" status value
  query_into line --password-export "$name"
  set +e
  "$timeout_command" -k 5 30 bash -c "set -eu"$'\n'"$line"$'\n'"printf %s \"\$CHUNKDB_PASSWORD\" > $(printf %q "$password")" \
    >/dev/null 2>&1
  status=$?
  set -e
  if ((status == 124)); then
    fail "password export timed out: $name"
  fi
  if ((status != 0)); then
    fail "password export failed: $name"
  fi
  read_file value "$password"
  rm -f "$password"
  [[ -n "$value" ]] || fail "page exported an empty password: $name"
  add_secret "$value"
}

watch_until() {
  local needle="$1" deadline=$((SECONDS + 30)) data="" line="" remaining status
  while [[ "$data" != *"$needle"* ]]; do
    remaining=$((deadline - SECONDS))
    ((remaining > 0)) || fail "watch did not produce: $needle"
    if IFS= read -r -t "$remaining" -u 3 line; then
      data+="$line"$'\n'
      continue
    else
      status=$?
    fi
    data+="$line"
    if ((status > 128)); then
      fail "watch did not produce: $needle"
    fi
    if [[ "$data" != *"$needle"* ]]; then
      fail "watch ended before: $needle"
    fi
  done
  record "$data"
}

# Waits up to SECONDS for the watch to close its output; status 124 is a timeout.
drain_watch() {
  "$timeout_command" "$1" cat <&3 >"$work/watch-rest"
}

# Reaps the watch into watch_exit; a signal N is reported as -N.
wait_watch() {
  set +e
  wait "$watch_pid"
  watch_exit=$?
  set -e
  if ((watch_exit > 128)); then
    watch_exit=$((128 - watch_exit))
  fi
}

start_watch() {
  local code
  query_into code --code watch
  record "PAGE BLOCK watch"$'\n'"\$ $code"
  mkfifo "$work/watch.fifo"
  # Job control gives the watch its own process group and a default SIGINT
  # handler, as a new session would.
  set -m
  bash -c "exec $code" >"$work/watch.fifo" 2>&1 &
  watch_pid=$!
  set +m
  exec 3<"$work/watch.fifo"
  watch_open=1
}

stop_watch() {
  local status
  kill -INT "$watch_pid"
  set +e
  drain_watch 30
  status=$?
  set -e
  if ((status != 0)); then
    fail "watch did not stop within 30 seconds"
  fi
  record_file "$work/watch-rest"
  exec 3<&-
  watch_open=0
  wait_watch
  watch_pid=""
  if ((watch_exit != 0)); then
    fail "watch Ctrl-C returned $watch_exit"
  fi
}

write_checks() {
  local file="$logs/checks.json" i
  if ((${#passed[@]} == 0)); then
    printf '[]\n' >"$file"
    return
  fi
  {
    printf '[\n'
    for i in "${!passed[@]}"; do
      printf '  "%s"' "${passed[$i]}"
      if ((i + 1 < ${#passed[@]})); then
        printf ','
      fi
      printf '\n'
    done
    printf ']\n'
  } >"$file"
}

cleanup() {
  local removed volume_removed
  if [[ -n "$watch_pid" ]]; then
    if kill -0 "$watch_pid" 2>/dev/null; then
      kill -INT "$watch_pid" 2>/dev/null || true
    fi
    if ((watch_open)); then
      if ! drain_watch 10; then
        kill -KILL "$watch_pid" 2>/dev/null || true
        drain_watch 10 || true
      fi
      record_file "$work/watch-rest"
      exec 3<&-
      watch_open=0
    else
      kill -KILL "$watch_pid" 2>/dev/null || true
    fi
    wait_watch
    watch_pid=""
  fi
  # Capture logs even on failure; remove only this harness's unique fixture.
  run docker logs "$container"
  run docker rm -f "$container"
  removed=$run_status
  run docker volume rm "$volume"
  volume_removed=$run_status
  if ((removed != 0 || volume_removed != 0)); then
    cleanup_error="fixture cleanup failed; see commands.log"
  fi
}

on_exit() {
  local status=$?
  set +e
  trap - EXIT
  # Interruptions still release this check's Docker fixture. A no-op trap,
  # unlike an ignored signal, is not inherited by the cleanup commands.
  trap ':' INT TERM
  if ((fixture_started)); then
    cleanup
    write_checks
  fi
  rm -rf "$work"
  if ((status != 0)); then
    local message="${error_message:-failed with status $status}"
    if [[ -n "$cleanup_error" ]]; then
      message="quick-start failed: $message; cleanup also failed: $cleanup_error"
    fi
    echo "docker-quickstart: $message" >&2
  elif [[ -n "$cleanup_error" ]]; then
    echo "docker-quickstart: $cleanup_error" >&2
    status=1
  else
    echo "PASS: ${#passed[@]} Docker quick-start groups"
  fi
  exit "$status"
}
trap on_exit EXIT
trap 'error_message="interrupted"; exit 130' INT
trap 'error_message="terminated"; exit 143' TERM

[[ "$(basename "$cli")" == chunk-cli ]] || fail "--cli must be named chunk-cli"
# Extracts all nine blocks and checks the page still names the published image.
query --code start >/dev/null || fail "invalid quick-start pages"

fixture_started=1
block start
run_checked docker image inspect "$image" --format '{{.Id}}'
built_image="$(trim "$(<"$run_out")")"
run_checked docker inspect "$container" --format '{{.Image}}'
running_image="$(trim "$(<"$run_out")")"
if [[ "$built_image" != "$running_image" ]]; then
  fail "page started an image other than the freshly built image"
fi
health
login login
passed+=("generated-password login and health")

block write
block read
expect_output "kind = 1" "name = 'grass'"
passed+=("create, write, block reads")

start_watch
watch_until "start "
block watch-update
watch_until "'door'"
stop_watch
passed+=("watch start, change, Ctrl-C")

old_password="$CHUNKDB_PASSWORD"
exported_password reset-login
block reset
health # Docker may assign a different ephemeral port after restart.
login reset-login
block read
expect_output "kind = 3" "name = 'door'"
export CHUNKDB_PASSWORD="$old_password"
# Reuse the page's PING command with the old password to prove reset took effect.
query_into ping --last-line reset-login
run -c "$ping"
if ((run_status == 124)); then
  fail "command timed out after 60 seconds: $ping"
fi
if ((run_status == 0)) || ! grep -qF AUTH_FAILED "$run_out"; then
  fail "old password was not rejected"
fi
passed+=("offline admin reset, old password refusal, persisted data")

run_checked docker logs "$container"
if (($(grep -oF 'Generated password: ' "$run_out" | wc -l) != 1)); then
  fail "restart generated a new bootstrap password"
fi
run_checked docker exec "$container" id -u
if [[ "$(trim "$(<"$run_out")")" == 0 ]]; then
  fail "container runs as root"
fi
passed+=("single bootstrap password and non-root runtime")
