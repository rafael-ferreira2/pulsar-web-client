#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Pulsar Web Client — daily run wrapper
#
# Does the bare minimum every launch needs:
#   1. Pre-flight: make sure `node`, `pulsar-client`, `node_modules/`, and a
#      populated `tokens.sh` are all in place. If any are missing, point the
#      user at `./setup.sh` and bail out (instead of crashing inside Node).
#   2. Source `tokens.sh` so the JWT env vars land in this shell.
#   3. exec `npm start` — replaces this process so Ctrl-C cleanly stops Node.
#
# Usage:
#   ./run.sh            normal run (pre-flight then start the server)
#   ./run.sh --skip-check     skip pre-flight (use when you know it's fine)
#   ./run.sh --help     show this header and exit
# ──────────────────────────────────────────────────────────────────────────────
set -u
set -o pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

SKIP_CHECK=0
for arg in "$@"; do
  case "$arg" in
    --skip-check) SKIP_CHECK=1 ;;
    --help|-h) sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown flag: $arg (try --help)" >&2; exit 2 ;;
  esac
done

# ─── ANSI helpers (gracefully degrade when stdout isn't a TTY) ────────────────
if [ -t 1 ]; then
  C_RESET=$'\033[0m'; C_BOLD=$'\033[1m'; C_DIM=$'\033[2m'
  C_RED=$'\033[31m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_CYAN=$'\033[36m'
else
  C_RESET=""; C_BOLD=""; C_DIM=""
  C_RED=""; C_GREEN=""; C_YELLOW=""; C_CYAN=""
fi

ok()    { printf "%b✓%b %s\n" "$C_GREEN"  "$C_RESET" "$1"; }
warn()  { printf "%b!%b %s\n" "$C_YELLOW" "$C_RESET" "$1"; }
fail()  { printf "%b✗%b %s\n" "$C_RED"    "$C_RESET" "$1"; }
info()  { printf "%b·%b %s\n" "$C_CYAN"   "$C_RESET" "$1"; }

have()  { command -v "$1" >/dev/null 2>&1; }

# ─── Pre-flight ───────────────────────────────────────────────────────────────
preflight() {
  local problems=0

  have node          || { fail "node is not installed";          problems=$((problems+1)); }
  have npm           || { fail "npm is not installed";           problems=$((problems+1)); }
  have pulsar-client || { fail "pulsar-client is not on PATH";   problems=$((problems+1)); }

  if [ ! -d "node_modules" ]; then
    fail "node_modules is missing — dependencies not installed"
    problems=$((problems+1))
  fi

  if [ ! -f "tokens.sh" ]; then
    fail "tokens.sh is missing — Pulsar consume/produce will fail"
    problems=$((problems+1))
  else
    # Look at one well-known token to gauge whether the file was filled in.
    local sentinel
    sentinel="$(
      set +u
      # shellcheck disable=SC1091
      . ./tokens.sh 2>/dev/null
      printf '%s' "${PULSAR_STG_GAMESTATE:-}"
    )"
    if [ -z "$sentinel" ]; then
      fail "tokens.sh is present but PULSAR_STG_GAMESTATE is empty — open the file and paste your JWTs"
      problems=$((problems+1))
    fi
  fi

  if [ "$problems" -gt 0 ]; then
    echo
    warn "$problems pre-flight check(s) failed."
    info "Run ${C_BOLD}./setup.sh${C_RESET} to fix the gaps automatically, then try ${C_BOLD}./run.sh${C_RESET} again."
    info "Or skip pre-flight with: ${C_BOLD}./run.sh --skip-check${C_RESET} (advanced — you'll see the actual Node errors)"
    return 1
  fi

  ok "Pre-flight passed"
  return 0
}

if [ "$SKIP_CHECK" -eq 0 ]; then
  preflight || exit 1
fi

# ─── Load tokens.sh into THIS shell so the env vars reach Node ───────────────
if [ -f "tokens.sh" ]; then
  # shellcheck disable=SC1091
  set +u
  . ./tokens.sh
  set -u
  ok "Loaded tokens.sh"
fi

# ─── Friendly banner with the local URL ──────────────────────────────────────
PORT="${PORT:-3456}"
echo
printf "%b━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%b\n" "$C_DIM" "$C_RESET"
printf "%b Pulsar Web Client%b\n"   "$C_BOLD$C_CYAN"  "$C_RESET"
printf "%b   Listener   %b http://localhost:%s/\n"            "$C_BOLD" "$C_RESET" "$PORT"
printf "%b   Compare    %b http://localhost:%s/compare.html\n" "$C_BOLD" "$C_RESET" "$PORT"
printf "%b   Stop with  %b Ctrl-C\n"                          "$C_BOLD" "$C_RESET"
printf "%b━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%b\n\n" "$C_DIM" "$C_RESET"

# `exec` so the Node process inherits this shell's pid → Ctrl-C just works.
exec npm start
