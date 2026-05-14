#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Pulsar Web Client — first-run setup
#
# Walks a brand-new clone through every prerequisite the app needs:
#   • Node.js ≥ 18 and npm
#   • Apache Pulsar's `pulsar-client` CLI (and Java, which it shells into)
#   • `nc` (netcat) for the connectivity-check endpoints
#   • The local npm dependency tree (`node_modules/`)
#   • A populated `tokens.sh` with at least the Pulsar JWTs
#
# Usage:
#   ./setup.sh                      interactive — installs missing pieces
#                                   on macOS (Homebrew), prompts before each
#                                   destructive action
#   ./setup.sh --check              read-only diagnostic; never installs,
#                                   exits 0 if ready / 1 if there are gaps
#   ./setup.sh --yes                non-interactive, assume "yes" to every
#                                   install prompt (CI-friendly)
#   ./setup.sh --help               show this header and exit
#
# Exit codes:
#   0  All checks passed — you're ready for `./run.sh` or `npm start`
#   1  Setup completed but the user must still edit tokens.sh (or similar)
#   2  Hard failure — a required tool could not be installed automatically
# ──────────────────────────────────────────────────────────────────────────────
set -u
set -o pipefail

# ─── Locate the project root (so the script works from any cwd) ──────────────
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# ─── Argument parsing ─────────────────────────────────────────────────────────
MODE="interactive"   # interactive | check | yes
for arg in "$@"; do
  case "$arg" in
    --check) MODE="check" ;;
    --yes|-y) MODE="yes" ;;
    --help|-h)
      sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "Unknown flag: $arg (try --help)" >&2; exit 2 ;;
  esac
done

# ─── ANSI helpers (gracefully degrade when stdout isn't a TTY) ────────────────
if [ -t 1 ]; then
  C_RESET=$'\033[0m'; C_DIM=$'\033[2m'; C_BOLD=$'\033[1m'
  C_RED=$'\033[31m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'
  C_BLUE=$'\033[34m'; C_CYAN=$'\033[36m'
else
  C_RESET=""; C_DIM=""; C_BOLD=""
  C_RED=""; C_GREEN=""; C_YELLOW=""; C_BLUE=""; C_CYAN=""
fi

ok()    { printf "%b✓%b %s\n"  "$C_GREEN"  "$C_RESET" "$1"; }
warn()  { printf "%b!%b %s\n"  "$C_YELLOW" "$C_RESET" "$1"; }
fail()  { printf "%b✗%b %s\n"  "$C_RED"    "$C_RESET" "$1"; }
info()  { printf "%b·%b %s\n"  "$C_BLUE"   "$C_RESET" "$1"; }
step()  { printf "\n%b▶ %s%b\n" "$C_BOLD$C_CYAN" "$1" "$C_RESET"; }
dim()   { printf "%b%s%b\n"    "$C_DIM"   "$1"   "$C_RESET"; }

# ─── State the rest of the script reads ───────────────────────────────────────
NEEDS_USER_ACTION=0     # 1 → user must still edit tokens.sh etc.
HARD_FAIL=0             # 1 → could not auto-install something required
declare -a SUMMARY      # one line per check for the final report

note() { SUMMARY+=("$1"); }

# Ask y/N. Respects --yes (always yes) and --check (never installs, always no).
ask_yes() {
  local prompt="$1"
  if [ "$MODE" = "yes" ];   then return 0; fi
  if [ "$MODE" = "check" ]; then return 1; fi
  local reply
  printf "%b?%b %s [Y/n] " "$C_CYAN" "$C_RESET" "$prompt"
  read -r reply
  case "${reply:-Y}" in
    Y|y|Yes|YES|yes|"") return 0 ;;
    *) return 1 ;;
  esac
}

have() { command -v "$1" >/dev/null 2>&1; }

# ─── 1. OS + package manager fingerprint ──────────────────────────────────────
step "Detecting platform"

OS="$(uname -s)"
PKG=""        # "brew" | "apt" | "dnf" | "yum" | ""
if [ "$OS" = "Darwin" ]; then
  if have brew; then PKG="brew"; ok "macOS with Homebrew detected"
  else
    warn "macOS detected, but Homebrew is not installed"
    info "Install Homebrew first:  /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
    HARD_FAIL=1
  fi
elif [ "$OS" = "Linux" ]; then
  if   have apt-get; then PKG="apt"; ok "Linux with apt detected"
  elif have dnf;     then PKG="dnf"; ok "Linux with dnf detected"
  elif have yum;     then PKG="yum"; ok "Linux with yum detected"
  else                                ok "Linux detected (no recognised package manager — manual installs only)"
  fi
else
  warn "Unrecognised OS '$OS' — falling back to manual install instructions"
fi

# Wrapper that installs a brew package if Homebrew is present, otherwise
# prints the user-runnable command and returns 1.
pkg_install_brew() {
  local formula="$1"
  if [ -z "$PKG" ] || [ "$PKG" != "brew" ]; then
    warn "Cannot auto-install '$formula' (no Homebrew); install manually then re-run."
    return 1
  fi
  if ask_yes "Install '$formula' with Homebrew now?"; then
    info "Running: brew install $formula"
    brew install "$formula" || { fail "brew install $formula failed"; return 1; }
    return 0
  fi
  warn "Skipped Homebrew install of $formula. You'll need to install it before running the app."
  return 1
}

# ─── 2. Node.js + npm ─────────────────────────────────────────────────────────
step "Checking Node.js & npm"

NODE_MIN_MAJOR=18
if have node; then
  NODE_VER="$(node -v)"                                 # eg "v20.11.0"
  NODE_MAJOR="${NODE_VER#v}"; NODE_MAJOR="${NODE_MAJOR%%.*}"
  if [ "$NODE_MAJOR" -ge "$NODE_MIN_MAJOR" ]; then
    ok "Node $NODE_VER  (need ≥ v$NODE_MIN_MAJOR)"
    note "Node.js: $NODE_VER"
  else
    warn "Node $NODE_VER is below the recommended v$NODE_MIN_MAJOR — kafkajs and protobufjs may misbehave"
    note "Node.js: $NODE_VER (below v$NODE_MIN_MAJOR)"
    NEEDS_USER_ACTION=1
  fi
else
  fail "node is not installed"
  pkg_install_brew node || HARD_FAIL=1
  note "Node.js: missing"
fi

if have npm; then
  ok "npm $(npm -v)"
else
  fail "npm is not installed (comes with Node — install Node first)"
  HARD_FAIL=1
fi

# ─── 3. Apache Pulsar CLI (the app shells into `pulsar-client`) ───────────────
step "Checking Apache Pulsar CLI"

if have pulsar-client; then
  PCLI_PATH="$(command -v pulsar-client)"
  ok "pulsar-client found at $PCLI_PATH"
  note "pulsar-client: $PCLI_PATH"
else
  fail "pulsar-client is not on PATH"
  info "The app spawns 'pulsar-client consume/produce' under the hood; without it the Pulsar side is non-functional."
  if pkg_install_brew apache-pulsar; then
    if have pulsar-client; then
      ok "pulsar-client installed at $(command -v pulsar-client)"
      note "pulsar-client: $(command -v pulsar-client)"
    else
      warn "Homebrew finished but pulsar-client still isn't on PATH — restart your shell and try again."
      HARD_FAIL=1
    fi
  else
    info "Manual options:"
    info "  • macOS:  brew install apache-pulsar"
    info "  • Linux:  download from https://pulsar.apache.org/download/ and add the bin/ dir to PATH"
    HARD_FAIL=1
  fi
fi

# Java — pulsar-client is a Java app and exec()s under it. Check JAVA_HOME too.
if have java; then
  JAVA_VER="$(java -version 2>&1 | head -1 | sed 's/.*"\(.*\)".*/\1/')"
  ok "Java $JAVA_VER"
  if [ -n "${JAVA_HOME:-}" ]; then
    dim "  JAVA_HOME=$JAVA_HOME"
  else
    dim "  JAVA_HOME is unset (most setups still work, but pulsar-client occasionally needs it)."
  fi
else
  warn "java is not on PATH — pulsar-client needs a JVM"
  if [ "$PKG" = "brew" ]; then pkg_install_brew openjdk || NEEDS_USER_ACTION=1; fi
fi

# ─── 4. netcat (for the Kafka/Pulsar TCP reachability endpoints) ──────────────
step "Checking netcat (nc)"

if have nc; then
  ok "nc found at $(command -v nc)"
else
  warn "nc (netcat) not found — the 'Test connectivity' button on the UI won't work without it"
  case "$PKG" in
    brew) pkg_install_brew netcat ;;
    apt)  info "Install with: sudo apt-get install -y netcat-openbsd" ;;
    dnf|yum) info "Install with: sudo $PKG install -y nmap-ncat" ;;
    *)    info "Install netcat through your distribution's package manager." ;;
  esac
  NEEDS_USER_ACTION=1
fi

# ─── 5. npm dependencies ──────────────────────────────────────────────────────
step "Checking npm dependencies"

NEEDS_INSTALL=0
if [ ! -d "node_modules" ]; then
  NEEDS_INSTALL=1
# Only treat node_modules as stale when the LOCKFILE has changed since the
# last install (npm bumps node_modules/.package-lock.json on each install).
# Editing the "scripts" section of package.json does NOT require reinstalling.
elif [ -f "package-lock.json" ] && [ -f "node_modules/.package-lock.json" ] \
     && [ "package-lock.json" -nt "node_modules/.package-lock.json" ]; then
  NEEDS_INSTALL=1
  info "package-lock.json is newer than node_modules — refresh recommended"
fi

if [ "$NEEDS_INSTALL" -eq 1 ]; then
  if [ "$MODE" = "check" ]; then
    warn "node_modules is missing or stale (run setup.sh without --check to install)"
    NEEDS_USER_ACTION=1
  elif have npm; then
    if ask_yes "Run 'npm install' now? (~30s on a warm cache)"; then
      info "Running: npm install"
      npm install || { fail "npm install failed"; HARD_FAIL=1; }
      [ -d "node_modules" ] && ok "Dependencies installed"
    else
      warn "Skipped npm install. You must run it before starting the app."
      NEEDS_USER_ACTION=1
    fi
  fi
else
  ok "node_modules is present and up to date"
fi
note "npm deps: $([ -d node_modules ] && echo 'installed' || echo 'MISSING')"

# ─── 6. tokens.sh — the Pulsar JWTs the app refuses to start without ─────────
step "Checking tokens.sh"

if [ ! -f "tokens.sh" ]; then
  if [ -f "tokens.sh.example" ]; then
    warn "tokens.sh does not exist"
    if [ "$MODE" = "check" ]; then
      info "Run setup.sh without --check to scaffold one from tokens.sh.example"
      NEEDS_USER_ACTION=1
      note "tokens.sh: MISSING (scaffold from tokens.sh.example)"
    elif ask_yes "Create tokens.sh from tokens.sh.example?"; then
      cp tokens.sh.example tokens.sh
      chmod 600 tokens.sh
      ok "Created tokens.sh (mode 600). Open it and paste your JWTs."
      info "Edit it with:  \$EDITOR tokens.sh   (or:  open -e tokens.sh)"
      NEEDS_USER_ACTION=1
      note "tokens.sh: NEW (needs editing — empty placeholders)"
    else
      warn "Skipped tokens.sh scaffold — Pulsar consume/produce will fail."
      NEEDS_USER_ACTION=1
      note "tokens.sh: MISSING (declined scaffold)"
    fi
  else
    fail "Neither tokens.sh nor tokens.sh.example is present — your clone is incomplete."
    HARD_FAIL=1
    note "tokens.sh: MISSING & no template — broken clone"
  fi
else
  # Source it in a subshell to inspect values without polluting our env.
  REQUIRED_VARS=(
    PULSAR_STG_GAMESTATE   PULSAR_STG_MAPPINGS
    PULSAR_STG_COMP_STATS  PULSAR_STG_FIXTURE_STATS
    PULSAR_PRD_GAMESTATE   PULSAR_PRD_MAPPINGS
    PULSAR_PRD_COMP_STATS  PULSAR_PRD_FIXTURE_STATS
  )
  OPTIONAL_VARS=(
    KAFKA_STG_CONFLUENT_API_KEY KAFKA_STG_CONFLUENT_API_SECRET
    KAFKA_PRD_CONFLUENT_API_KEY KAFKA_PRD_CONFLUENT_API_SECRET
  )

  # JWT sanity: three dot-separated base64url segments, total ≥ 80 chars.
  jwt_looks_ok() {
    local v="$1"
    [ -n "$v" ] || return 1
    [ "${#v}" -ge 80 ] || return 1
    [[ "$v" == *.*.* ]] || return 1
    return 0
  }

  REPORT="$(
    set +u
    # shellcheck disable=SC1091
    . ./tokens.sh 2>/dev/null
    for var in "${REQUIRED_VARS[@]}"; do
      val="${!var:-}"
      if   [ -z "$val" ];             then printf 'EMPTY|%s\n'  "$var"
      elif jwt_looks_ok "$val";       then printf 'OK|%s\n'     "$var"
      else                                 printf 'BAD|%s\n'    "$var"
      fi
    done
    for var in "${OPTIONAL_VARS[@]}"; do
      val="${!var:-}"
      [ -n "$val" ] && printf 'KOK|%s\n' "$var"
    done
  )"

  EMPTY=0; BAD=0; GOOD=0; OPT=0
  while IFS='|' read -r status name; do
    case "$status" in
      OK)    GOOD=$((GOOD+1));   ok    "$name is set (JWT-shaped)" ;;
      EMPTY) EMPTY=$((EMPTY+1)); warn  "$name is empty" ;;
      BAD)   BAD=$((BAD+1));     fail  "$name doesn't look like a JWT (expected three dot-separated segments, ≥ 80 chars)" ;;
      KOK)   OPT=$((OPT+1));     ok    "$name set (optional Confluent key)" ;;
    esac
  done <<< "$REPORT"

  TOTAL_REQ=${#REQUIRED_VARS[@]}
  if [ "$GOOD" -eq "$TOTAL_REQ" ]; then
    ok "All $TOTAL_REQ required Pulsar tokens look correctly populated"
    note "tokens.sh: $GOOD/$TOTAL_REQ Pulsar tokens populated · $OPT optional Confluent keys"
  else
    warn "$GOOD/$TOTAL_REQ Pulsar tokens populated  ($EMPTY empty, $BAD malformed)"
    info "Edit tokens.sh and paste the JWTs you received from the platform team."
    info "  See \"Pulsar — getting credentials\" in README.md for who to ask and what to ask for."
    NEEDS_USER_ACTION=1
    note "tokens.sh: $GOOD/$TOTAL_REQ Pulsar tokens populated · NEEDS EDIT"
  fi

  # Owner-only permissions on tokens.sh (heuristic; only fix on Unix).
  if [ "$(uname)" != "Windows_NT" ]; then
    PERMS="$(stat -f '%Lp' tokens.sh 2>/dev/null || stat -c '%a' tokens.sh 2>/dev/null || echo '?')"
    if [ "$PERMS" != "600" ] && [ "$PERMS" != "?" ]; then
      warn "tokens.sh permissions are $PERMS — tightening to 600 (owner read/write only)"
      chmod 600 tokens.sh
    fi
  fi
fi

# ─── 7. Final summary ─────────────────────────────────────────────────────────
step "Summary"

for line in "${SUMMARY[@]}"; do
  printf "  %s\n" "$line"
done

echo
if [ "$HARD_FAIL" -eq 1 ]; then
  fail "Setup hit one or more hard failures. Review the messages above and rerun setup.sh."
  exit 2
elif [ "$NEEDS_USER_ACTION" -eq 1 ]; then
  warn "Setup almost complete — review the warnings above (typically: paste your JWTs into tokens.sh)."
  info "Then start the app with:  ${C_BOLD}./run.sh${C_RESET}    (or:  source tokens.sh && npm start)"
  exit 1
else
  ok "Everything checks out. You're ready to roll."
  info "Start the app with:  ${C_BOLD}./run.sh${C_RESET}    (or:  source tokens.sh && npm start)"
  exit 0
fi
