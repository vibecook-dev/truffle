#!/usr/bin/env bash
# run-all-tests-and-benches.sh — build, test and benchmark everything once.
# A thin wrapper (RFC 027 §9.2): every phase is a single cargo command, and the
# benchmark numbers are printed by `truffle-bench report`, not parsed here.
#
# Usage:
#   scripts/run-all-tests-and-benches.sh            # full run (suite profile: standard)
#   scripts/run-all-tests-and-benches.sh --fast     # suite profile: smoke
#   scripts/run-all-tests-and-benches.sh --no-open  # don't open reports at the end
#   scripts/run-all-tests-and-benches.sh --help
#
# Without an auth key (.env or shell), the real-network phases are skipped.

set -uo pipefail

# ─── config ─────────────────────────────────────────────────────────────
PROFILE=standard
OPEN_REPORTS=1

for arg in "$@"; do
  case "$arg" in
    --fast) PROFILE=smoke ;;
    --no-open) OPEN_REPORTS=0 ;;
    -h|--help)
      sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown arg: $arg (try --help)" >&2
      exit 2
      ;;
  esac
done

# ─── colour / symbol helpers ────────────────────────────────────────────
if [ -t 1 ] && tput colors >/dev/null 2>&1; then
  BOLD=$'\033[1m'
  DIM=$'\033[2m'
  RED=$'\033[0;31m'
  GREEN=$'\033[0;32m'
  YELLOW=$'\033[0;33m'
  BLUE=$'\033[0;34m'
  CYAN=$'\033[0;36m'
  NC=$'\033[0m'
else
  BOLD='' DIM='' RED='' GREEN='' YELLOW='' BLUE='' CYAN='' NC=''
fi

OK="${GREEN}✔${NC}"
FAIL="${RED}✘${NC}"
SKIP="${YELLOW}↷${NC}"
ARROW="${CYAN}▸${NC}"

hr() { printf '%s%s%s\n' "$DIM" "────────────────────────────────────────────────────────────────────" "$NC"; }
h1() { printf '\n%s%s== %s ==%s\n' "$BOLD" "$BLUE" "$*" "$NC"; }
h2() { printf '%s%s%s\n' "$BOLD" "$*" "$NC"; }
info() { printf '  %s %s\n' "$ARROW" "$*"; }
note() { printf '  %s%s%s\n' "$DIM" "$*" "$NC"; }

# fmt_secs <seconds> → e.g. "42s" or "1m 23s"
fmt_secs() {
  awk -v s="$1" 'BEGIN {
    if (s < 60) { printf "%ds", s }
    else if (s < 3600) { printf "%dm %ds", int(s/60), int(s)%60 }
    else { printf "%dh %dm", int(s/3600), int((s%3600)/60) }
  }'
}

# Tracked results, aligned arrays.
PHASE_NAMES=()
PHASE_STATUSES=()  # ok | fail | skip
PHASE_DURATIONS=()

record() {
  PHASE_NAMES+=("$1")
  PHASE_STATUSES+=("$2")
  PHASE_DURATIONS+=("$3")
}

# run_phase "<label>" <cmd...> — run one phase with timing + status.
run_phase() {
  local label="$1"
  shift
  h2 "${ARROW} ${label}"
  note "$*"
  local t0=$SECONDS rc=0
  "$@" || rc=$?
  local pretty
  pretty=$(fmt_secs $((SECONDS - t0)))
  if [ "$rc" -eq 0 ]; then
    printf '  %s %sok%s %s(%s)%s\n\n' "$OK" "$GREEN" "$NC" "$DIM" "$pretty" "$NC"
    record "$label" ok "$pretty"
  else
    printf '  %s %sFAILED%s (exit %d, %s)\n\n' "$FAIL" "$RED" "$NC" "$rc" "$pretty"
    record "$label" fail "$pretty"
  fi
  return "$rc"
}

skip_phase() {
  h2 "${ARROW} $1"
  printf '  %s %sskipped%s — %s\n\n' "$SKIP" "$YELLOW" "$NC" "$2"
  record "$1" skip "—"
}

# ─── preflight ──────────────────────────────────────────────────────────
ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT" || exit 1

CRITERION_DIR="$ROOT/target/criterion"
BENCH_DIR="target/bench-results"

h1 "truffle — full test + benchmark run"
info "Working dir: $ROOT"
info "Suite profile: $PROFILE"
info "Auto-open reports: $([ "$OPEN_REPORTS" = "1" ] && echo yes || echo no)"

# Load .env so the auth keys reach the test harness. truffle-bench also
# reads .env itself; values are never printed.
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
  info ".env loaded"
fi

# truffle-core's real-network tests read TRUFFLE_TEST_AUTHKEY (TS_AUTHKEY is
# the legacy fallback). truffle-bench also accepts TRUFFLE_BENCH_AUTH_KEY.
HAVE_TEST_KEY=0
if [ -n "${TRUFFLE_TEST_AUTHKEY:-}" ] || [ -n "${TS_AUTHKEY:-}" ]; then
  HAVE_TEST_KEY=1
fi
HAVE_BENCH_KEY=$HAVE_TEST_KEY
if [ -n "${TRUFFLE_BENCH_AUTH_KEY:-}" ]; then
  HAVE_BENCH_KEY=1
fi
if [ "$HAVE_BENCH_KEY" = "1" ]; then
  info "auth key present → real-network phases will run"
else
  info "${YELLOW}no auth key${NC} → real-network phases will be skipped"
fi

TOTAL_T0=$SECONDS

# The GUI app crate is excluded everywhere, as in CI.
EXCLUDE_APP=(--exclude truffle-bench-app)

# ─── 1. build (also runs truffle-core's build.rs → test-sidecar) ────────
hr
run_phase "build workspace" \
  cargo build --locked --workspace "${EXCLUDE_APP[@]}" --quiet || :

# ─── 2. unit tests (no network: live tests print [skip]) ────────────────
# Keys are set to empty rather than unset: the test harnesses load .env via
# dotenvy, which never overrides a variable that exists, and treat an empty
# key as missing.
hr
run_phase "unit tests" \
  env TRUFFLE_TEST_AUTHKEY= TS_AUTHKEY= TRUFFLE_BENCH_AUTH_KEY= \
  cargo test --locked --workspace "${EXCLUDE_APP[@]}" --quiet || :

# ─── 3. real-network tests (same command as integration-tests.yml) ──────
hr
if [ "$HAVE_TEST_KEY" = "1" ]; then
  run_phase "real-network tests" \
    cargo test --locked -p truffle-core -p truffle-bench --tests --quiet -- --test-threads=1 || :
else
  skip_phase "real-network tests" "TRUFFLE_TEST_AUTHKEY not set"
fi

# ─── 4. criterion micro benchmarks ──────────────────────────────────────
hr
run_phase "criterion micro benchmarks" \
  env CRITERION_HOME="$CRITERION_DIR" cargo bench --locked -p truffle-core --quiet || :

# ─── 5. benchmark suite on a same-process pair ──────────────────────────
hr
NEW_REPORTS=()
if [ "$HAVE_BENCH_KEY" = "1" ]; then
  mkdir -p "$BENCH_DIR"
  marker="$(mktemp)"
  run_phase "benchmark suite ($PROFILE, pair)" \
    cargo run --locked -q -p truffle-bench --release -- \
      suite --profile "$PROFILE" --pair --out-dir "$BENCH_DIR" || :
  while IFS= read -r f; do
    [ -n "$f" ] && NEW_REPORTS+=("$f")
  done < <(find "$BENCH_DIR" -maxdepth 1 -type f -name '*.json' -newer "$marker" | sort)
  rm -f "$marker"
else
  skip_phase "benchmark suite ($PROFILE, pair)" "no auth key"
fi

# ─── summary ────────────────────────────────────────────────────────────
hr
printf '\n%sSummary%s   (total %s)\n' "$BOLD" "$NC" "$(fmt_secs $((SECONDS - TOTAL_T0)))"
hr
printf '  %-40s  %-10s  %s\n' "Phase" "Status" "Time"
printf '  %-40s  %-10s  %s\n' "----------------------------------------" "----------" "----"
ok_count=0 fail_count=0 skip_count=0
for i in "${!PHASE_NAMES[@]}"; do
  case "${PHASE_STATUSES[$i]}" in
    ok)   badge="${OK} ${GREEN}ok${NC}      "; ok_count=$((ok_count + 1)) ;;
    fail) badge="${FAIL} ${RED}FAILED${NC}  "; fail_count=$((fail_count + 1)) ;;
    skip) badge="${SKIP} ${YELLOW}skipped${NC} "; skip_count=$((skip_count + 1)) ;;
  esac
  printf '  %-40s  %s  %s\n' "${PHASE_NAMES[$i]}" "$badge" "${PHASE_DURATIONS[$i]}"
done
hr
printf '  %s %d ok   %s %d failed   %s %d skipped\n\n' \
  "$OK" "$ok_count" "$FAIL" "$fail_count" "$SKIP" "$skip_count"

# ─── artefacts ──────────────────────────────────────────────────────────
CRIT_REPORT="$CRITERION_DIR/report/index.html"
h2 "Artefacts"
if [ -f "$CRIT_REPORT" ]; then
  info "Criterion HTML:  $CRIT_REPORT"
fi
if [ "${#NEW_REPORTS[@]}" -gt 0 ]; then
  info "Suite reports:   $BENCH_DIR (${#NEW_REPORTS[@]} new)"
  echo
  # The engine prints v1/v2/v3 summaries; nothing is parsed here.
  cargo run --locked -q -p truffle-bench --release -- report "${NEW_REPORTS[@]}" || :
fi
echo

# ─── open reports ───────────────────────────────────────────────────────
if [ "$OPEN_REPORTS" = "1" ]; then
  opener=""
  if command -v open >/dev/null 2>&1; then
    opener="open"
  elif command -v xdg-open >/dev/null 2>&1; then
    opener="xdg-open"
  fi
  if [ -n "$opener" ]; then
    if [ -f "$CRIT_REPORT" ]; then
      "$opener" "$CRIT_REPORT" >/dev/null 2>&1 || true
    fi
    if [ "${#NEW_REPORTS[@]}" -gt 0 ]; then
      "$opener" "$BENCH_DIR" >/dev/null 2>&1 || true
    fi
  else
    note "No 'open' or 'xdg-open' on PATH — skipping auto-open."
  fi
fi

# ─── final exit status ─────────────────────────────────────────────────
if [ "$fail_count" -gt 0 ]; then
  printf '%s%s%d phase(s) failed.%s\n' "$RED" "$BOLD" "$fail_count" "$NC"
  exit 1
fi
printf '%s%sAll phases clean.%s\n' "$GREEN" "$BOLD" "$NC"
