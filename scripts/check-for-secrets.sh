#!/usr/bin/env bash
# Scan staged files for obvious secrets.
#
# Currently catches:
#   - Tailscale auth keys: `tskey-` prefix
#   - OAuth client secrets: `tskey-client-` is still `tskey-` prefixed
#
# Extend by adding patterns below. Keep this fast — it runs on every commit.
# Skip with: git commit --no-verify

set -euo pipefail

RED=$'\033[0;31m'
GREEN=$'\033[0;32m'
YELLOW=$'\033[0;33m'
NC=$'\033[0m'

patterns=(
  'tskey-[A-Za-z0-9_-]{8,}'   # Tailscale auth key / OAuth secret
)

# Files we know can legitimately mention `tskey-` as documentation/templates.
allowed_files=(
  '.env.example'
  'docs/TESTING.md'
  'docs/rfcs/019-local-testing-and-benchmarking.md'
  'scripts/check-for-secrets.sh'
  'crates/truffle-core/tests/common/mod.rs'
)

is_allowed() {
  local f="$1"
  for a in "${allowed_files[@]}"; do
    if [ "$f" = "$a" ]; then
      return 0
    fi
  done
  return 1
}

staged="$(git diff --cached --name-only --diff-filter=ACM || true)"
if [ -z "$staged" ]; then
  exit 0
fi

fail=0
hit_count=0

while IFS= read -r f; do
  [ -z "$f" ] && continue
  [ ! -f "$f" ] && continue
  if is_allowed "$f"; then
    continue
  fi

  for p in "${patterns[@]}"; do
    # -E extended regex, -I skip binary, --color=never for clean piping
    if matches="$(git diff --cached -U0 -- "$f" | grep -E "^\+.*$p" || true)"; then
      if [ -n "$matches" ]; then
        echo "${RED}[secrets] potential Tailscale auth key in staged change: $f${NC}"
        echo "$matches" | head -3 | sed 's/^/            /'
        fail=1
        hit_count=$((hit_count + 1))
      fi
    fi
  done
done <<< "$staged"

if [ "$fail" -eq 1 ]; then
  echo ""
  echo "${RED}[secrets] $hit_count file(s) appear to contain auth keys.${NC}"
  echo "${YELLOW}          Keep keys in .env (gitignored) or the shell env.${NC}"
  echo "${YELLOW}          If this is a false positive, add the file to allowed_files in${NC}"
  echo "${YELLOW}          scripts/check-for-secrets.sh.${NC}"
  exit 1
fi

exit 0
