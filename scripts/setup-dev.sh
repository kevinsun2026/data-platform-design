#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# AIDP monorepo — one-shot dev environment setup.
#
# What it does:
#   1. Verifies required tools (uv, docker, docker compose, node, pnpm).
#   2. Installs Python workspace dependencies via `uv sync`.
#   3. (Optional) Installs Node workspace dependencies via `pnpm install`.
#   4. Brings up Postgres + Redis + Kafka via docker compose for local dev.
#   5. Installs pre-commit hooks.
#
# This script is idempotent and safe to re-run.
# ----------------------------------------------------------------------------
set -euo pipefail

# Color helpers (only when stdout is a TTY)
if [[ -t 1 ]]; then
  RED='\033[0;31m'; YELLOW='\033[0;33m'; GREEN='\033[0;32m'
  BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
else
  RED=''; YELLOW=''; GREEN=''; BLUE=''; BOLD=''; NC=''
fi

log()   { printf "${BLUE}==>${NC} %s\n" "$*"; }
ok()    { printf "${GREEN}✔${NC}  %s\n" "$*"; }
warn()  { printf "${YELLOW}!${NC}  %s\n" "$*"; }
fail()  { printf "${RED}✘${NC}  %s\n" "$*" >&2; exit 1; }

# Resolve repo root regardless of where the script was invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

printf "${BOLD}AIDP dev setup${NC}\n"
printf "    repo: %s\n" "${REPO_ROOT}"
printf "    user: %s\n" "${USER:-unknown}"
printf "\n"

# ---- 1. Required tools ----------------------------------------------------
log "Checking required tools"

need() {
  local cmd="$1" install_hint="$2"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    fail "missing required tool: ${cmd}
    Install: ${install_hint}"
  fi
  ok "${cmd}: $(command -v "$cmd")"
}

need uv    "curl -LsSf https://astral.sh/uv/install.sh | sh"
need python3 "brew install python@3.11  (or use pyenv)"
need docker "https://www.docker.com/products/docker-desktop"
need node  "brew install node  (or use nvm)"

# pnpm via corepack
if ! command -v pnpm >/dev/null 2>&1; then
  warn "pnpm not found, attempting to enable via corepack"
  if command -v corepack >/dev/null 2>&1; then
    corepack enable
    corepack prepare pnpm@latest --activate
  else
    fail "pnpm not found and corepack is unavailable. Install: npm i -g pnpm"
  fi
fi
need pnpm "npm i -g pnpm"

# docker compose v2 (subcommand of `docker`)
if ! docker compose version >/dev/null 2>&1; then
  fail "docker compose v2 not found. Docker Desktop 4.x+ includes it; on Linux
    install the docker-compose-plugin package."
fi
ok "docker compose: $(docker compose version)"

# ---- 2. Python workspace --------------------------------------------------
log "Syncing Python workspace (uv sync)"
if uv sync; then
  ok "Python workspace synced"
else
  warn "uv sync reported issues — continuing. You may need to fix them later."
fi

# ---- 3. Node workspace (only if web/ exists) ------------------------------
if [[ -d "${REPO_ROOT}/web" && -f "${REPO_ROOT}/web/package.json" ]]; then
  log "Installing web/ dependencies (pnpm install)"
  (cd "${REPO_ROOT}/web" && pnpm install --frozen-lockfile=false) \
    && ok "web/ deps installed" \
    || warn "pnpm install had issues — you can retry with: cd web && pnpm install"
else
  warn "web/ not initialised yet — skipping pnpm install (Task 1+ will create it)"
fi

# ---- 4. Dev infra via docker compose -------------------------------------
COMPOSE_FILE="${REPO_ROOT}/docker/compose.dev.yml"
if [[ -f "${COMPOSE_FILE}" ]]; then
  log "Starting dev infra (Postgres + Redis + Kafka) via docker compose"
  if ! docker info >/dev/null 2>&1; then
    warn "docker daemon is not reachable. Skipping infra startup.
      Start Docker Desktop, then run: docker compose -f ${COMPOSE_FILE} up -d"
  else
    docker compose -f "${COMPOSE_FILE}" up -d
    ok "dev infra started"
  fi
else
  warn "no compose file at ${COMPOSE_FILE} — skipping infra startup
    (it will be created by a later task in this plan)"
fi

# ---- 5. Pre-commit hooks --------------------------------------------------
if [[ -d "${REPO_ROOT}/.git" ]]; then
  log "Installing pre-commit hooks"
  if uv run pre-commit install 2>/dev/null; then
    uv run pre-commit install --hook-type commit-msg 2>/dev/null || true
    ok "pre-commit hooks installed"
  else
    warn "pre-commit install failed — you can retry with: task precommit.install"
  fi
fi

printf "\n${GREEN}${BOLD}Setup complete.${NC}\n\n"
printf "Next steps:\n"
printf "  ${BOLD}task lint${NC}     # ruff + mypy + pnpm lint\n"
printf "  ${BOLD}task test${NC}     # pytest + pnpm test\n"
printf "  ${BOLD}task --list${NC}   # see all available tasks\n"
