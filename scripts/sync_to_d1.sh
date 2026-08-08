#!/usr/bin/env bash
# Upserts the last N days of measurements into the hydro-dashboard D1
# database (../hydro-dashboard). Called from run_pipeline.R after each run.
# Re-upserting a rolling window rather than tracking a sync cursor is
# deliberate: data volume is tiny (~1000 rows/day across all stations) and
# this is self-healing against any gap, since the schema's
# ON CONFLICT REPLACE makes every upsert idempotent.
set -uo pipefail
cd "$(dirname "$0")/.."

# cron doesn't source ~/.bashrc, so nvm's node/npx aren't on PATH by default.
export NVM_DIR="$HOME/.nvm"
# shellcheck disable=SC1091
[[ -s "$NVM_DIR/nvm.sh" ]] && . "$NVM_DIR/nvm.sh"

CONFIG_FILE="config/.d1_config"
DB_FILE="hydro_data.db"
DASHBOARD_DIR="../hydro-dashboard"
MONITOR_KEY="rfh-d1-sync"
SYNC_DAYS=7

cronitor_ping() {
  command -v cronitor >/dev/null 2>&1 && cronitor ping "$MONITOR_KEY" "$@" >/dev/null 2>&1
}

cronitor_ping --run

if [[ ! -f "$CONFIG_FILE" ]]; then
  msg="D1 sync skipped: $CONFIG_FILE not found (see config/.d1_config.example)"
  echo "$msg" >&2
  cronitor_ping --fail --msg "$msg"
  exit 1
fi
source "$CONFIG_FILE"

if [[ ! -f "$DB_FILE" ]]; then
  msg="D1 sync skipped: $DB_FILE not found"
  echo "$msg" >&2
  cronitor_ping --fail --msg "$msg"
  exit 1
fi

if [[ ! -d "$DASHBOARD_DIR/worker" ]]; then
  msg="D1 sync skipped: $DASHBOARD_DIR/worker not found"
  echo "$msg" >&2
  cronitor_ping --fail --msg "$msg"
  exit 1
fi

TMP_SQL="$(mktemp /tmp/hydro_d1_sync_XXXXXX.sql)"
trap 'rm -f "$TMP_SQL"' EXIT

sqlite3 "$DB_FILE" ".mode insert measurements" \
  "SELECT * FROM measurements WHERE Date_UTC >= date('now', '-${SYNC_DAYS} days');" > "$TMP_SQL"

if [[ ! -s "$TMP_SQL" ]]; then
  echo "D1 sync: no rows in the last ${SYNC_DAYS} days, nothing to sync."
  cronitor_ping --complete
  exit 0
fi

if (cd "$DASHBOARD_DIR/worker" && npx wrangler d1 execute "$D1_DB_NAME" --remote --yes --file="$TMP_SQL"); then
  cronitor_ping --complete
else
  status=$?
  cronitor_ping --fail --msg "wrangler d1 execute exited with status $status"
  exit "$status"
fi
