#!/usr/bin/env bash
# Uploads hydro_data.db to Cloudflare R2. Called from run_pipeline.R after each run.
# Pings the Cronitor "rfh-r2-backup" monitor so a failed/missing upload alerts
# instead of only showing up as a WARNING line in pipeline_execution.log.
set -uo pipefail
cd "$(dirname "$0")/.."

CONFIG_FILE="config/.r2_config"
DB_FILE="hydro_data.db"
MONITOR_KEY="rfh-r2-backup"

cronitor_ping() {
  command -v cronitor >/dev/null 2>&1 && cronitor ping "$MONITOR_KEY" "$@" >/dev/null 2>&1
}

cronitor_ping --run

if [[ ! -f "$CONFIG_FILE" ]]; then
  msg="R2 backup skipped: $CONFIG_FILE not found (see config/.r2_config.example)"
  echo "$msg" >&2
  cronitor_ping --fail --msg "$msg"
  exit 1
fi
source "$CONFIG_FILE"

if [[ ! -f "$DB_FILE" ]]; then
  msg="R2 backup skipped: $DB_FILE not found"
  echo "$msg" >&2
  cronitor_ping --fail --msg "$msg"
  exit 1
fi

if aws s3 cp "$DB_FILE" "s3://${R2_BUCKET}/hydro_data.db" \
  --endpoint-url "https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com" \
  --profile r2; then
  cronitor_ping --complete
else
  status=$?
  cronitor_ping --fail --msg "aws s3 cp exited with status $status"
  exit "$status"
fi
