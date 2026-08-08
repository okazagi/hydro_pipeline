#!/usr/bin/env bash
# Uploads hydro_data.db to Cloudflare R2. Called from run_pipeline.R after each run.
set -uo pipefail
cd "$(dirname "$0")/.."

CONFIG_FILE="config/.r2_config"
DB_FILE="hydro_data.db"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "R2 backup skipped: $CONFIG_FILE not found (see config/.r2_config.example)" >&2
  exit 1
fi
source "$CONFIG_FILE"

if [[ ! -f "$DB_FILE" ]]; then
  echo "R2 backup skipped: $DB_FILE not found" >&2
  exit 1
fi

aws s3 cp "$DB_FILE" "s3://${R2_BUCKET}/hydro_data.db" \
  --endpoint-url "https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com" \
  --profile r2
