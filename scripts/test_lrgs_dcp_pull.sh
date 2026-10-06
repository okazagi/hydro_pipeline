#!/usr/bin/env bash
# Pull a station's raw DCP messages from GOES DADDS via LRGS/DDS, bypassing
# HADS. Originally a one-off test for ASEC2 (dropping soil moisture since
# 2026-04-01); generalized to take any DCP address so it can also pull IDWC2
# (HADS only exposes 30-min data for it, but its datalogger transmits at
# 20-min resolution).
#
# Usage: ./scripts/test_lrgs_dcp_pull.sh [since] [until] [dcp_address]
#   since/until use OpenDCS search-criteria time syntax, e.g. "now - 2 days".
#   dcp_address defaults to ASEC2 (28A0044E). IDWC2 is 28A00A9C.
set -uo pipefail
cd "$(dirname "$0")/.."

CONFIG_FILE="config/.lrgs_credentials"
OPENDCS_HOME="../opt/opendcs/opendcs-7.0.17"
LRGS_HOST="cdadata.wcda.noaa.gov"
SINCE="${1:-now - 2 days}"
UNTIL="${2:-now}"
DCP_ADDRESS="${3:-28A0044E}"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Missing $CONFIG_FILE (expected LRGS_USER=... / LRGS_PASS=...)" >&2
  exit 1
fi
source "$CONFIG_FILE"

if [[ -z "${LRGS_USER:-}" || -z "${LRGS_PASS:-}" ]]; then
  echo "LRGS_USER/LRGS_PASS not set in $CONFIG_FILE" >&2
  exit 1
fi

SC_FILE="$(mktemp /tmp/dcp_searchcrit_XXXXXX.sc)"
trap 'rm -f "$SC_FILE"' EXIT
cat > "$SC_FILE" <<EOF
DRS_SINCE: ${SINCE}
DRS_UNTIL: ${UNTIL}
DCP_ADDRESS: ${DCP_ADDRESS}
EOF

echo "Search criteria:"
cat "$SC_FILE"
echo "---"

# decj (the OpenDCS launcher getDcpMessages wraps) echoes its full java
# command line -- including our -P password -- to stdout as a "cli: ..."
# diagnostic before running. Strip that line so the password never reaches
# the terminal/log.
"$OPENDCS_HOME/bin/getDcpMessages" \
  -h "$LRGS_HOST" \
  -u "$LRGS_USER" \
  -P "$LRGS_PASS" \
  -f "$SC_FILE" \
  2>&1 | grep -v '^cli: '
