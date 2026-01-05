import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
import smtplib
from email.message import EmailMessage

# Load environment variables
load_dotenv()

API_TOKEN = os.getenv("SYNOPTIC_API_TOKEN")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
ALERT_TO = os.getenv("ALERT_TO", "adehaan@agci.org")

# Paths
BASE_DIR = Path("/home/okazagi/synoptic_tracker/data")
BASE_DIR.mkdir(parents=True, exist_ok=True)
TIMESTAMP_FILE = BASE_DIR / "last_timestamps.json"
CONFIG_FILE = Path("/home/okazagi/synoptic_tracker/config.json")

# ---------------- Helpers ---------------- #

def load_json(path: Path, default):
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return default

def save_json(path: Path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def get_current_time_str():
    # Synoptic expects UTC in YYYYMMDDHHMM
    return datetime.utcnow().strftime("%Y%m%d%H%M")

def send_email_alert(subject: str, body: str):
    """Send a simple email. No-op if EMAIL_USER/PASS are unset."""
    if not (EMAIL_USER and EMAIL_PASS):
        print("[WARN] EMAIL_USER/EMAIL_PASS not set; skipping email.")
        return
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = ALERT_TO
    msg.set_content(body)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL_USER, EMAIL_PASS)
        smtp.send_message(msg)

def append_and_check_qc(station_id: str, new_data: list):
    """
    Append new station data into <station_id>.json, sort by OBSERVATIONS.date_time,
    and send an alert if any records are QC-flagged.
    """
    file_path = BASE_DIR / f"{station_id}.json"
    try:
        with open(file_path, "r") as f:
            existing = json.load(f)
            if not isinstance(existing, list):
                existing = []
    except FileNotFoundError:
        existing = []

    # Merge & sort
    existing.extend(new_data)

    def _dt_key(entry):
        # Sort by the first timestamp if array-like; fallback to empty string.
        dt = entry.get("OBSERVATIONS", {}).get("date_time", "")
        if isinstance(dt, list):
            return dt[0] if dt else ""
        return dt

    existing.sort(key=_dt_key)

    with open(file_path, "w") as f:
        json.dump(existing, f, indent=2)

    # Check QC flags in the batch we just added
    # Many Synoptic payloads place per-variable QC flags under OBSERVATIONS.*_qc
    # but your code also references a top-level "QC_FLAGGED". We'll check both.
    flagged = []
    for entry in new_data:
        if entry.get("QC_FLAGGED") is True:
            flagged.append({"reason": entry.get("QC"), "entry": entry})
            continue

        # Heuristic: look for any *_qc arrays within OBSERVATIONS that contain non-zero values
        obs = entry.get("OBSERVATIONS", {})
        for k, v in obs.items():
            if k.endswith("_qc"):
                # v might be a list aligned with date_time
                if isinstance(v, list) and any(x not in (None, 0, "0") for x in v):
                    flagged.append({"reason": k, "entry": entry})
                    break

    if flagged:
        flagged_count = len(flagged)
        percent_flagged = round(100 * flagged_count / max(1, len(new_data)), 2)
        subject = f"[ALERT] QC Flags Detected - {station_id}"
        body_lines = [
            f"Station: {station_id}",
            f"New records: {len(new_data)}",
            f"Flagged records: {flagged_count} ({percent_flagged}%)",
            "Examples of flag reasons (up to 5):",
        ]
        for ex in flagged[:5]:
            body_lines.append(f" - {ex.get('reason')}")
        send_email_alert(subject, "\n".join(body_lines))

# ---------------- Main ---------------- #

config = load_json(CONFIG_FILE, {})
stations_config = config.get("stations", {})
base_url = config.get("base_url", "https://api.synopticdata.com/v2/stations/timeseries")
retry_attempts = int(config.get("retry_attempts", 3))
units = config.get("units", "metric")

if not API_TOKEN:
    print("[ERROR] SYNOPTIC_API_TOKEN is not set in environment.")
timestamps = load_json(TIMESTAMP_FILE, {})
current_time = get_current_time_str()

for station_id, variables in stations_config.items():
    # Determine start time for this station (default: today 00:00 UTC)
    start_time = timestamps.get(station_id, datetime.utcnow().strftime("%Y%m%d0000"))

    params = {
        "token": API_TOKEN,
        "stid": station_id,
        "vars": ",".join(variables),
        "units": units,
        "start": start_time,
        "end": current_time,
        "output": "json",
        "qc": "on",
        "qc_remove_data": "off",
        "qc_flags": "on",
        "sensorvars": 1,
        "precip": 1,
    }

    for attempt in range(retry_attempts):
        try:
            response = requests.get(base_url, params=params, timeout=30)
            if response.status_code == 200:
                result = response.json()
                if "STATION" in result and result["STATION"]:
                    station_data = result["STATION"]
                    append_and_check_qc(station_id, station_data)

                    # Update last timestamp using the latest observation time from the last entry
                    last_entry = station_data[-1]
                    dt_field = last_entry.get("OBSERVATIONS", {}).get("date_time", [])
                    # dt_field may be a single string or a list
                    if isinstance(dt_field, list) and dt_field:
                        last_obs = dt_field[-1]
                    elif isinstance(dt_field, str):
                        last_obs = dt_field
                    else:
                        last_obs = None

                    if last_obs:
                        # Convert e.g. "2025-10-20T07:40:00Z" -> "202510200740"
                        clean = (
                            last_obs.replace("-", "")
                            .replace(":", "")
                            .replace("T", "")
                            .replace("Z", "")
                        )[:12]
                        timestamps[station_id] = clean
                else:
                    print(f"[INFO] No data returned for {station_id}.")
                break  # success or no data; stop retrying
            else:
                print(f"Attempt {attempt + 1}: Failed for {station_id}, status {response.status_code}")
        except Exception as e:
            print(f"Attempt {attempt + 1}: Error fetching data for {station_id}: {e}")

# Persist updated timestamps
save_json(TIMESTAMP_FILE, timestamps)

