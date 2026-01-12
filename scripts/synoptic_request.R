library(httr)
library(jsonlite)
library(lubridate)
library(here) # Essential for multi-computer setups
library(optparse)
library(lubridate)

# Setup CLI Options
option_list <- list(
  make_option(c("-s", "--start"), type="character", default=NULL,
              help="Override start time (YYYYMMDDHHMM). Ignored if not set.",
              metavar="character")
)

opt_parser <- OptionParser(option_list=option_list)
opt <- parse_args(opt_parser)
# ---------------- Setup & Paths ---------------- #

# Use here() to anchor paths to your project root
# Assumes your folder structure is project_root/data_raw/synoptic
base_dir <- here("data_raw", "synoptic")
timestamp_file <- file.path(base_dir, "last_timestamps.json")
config_path <- here("config", "syn_config.json")

# Create directories if they don't exist (helpful for new computers)
if (!dir.exists(base_dir)) dir.create(base_dir, recursive = TRUE)

# Load Config
if (!file.exists(config_path)) stop("Missing syn_config.json")
config_file <- read_json(config_path, simplifyVector = FALSE)
stations_config <- config_file$stations

API_TOKEN <- Sys.getenv("SYNOPTIC_TOKEN")
if (API_TOKEN == "") stop("SYNOPTIC_TOKEN not found in environment variables.")

# ---------------- Helpers ---------------- #

# 1. simplified Time Helper (Synoptic format: YYYYMMDDHHMM)
get_synoptic_time <- function(time_obj) {
  format(time_obj, "%Y%m%d%H%M")
}

append_data <- function(station_id, new_data) {
  file_path <- file.path(base_dir, paste0(station_id, ".json"))

  existing <- list()
  if (file.exists(file_path)) {
    tryCatch({
      existing <- read_json(file_path, simplifyVector = FALSE)
    }, error = function(e) { warning("Read error, starting fresh.") })
  }

  combined <- c(existing, new_data)

  # RENAMED: This helper just finds the start of a chunk for sorting purposes
  get_chunk_start <- function(chunk) {
    obs <- chunk$OBSERVATIONS$date_time
    if (is.null(obs)) return("0000-00-00")
    return(head(unlist(obs), 1)) # We sort based on the FIRST timestamp
  }

  # Sort the file contents chronologically
  combined <- combined[order(sapply(combined, get_chunk_start))]

  write_json(combined, file_path, auto_unbox = TRUE, pretty = TRUE)
}

# ---------------- Main Logic ---------------- #

# Load timestamps (safely)
timestamps <- if (file.exists(timestamp_file)) read_json(timestamp_file) else list()

# Defaults
current_time_str <- get_synoptic_time(now("UTC"))
default_units <- ifelse(is.null(config_file$units), "metric", config_file$units)
base_url <- "https://api.synopticdata.com/v2/stations/timeseries"

# Loop through stations
for (station_id in names(stations_config)) {

  # 1. Determine Start Time
  if (!is.null(opt$start)) {
    # CLI Flag takes priority over everything
    start_str <- opt$start
    message(sprintf("   [CLI OVERRIDE] Using custom start: %s", start_str))

  } else {
    # AUTOMATIC MODE: Use the cursor file or default to today
    last_ts <- timestamps[[station_id]]

    if (is.null(last_ts)) {
      start_str <- get_synoptic_time(floor_date(now("UTC"), "day"))
    } else {
      start_str <- last_ts
    }
  }

  message(sprintf("Fetching %s (Start: %s)...", station_id, start_str))

  # 2. Build Params
  params <- list(
    token = API_TOKEN,
    stid = station_id,
    vars = paste(stations_config[[station_id]], collapse = ","),
    units = default_units,
    start = start_str,
    end = current_time_str,
    output = "json",
    qc = "on",
    qc_remove_data = "off",
    qc_flags = "on",
    sensorvars = 1,
    precip = 1
  )

  # 3. Fetch Data using RETRY (Simplifies the loop logic)
  response <- RETRY(
    verb = "GET",
    url = base_url,
    query = params,
    times = 3, # Retry 3 times
    pause_min = 1,
    terminate_on = c(400, 401, 403, 404) # Don't retry client errors
  )

  # 4. Process Response
  if (status_code(response) == 200) {
    result <- content(response, as = "parsed", type = "application/json")

    if (!is.null(result$STATION)) {
      # Synoptic returns a list of stations. We typically want the first one.
      station_data <- result$STATION

      # Append to disk
      append_data(station_id, station_data)

      # Extract last timestamp for the tracker
      # Access the last chunk -> OBSERVATIONS -> date_time -> last item
      last_chunk <- station_data[[length(station_data)]]
      all_dates <- unlist(last_chunk$OBSERVATIONS$date_time)

      if (length(all_dates) > 0) {
        last_obs_iso <- tail(all_dates, 1)
        # Convert ISO (2025-01-01T12:00:00Z) to Synoptic (202501011200)
        # ymd_hms parses the ISO string, then we format it back
        new_last_ts <- get_synoptic_time(ymd_hms(last_obs_iso))
        timestamps[[station_id]] <- new_last_ts
        message(sprintf("  -> Success. New cursor: %s", new_last_ts))
      }
    } else {
      message("  -> No data returned (Empty STATION list).")
    }
  } else {
    warning(sprintf("  -> Failed: %s", status_code(response)))
  }
}

# Save updated timestamps
write_json(timestamps, timestamp_file, auto_unbox = TRUE, pretty = TRUE)
