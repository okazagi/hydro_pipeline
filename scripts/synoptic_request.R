library(httr)
library(jsonlite)
library(lubridate)

# ---------------- Setup & Paths ---------------- #

API_TOKEN <- Sys.getenv("SYNOPTIC_TOKEN")

# Paths - Adjusted to match your Python script
# Note: In R, file.path is safer for cross-platform, but hardcoding works if on same machine
base_dir <- "/home/okazagi/synoptic_tracker/data"
if (!dir.exists(base_dir)) {
  dir.create(base_dir, recursive = TRUE)
}

timestamp_file <- file.path(base_dir, "last_timestamps.json")
config_file <- "/home/okazagi/synoptic_tracker/config.json"

# ---------------- Helpers ---------------- #

# Helper to load JSON safely
load_json_safe <- function(path, default = list()) {
  if (file.exists(path)) {
    return(read_json(path, simplifyVector = FALSE)) # simplifyVector=FALSE keeps structure close to Python dicts
  }
  return(default)
}

# Helper to get current UTC time in Synoptic format (YYYYMMDDHHMM)
get_current_time_str <- function() {
  format(now(tzone = "UTC"), "%Y%m%d%H%M")
}

# Function to append data and save
append_data <- function(station_id, new_data) {
  file_path <- file.path(base_dir, paste0(station_id, ".json"))

  existing <- list()
  if (file.exists(file_path)) {
    tryCatch({
      existing <- read_json(file_path, simplifyVector = FALSE)
    }, error = function(e) {
      existing <- list()
    })
  }

  # Merge existing and new
  combined <- c(existing, new_data)

  # Sort by date_time (Helper function to extract date for sorting)
  # We assume structure is entry$OBSERVATIONS$date_time
  get_dt <- function(entry) {
    dt <- entry$OBSERVATIONS$date_time
    if (is.null(dt)) return("")
    if (is.list(dt) || length(dt) > 1) return(dt[[1]]) # Handle list or vector
    return(dt)
  }

  # Sort the list based on the date_time
  combined <- combined[order(sapply(combined, get_dt))]

  # Write back to file
  write_json(combined, file_path, auto_unbox = TRUE, pretty = TRUE)
}

# ---------------- Main ---------------- #

# Load Config
config <- load_json_safe(config_file)
stations_config <- config$stations
base_url <- ifelse(is.null(config$base_url), "https://api.synopticdata.com/v2/stations/timeseries", config$base_url)
retry_attempts <- ifelse(is.null(config$retry_attempts), 3, as.integer(config$retry_attempts))
units <- ifelse(is.null(config$units), "metric", config$units)

if (API_TOKEN == "") {
  stop("[ERROR] SYNOPTIC_API_TOKEN is not set in environment.")
}

timestamps <- load_json_safe(timestamp_file)
current_time <- get_current_time_str()

# Iterate through stations
station_ids <- names(stations_config)

for (station_id in station_ids) {
  variables <- stations_config[[station_id]]

  # Determine start time (Default: Today 00:00 UTC)
  default_start <- format(floor_date(now("UTC"), "day"), "%Y%m%d0000")
  start_time <- ifelse(!is.null(timestamps[[station_id]]), timestamps[[station_id]], default_start)

  # Construct Query Parameters
  # Note: Synoptic 'vars' expects comma-separated string
  params <- list(
    token = API_TOKEN,
    stid = station_id,
    vars = paste(variables, collapse = ","),
    units = units,
    start = start_time,
    end = current_time,
    output = "json",
    qc = "on",
    qc_remove_data = "off",
    qc_flags = "on",
    sensorvars = 1,
    precip = 1
  )

  success <- FALSE

  for (attempt in 1:retry_attempts) {
    tryCatch({
      response <- GET(url = base_url, query = params, timeout(30))

      if (status_code(response) == 200) {
        result <- content(response, as = "parsed", type = "application/json")

        if (!is.null(result$STATION)) {
          station_data <- result$STATION

          # Append Data Logic
          append_data(station_id, station_data)

          # Update Timestamps Logic
          last_entry <- station_data[[length(station_data)]]
          dt_field <- last_entry$OBSERVATIONS$date_time

          last_obs <- NULL
          if (is.list(dt_field)) {
            last_obs <- dt_field[[length(dt_field)]]
          } else {
            last_obs <- dt_field
          }

          if (!is.null(last_obs)) {
            # Clean string: 2025-10-20T07:40:00Z -> 202510200740
            clean <- gsub("[-:TZ]", "", last_obs)
            clean <- substr(clean, 1, 12)
            timestamps[[station_id]] <- clean
          }
        } else {
          message(sprintf("[INFO] No data returned for %s.", station_id))
        }

        success <- TRUE
        break # Exit retry loop

      } else {
        message(sprintf("Attempt %d: Failed for %s, status %d", attempt, station_id, status_code(response)))
      }
    }, error = function(e) {
      message(sprintf("Attempt %d: Error fetching data for %s: %s", attempt, station_id, e$message))
    })

    if (success) break
  }
}

# Persist updated timestamps
write_json(timestamps, timestamp_file, auto_unbox = TRUE, pretty = TRUE)
