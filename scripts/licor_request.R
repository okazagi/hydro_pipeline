#!/usr/bin/env Rscript

if (file.exists(".env")) readRenviron(".env")
suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(lubridate)
  library(here)
  library(purrr)
})

# ---------------- Setup ---------------- #
API_TOKEN  <- Sys.getenv("LICOR_TOKEN")
STID_PATH  <- here("config", "station_key.json")
OUTPUT_DIR <- here("data_raw", "licor", "daily_json") # Keep daily pulls separate
TIMESTAMP_FILE <- here("config", "licor_last_timestamp.txt")

if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)
station_key <- read_json(STID_PATH)

# ---------------- Date Logic ---------------- #

get_start_time <- function() {
  if (file.exists(TIMESTAMP_FILE)) {
    return(readLines(TIMESTAMP_FILE, n = 1, warn = FALSE))
  }
  return("2026-01-01 00:00:00")
}

save_last_timestamp <- function(ts) {
  writeLines(as.character(ts), TIMESTAMP_FILE)
}

# ---------------- Fetch Function ---------------- #

fetch_incremental <- function(stid, logger_id, start_ts) {
  url <- "https://api.licor.cloud/v1/data"
  end_ts_obj <- now(tzone = "UTC")
  end_ts     <- format(end_ts_obj, "%Y-%m-%d %H:%M:%S")
  
  # Support flexible start date parsing
  start_ts_obj <- parse_date_time(start_ts, orders = c("Ymd HMS", "Ymd", "Y-m-d H:M:S", "Y-m-d"), tz = "UTC")

  if (is.na(start_ts_obj)) {
    message(sprintf("[%s] Failed to parse start timestamp: %s", stid, start_ts))
    return(NULL)
  }

  # Format back to string for API
  api_start_ts <- format(start_ts_obj, "%Y-%m-%d %H:%M:%S")

  # Don't request if the gap is too small (e.g. < 5 minutes)
  if (as.numeric(difftime(end_ts_obj, start_ts_obj, units = "mins")) < 5) {
    message(sprintf("[%s] Already up to date.", stid))
    return(NULL)
  }

  params <- list(
    loggers = logger_id,
    start_date_time = api_start_ts,
    end_date_time = end_ts
  )

  headers <- add_headers(`Authorization` = paste("Bearer", API_TOKEN))

  message(sprintf("[%s] Fetching from %s to %s", stid, api_start_ts, end_ts))

  response <- GET(url, query = params, headers)
  
  message(sprintf("[%s] Response Status: %s", stid, status_code(response)))

  if (status_code(response) == 200) {
    raw_data <- content(response, as = "parsed", type = "application/json")

    # Save with a timestamp in the filename so we don't overwrite
    # This creates a "transaction log" of data
    file_ts <- format(now(), "%Y%m%d_%H%M")
    out_file <- file.path(OUTPUT_DIR, sprintf("%s_%s.json", stid, file_ts))

    write_json(raw_data, out_file, auto_unbox = TRUE, pretty = TRUE)
    message(sprintf("[%s] Saved raw data to %s", stid, basename(out_file)))

    # Extract the last timestamp from the response to return
    # New LI-COR API structure uses a 'data' field with flat records
    if (!is.null(raw_data$data) && length(raw_data$data) > 0) {
      # Get the last timestamp from the data list
      last_record <- raw_data$data[[length(raw_data$data)]]
      last_ts_str <- last_record$timestamp
      # Convert "2026-02-20 00:00:00Z" to "2026-02-20 00:00:00"
      last_ts <- ymd_hms(last_ts_str)
      return(format(last_ts, "%Y-%m-%d %H:%M:%S"))
    }
    return(end_ts)
  }
  return(NULL)
}

# ---------------- Main ---------------- #
if (API_TOKEN == "") stop("Token not found.")

# Check for manual start date from CLI
cli_args <- commandArgs(trailingOnly = TRUE)
current_start_ts <- if (length(cli_args) > 0) cli_args[1] else get_start_time()

message(sprintf("Starting sync from: %s", current_start_ts))

results <- map2(names(station_key), station_key, function(name, meta) {
  fetch_incremental(name, meta$logger, current_start_ts)
})

# Filter out NULL results and find the max timestamp retrieved
valid_timestamps <- unlist(compact(results))
if (length(valid_timestamps) > 0) {
  max_ts <- max(valid_timestamps)
  save_last_timestamp(max_ts)
  message(sprintf("Updated last timestamp to %s", max_ts))
}

message("Incremental sync complete.")
