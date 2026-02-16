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

if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)
station_key <- read_json(STID_PATH)

# ---------------- Date Logic ---------------- #

get_start_time <- function(stid) {
  # Look for the most recent file for this station
  pattern <- sprintf("^%s_.*\\.json$", stid)
  existing_files <- list.files(OUTPUT_DIR, pattern = pattern, full.names = TRUE)

  if (length(existing_files) == 0) {
    # If no files exist, start from the beginning of your automated record
    return("2026-01-01 00:00:00")
  }

  # Find the max timestamp within the most recent file
  # This mimics your 'last_timestamp' logic from the Synoptic script
  latest_file <- existing_files[order(file.info(existing_files)$mtime, decreasing = TRUE)][1]
  data <- read_json(latest_file, simplifyVector = TRUE)

  # LI-COR structure: data is usually in [[timestamp, value], ...]
  # We extract the last timestamp from the first sensor found
  if (length(data$sensors) > 0 && length(data$sensors[[1]]$data[[1]]$records) > 0) {
    records <- data$sensors[[1]]$data[[1]]$records
    last_ms <- records[[length(records)]][[1]]
    # Convert MS to ISO string and add 1 second to avoid overlap
    last_ts <- as.POSIXct(last_ms / 1000, origin="1970-01-01", tz="UTC") + 1
    return(format(last_ts, "%Y-%m-%d %H:%M:%S"))
  }

  return("2026-01-01 00:00:00")
}

# ---------------- Fetch Function ---------------- #

fetch_incremental <- function(stid, logger_id) {
  url <- "https://api.licor.cloud/v1/data"

  start_ts <- get_start_time(stid)
  end_ts   <- format(now(tzone = "UTC"), "%Y-%m-%d %H:%M:%S")

  # Don't request if the gap is too small (e.g. < 5 minutes)
  if (as.numeric(difftime(ymd_hms(end_ts), ymd_hms(start_ts), units = "mins")) < 5) {
    message(sprintf("[%s] Already up to date.", stid))
    return(NULL)
  }

  params <- list(
    loggers = logger_id,
    start_date_time = start_ts,
    end_date_time = end_ts
  )

  headers <- add_headers(`Authorization` = paste("Bearer", API_TOKEN))

  message(sprintf("[%s] Fetching from %s to %s", stid, start_ts, end_ts))

  response <- GET(url, query = params, headers)

  if (status_code(response) == 200) {
    raw_data <- content(response, as = "parsed", type = "application/json")

    # Save with a timestamp in the filename so we don't overwrite
    # This creates a "transaction log" of data
    file_ts <- format(now(), "%Y%m%d_%H%M")
    out_file <- file.path(OUTPUT_DIR, sprintf("%s_%s.json", stid, file_ts))

    write_json(raw_data, out_file, auto_unbox = TRUE, pretty = TRUE)
    return(TRUE)
  }
  return(FALSE)
}

# ---------------- Main ---------------- #
if (API_TOKEN == "") stop("Token not found.")

walk2(names(station_key), station_key, fetch_incremental)
message("Incremental sync complete.")
