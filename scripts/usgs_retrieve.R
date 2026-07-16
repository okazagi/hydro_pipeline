library(dataRetrieval)
library(dplyr)
library(readr)
library(lubridate)
library(here)

TIMESTAMP_DIR <- here("config", "usgs_timestamps")

get_last_timestamp <- function(site_id) {
  ts_file <- file.path(TIMESTAMP_DIR, paste0(site_id, "_last_timestamp.txt"))
  if (file.exists(ts_file)) {
    return(readLines(ts_file, n = 1, warn = FALSE))
  }
  return(NULL)
}

save_last_timestamp <- function(site_id, ts) {
  if (!dir.exists(TIMESTAMP_DIR)) dir.create(TIMESTAMP_DIR, recursive = TRUE)
  ts_file <- file.path(TIMESTAMP_DIR, paste0(site_id, "_last_timestamp.txt"))
  writeLines(as.character(ts), ts_file)
}

retrieve_station_data <- function(site_id, start_date = NULL, end_date = NULL) {

  # 1. Clean the Site ID
  clean_site_id <- gsub("USGS-", "", site_id)

  # 2. Determine Dates
  if (is.null(start_date)) {
    start_date <- get_last_timestamp(clean_site_id)
    if (is.null(start_date)) start_date <- "2023-01-01T00:00:00Z"
  } else {
    # Ensure manual start date is full ISO if only date provided
    parsed_start <- parse_date_time(start_date, orders = c("Ymd HMS", "Ymd", "Y-m-d H:M:S", "Y-m-d"), tz = "UTC")
    if (!is.na(parsed_start)) {
      start_date <- format(parsed_start, "%Y-%m-%dT%H:%M:%SZ")
    }
  }

  if (is.null(end_date)) {
    end_date <- format(now(tzone = "UTC"), "%Y-%m-%dT%H:%M:%SZ")
  }

  # 3. Define parameters
  p_codes <- c("72253", "74207", "72431", "00052", "00020", "75969")

  message(paste("Retrieving data for site:", clean_site_id, "from", start_date, "to", end_date))

  # 4. Fetch Data
  raw_data <- tryCatch({
    readNWISuv(siteNumbers = clean_site_id,
               parameterCd = p_codes,
               startDate = start_date,
               endDate = end_date)
  }, error = function(e) {
    message(paste("Error fetching", clean_site_id, ":", e$message))
    return(data.frame())
  })

  if (nrow(raw_data) == 0) {
    message(paste("No data found for", clean_site_id))
    return(NULL)
  }

  # 5. Rename columns
  latest_usgs_data <- renameNWISColumns(raw_data)

  # 6. Save Raw Data
  file_path <- here("data_raw", paste0("USGS_", clean_site_id, "_raw.csv"))

  if(!dir.exists(here("data_raw"))) dir.create(here("data_raw"), recursive = TRUE)

  write_csv(latest_usgs_data, file_path)
  message(sprintf("[%s] Raw data saved to: %s", clean_site_id, basename(file_path)))

  # 7. Update last timestamp
  if ("dateTime" %in% names(latest_usgs_data)) {
    max_ts <- max(latest_usgs_data$dateTime)
    save_last_timestamp(clean_site_id, format(max_ts, "%Y-%m-%dT%H:%M:%SZ"))
    message(sprintf("[%s] Updated last timestamp to %s", clean_site_id, format(max_ts, "%Y-%m-%dT%H:%M:%SZ")))
  }

  return(file_path)
}

# --- Main Execution ---
if (!interactive()) {
  # Check for manual start date from CLI
  cli_args <- commandArgs(trailingOnly = TRUE)
  manual_start <- if (length(cli_args) > 0) cli_args[1] else NULL
  
  usgs_sites <- read_csv(here("config", "usgs_sites.csv"), show_col_types = FALSE)
  message(sprintf("Starting USGS retrieval for %d sites...", nrow(usgs_sites)))
  
  for (site in usgs_sites$site_id) {
    tryCatch({
      # Pass the manual start date if provided, otherwise it uses internal logic
      retrieve_station_data(site, start_date = manual_start)
    }, error = function(e) {
      message(sprintf("Error retrieving site %s: %s", site, e$message))
    })
  }
  message("USGS retrieval complete.")
}

