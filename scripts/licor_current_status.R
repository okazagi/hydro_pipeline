#!/usr/bin/env Rscript

if (file.exists(".env")) readRenviron(".env")
suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(here)
  library(purrr)
})

# ---------------- Setup ---------------- #
API_TOKEN <- Sys.getenv("LICOR_TOKEN")
KEY_PATH  <- here("config", "station_key.json")

if (!file.exists(KEY_PATH)) stop("Missing config/station_key.json")
station_key <- read_json(KEY_PATH)

# Inverse the key so we can look up "RFGLR" by its Logger ID
logger_to_stid <- setNames(names(station_key), unlist(station_key))

# ---------------- API Request ---------------- #
url <- "https://api.licor.cloud/v2/devices"
headers <- add_headers(
  `Authorization` = paste("Bearer", API_TOKEN),
  `accept` = "application/json"
)

message("Fetching current sensor data for all devices...")
response <- GET(url, query = list(includeSensors = "true"), headers)

if (status_code(response) != 200) {
  stop(sprintf("API Error %s: %s", status_code(response), content(response, "text")))
}

raw_content <- content(response, as = "parsed", type = "application/json")

# ---------------- Data Extraction ---------------- #

# 1. Inspect the response if it feels empty
if (length(raw_content$devices) == 0) {
  stop("No devices found in the API response. Check your Token permissions.")
}

# 2. Robust filtering using a standard loop or a safer 'keep'
# This version skips items that don't have a 'serial' field
my_devices <- list()
for (dev in raw_content$devices) {
  if (!is.null(dev$serial) && dev$serial %in% names(logger_to_stid)) {
    my_devices[[length(my_devices) + 1]] <- dev
  }
}

if (length(my_devices) == 0) {
  message("!!! No matching Logger IDs found. Available IDs in API were:")
  print(sapply(raw_content$devices, function(x) x$serial))
  stop("Check if station_key.json matches the Serial Numbers in your LI-COR account.")
}

message(sprintf("Found %d of your stations. Parsing latest readings...", length(my_devices)))

# 3. Updated Parser with extra safety for 'last_reading'
parse_latest <- function(dev) {
  stid <- logger_to_stid[[dev$serial]]

  # Ensure sensors exist
  if (is.null(dev$sensors) || length(dev$sensors) == 0) return(NULL)

  sensors_list <- lapply(dev$sensors, function(s) {
    # Some sensors might not have a 'last_reading' if they are new or offline
    lr <- s$last_reading

    data.frame(
      Station = stid,
      Sensor_SN = ifelse(is.null(s$serial), "Unknown", s$serial),
      Label = ifelse(is.null(s$label), "No Label", s$label),
      Value = ifelse(is.null(lr$value), NA, lr$value),
      Units = ifelse(is.null(lr$units), "", lr$units),
      Timestamp = ifelse(is.null(lr$timestamp), "N/A", lr$timestamp),
      stringsAsFactors = FALSE
    )
  })

  return(do.call(rbind, sensors_list))
}

final_report <- bind_rows(lapply(my_devices, parse_latest))

# 4. View the results
print(as_tibble(final_report))

# Optional: Save a snapshot
write_csv(final_report, here("data_raw", "licor", "current_snapshot.csv"))
