library(httr)
library(jsonlite)
library(lubridate)
library(here)
library(optparse)

# Setup CLI Options
option_list <- list(
  make_option(c("-s", "--start"), type="character", default=NULL,
              help="Override start time (YYYYMMDDHHMM).",
              metavar="character")
)

opt_parser <- OptionParser(option_list=option_list)
opt <- parse_args(opt_parser)

# ---------------- Setup & Paths ---------------- #
base_dir <- here("data_raw", "synoptic")
timestamp_file <- file.path(base_dir, "last_timestamps.json")
config_path <- here("config", "syn_config.json")

if (!dir.exists(base_dir)) dir.create(base_dir, recursive = TRUE)
if (!file.exists(config_path)) stop("Missing syn_config.json")

config_file <- read_json(config_path, simplifyVector = FALSE)
stations_config <- config_file$stations

API_TOKEN <- Sys.getenv("SYNOPTIC_TOKEN")
if (API_TOKEN == "") stop("SYNOPTIC_TOKEN not found in environment variables.")

# ---------------- Helpers ---------------- #

get_station_true_start <- function(station_id, token) {
  url <- "https://api.synopticdata.com/v2/stations/metadata"
  resp <- RETRY("GET", url, query = list(token = token, stid = station_id, complete = "1"), times = 3)
  if (status_code(resp) == 200) {
    data <- fromJSON(content(resp, as = "text", encoding = "UTF-8"), simplifyVector = FALSE)
    if (!is.null(data$STATION) && length(data$STATION) > 0) {
      return(get_synoptic_time(ymd_hms(data$STATION[[1]]$PERIOD_OF_RECORD$start)))
    }
  }
  return(NULL)
}

get_synoptic_time <- function(time_obj) format(time_obj, "%Y%m%d%H%M")

append_data <- function(station_id, new_data) {
  file_path <- file.path(base_dir, paste0(station_id, ".json"))
  existing <- if (file.exists(file_path)) read_json(file_path, simplifyVector = FALSE) else list()
  combined <- c(existing, new_data)
  combined <- combined[!duplicated(sapply(combined, serialize, connection=NULL))]
  get_chunk_start <- function(chunk) {
    obs <- chunk$OBSERVATIONS$date_time
    if (is.null(obs)) return("0000-00-00")
    return(head(unlist(obs), 1))
  }
  combined <- combined[order(sapply(combined, get_chunk_start))]
  write_json(combined, file_path, auto_unbox = TRUE, pretty = TRUE)
}

# ---------------- Main Logic ---------------- #

timestamps <- if (file.exists(timestamp_file)) read_json(timestamp_file) else list()
current_time_utc <- now("UTC")
default_units <- ifelse(is.null(config_file$units), "metric", config_file$units)
base_url <- "https://api.synopticdata.com/v2/stations/timeseries"
chunk_days <- 30

for (station_id in names(stations_config)) {

  if (!is.null(opt$start)) {
    start_str <- opt$start
  } else {
    last_ts <- timestamps[[station_id]]
    if (is.null(last_ts)) {
      true_start <- get_station_true_start(station_id, API_TOKEN)
      start_str <- if (!is.null(true_start)) true_start else get_synoptic_time(floor_date(current_time_utc, "day") - days(1))
    } else {
      start_str <- last_ts
    }
  }

  catching_up <- TRUE

  while (catching_up) {
    chunk_start_date <- ymd_hm(start_str)
    chunk_end_date <- chunk_start_date + days(chunk_days)

    if (chunk_end_date > current_time_utc) {
      chunk_end_date <- current_time_utc
      catching_up <- FALSE
    }

    end_str <- get_synoptic_time(chunk_end_date)
    message(sprintf("[%s] Requesting: %s to %s", station_id, start_str, end_str))

    params <- list(
      token = API_TOKEN, stid = station_id,
      vars = paste(stations_config[[station_id]], collapse = ","),
      units = default_units, start = start_str, end = end_str,
      output = "json", qc = "on", qc_remove_data = "off",
      qc_flags = "on", sensorvars = 1, precip = 1
    )

    # Use RETRY to handle minor blips, but check results carefully
    response <- RETRY("GET", base_url, query = params, times = 2)

    # --- ERROR HANDLING BLOCK ---

    # 1. Check for 502/Server Errors
    if (status_code(response) >= 500) {
      message(sprintf("!!! Server Error %s. API might be down. Stopping for this station.", status_code(response)))
      catching_up <- FALSE
      next
    }

    # 2. Check if the response is actually JSON (Prevents Lexical Error)
    content_type <- headers(response)$`content-type`
    if (!grepl("application/json", content_type)) {
      message("!!! Received non-JSON response (probably an HTML error page). Stopping.")
      catching_up <- FALSE
      next
    }

    # 3. Safe Parse
    raw_text <- content(response, as = "text", encoding = "UTF-8")
    result <- fromJSON(raw_text, simplifyVector = FALSE)

    # 4. Check for Synoptic-specific errors inside the JSON
    if (!is.null(result$SUMMARY$RESPONSE_CODE) && result$SUMMARY$RESPONSE_CODE != 1) {
      message(sprintf("!!! API returned error code %s: %s", result$SUMMARY$RESPONSE_CODE, result$SUMMARY$RESPONSE_MESSAGE))
      catching_up <- FALSE
      next
    }

    # --- DATA PROCESSING BLOCK ---

    if (!is.null(result$STATION) && length(result$STATION) > 0) {
      append_data(station_id, result$STATION)

      # Determine new start time from last observation
      last_chunk <- result$STATION[[length(result$STATION)]]
      all_dates <- unlist(last_chunk$OBSERVATIONS$date_time)

      if (length(all_dates) > 0) {
        last_obs_iso <- tail(all_dates, 1)
        new_start_str <- get_synoptic_time(ymd_hms(last_obs_iso))

        # Prevent infinite loop if the API keeps returning the same last record
        if (new_start_str == start_str) {
          message("    No new time progress. Advancing manually by 1 minute.")
          start_str <- get_synoptic_time(ymd_hms(last_obs_iso) + minutes(1))
        } else {
          start_str <- new_start_str
        }

        timestamps[[station_id]] <- start_str
        write_json(timestamps, timestamp_file, auto_unbox = TRUE, pretty = TRUE)
      }
    } else {
      message("    No data in this window. Advancing to next chunk.")
      start_str <- end_str
    }

    Sys.sleep(1) # Polite pause
  }
}

write_json(timestamps, timestamp_file, auto_unbox = TRUE, pretty = TRUE)
message("\nDone.")
