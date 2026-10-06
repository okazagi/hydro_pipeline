#!/usr/bin/env Rscript

# Pulls IDWC2's raw GOES DCP messages via LRGS/DDS (scripts/test_lrgs_dcp_pull.sh)
# and decodes them into the station's actual sensor readings, bypassing HADS.
# HADS's SHEF product for IDWC2 (PE code M1/M3/M4) truncates soil moisture to
# 2 decimal places (0.01 m3/m3 steps) -- IDWC2's actual diurnal swing at 5cm is
# only ~0.01 m3/m3, so that truncation collapses the value into a near-binary
# square wave when graphed. The raw DCP messages carry 4-decimal precision
# (e.g. 0.0420, 0.0430, 0.0440), ~100x finer. See project_hads_soil_moisture_gap
# memory for the ASEC2 case this mirrors, and field-order/timing verification.
#
# Field order and units are taken from the transmitted ST_DATA table in
# IndyPass_20min_Aug_complete.CR1X (GOES ID 28A00A9C), but note the file's own
# DataInterval (20 min) does NOT match what's actually on the wire -- live
# pulls cross-checked against overlapping HADS data confirmed IDWC2 transmits
# 3 readings per message at 30-min spacing (same cadence as ASEC2, and matching
# HADS timestamps exactly), not 20 min. Also: the file's WindVector field is
# NOT present in the live message -- actual transmitted format is 11 values
# (no wind), not the 14 the file's code would suggest. IDWC2 has no air temp
# sensor, so there's no AirTemp_C to decode here.
#
# Output columns match roaring-fork-hydro's existing measurements schema (see
# scripts/hads_retrieval.R's idwc2_mapping) so this can eventually be merged
# in, but this script intentionally writes to dds_fallback/, NOT data_clean/,
# so it is never auto-ingested by build_database.R until reviewed.
#
# Usage: ./scripts/parse_idwc2_dcp.R [since] [until]
#   since/until use OpenDCS search-criteria time syntax, e.g. "now - 2 days".
#   Defaults to the last 2 days. LRGS's retrospective archive has a fixed
#   floor (2026-04-19 for ASEC2 -- unconfirmed whether the same applies here),
#   so this is primarily a fix for ongoing/future data, not guaranteed to
#   reach arbitrarily far back.

suppressPackageStartupMessages({
  library(here)
  library(lubridate)
  library(dplyr)
  library(purrr)
  library(readr)
  library(stringr)
})

args <- commandArgs(trailingOnly = TRUE)
SINCE <- if (length(args) >= 1) args[1] else "now - 2 days"
UNTIL <- if (length(args) >= 2) args[2] else "now"

DCP_ADDRESS <- "28A00A9C"
READING_INTERVAL_MIN <- 30

OUTPUT_DIR <- here("dds_fallback")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR)

# 1. Pull raw DCP messages ---------------------------------------------------

pull_raw_lines <- function(since, until) {
  cmd <- sprintf(
    "cd %s && ./scripts/test_lrgs_dcp_pull.sh %s %s %s",
    shQuote(here()), shQuote(since), shQuote(until), shQuote(DCP_ADDRESS)
  )
  result <- system2("bash", args = c("-c", shQuote(cmd)), stdout = TRUE, stderr = TRUE)
  status <- attr(result, "status")
  if (!is.null(status) && status != 0) {
    stop(paste("test_lrgs_dcp_pull.sh failed:\n", paste(result, collapse = "\n")))
  }
  result
}

# 2. Parse header + data lines into per-reading rows -------------------------
#
# Each message looks like:
#   28A00A9C26233181545G40-0NN054WUP00322
#     13.5,  0.00, 10.01,0.0420,0.0040, 10.08,0.0540,0.0020,  8.39,0.1240,0.0220,
#     13.5,  0.00, 10.78,0.0420,0.0040, 10.04,0.0540,0.0020,  8.39,0.1240,0.0220,
#
#     13.5,  0.00, 10.78,0.0420,0.0040, 10.04,0.0540,0.0020,  8.39,0.1240,0.0220,
#     13.4,  0.00, 11.62,0.0420,0.0040,  9.97,0.0540,0.0030,  8.39,0.1240,0.0210,
# The blank-line-separated halves overlap by one reading (the last row of the
# first half repeats as the first row of the second half) -- a redundancy the
# transmitter adds against a missed message, not a distinct reading. Dedupe
# consecutive identical rows before assigning timestamps, or every block
# reports 4 slots for only 3 real readings.
#
# Header's 11 digits after the 8-char DCP address are YYDDDHHMMSS. Transmission
# happens at :15:45 past the hour, carrying that hour's 3 readings at :00, :30,
# and :00 of the next hour (i.e. the last reading lands exactly on the
# transmission's own top-of-hour) -- confirmed by matching decoded values
# against overlapping HADS timestamps for IDWC2, 18 readings, zero mismatches.

HEADER_RE <- "^\\s*([0-9A-Fa-f]{8})(\\d{11})"
DATA_ROW_RE <- "^\\s*-?\\d+\\.\\d+\\s*,"

parse_header_time <- function(ts_digits) {
  yy  <- as.integer(substr(ts_digits, 1, 2))
  ddd <- as.integer(substr(ts_digits, 3, 5))
  hh  <- as.integer(substr(ts_digits, 6, 7))
  mm  <- as.integer(substr(ts_digits, 8, 9))
  ss  <- as.integer(substr(ts_digits, 10, 11))
  as.POSIXct(sprintf("%d-01-01", 2000 + yy), tz = "UTC") +
    days(ddd - 1) + hours(hh) + minutes(mm) + seconds(ss)
}

parse_data_row <- function(line) {
  vals <- str_split(line, ",")[[1]]
  vals <- trimws(vals)
  vals <- vals[vals != ""]
  vals <- suppressWarnings(as.numeric(vals))
  if (length(vals) < 11 || any(is.na(vals[1:11]))) return(NULL)
  vals[1:11]
}

parse_dcp_lines <- function(lines) {
  blocks <- list()
  current_anchor <- NA
  pending <- list()

  flush_block <- function() {
    n <- length(pending)
    if (n == 0 || is.na(current_anchor)) return(invisible(NULL))
    anchor_hour <- floor_date(current_anchor, "hour")
    times <- anchor_hour - minutes(READING_INTERVAL_MIN * rev(seq_len(n) - 1))
    for (i in seq_len(n)) {
      blocks[[length(blocks) + 1]] <<- c(list(datetime = times[i]), as.list(pending[[i]]))
    }
  }

  for (line in lines) {
    m <- regmatches(line, regexec(HEADER_RE, line))[[1]]
    if (length(m) == 3) {
      flush_block()
      pending <- list()
      current_anchor <- parse_header_time(m[3])
    } else if (grepl(DATA_ROW_RE, line)) {
      row <- parse_data_row(line)
      if (!is.null(row)) {
        is_dup <- length(pending) > 0 && identical(row, pending[[length(pending)]])
        if (!is_dup) pending[[length(pending) + 1]] <- row
      }
    }
  }
  flush_block()
  blocks
}

# 3. Map positional fields to named columns -----------------------------------
#
# Column names match the existing measurements table where a direct
# equivalent exists (see hads_retrieval.R's idwc2_mapping: 2in/8in/20in map to
# the 5cm/20cm/50cm columns). EC has no home in the current schema and is kept
# as a DDS_-prefixed extra. Unlike ASEC2, no F->C correction is needed here --
# raw soil temps decode directly to plausible degC values.

in_to_cm <- function(i) round(i * 2.54, 2)

build_row_df <- function(block) {
  v <- block[2:12]
  tibble(
    Date_UTC = as.character(as.Date(block$datetime)),
    Time_UTC = format(block$datetime, "%H:%M:%S"),
    Station_Name = "IDWC2",
    Battery_Voltage = v[[1]],
    Rain_cm = in_to_cm(v[[2]]),
    SoilTemp_C_5cm = v[[3]],
    WaterCont_5cm_m3m3 = v[[4]],
    SoilTemp_C_20cm = v[[6]],
    WaterCont_20cm_m3m3 = v[[7]],
    SoilTemp_C_50cm = v[[9]],
    WaterCont_50cm_m3m3 = v[[10]],
    DataFlag = "DDS_Fallback",
    DDS_EC_5cm = v[[5]],
    DDS_EC_20cm = v[[8]],
    DDS_EC_50cm = v[[11]]
  )
}

# 4. Run ----------------------------------------------------------------------

message(sprintf("Pulling IDWC2 DCP messages: %s -> %s", SINCE, UNTIL))
raw_lines <- pull_raw_lines(SINCE, UNTIL)
blocks <- parse_dcp_lines(raw_lines)

if (length(blocks) == 0) {
  stop("No decodable data rows found in the DDS pull. Check the raw output for errors.")
}

result <- map_dfr(blocks, build_row_df) %>%
  distinct(Date_UTC, Time_UTC, .keep_all = TRUE) %>%
  arrange(Date_UTC, Time_UTC)

out_file <- file.path(OUTPUT_DIR, "IDWC2_dds_fallback.csv")
write_csv(result, out_file)

message(sprintf("Decoded %d readings (%s to %s).", nrow(result),
                 min(result$Date_UTC), max(result$Date_UTC)))
message(sprintf("Soil moisture (5cm) non-NA: %d / %d",
                 sum(!is.na(result$WaterCont_5cm_m3m3)), nrow(result)))
message(sprintf("Wrote %s", out_file))
