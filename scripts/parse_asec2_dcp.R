#!/usr/bin/env Rscript

# Pulls ASEC2's raw GOES DCP messages via LRGS/DDS (scripts/test_lrgs_dcp_pull.sh)
# and decodes them into the station's actual sensor readings, bypassing HADS
# (which has been dropping ASEC2 soil moisture since 2026-04-01). Field order
# and units are taken directly from the datalogger program transmitted to
# this station (CastleCreek_20min_Sept3.CR1X, GOES ID 28A0044E), specifically
# its ST_DATA table -- see project_hads_soil_moisture_gap memory for details.
#
# Output columns are named to match roaring-fork-hydro's existing measurements
# schema (see scripts/hads_retrieval.R) so this can eventually be merged in,
# but this script intentionally writes to dds_fallback/, NOT data_clean/, so
# it is never auto-ingested by build_database.R until reviewed.
#
# Usage: ./scripts/parse_asec2_dcp.R [since] [until]
#   since/until use OpenDCS search-criteria time syntax, e.g. "now - 2 days".
#   Defaults to the last 2 days. Note: LRGS's retrospective message archive
#   is a rolling window (commonly 30-60 days depending on the server) -- it
#   likely does NOT reach back to the 2026-04-01 HADS gap. This is primarily
#   a fix for ongoing/future data, not a guaranteed full historical backfill.
#   Ask NOAA about a longer-range retrospective pull if that's needed.

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

OUTPUT_DIR <- here("dds_fallback")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR)

# 1. Pull raw DCP messages ---------------------------------------------------

pull_raw_lines <- function(since, until) {
  cmd <- sprintf(
    "cd %s && ./scripts/test_lrgs_dcp_pull.sh %s %s",
    shQuote(here()), shQuote(since), shQuote(until)
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
#   28A0044E26229171610G33+0NN054WUP00302
#     14.3, 16.08, 29.72, ...,  <21 comma-separated values>
#     14.1, 17.04, 27.34, ...,  <21 comma-separated values>
# The header's 11 digits after the 8-char DCP address are YYDDDHHMMSS -- the
# transmission time. Each self-timed transmission carries the two most recent
# 30-minute readings (DataInterval=30min, Newest_First=False), so the last
# data row's timestamp is the transmission time floored to the hour, and
# earlier rows step back 30 minutes each. This offset assumption hasn't been
# cross-validated against an independent timestamp source yet -- worth
# spot-checking against Synoptic/HADS once there's overlapping data.

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
  if (length(vals) < 21 || any(is.na(vals[1:21]))) return(NULL)
  vals[1:21]
}

parse_dcp_lines <- function(lines) {
  blocks <- list()
  current_anchor <- NA
  pending <- list()

  flush_block <- function() {
    n <- length(pending)
    if (n == 0 || is.na(current_anchor)) return(invisible(NULL))
    anchor_hour <- floor_date(current_anchor, "hour")
    times <- anchor_hour - minutes(30 * rev(seq_len(n) - 1))
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
      if (!is.null(row)) pending[[length(pending) + 1]] <- row
    }
  }
  flush_block()
  blocks
}

# 3. Map positional fields to named, unit-corrected columns ------------------
#
# Positions 12/14/16 (soil temp) are mislabeled °F in the datalogger program
# (confirmed 2026-08-17, cross-checked against Synoptic showing the same
# mis-conversion) -- convert to °C here. Column names match the existing
# measurements table where a direct equivalent exists (see hads_retrieval.R's
# asec2_mapping: 2in/8in/20in map to the 5cm/20cm/50cm columns). Fields with
# no home in the current schema are kept as DDS_-prefixed extras.

f_to_c <- function(f) round((f - 32) * 5 / 9, 2)
in_to_cm <- function(i) round(i * 2.54, 2)

build_row_df <- function(block) {
  v <- block[2:22]
  tibble(
    Date_UTC = as.character(as.Date(block$datetime)),
    Time_UTC = format(block$datetime, "%H:%M:%S"),
    Station_Name = "ASEC2",
    Battery_Voltage = v[[1]],
    AirTemp_C = v[[2]],
    RH = v[[3]],
    Rain_cm = in_to_cm(v[[4]]),
    Barometric_Pressure = v[[5]],
    Wind_Speed = v[[6]],
    Wind_Direction = v[[7]],
    SoilTemp_C_5cm = f_to_c(v[[12]]),
    WaterCont_5cm_m3m3 = v[[13]],
    SoilTemp_C_20cm = f_to_c(v[[14]]),
    WaterCont_20cm_m3m3 = v[[15]],
    SoilTemp_C_50cm = f_to_c(v[[16]]),
    WaterCont_50cm_m3m3 = v[[17]],
    Solar_Radiation = v[[18]],
    DataFlag = "DDS_Fallback",
    DDS_WindDir_SD = v[[8]],
    DDS_SnowDist_TCDT_m = v[[9]],
    DDS_SnowDepth_m = v[[10]],
    DDS_SWE_mm = v[[11]],
    DDS_SWout = v[[19]],
    DDS_LWin = v[[20]],
    DDS_LWout = v[[21]]
  )
}

# 4. Run ----------------------------------------------------------------------

message(sprintf("Pulling ASEC2 DCP messages: %s -> %s", SINCE, UNTIL))
raw_lines <- pull_raw_lines(SINCE, UNTIL)
blocks <- parse_dcp_lines(raw_lines)

if (length(blocks) == 0) {
  stop("No decodable data rows found in the DDS pull. Check the raw output for errors.")
}

result <- map_dfr(blocks, build_row_df) %>%
  distinct(Date_UTC, Time_UTC, .keep_all = TRUE) %>%
  arrange(Date_UTC, Time_UTC)

out_file <- file.path(OUTPUT_DIR, "ASEC2_dds_fallback.csv")
write_csv(result, out_file)

message(sprintf("Decoded %d readings (%s to %s).", nrow(result),
                 min(result$Date_UTC), max(result$Date_UTC)))
message(sprintf("Soil moisture (5cm) non-NA: %d / %d",
                 sum(!is.na(result$WaterCont_5cm_m3m3)), nrow(result)))
message(sprintf("Wrote %s", out_file))
