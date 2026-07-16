#!/usr/bin/env Rscript

# quality_control.R
# Incremental QC: only processes data newer than the last run timestamp.
# Appends new flags to the audit log rather than wiping and rebuilding.

suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(here)
})

# ---------------- CONFIGURATION ---------------- #
INPUT_DIR         <- here("data_clean")
LOG_FILE          <- here("qc_audit_log.csv")
QC_TIMESTAMP_FILE <- here("config", "qc_last_timestamp.txt")
LOOKBACK_HOURS    <- 24  # Extra lookback window for persistence/ROC context

RANGE_LIMITS <- list(
  AirTemp_C           = c(-45, 45),
  RH                  = c(0, 100),
  SoilTemp_C          = c(-30, 40),
  WaterCont           = c(0, 0.7),
  Rain_cm             = c(0, 5),
  Barometric_Pressure = c(25, 1100),
  Wind_Speed          = c(0, 120),
  Solar_Radiation     = c(0, 1400),
  Battery_Voltage     = c(3.5, 15)
)

ROC_MAX_PER_HOUR <- list(
  AirTemp_C  = 15,
  RH         = 50,
  SoilTemp_C = 5,
  WaterCont  = 0.1
)

# ---------------- HELPERS ---------------- #

get_qc_start <- function() {
  if (file.exists(QC_TIMESTAMP_FILE)) {
    ts <- trimws(readLines(QC_TIMESTAMP_FILE, n = 1, warn = FALSE))
    return(ymd_hms(ts, quiet = TRUE))
  }
  message("No QC timestamp found — running full QC on first pass.")
  return(ymd_hms("2000-01-01 00:00:00"))
}

save_qc_timestamp <- function(ts) {
  writeLines(as.character(ts), QC_TIMESTAMP_FILE)
}

log_flag <- function(station_name, ts, var, val, flag) {
  entry <- data.frame(
    Station   = station_name,
    Timestamp = as.character(ts),
    Variable  = var,
    Value     = val,
    QC_Flag   = flag,
    stringsAsFactors = FALSE
  )
  # Write header only if log doesn't exist yet
  write_csv(entry, LOG_FILE, append = file.exists(LOG_FILE))
}

# ---------------- QC ENGINE ---------------- #

run_qc <- function(df, qc_start) {
  if (nrow(df) < 2) return(NULL)

  df <- df %>%
    mutate(dt = ymd_hms(paste(Date_UTC, Time_UTC))) %>%
    arrange(dt)

  new_df <- df %>% filter(dt > qc_start)
  if (nrow(new_df) == 0) return(NULL)

  station_name <- unique(df$Station_Name)[1]

  # Compute modal interval from full record for stable frequency estimate
  intervals <- as.numeric(diff(df$dt), units = "mins")
  intervals <- intervals[intervals > 0]
  if (length(intervals) == 0) return(NULL)
  mode_interval  <- as.numeric(names(sort(table(intervals), decreasing = TRUE)[1]))
  interval_hours <- mode_interval / 60

  message(sprintf("QC: %s — %d new rows", station_name, nrow(new_df)))

  vars_to_check <- setdiff(
    names(new_df),
    c("Date_UTC", "Time_UTC", "Station_ID", "Station_Name", "DataFlag", "dt")
  )

  for (var in vars_to_check) {
    new_vals <- new_df[[var]]
    new_ts   <- new_df$dt

    if (all(is.na(new_vals))) next

    # 1. MISSING DATA — gaps within new data
    gaps <- which(diff(new_ts) > minutes(mode_interval * 2))
    for (idx in gaps) {
      log_flag(station_name, new_ts[idx], var, NA, "MISSING_DATA_GAP")
    }

    # 2. RANGE CHECK — new data only
    limit_key <- names(RANGE_LIMITS)[map_lgl(names(RANGE_LIMITS), ~str_detect(var, .x))][1]
    if (!is.na(limit_key)) {
      limits <- RANGE_LIMITS[[limit_key]]
      out_of_range <- which(new_vals < limits[1] | new_vals > limits[2])
      for (idx in out_of_range) {
        log_flag(station_name, new_ts[idx], var, new_vals[idx], "RANGE_ERROR")
      }
    }

    # 3. RATE OF CHANGE — include one pre-window row so first diff is valid
    roc_key <- names(ROC_MAX_PER_HOUR)[map_lgl(names(ROC_MAX_PER_HOUR), ~str_detect(var, .x))][1]
    if (!is.na(roc_key)) {
      prior_row  <- df %>% filter(dt <= qc_start) %>% tail(1)
      roc_series <- bind_rows(prior_row, new_df) %>% arrange(dt)
      roc_vals   <- roc_series[[var]]
      allowed    <- ROC_MAX_PER_HOUR[[roc_key]] * interval_hours
      diffs      <- abs(diff(roc_vals))
      offset     <- nrow(prior_row)  # 1 if prior row exists, 0 otherwise
      spikes     <- which(diffs > allowed & (seq_along(diffs) > offset))
      for (idx in spikes) {
        new_idx <- idx - offset
        log_flag(station_name, new_ts[new_idx], var, new_vals[new_idx], "RATE_OF_CHANGE_SPIKE")
      }
    }

    # 4. PERSISTENCE — look back LOOKBACK_HOURS before window to catch ongoing runs
    persist_df   <- df %>% filter(dt >= (qc_start - hours(LOOKBACK_HOURS))) %>% arrange(dt)
    persist_vals <- persist_df[[var]]
    persist_ts   <- persist_df$dt
    max_steps    <- ceiling(12 / interval_hours)

    runs         <- rle(persist_vals)
    stuck        <- which(runs$lengths > max_steps & !is.na(runs$values))
    end_points   <- cumsum(runs$lengths)

    for (si in stuck) {
      start_idx <- if (si == 1) 1 else end_points[si - 1] + 1
      run_start <- persist_ts[start_idx]
      run_end   <- persist_ts[end_points[si]]
      # Only flag if the stuck run extends into the new data window
      if (run_end > qc_start && run_start > qc_start) {
        log_flag(station_name, run_start, var, persist_vals[start_idx], "PERSISTENCE_STUCK")
      }
    }

    # 5. STATISTICAL OUTLIER — limits from full record, flag new data only
    full_vals <- df[[var]]
    p_limits  <- quantile(full_vals, probs = c(0.001, 0.999), na.rm = TRUE)
    for (idx in which(!is.na(new_vals))) {
      if (new_vals[idx] < p_limits[1]) {
        log_flag(station_name, new_ts[idx], var, new_vals[idx], "STATISTICAL_OUTLIER_LOW")
      } else if (new_vals[idx] > p_limits[2]) {
        log_flag(station_name, new_ts[idx], var, new_vals[idx], "STATISTICAL_OUTLIER_HIGH")
      }
    }
  }
}

# ---------------- EXECUTION ---------------- #

qc_start <- get_qc_start()
run_time <- now(tzone = "UTC")
message(sprintf("Starting incremental QC (window: %s → %s)", qc_start, run_time))

# Initialise log with header if it doesn't exist
if (!file.exists(LOG_FILE)) {
  write_csv(
    data.frame(Station=character(), Timestamp=character(),
               Variable=character(), Value=numeric(), QC_Flag=character()),
    LOG_FILE
  )
}

files <- list.files(INPUT_DIR, pattern = "\\.csv$", full.names = TRUE)
all_data_list <- list()

for (f in files) {
  df <- read_csv(f, show_col_types = FALSE, col_types = cols(Station_ID = "c"))
  run_qc(df, qc_start)
  all_data_list[[basename(f)]] <- df
}

# SPATIAL CHECK — new data only
message("Performing spatial AirTemp comparison...")
all_combined <- bind_rows(all_data_list) %>%
  mutate(dt = ymd_hms(paste(Date_UTC, Time_UTC))) %>%
  filter(dt > qc_start)

if (nrow(all_combined) > 0 && "AirTemp_C" %in% names(all_combined)) {
  spatial_stats <- all_combined %>%
    group_by(dt) %>%
    summarise(median_val = median(AirTemp_C, na.rm = TRUE), .groups = "drop")

  check_spatial <- all_combined %>%
    left_join(spatial_stats, by = "dt") %>%
    mutate(diff_from_median = abs(AirTemp_C - median_val)) %>%
    filter(diff_from_median > 15)

  for (i in seq_len(nrow(check_spatial))) {
    log_flag(check_spatial$Station_Name[i], check_spatial$dt[i],
             "AirTemp_C", check_spatial$AirTemp_C[i], "SPATIAL_DISCREPANCY")
  }
}

save_qc_timestamp(run_time)
message(sprintf("QC complete. Log: %s", LOG_FILE))
