#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
  library(lubridate)
  library(tools)
  library(dplyr) # Using dplyr for efficient row-binding
})

`%||%` <- function(a, b) if (!is.null(a)) a else b

# ----------------------------
# Master schema (exact order)
# ----------------------------
MASTER_COLS <- c(
  "Date_UTC", "Time_UTC",
  "Station_ID", "Station_Name",
  "AirTemp_C", "RH", "Barometric_Pressure",
  "Epithermal_Neutron_counts", "Thermal_Neutron_counts", "Blw_Grnd_Epithermal_Neutron_counts",
  "SoilTemp_C_5cm", "SoilTemp_C_10cm", "SoilTemp_C_20cm", "SoilTemp_C_50cm",
  "WaterCont_5cm_m3m3", "WaterCont_10cm_m3m3", "WaterCont_20cm_m3m3", "WaterCont_50cm_m3m3",
  "Rain_cm", "Dewpoint_C", "DataFlag",
  "WaterCont_5cm_m3m3_2", "WaterCont_20cm_m3m3_2", "WaterCont_50cm_m3m3_2",
  "WaterCont_100cm_m3m3", "SoilTemp_C_100cm"
)

# ----------------------------
# Helpers (Keep your existing conversion logic)
# ----------------------------

obs_to_num <- function(arr, n_target) {
  if (is.null(arr)) return(rep(NA_real_, n_target))
  if (!is.list(arr)) arr <- as.list(arr)
  out <- vapply(arr, function(v) {
    if (is.null(v)) return(NA_real_)
    if (is.list(v) && length(v) == 0) return(NA_real_)
    if (is.list(v) && length(v) == 1) v <- v[[1]]
    suppressWarnings(as.numeric(v))
  }, numeric(1))
  if (length(out) < n_target) out <- c(out, rep(NA_real_, n_target - length(out)))
  if (length(out) > n_target) out <- out[seq_len(n_target)]
  out
}

get_obs_first <- function(obs, candidates, n_target) {
  for (k in candidates) {
    if (!is.null(obs[[k]])) return(obs_to_num(obs[[k]], n_target))
  }
  rep(NA_real_, n_target)
}

pos_to_depth_cm <- function(pos, tol_m = 0.03) {
  if (is.null(pos)) return(NA_integer_)
  if (is.list(pos)) {
    if (length(pos) == 0) return(NA_integer_)
    pos <- pos[[1]]
  } else if (length(pos) == 0) return(NA_integer_)
  p <- suppressWarnings(as.numeric(as.character(pos)))
  if (is.na(p)) return(NA_integer_)
  p <- abs(p)
  targets_m <- c(0.05, 0.10, 0.20, 0.50, 1.00)
  diffs <- abs(targets_m - p)
  i <- which.min(diffs)
  if (length(i) == 0 || is.na(diffs[i]) || diffs[i] > tol_m) return(NA_integer_)
  as.integer(targets_m[i] * 100)
}

convert_sm_to_m3m3 <- function(x, units) {
  if (all(is.na(x))) return(x)
  u <- tolower(as.character(units %||% ""))
  if (grepl("percent|%", u)) return(x / 100)
  med <- suppressWarnings(median(x, na.rm = TRUE))
  if (is.finite(med) && med > 1 && med <= 100) return(x / 100)
  x
}

convert_precip_to_cm <- function(x, units) {
  if (all(is.na(x))) return(x)
  u <- tolower(as.character(units %||% ""))
  if (u %in% c("mm", "millimeter", "millimeters")) return(x / 10)
  if (u %in% c("cm", "centimeter", "centimeters")) return(x)
  if (u %in% c("in", "inch", "inches")) return(x * 2.54)
  x / 10
}

empty_master <- function(n) {
  out <- list()
  out$Date_UTC <- as.Date(rep(NA_character_, n))
  out$Time_UTC <- rep(NA_character_, n)
  out$Station_ID <- rep(NA_character_, n)
  out$Station_Name <- rep(NA_character_, n)
  num_cols <- setdiff(MASTER_COLS, c("Date_UTC", "Time_UTC", "Station_ID", "Station_Name", "DataFlag"))
  for (nm in num_cols) out[[nm]] <- rep(NA_real_, n)
  out$DataFlag <- rep(NA_character_, n)
  out <- as.data.frame(out, check.names = FALSE)
  out[, MASTER_COLS, drop = FALSE]
}

build_set_to_depth_cm <- function(st, var_group) {
  sv <- st$SENSOR_VARIABLES %||% list()
  sets <- sv[[var_group]] %||% list()
  if (length(sets) == 0) return(list())
  out <- list()
  for (set_name in names(sets)) {
    pos <- sets[[set_name]]$position
    d <- pos_to_depth_cm(pos)
    if (is.na(d) && var_group == "soil_temp") d <- 20
    out[[set_name]] <- d
  }
  out
}

assign_depth_columns <- function(out, obs, set_depth_map, prefix_master, units_sm, n_target) {
  depth_to_sets <- list()
  for (set_name in names(set_depth_map)) {
    d <- set_depth_map[[set_name]]
    if (is.na(d)) next
    key <- as.character(d)
    depth_to_sets[[key]] <- c(depth_to_sets[[key]] %||% character(), set_name)
  }
  for (d in c(5, 10, 20, 50, 100)) {
    sets_here <- depth_to_sets[[as.character(d)]] %||% character()
    if (length(sets_here) == 0) next
    s1 <- sets_here[[1]]
    if (!is.null(obs[[s1]])) {
      v1 <- obs_to_num(obs[[s1]], n_target)
      if (prefix_master == "WaterCont") v1 <- convert_sm_to_m3m3(v1, units_sm)
      col1 <- if (prefix_master == "WaterCont") paste0("WaterCont_", d, "cm_m3m3") else paste0("SoilTemp_C_", d, "cm")
      out[[col1]] <- v1
    }
    if (prefix_master == "WaterCont" && d %in% c(5, 20, 50) && length(sets_here) >= 2) {
      s2 <- sets_here[[2]]
      if (!is.null(obs[[s2]])) {
        v2 <- obs_to_num(obs[[s2]], n_target)
        v2 <- convert_sm_to_m3m3(v2, units_sm)
        out[[paste0("WaterCont_", d, "cm_m3m3_2")]] <- v2
      }
    }
  }
  out
}

# ----------------------------
# Process a SINGLE chunk (Internal helper)
# ----------------------------
process_chunk <- function(st, code) {
  obs <- st$OBSERVATIONS %||% list()
  if (is.null(obs$date_time) || length(obs$date_time) == 0) return(NULL)

  dt <- suppressWarnings(ymd_hms(unlist(obs$date_time), tz = "UTC"))
  n  <- length(dt)

  station_id   <- st$STID %||% code
  station_name <- st$NAME %||% station_id

  out <- empty_master(n)
  out$Date_UTC <- as.Date(dt)
  out$Time_UTC <- format(as.POSIXct(dt, tz = "UTC"), "%H:%M:%S")
  out$Station_ID <- rep(as.character(station_id), n)
  out$Station_Name <- rep(as.character(station_name), n)

  units <- st$UNITS %||% list()
  sm_units <- units$soil_moisture %||% units$soil_moisture_set_1
  pr_units <- units$precip_intervals %||% units$precip_interval

  out$AirTemp_C <- get_obs_first(obs, c("air_temp_set_1", "air_temp"), n)
  out$RH        <- get_obs_first(obs, c("relative_humidity_set_1", "relative_humidity"), n)
  out$Barometric_Pressure <- get_obs_first(obs, c("station_pressure_set_1", "pressure"), n)
  out$Dewpoint_C <- get_obs_first(obs, c("dew_point_temperature_set_1", "dew_point_temperature"), n)

  rain_raw <- get_obs_first(obs, c("precip_intervals_set_1d", "precip_intervals"), n)
  out$Rain_cm <- convert_precip_to_cm(rain_raw, pr_units)

  sm_set_depth <- build_set_to_depth_cm(st, "soil_moisture")
  st_set_depth <- build_set_to_depth_cm(st, "soil_temp")

  out <- assign_depth_columns(out, obs, st_set_depth, "SoilTemp_C", NULL, n)
  out <- assign_depth_columns(out, obs, sm_set_depth, "WaterCont", sm_units, n)

  out
}

# ----------------------------
# Main Logic: Process Whole File (Array of Chunks)
# ----------------------------
process_file_to_master <- function(json_file, out_dir) {
  # Load the file as a list of chunks
  chunks <- fromJSON(json_file, simplifyVector = FALSE)

  # Handle empty files
  if (length(chunks) == 0) return(NULL)

  # Filter out empty list elements that sometimes occur in these batches
  chunks <- chunks[sapply(chunks, function(x) length(x) > 0)]

  code <- file_path_sans_ext(basename(json_file))
  cat("→", code, "(Processing", length(chunks), "chunks)\n")

  # Process each chunk into a list of data frames
  df_list <- lapply(chunks, function(st) process_chunk(st, code))

  # Remove NULLs (chunks with no data)
  df_list <- df_list[!sapply(df_list, is.null)]

  if (length(df_list) == 0) {
    cat("   No valid data found in any chunk.\n")
    return(NULL)
  }

  # Stack all chunks into one master data frame and deduplicate
  final_df <- bind_rows(df_list) %>%
    distinct(Date_UTC, Time_UTC, .keep_all = TRUE) %>%
    arrange(Date_UTC, Time_UTC)

  out_csv <- file.path(out_dir, paste0(code, "_master.csv"))
  write.csv(final_df, out_csv, row.names = FALSE, na = "")
  cat("   wrote", out_csv, "(", nrow(final_df), "rows )\n")
}

main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) < 2) {
    cat("Usage: synoptic_transform.R <input_dir> <out_dir>\n", file = stderr())
    quit(status = 1)
  }

  in_dir  <- args[[1]]
  out_dir <- args[[2]]
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  files <- list.files(in_dir, pattern = "\\.json$", full.names = TRUE)
  ignore <- c("last_timestamps.json")
  files <- files[!basename(files) %in% ignore]

  for (f in files) process_file_to_master(f, out_dir)
}

main()
