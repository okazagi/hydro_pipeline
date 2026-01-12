#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
  library(lubridate)
  library(tools)
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
# Helpers
# ----------------------------

# Convert Synoptic observation arrays (may include {} -> empty list) to numeric vector
obs_to_num <- function(arr, n_target) {
  if (is.null(arr)) return(rep(NA_real_, n_target))
  if (!is.list(arr)) arr <- as.list(arr)

  out <- vapply(arr, function(v) {
    if (is.null(v)) return(NA_real_)
    if (is.list(v) && length(v) == 0) return(NA_real_)  # {}
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

# ---- FIXED: robust scalar extraction + NA-safe logic ----
pos_to_depth_cm <- function(pos, tol_m = 0.03) {
  # Handle NULL / empty list / vectors
  if (is.null(pos)) return(NA_integer_)

  # Some JSON parsers can give position as list or vector
  if (is.list(pos)) {
    if (length(pos) == 0) return(NA_integer_)
    pos <- pos[[1]]
  } else if (length(pos) == 0) {
    return(NA_integer_)
  } else if (length(pos) > 1) {
    pos <- pos[1]
  }

  pos_chr <- suppressWarnings(as.character(pos))
  if (length(pos_chr) == 0) return(NA_integer_)
  if (is.na(pos_chr)) return(NA_integer_)
  if (!nzchar(pos_chr)) return(NA_integer_)

  p <- suppressWarnings(as.numeric(pos_chr))
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

  # explicit percent
  if (grepl("percent|%", u)) return(x / 100)

  # if units missing, auto-detect percent-ish values
  if (u == "" || is.na(u)) {
    med <- suppressWarnings(median(x, na.rm = TRUE))
    if (is.finite(med) && med > 1 && med <= 100) return(x / 100)
  }

  # otherwise assume already fraction
  x
}

convert_precip_to_cm <- function(x, units) {
  if (all(is.na(x))) return(x)
  u <- tolower(as.character(units %||% ""))

  if (u %in% c("mm", "millimeter", "millimeters")) return(x / 10)
  if (u %in% c("cm", "centimeter", "centimeters")) return(x)
  if (u %in% c("m", "meter", "meters")) return(x * 100)
  if (u %in% c("in", "inch", "inches")) return(x * 2.54)

  # fallback: assume mm -> cm
  x / 10
}

empty_master <- function(n) {
  # Create correctly typed columns
  out <- list()

  out$Date_UTC <- as.Date(rep(NA_character_, n))
  out$Time_UTC <- rep(NA_character_, n)
  out$Station_ID <- rep(NA_character_, n)
  out$Station_Name <- rep(NA_character_, n)

  # numeric fields
  num_cols <- setdiff(MASTER_COLS, c("Date_UTC", "Time_UTC", "Station_ID", "Station_Name", "DataFlag"))
  for (nm in num_cols) out[[nm]] <- rep(NA_real_, n)

  out$DataFlag <- rep(NA_character_, n)

  out <- as.data.frame(out, check.names = FALSE)
  out <- out[, MASTER_COLS, drop = FALSE]
  out
}

# ----------------------------
# Read station JSON file in common shapes
# ----------------------------
read_station_from_file <- function(json_file) {
  x <- jsonlite::fromJSON(json_file, simplifyVector = FALSE)

  if (is.list(x) && !is.null(x$STATION) && is.list(x$STATION) && length(x$STATION) >= 1) {
    return(x$STATION[[1]])
  }
  if (is.list(x) && length(x) >= 1 && is.list(x[[1]]) && !is.null(x[[1]]$STID)) {
    return(x[[1]])
  }
  if (is.list(x) && !is.null(x$STID)) {
    return(x)
  }

  stop("Unrecognized JSON structure in: ", json_file)
}

# ----------------------------
# Depth-based mapping using SENSOR_VARIABLES positions
# ----------------------------
build_set_to_depth_cm <- function(st, var_group) {
  sv <- st$SENSOR_VARIABLES %||% list()
  sets <- sv[[var_group]] %||% list()
  if (length(sets) == 0) return(list())

  out <- list()
  for (set_name in names(sets)) {
    pos <- sets[[set_name]]$position
    out[[set_name]] <- pos_to_depth_cm(pos)
  }
  out
}

assign_depth_columns <- function(out, obs, set_depth_map, prefix_master, units_sm, n_target) {
  # prefix_master: "SoilTemp_C" or "WaterCont"
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

    # first sensor
    s1 <- sets_here[[1]]
    if (!is.null(obs[[s1]])) {
      v1 <- obs_to_num(obs[[s1]], n_target)
      if (prefix_master == "WaterCont") v1 <- convert_sm_to_m3m3(v1, units_sm)

      col1 <- if (prefix_master == "WaterCont") {
        paste0("WaterCont_", d, "cm_m3m3")
      } else {
        paste0("SoilTemp_C_", d, "cm")
      }
      out[[col1]] <- v1
    }

    # second sensor for duplicates (WaterCont only at 5/20/50)
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
# Core transform: station -> master df
# ----------------------------
process_station_to_master <- function(code, st, out_csv) {
  cat("→", code, "\n")

  obs <- st$OBSERVATIONS %||% list()
  if (is.null(obs$date_time)) {
    cat("   no date_time, skipping\n")
    return(invisible(NULL))
  }

  dt <- suppressWarnings(ymd_hms(unlist(obs$date_time), tz = "UTC"))
  n  <- length(dt)

  station_id   <- st$STID %||% code
  station_name <- st$NAME %||% station_id

  out <- empty_master(n)

  out$Date_UTC <- as.Date(dt)
  out$Time_UTC <- format(as.POSIXct(dt, tz = "UTC"), "%H:%M:%S")
  out$Station_ID <- rep(as.character(station_id), n)
  out$Station_Name <- rep(as.character(station_name), n)

  # Units
  units <- st$UNITS %||% list()
  sm_units <- units$soil_moisture %||% units$soil_moisture_set_1 %||% units$soil_moisture_set_1d
  pr_units <- units$precip_intervals %||% units$precip_interval %||% units$precip %||% units$precip_accumulated

  # Core met vars
  out$AirTemp_C <- get_obs_first(obs, c("air_temp_set_1", "air_temp_set_1d", "air_temp"), n)
  out$RH        <- get_obs_first(obs, c("relative_humidity_set_1", "relative_humidity_set_1d", "relative_humidity"), n)

  out$Barometric_Pressure <- get_obs_first(
    obs,
    c("station_pressure_set_1", "pressure_set_1", "sea_level_pressure_set_1", "altimeter_set_1", "pressure"),
    n
  )

  out$Dewpoint_C <- get_obs_first(
    obs,
    c("dew_point_temperature_set_1", "dew_point_temperature_set_1d", "dew_point_temperature"),
    n
  )

  # Rain: precip_intervals (best)
  rain_raw <- get_obs_first(obs, c("precip_intervals_set_1d", "precip_intervals_set_1", "precip_accumulated_set_1d"), n)
  out$Rain_cm <- convert_precip_to_cm(rain_raw, pr_units)

  # Depth mapping
  sm_set_depth <- build_set_to_depth_cm(st, "soil_moisture")
  st_set_depth <- build_set_to_depth_cm(st, "soil_temp")

  out <- assign_depth_columns(out, obs, st_set_depth, prefix_master = "SoilTemp_C", units_sm = NULL, n_target = n)
  out <- assign_depth_columns(out, obs, sm_set_depth, prefix_master = "WaterCont", units_sm = sm_units, n_target = n)

  # Everything else stays NA (DataFlag, neutron counts, etc.)
  write.csv(out, out_csv, row.names = FALSE, na = "")
  cat("   wrote", out_csv, "\n")
  invisible(out_csv)
}

# ----------------------------
# Main: directory walker with robust ignore
# ----------------------------
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
  files <- files[grepl("^[A-Z0-9]{4,6}\\.json$", basename(files))]

  if (length(files) == 0) stop("No station .json files found in: ", in_dir)

  for (f in files) {
    if (basename(f) %in% ignore) next

    st <- read_station_from_file(f)
    code <- st$STID %||% file_path_sans_ext(basename(f))
    out_csv <- file.path(out_dir, paste0(code, "_master.csv"))
    process_station_to_master(code, st, out_csv)
  }
}

main()
