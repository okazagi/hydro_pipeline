#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(tidyverse)
  library(jsonlite)
  library(here)
  library(lubridate)
  library(purrr)
  library(fs)
})

# --- MASTER SCHEMA ---
MASTER_COLS <- c(
  "Date_UTC", "Time_UTC", "Station_ID", "Station_Name",
  "Battery_Voltage", "AirTemp_C", "RH", "Barometric_Pressure",
  "Epithermal_Neutron_counts", "Thermal_Neutron_counts", "Blw_Grnd_Epithermal_Neutron_counts",
  "SoilTemp_C_5cm", "SoilTemp_C_10cm", "SoilTemp_C_20cm", "SoilTemp_C_50cm", "SoilTemp_C_100cm",
  "WaterCont_5cm_m3m3", "WaterCont_10cm_m3m3", "WaterCont_20cm_m3m3", "WaterCont_50cm_m3m3", "WaterCont_100cm_m3m3",
  "WaterCont_5cm_m3m3_B", "WaterCont_20cm_m3m3_B", "WaterCont_50cm_m3m3_B",
  "SoilTemp_C_20cm_B",
  "Rain_cm", "Dewpoint_C", "DataFlag",
  "Wind_Speed", "Wind_Direction", "Solar_Radiation",
  "Snow_Depth_cm", "SWE_cm",
  "Discharge_cfs", "Gage_Height_ft", "WaterTemp_C"
)

# ---------------- Setup ---------------- #
SENS_PATH <- here("config", "sensor_key.json")
STID_PATH <- here("config", "station_key.json")
sensor_key <- read_json(SENS_PATH, simplifyVector = TRUE)
station_key <- read_json(STID_PATH, simplifyVector = FALSE)

RAW_DIR   <- here("data_raw", "licor", "daily_json")
CLEAN_DIR <- here("data_clean")
ARCHIVE_DIR <- here("data_raw", "licor", "daily_json", "archive")
if (!dir.exists(CLEAN_DIR))   dir.create(CLEAN_DIR,   recursive = TRUE)
if (!dir.exists(ARCHIVE_DIR)) dir.create(ARCHIVE_DIR, recursive = TRUE)

# Helpers
f_to_c   <- function(f) round((as.numeric(f) - 32) * (5/9), 2)
in_to_cm <- function(i) round(as.numeric(i) * 2.54, 2)

process_licor_json <- function(file_path) {
  message(sprintf("Processing: %s", basename(file_path)))

  stid <- str_split(basename(file_path), "_")[[1]][1]

  raw_data <- fromJSON(file_path, simplifyVector = TRUE)

  if (!is.data.frame(raw_data$data) || nrow(raw_data$data) == 0) {
    if (is.list(raw_data$data) && length(raw_data$data) > 0) {
      all_records <- as_tibble(raw_data$data)
    } else {
      return(NULL)
    }
  } else {
    all_records <- as_tibble(raw_data$data)
  }

  if (nrow(all_records) == 0) return(NULL)

  # Force timestamp to character to prevent jsonlite auto-converting ISO strings
  # to POSIXct, which would produce epoch day numbers instead of ISO date strings.
  all_records <- all_records %>%
    mutate(timestamp = as.character(timestamp))

  # Map sensor serials to variable names
  all_records <- all_records %>%
    mutate(var_name = map_chr(sensor_sn, function(sn) {
      if (!is.null(sensor_key[[sn]])) as.character(sensor_key[[sn]])
      else                            as.character(sn)
    }))

  # Pivot wide; filter sentinel/physically impossible values before summarising
  wide_df <- all_records %>%
    select(timestamp, var_name, value) %>%
    mutate(value = as.numeric(value)) %>%
    mutate(value = if_else(abs(value) > 9000, NA_real_, value)) %>%
    group_by(timestamp, var_name) %>%
    summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
    pivot_wider(names_from = var_name, values_from = value)

  # Resolve numeric Station_ID from nested station_key
  station_meta <- station_key[[stid]]
  station_id_val <- if (!is.null(station_meta) && !is.null(station_meta$id) && !is.na(station_meta$id))
                      as.character(station_meta$id)
                    else
                      NA_character_

  if (is.na(station_id_val)) {
    stop(sprintf("station_key.json is missing numeric id for %s — refusing to write rows.", stid))
  }

  clean_df <- wide_df %>%
    mutate(
      dt_obj     = ymd_hms(timestamp),
      Date_UTC   = as.character(as.Date(dt_obj)),
      Time_UTC   = format(dt_obj, "%H:%M:%S"),
      Station_ID = station_id_val,
      Station_Name = stid,
      DataFlag   = "Normal"
    )

  missing_cols <- setdiff(MASTER_COLS, names(clean_df))
  clean_df[missing_cols] <- NA_real_

  clean_df <- clean_df %>%
    mutate(
      across(contains("Temp") | contains("Dewpoint"), f_to_c),
      across(contains("Rain") | contains("Snow"),     in_to_cm)
    )

  sensor_cols <- setdiff(MASTER_COLS,
    c("Date_UTC", "Time_UTC", "Station_ID", "Station_Name", "Battery_Voltage", "DataFlag"))

  final_df <- clean_df %>%
    select(all_of(MASTER_COLS)) %>%
    filter(!if_all(all_of(sensor_cols), is.na)) %>%
    arrange(Date_UTC, Time_UTC)

  return(final_df)
}

# Process only new (unarchived) LI-COR JSON files
json_files <- list.files(RAW_DIR, pattern = "\\.json$", full.names = TRUE, recursive = FALSE)

if (length(json_files) > 0) {
  all_data <- map_df(json_files, process_licor_json)

  if (!is.null(all_data) && nrow(all_data) > 0) {
    char_cols <- c("Date_UTC", "Time_UTC", "Station_ID", "Station_Name", "DataFlag")

    stations_in_data <- unique(all_data$Station_Name)
    for (s in stations_in_data) {
      station_df <- all_data %>%
        filter(Station_Name == s) %>%
        arrange(Date_UTC, Time_UTC)

      out_path <- file.path(CLEAN_DIR, paste0(s, "_licor_clean.csv"))

      # Merge with existing clean file and deduplicate on (Date_UTC, Time_UTC)
      if (file.exists(out_path)) {
        existing_df <- read_csv(out_path, show_col_types = FALSE,
          col_types = do.call(cols, c(
            list(.default = col_double()),
            setNames(rep(list(col_character()), length(char_cols)), char_cols)
          ))
        )
        station_df <- bind_rows(existing_df, station_df) %>%
          arrange(Date_UTC, Time_UTC) %>%
          distinct(Date_UTC, Time_UTC, .keep_all = TRUE)
      }

      write_csv(station_df, out_path)
      message(sprintf("Saved/appended cleaned LI-COR data for %s to %s", s, out_path))
    }
  }

  # Archive processed JSON files
  walk(json_files, ~{
    file_move(.x, file.path(ARCHIVE_DIR, basename(.x)))
  })
  message(sprintf("Archived %d processed JSON files.", length(json_files)))
}
