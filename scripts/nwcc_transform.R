library(jsonlite)
library(dplyr)
library(purrr)
library(readr)
library(tidyr)
library(stringr)
library(lubridate)
library(here)

# --- MASTER SCHEMA ---
MASTER_COLS <- c(
  "Date_UTC", "Time_UTC", "Station_ID", "Station_Name",
  "Battery_Voltage", "AirTemp_C", "RH", "Barometric_Pressure",
  "Epithermal_Neutron_counts", "Thermal_Neutron_counts", "Blw_Grnd_Epithermal_Neutron_counts",
  "SoilTemp_C_5cm", "SoilTemp_C_10cm", "SoilTemp_C_20cm", "SoilTemp_C_50cm", "SoilTemp_C_100cm",
  "WaterCont_5cm_m3m3", "WaterCont_10cm_m3m3", "WaterCont_20cm_m3m3", "WaterCont_50cm_m3m3", "WaterCont_100cm_m3m3",
  "Rain_cm", "Dewpoint_C", "DataFlag",
  "Wind_Speed", "Wind_Direction", "Solar_Radiation",
  "Snow_Depth_cm", "SWE_cm",
  "Discharge_cfs", "Gage_Height_ft", "WaterTemp_C"
)

# --- Define Station Metadata (single source of truth, shared with nwcc_request.R) ---
STATION_KEY_PATH <- here("config", "nwcc_stations.json")
station_key <- read_json(STATION_KEY_PATH, simplifyVector = FALSE)

# Get station name and id
station_lookup <- setNames(
  map_chr(station_key, "id"),
  names(station_key)
)

# 1. Setup Directories
raw_dir   <- here("data_raw")
clean_dir <- here("data_clean")

if (!dir.exists(clean_dir)) dir.create(clean_dir, recursive = TRUE)

# Helpers
f_to_c <- function(f) round((as.numeric(f) - 32) * (5/9), 2)
in_to_cm <- function(i) round(as.numeric(i) * 2.54, 2)

# Get list of JSON files
json_files <- list.files(raw_dir, pattern = "\\.json$", full.names = TRUE)

message(paste("Found", length(json_files), "files to process."))

# 2. Process Loop
for (file in json_files) {
  station_name <- tools::file_path_sans_ext(basename(file))
  real_station_id <- station_lookup[station_name]

  if (is.na(real_station_id)) {
    next # Skip files that aren't NWCC stations we know
  }

  message(sprintf("Processing: %s (ID: %s)", station_name, real_station_id))
  json_res <- fromJSON(file, flatten = TRUE)

  if (!is.data.frame(json_res) || !"data" %in% names(json_res)) {
    warning(paste("Skipping", station_name, "- Invalid structure"))
    next
  }

  raw_data <- json_res$data[[1]]
  if (is.null(raw_data) || length(raw_data) == 0) {
    warning(paste("Skipping", station_name, "- Data list is empty"))
    next
  }

  data_frames_list <- list()
  used_names <- c()

  for (i in 1:nrow(raw_data)) {
    elem_code <- raw_data$stationElement.elementCode[i]
    depth_val <- if ("stationElement.heightDepth" %in% names(raw_data)) raw_data$stationElement.heightDepth[i] else NA
    
    col_name <- elem_code
    if (!is.na(depth_val)) col_name <- paste0(col_name, "_", depth_val, "in")
    
    values_df <- raw_data$values[[i]]
    if (!is.null(values_df) && nrow(values_df) > 0) {
      clean_df <- values_df %>%
        select(date, value) %>%
        rename(!!col_name := value) %>%
        mutate(!!col_name := as.numeric(!!sym(col_name)))
      data_frames_list[[length(data_frames_list) + 1]] <- clean_df
    }
  }

  if (length(data_frames_list) > 0) {
    merged_df <- data_frames_list %>% reduce(full_join, by = "date") %>% arrange(date)
    
    get_col <- function(df, col_name) {
      if (col_name %in% names(df)) return(df[[col_name]])
      return(NA_real_)
    }

    final_df <- merged_df %>%
      mutate(temp_datetime = ymd_hm(date)) %>%
      mutate(
        Date_UTC = as.character(as_date(temp_datetime)),
        Time_UTC = format(temp_datetime, "%H:%M:%S"),
        Station_ID   = real_station_id,
        Station_Name = station_name,
        AirTemp_C           = f_to_c(get_col(., "TOBS")),
        RH                  = get_col(., "RHUM"),
        Battery_Voltage     = NA_real_,
        Barometric_Pressure = NA_real_,
        Epithermal_Neutron_counts          = NA_real_,
        Thermal_Neutron_counts             = NA_real_,
        Blw_Grnd_Epithermal_Neutron_counts = NA_real_,
        SoilTemp_C_5cm   = f_to_c(get_col(., "STO_-2in")),
        SoilTemp_C_10cm  = f_to_c(get_col(., "STO_-4in")),
        SoilTemp_C_20cm  = f_to_c(get_col(., "STO_-8in")),
        SoilTemp_C_50cm  = f_to_c(get_col(., "STO_-20in")),
        SoilTemp_C_100cm = f_to_c(get_col(., "STO_-40in")),
        WaterCont_5cm_m3m3   = get_col(., "SMS_-2in") / 100,
        WaterCont_10cm_m3m3  = get_col(., "SMS_-4in") / 100,
        WaterCont_20cm_m3m3  = get_col(., "SMS_-8in") / 100,
        WaterCont_50cm_m3m3  = get_col(., "SMS_-20in") / 100,
        WaterCont_100cm_m3m3 = get_col(., "SMS_-40in") / 100,
        Rain_cm    = in_to_cm(get_col(., "PRCP")),
        Dewpoint_C = f_to_c(get_col(., "DPTP")),
        DataFlag   = "Normal",
        Wind_Speed      = get_col(., "WSPDV"),
        Wind_Direction  = get_col(., "WDIRV"),
        Solar_Radiation = NA_real_,
        Snow_Depth_cm   = in_to_cm(get_col(., "SNWD")),
        SWE_cm          = in_to_cm(get_col(., "WTEQ")),
        Discharge_cfs   = NA_real_,
        Gage_Height_ft  = NA_real_,
        WaterTemp_C     = NA_real_
      ) %>%
      select(all_of(MASTER_COLS)) %>%
      arrange(Date_UTC, Time_UTC)

    save_path <- file.path(clean_dir, paste0(station_name, ".csv"))
    write_csv(final_df, save_path)
    message(paste("Success! Saved to:", save_path))
  }
}
