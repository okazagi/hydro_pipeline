library(tidyverse)
library(lubridate)
library(stringr)
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

# 1. Load and Standardize Config
site_config <- read_csv(here("config", "usgs_sites.csv"), col_types = cols(.default = "c")) %>%
  select(site_id, station_name) %>%
  mutate(
    join_id = str_remove(site_id, "(?i)USGS-") %>% trimws()
  )

process_usgs_data <- function(input_file) {
  message(paste("Processing file:", input_file))

  get_col <- function(df, candidates) {
    match <- intersect(candidates, names(df))[1]
    if (is.na(match)) return(NA_real_)
    vals <- as.numeric(df[[match]])
    # Replace USGS sentinel values (-9999, -9999.99, etc.) with NA
    vals[!is.na(vals) & abs(vals) > 9000] <- NA_real_
    return(vals)
  }

  raw_df <- read_csv(input_file, col_types = cols(.default = "?", site_no = "c")) %>%
    mutate(join_id = str_remove(site_no, "(?i)USGS-") %>% trimws())

  joined_df <- raw_df %>% left_join(site_config, by = "join_id")

  clean_df <- joined_df %>%
    mutate(
      Date_UTC = as.character(as.Date(dateTime)),
      Time_UTC = format(as.POSIXct(dateTime), format = "%H:%M:%S"),
      Station_ID          = site_no,
      Station_Name        = station_name,
      Battery_Voltage     = NA_real_,
      AirTemp_C           = get_col(., c("X_00020_Inst")),
      RH                  = get_col(., c("X_00052_Inst")),
      Barometric_Pressure = get_col(., c("X_75969_Inst")),
      Epithermal_Neutron_counts = get_col(., c("X_.Epi.thermal.neutron.counts._72431_Inst")),
      Thermal_Neutron_counts    = get_col(., c("X_.Thermal.neutron.counts._72431_Inst")),
      Blw_Grnd_Epithermal_Neutron_counts = get_col(., c("X_.Blw.ground.surface..epi.therma._72431_Inst")),
      SoilTemp_C_5cm  = get_col(., c("X_.0.05.m.depth.CS655._72253_Inst", "X_.5.cm.depth.CS655._72253_Inst", "X_72253_Inst")),
      SoilTemp_C_10cm = get_col(., c("X_.0.10.m.depth.CS655._72253_Inst")),
      SoilTemp_C_20cm = get_col(., c("X_.0.20.m.depth.CS655._72253_Inst", "X_.20.cm.depth.CS655._72253_Inst")),
      SoilTemp_C_50cm = get_col(., c("X_.0.50.m.depth.CS655._72253_Inst")),
      SoilTemp_C_100cm = NA_real_,
      WaterCont_5cm_m3m3  = get_col(., c("X_.0.05.m.depth.CS655._74207_Inst", "X_.5.cm.depth.CS655._74207_Inst", "X_74207_Inst")) / 100,
      WaterCont_10cm_m3m3 = get_col(., c("X_.0.10.m.depth.CS655._74207_Inst")) / 100,
      WaterCont_20cm_m3m3 = get_col(., c("X_.0.20.m.depth.CS655._74207_Inst", "X_.20.cm.depth.CS655._74207_Inst")) / 100,
      WaterCont_50cm_m3m3 = get_col(., c("X_.0.50.m.depth.CS655._74207_Inst")) / 100,
      WaterCont_100cm_m3m3 = NA_real_,
      Rain_cm    = NA_real_,
      Dewpoint_C = NA_real_,
      DataFlag   = "Normal",
      Wind_Speed      = NA_real_,
      Wind_Direction  = NA_real_,
      Solar_Radiation = NA_real_,
      Snow_Depth_cm   = NA_real_,
      SWE_cm          = NA_real_,
      Discharge_cfs   = get_col(., c("X_00060_Inst", "Flow_Inst")),
      Gage_Height_ft  = get_col(., c("X_00065_Inst", "GH_Inst")),
      WaterTemp_C     = get_col(., c("X_00010_Inst", "Wtemp_Inst"))
    ) %>%
    select(all_of(MASTER_COLS)) %>%
    arrange(Date_UTC, Time_UTC)

  # 5. Save Processed Data
  # Use the Station_Name for the filename if available, otherwise fall back to original site ID
  final_station_name <- if (is.na(unique(clean_df$Station_Name)[1])) {
    gsub("_raw.csv", "_clean", basename(input_file))
  } else {
    unique(clean_df$Station_Name)[1]
  }
  
  if(!dir.exists(here("data_clean"))) dir.create(here("data_clean"))
  output_path <- here("data_clean", paste0(final_station_name, ".csv"))

  write_csv(clean_df, output_path)
  message(paste("Processed data saved to:", output_path))
}

# --- Main Execution ---
if (!interactive()) {
  raw_files <- list.files(here("data_raw"), pattern = "^USGS_.*_raw\\.csv$", full.names = TRUE)
  message(sprintf("Found %d USGS raw files to process...", length(raw_files)))
  
  for (f in raw_files) {
    tryCatch({
      process_usgs_data(f)
    }, error = function(e) {
      message(sprintf("Error processing file %s: %s", f, e$message))
    })
  }
  message("USGS transform complete.")
}
