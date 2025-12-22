library(tidyverse)
library(lubridate)
library(stringr) # for str_remove

# 1. Load and Standardize Config
# We create a 'join_id' that strips "USGS-" and whitespace so matches are guaranteed
site_config <- read_csv("config/usgs_sites.csv", col_types = cols(.default = "c")) %>%
  select(site_id, station_name) %>%
  mutate(
    # Remove "USGS-" (case insensitive) and whitespace to create a clean join key
    join_id = str_remove(site_id, "(?i)USGS-") %>% trimws()
  )

process_usgs_data <- function(input_file) {

  message(paste("Processing file:", input_file))

  # --- Helper Function ---
  get_col <- function(df, candidates) {
    match <- intersect(candidates, names(df))[1]
    if (is.na(match)) return(NA) else return(df[[match]])
  }

  # 2. Read Raw Data & Standardize ID
  raw_df <- read_csv(input_file, col_types = cols(.default = "?", site_no = "c")) %>%
    mutate(
      # Create the same clean join key here
      join_id = str_remove(site_no, "(?i)USGS-") %>% trimws()
    )

  # 3. Join on the clean 'join_id'
  # We assume 1 raw file = 1 station, but this works even if there are multiple
  joined_df <- raw_df %>%
    left_join(site_config, by = "join_id")

  # --- DIAGNOSTIC CHECK ---
  # If the join failed (Station_Name is NA), print a warning to the console
  if (any(is.na(joined_df$station_name))) {
    warning(paste("MATCH FAILED for:", unique(raw_df$site_no)[1]))
    message("Top 3 Config IDs (Cleaned): ", paste(head(site_config$join_id, 3), collapse=", "))
    message("File ID (Cleaned): ", unique(raw_df$join_id)[1])
  }

  # 4. Transform to Master Schema
  clean_df <- joined_df %>%
    mutate(
      Date_UTC = as.Date(dateTime),
      Time_UTC = format(as.POSIXct(dateTime), format = "%H:%M:%S"),

      # Use the original site_no from the file for the ID column
      Station_ID          = site_no,
      Station_Name        = station_name, # From the config join

      AirTemp_C           = get_col(., c("X_00020_Inst")),
      RH                  = get_col(., c("X_00052_Inst")),
      Barometric_Pressure = get_col(., c("X_75969_Inst")),

      Epithermal_Neutron_counts = get_col(., c("X_.Epi.thermal.neutron.counts._72431_Inst")),
      Thermal_Neutron_counts    = get_col(., c("X_.Thermal.neutron.counts._72431_Inst")),
      Blw_Grnd_Epithermal_Neutron_counts = get_col(., c("X_.Blw.ground.surface..epi.therma._72431_Inst")),

      SoilTemp_C_5cm  = get_col(., c("X_.0.05.m.depth.CS655._72253_Inst",
                                     "X_.5.cm.depth.CS655._72253_Inst",
                                     "X_72253_Inst")),
      SoilTemp_C_10cm = get_col(., c("X_.0.10.m.depth.CS655._72253_Inst")),
      SoilTemp_C_20cm = get_col(., c("X_.0.20.m.depth.CS655._72253_Inst",
                                     "X_.20.cm.depth.CS655._72253_Inst")),
      SoilTemp_C_50cm = get_col(., c("X_.0.50.m.depth.CS655._72253_Inst")),

      WaterCont_5cm_m3m3  = get_col(., c("X_.0.05.m.depth.CS655._74207_Inst",
                                         "X_.5.cm.depth.CS655._74207_Inst",
                                         "X_74207_Inst")),
      WaterCont_10cm_m3m3 = get_col(., c("X_.0.10.m.depth.CS655._74207_Inst")),
      WaterCont_20cm_m3m3 = get_col(., c("X_.0.20.m.depth.CS655._74207_Inst",
                                         "X_.20.cm.depth.CS655._74207_Inst")),
      WaterCont_50cm_m3m3 = get_col(., c("X_.0.50.m.depth.CS655._74207_Inst")),

      Rain_cm               = NA_real_,
      Dewpoint_C            = NA_real_,
      DataFlag              = NA_character_,
      WaterCont_5cm_m3m3_2  = NA_real_,
      WaterCont_20cm_m3m3_2 = NA_real_,
      WaterCont_50cm_m3m3_2 = NA_real_,
      WaterCont_100cm_m3m3 = NA_real_,
      SoilTemp_C_100cm = NA_real_
    ) %>%
    select(
      Station_ID, Station_Name, Date_UTC, Time_UTC,
      AirTemp_C, Rain_cm,
      SoilTemp_C_5cm, SoilTemp_C_10cm, SoilTemp_C_20cm, SoilTemp_C_50cm,
      RH, Dewpoint_C, Barometric_Pressure,
      WaterCont_5cm_m3m3, WaterCont_10cm_m3m3, WaterCont_20cm_m3m3, WaterCont_50cm_m3m3,
      DataFlag,
      WaterCont_5cm_m3m3_2, WaterCont_20cm_m3m3_2, WaterCont_50cm_m3m3_2,
      Epithermal_Neutron_counts, Thermal_Neutron_counts, Blw_Grnd_Epithermal_Neutron_counts
    )

  # 5. Save Processed Data
  output_filename <- gsub("_raw.csv", "_clean.csv", basename(input_file))
  if(!dir.exists("data_clean")) dir.create("data_clean")
  output_path <- file.path("data_clean", output_filename)

  write_csv(clean_df, output_path)
  message(paste("Processed data saved to:", output_path))
}
