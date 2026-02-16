#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(tidyverse)
  library(jsonlite)
  library(here)
  library(lubridate)
})

# ---------------- Master Schema Definition ---------------- #
MASTER_COLS <- c(
  "Date_UTC", "Time_UTC", "Station_ID", "Station_Name",
  "AirTemp_C", "RH", "Barometric_Pressure",
  "Epithermal_Neutron_counts", "Thermal_Neutron_counts", "Blw_Grnd_Epithermal_Neutron_counts",
  "SoilTemp_C_5cm", "SoilTemp_C_10cm", "SoilTemp_C_20cm", "SoilTemp_C_50cm",
  "WaterCont_5cm_m3m3", "WaterCont_10cm_m3m3", "WaterCont_20cm_m3m3", "WaterCont_50cm_m3m3",
  "Rain_cm", "Dewpoint_C", "DataFlag",
  "WaterCont_5cm_m3m3_2", "WaterCont_20cm_m3m3_2", "WaterCont_50cm_m3m3_2",
  "WaterCont_100cm_m3m3", "SoilTemp_C_100cm"
)

# ---------------- Helpers ---------------- #
f_to_c <- function(f) round((as.numeric(f) - 32) * (5/9), 2)
in_to_cm <- function(i) round(as.numeric(i) * 2.54, 2)

# ---------------- Setup ---------------- #
SENS_PATH <- here("config", "sensor_key.json")
STID_PATH <- here("config", "station_key.json")
sensor_key <- read_json(SENS_PATH, simplifyVector = TRUE)
station_key <- read_json(STID_PATH, simplifyVector = TRUE)

# File configuration
input_file <- here("old_data", "RFGLR_licor.csv")
STATION_CODE <- "RFGLR" # Adjust per file

# ---------------- Execution ---------------- #

# 1. Read Data
raw_df <- read_csv(input_file, na = c("", "NA", "ERROR"), show_col_types = FALSE)

# 2. Rename columns based on Serial Numbers
colnames(raw_df) <- map_chr(names(raw_df), function(cn) {
  sn <- str_extract(cn, "\\d{8}-\\d")
  if (!is.na(sn) && sn %in% names(sensor_key)) return(sensor_key[[sn]])
  return(cn)
})

# 3. Transform and Align to Master Schema
clean_df <- raw_df %>%
  rename(Raw_TS = 1) %>%
  mutate(
    # Date Handling
    dt_obj = parse_date_time(Raw_TS, orders = "mdy HM", tz = "UTC"),
    Date_UTC = as.character(as.Date(dt_obj)),
    Time_UTC = format(dt_obj, "%H:%M:%S"),

    # Metadata
    Station_ID = STATION_CODE,
    Station_Name = "Glassier Ranch",
    DataFlag = "P", # 'P' for Provisional/Processed

    # Conversions
    across(contains("Temp") | contains("Dewpoint"), f_to_c),
    across(contains("Rain") | contains("Snow"), in_to_cm)
  )

# 4. Final Alignment: Ensure all Master Columns exist
# Find which columns are missing from our clean_df
missing_cols <- setdiff(MASTER_COLS, names(clean_df))

# Add those missing columns as NA (properly typed)
clean_df[missing_cols] <- NA

# Select and Order exactly according to Master Schema
final_df <- clean_df %>%
  select(all_of(MASTER_COLS)) %>%
  filter(!is.na(Date_UTC)) %>%
  arrange(Date_UTC, Time_UTC)

# ---------------- Output ---------------- #
output_path <- here("data_processed", paste0(STATION_CODE, "_historical_standardized.csv"))
write_csv(final_df, output_path, na = "")

message(sprintf("Aligned %s to Master Schema. Saved to: %s", STATION_CODE, basename(output_path)))
print(head(final_df))
