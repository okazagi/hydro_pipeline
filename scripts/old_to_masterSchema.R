#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(lubridate)
  library(dplyr)
  library(readr)
})

# ---------------------------------------------------------
# HARDCODE YOUR PATHS HERE
# ---------------------------------------------------------
INPUT_FILE   <- "old_data/RFGLR.csv"
OUTPUT_FILE  <- "old_data/RFGLR_migrated.csv"
STATION_ID   <- "RFGLR"
STATION_NAME <- "Glassier Ranch"
# ---------------------------------------------------------

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

`%||%` <- function(a, b) if (!is.null(a)) a else b

# Helper to ensure soil moisture is 0-1 fraction
to_fraction <- function(x) {
  if(all(is.na(x))) return(x)
  med <- median(x, na.rm = TRUE)
  if(is.finite(med) && med > 1) return(x / 100)
  return(x)
}

# Load
if(!file.exists(INPUT_FILE)) stop("Input file not found!")
df_old <- read_csv(INPUT_FILE, show_col_types = FALSE)

# Parse Time
dt <- ymd_hms(df_old$date_time, tz = "UTC")

# Initialize Master
df_master <- data.frame(matrix(ncol = length(MASTER_COLS), nrow = nrow(df_old)))
colnames(df_master) <- MASTER_COLS

# Map Data
df_master$Date_UTC     <- as.Date(dt)
df_master$Time_UTC     <- format(dt, "%H:%M:%S")
df_master$Station_ID   <- STATION_ID
df_master$Station_Name <- STATION_NAME

# Atmospheric
df_master$AirTemp_C  <- df_old$air_temp_2m
df_master$RH         <- df_old$relative_humidity_2m
df_master$Dewpoint_C <- df_old$dew_point_temperature_set_1d %||% df_old$dew_point_temperature_set_1

# Rain (Meters to Centimeters)
precip_m <- df_old$precip_intervals_m %||% df_old$precip_accumulated_m
df_master$Rain_cm <- precip_m * 100

# Soil Temp (20 inch -> 50 cm)
df_master$SoilTemp_C_50cm <- df_old$soil_temp_20inch

# Soil Moisture (Inches/Meters to Master Schema Depths)
df_master$WaterCont_5cm_m3m3   <- to_fraction(df_old$soil_moisture_2inch)
df_master$WaterCont_20cm_m3m3  <- to_fraction(df_old$soil_moisture_8inch)
df_master$WaterCont_50cm_m3m3  <- to_fraction(df_old$soil_moisture_20inch)
df_master$WaterCont_100cm_m3m3 <- to_fraction(df_old$`soil_moisture_-1.0m`)

# Finalize
df_master <- df_master[, MASTER_COLS]
write_csv(df_master, OUTPUT_FILE, na = "")

message(sprintf("Success! Created %s", OUTPUT_FILE))
