#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(lubridate)
  library(dplyr)
  library(readr)
})

# ---------------------------------------------------------
# HARDCODE YOUR PATHS HERE
# ---------------------------------------------------------
INPUT_FILE   <- "old_data/RFGLR_2014_2023_legacy_v1.csv"
OUTPUT_FILE  <- "old_data/RFGLR_2014_2023_migrated_v1.csv"
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

# We add "ymd" to capture plain dates (like 2014-12-31)
dt <- parse_date_time(df_old$DATETIMEUTC,
                      orders = c("ymd HMS", "ymd HM", "mdy HMS", "mdy HM", "ymd"),
                      tz = "UTC")

# Initialize Master
df_master <- data.frame(matrix(ncol = length(MASTER_COLS), nrow = nrow(df_old)))
colnames(df_master) <- MASTER_COLS

# Map Metadata & Time
df_master$Date_UTC     <- as.Date(dt)
df_master$Time_UTC     <- format(dt, "%H:%M:%S")
df_master$Station_ID   <- STATION_ID
df_master$Station_Name <- STATION_NAME

# Atmospheric Variables
df_master$AirTemp_C  <- df_old$AIRTEMP_C
df_master$RH         <- df_old$RH_PERC
df_master$Dewpoint_C <- df_old$DEWPT_C

# Rain (MM to CM)
df_master$Rain_cm <- df_old$RAIN_MM / 10

# Soil Temperature (Mapped to 20cm as a default)
df_master$SoilTemp_C_20cm <- df_old$SOILTEMP_C

# Soil Moisture (Fractions)
# 2IN -> 5cm
# 8IN -> 20cm
# 20IN -> 50cm
# 40IN -> 100cm
df_master$WaterCont_5cm_m3m3   <- to_fraction(df_old$WATERCONT_2IN_FRAC)
df_master$WaterCont_20cm_m3m3  <- to_fraction(df_old$WATERCONT_8IN_FRAC)
df_master$WaterCont_50cm_m3m3  <- to_fraction(df_old$WATERCONT_20IN_FRAC)
df_master$WaterCont_100cm_m3m3 <- to_fraction(df_old$WATERCONT_40IN_FRAC)

# Finalize and Save
df_master <- df_master[, MASTER_COLS]
write_csv(df_master, OUTPUT_FILE, na = "")

message(sprintf("Success! Created %s", OUTPUT_FILE))
