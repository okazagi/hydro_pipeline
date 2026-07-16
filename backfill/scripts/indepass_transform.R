library(dplyr)
library(readr)
library(lubridate)
library(here)

# --- MASTER SCHEMA (from hydro_data.db `measurements` table) ---
MASTER_COLS <- c(
  "Date_UTC", "Time_UTC", "Station_ID", "Station_Name",
  "Battery_Voltage", "AirTemp_C", "RH", "Barometric_Pressure",
  "Epithermal_Neutron_counts", "Thermal_Neutron_counts", "Blw_Grnd_Epithermal_Neutron_counts",
  "SoilTemp_C_5cm", "SoilTemp_C_10cm", "SoilTemp_C_20cm", "SoilTemp_C_50cm", "SoilTemp_C_100cm",
  "WaterCont_5cm_m3m3", "WaterCont_10cm_m3m3", "WaterCont_20cm_m3m3", "WaterCont_50cm_m3m3", "WaterCont_100cm_m3m3",
  "Rain_cm", "Dewpoint_C", "DataFlag",
  "Wind_Speed", "Wind_Direction", "Solar_Radiation"
)

# Columns in the source CSV that have no match in the master schema.
# Kept alongside schema columns so no source data is lost.
EXTRA_COLS <- c("Wind_Gust_ms", "SnowDepth_cm")

in_dir  <- here("independence_pass")
in_file <- file.path(in_dir, "IndePass_munged_1_28_24.csv")
out_file <- file.path(in_dir, "IndePass_munged_1_28_24_schema.csv")

raw <- read_csv(in_file, show_col_types = FALSE)

# Parse datetime. Source format is M/D/YY H:M, already in UTC.
parsed <- raw %>%
  mutate(
    dt = mdy_hm(DATETIMEUTC, tz = "UTC"),
    Date_UTC = as.character(as_date(dt)),
    Time_UTC = format(dt, "%H:%M:%S"),
    Station_ID   = as.character(STATIONID),
    Station_Name = STATIONNAME,

    # Direct / unit-converted mappings
    AirTemp_C       = as.numeric(AIRTEMP_C),
    RH              = as.numeric(RH_PERC),
    Dewpoint_C      = as.numeric(DEWPT_C),
    # 2in ~ 5cm, 8in ~ 20cm, 20in ~ 50cm
    WaterCont_5cm_m3m3  = as.numeric(WATERCONT2INA_FRAC),
    WaterCont_20cm_m3m3 = as.numeric(WATERCONT8INA_FRAC),
    WaterCont_50cm_m3m3 = as.numeric(WATERCONT20INA_FRAC),
    SoilTemp_C_20cm = as.numeric(SOILTEMP8IN_C),
    Wind_Speed      = as.numeric(WINDSPD_MS),
    Wind_Direction  = as.numeric(WINDDIR_DEG),
    Rain_cm         = as.numeric(RAIN_MM) / 10,

    # Schema columns with no source data
    Battery_Voltage = NA_real_,
    Barometric_Pressure = NA_real_,
    Epithermal_Neutron_counts = NA_real_,
    Thermal_Neutron_counts = NA_real_,
    Blw_Grnd_Epithermal_Neutron_counts = NA_real_,
    SoilTemp_C_5cm   = NA_real_,
    SoilTemp_C_10cm  = NA_real_,
    SoilTemp_C_50cm  = NA_real_,
    SoilTemp_C_100cm = NA_real_,
    WaterCont_10cm_m3m3  = NA_real_,
    WaterCont_100cm_m3m3 = NA_real_,
    DataFlag = NA_character_,
    Solar_Radiation = NA_real_,

    # Extras preserved from source (no schema home)
    Wind_Gust_ms = as.numeric(WINDGUST_MS),
    SnowDepth_cm = as.numeric(SNOWDEPTH_CM)
  ) %>%
  select(all_of(c(MASTER_COLS, EXTRA_COLS))) %>%
  arrange(Date_UTC, Time_UTC)

write_csv(parsed, out_file)

message(sprintf("Wrote %d rows to %s", nrow(parsed), out_file))
message(sprintf("Date range: %s to %s", min(parsed$Date_UTC), max(parsed$Date_UTC)))
