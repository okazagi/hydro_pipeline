library(jsonlite)
library(tidyverse)
library(lubridate)
library(here)

# 1. Load the Data
file_path <- here("RFGLS.json") # Adjust path if needed
json_data <- read_json(file_path, simplifyVector = TRUE)

# 2. Extract Station Metadata & Observations
station_info <- json_data$STATION[[1]]
obs_list <- station_info$OBSERVATIONS

# Convert the list of vectors into a Data Frame
# Synoptic returns lists of values, so we bind them into a tibble
raw_df <- as_tibble(obs_list)

# 3. Transform to Your Schema
clean_df <- raw_df %>%
  mutate(
    # Time Conversions
    # Synoptic uses ISO format (e.g., "2025-09-24T00:00:00Z")
    date_parsed = ymd_hms(date_time),
    Date_UTC    = as.Date(date_parsed),
    Time_UTC    = format(date_parsed, format = "%H:%M:%S"),

    # Station Metadata
    Station_ID   = station_info$STID,
    Station_Name = station_info$NAME,

    # --- Mapped Variables (From RFGLS.json) ---

    # Air Temperature
    AirTemp_C = as.numeric(air_temp_set_1),

    # Relative Humidity
    RH = as.numeric(relative_humidity_set_1),

    # Dew Point (Calculated or Observed)
    Dewpoint_C = if("dew_point_temperature_set_1" %in% names(.)) as.numeric(dew_point_temperature_set_1) else NA_real_,

    # Soil Temp (RFGLS only has set_1 in the snippet, mapping to 5cm default)
    SoilTemp_C_5cm = if("soil_temp_set_1" %in% names(.)) as.numeric(soil_temp_set_1) else NA_real_,

    # Soil Moisture (Mapping sets 1, 2, 3 to depths 5, 20, 50 based on typical order)
    WaterCont_5cm_m3m3  = if("soil_moisture_set_1" %in% names(.)) as.numeric(soil_moisture_set_1) else NA_real_,
    WaterCont_20cm_m3m3 = if("soil_moisture_set_2" %in% names(.)) as.numeric(soil_moisture_set_2) else NA_real_,
    WaterCont_50cm_m3m3 = if("soil_moisture_set_3" %in% names(.)) as.numeric(soil_moisture_set_3) else NA_real_,

    # --- Missing Variables (Set to NA to match Schema) ---

    Barometric_Pressure = NA_real_,
    Rain_cm             = NA_real_, # Synoptic usually provides 'precip_accum_set_1' if available

    # Neutron Counts (Not in Synoptic file)
    Epithermal_Neutron_counts          = NA_real_,
    Thermal_Neutron_counts             = NA_real_,
    Blw_Grnd_Epithermal_Neutron_counts = NA_real_,

    # Missing Soil Depths
    SoilTemp_C_10cm  = NA_real_,
    SoilTemp_C_20cm  = NA_real_,
    SoilTemp_C_50cm  = NA_real_,
    SoilTemp_C_100cm = NA_real_,

    # Missing Water Cont Depths/Duplicates
    WaterCont_10cm_m3m3   = NA_real_,
    WaterCont_100cm_m3m3  = NA_real_,
    WaterCont_5cm_m3m3_2  = NA_real_,
    WaterCont_20cm_m3m3_2 = NA_real_,
    WaterCont_50cm_m3m3_2 = NA_real_,

    DataFlag = NA_character_
  ) %>%
  # 4. Final Selection (Order matches your schema)
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

# Inspect result
print(head(clean_df))
