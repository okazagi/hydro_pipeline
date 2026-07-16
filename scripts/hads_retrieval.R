library(tidyverse)
library(lubridate)

# 1. Configuration & Setup
output_folder <- "data_clean"
log_file <- "hads_scrape_log.txt"
if (!dir.exists(output_folder)) dir.create(output_folder)
timestamp_file <- "config/hads_last_timestamp.txt"

stations <- c("ASEC2", "IDWC2")

get_last_timestamp <- function() {
  if (file.exists(timestamp_file)) {
    return(ymd_hms(readLines(timestamp_file, n = 1, warn = FALSE)))
  }
  return(now() - days(1)) # Default to 1 day ago
}

save_last_timestamp <- function(ts) {
  if (!dir.exists("config")) dir.create("config")
  writeLines(as.character(ts), timestamp_file)
}

# --- Updated Station-Specific SHEF Mappings ---
asec2_mapping <- c(
  "VB"  = "Battery_Voltage",      # Added for ASEC2
  "TA"  = "AirTemp_C",
  "XR"  = "RH",
  "PA"  = "Barometric_Pressure",
  "PC"  = "Rain_cm",
  "TV0" = "SoilTemp_C_5cm",
  "TV5" = "SoilTemp_C_20cm",
  "TVA" = "SoilTemp_C_50cm",
  "MS0" = "WaterCont_5cm_m3m3",
  "MS5" = "WaterCont_20cm_m3m3",
  "MSA" = "WaterCont_50cm_m3m3",
  "US"  = "Wind_Speed",
  "UD"  = "Wind_Direction",
  "RW"  = "Solar_Radiation"
)

idwc2_mapping <- c(
  "VB"  = "Battery_Voltage",      # Existing for IDWC2
  "PP"  = "Rain_cm",
  "TV0" = "SoilTemp_C_5cm",
  "M1"  = "WaterCont_5cm_m3m3",
  "TV5" = "SoilTemp_C_20cm",
  "M3"  = "WaterCont_20cm_m3m3",
  "TVA" = "SoilTemp_C_50cm",
  "M4"  = "WaterCont_50cm_m3m3"
)

# --- Unit Conversion Helpers ---
f_to_c <- function(f) {
  if (is.numeric(f)) return(round((f - 32) * 5/9, 2))
  return(f)
}

in_to_cm <- function(i) {
  if (is.numeric(i)) return(round(i * 2.54, 2))
  return(i)
}

# 2. Processing Function
process_hads_to_wide <- function(station_id, sinceday = 1) {
  base_url <- "https://hads.ncep.noaa.gov/nexhads2/servlet/DecodedData"
  url <- paste0(base_url, "?sinceday=", sinceday, "&hsa=nil&state=nil&nwslis=", station_id, "&of=1")

  # Fetch data with character defaults
  raw_text <- tryCatch({
    read_delim(url, delim = "|", col_names = FALSE, show_col_types = FALSE,
               col_types = cols(.default = "c"), trim_ws = TRUE)
  }, error = function(e) return(NULL))

  if (is.null(raw_text) || nrow(raw_text) == 0) {
    cat(paste(now(), "-", station_id, "- Error: No data retrieved.\n"), file = log_file, append = TRUE)
    return(NULL)
  }

  # Select map
  current_map <- if(station_id == "ASEC2") asec2_mapping else idwc2_mapping

  raw <- raw_text %>%
    select(Station_ID = X2, shef = X3, dt_raw = X4, value_raw = X5) %>%
    mutate(
      dt = parse_date_time(dt_raw, orders = c("Ymd HM", "Y-m-d H:M")),
      value = as.numeric(value_raw)
    ) %>%
    filter(!is.na(dt))

  # Pivot to Wide
  wide_data <- raw %>%
    mutate(master_col = current_map[shef]) %>%
    filter(!is.na(master_col)) %>%
    group_by(Station_ID, dt, master_col) %>%
    summarise(value = mean(value, na.rm = TRUE), .groups = 'drop') %>%
    pivot_wider(names_from = master_col, values_from = value)

  if (nrow(wide_data) == 0) return(NULL)

  # Final formatting and schema enforcement
  final_df <- wide_data %>%
    mutate(
      Date_UTC = as.character(as.Date(dt)),
      Time_UTC = format(dt, "%H:%M:%S"),
      Station_Name = Station_ID,
      DataFlag = "Normal"
    ) %>%
    mutate(across(contains("Temp"), f_to_c)) %>%
    mutate(across(contains("Rain"), in_to_cm))

  # Full Master Schema (Now includes Battery_Voltage)
  master_cols <- c(
    "Date_UTC", "Time_UTC", "Station_ID", "Station_Name",
    "Battery_Voltage", "AirTemp_C", "RH", "Barometric_Pressure",
    "Epithermal_Neutron_counts", "Thermal_Neutron_counts", "Blw_Grnd_Epithermal_Neutron_counts",
    "SoilTemp_C_5cm", "SoilTemp_C_10cm", "SoilTemp_C_20cm", "SoilTemp_C_50cm", "SoilTemp_C_100cm",
    "WaterCont_5cm_m3m3", "WaterCont_10cm_m3m3", "WaterCont_20cm_m3m3", "WaterCont_50cm_m3m3", "WaterCont_100cm_m3m3",
    "Rain_cm", "Dewpoint_C", "DataFlag",
    "Wind_Speed", "Wind_Direction", "Solar_Radiation"
  )

  # Inject missing columns as NA
  final_df[setdiff(master_cols, names(final_df))] <- NA
  final_df <- final_df %>% select(all_of(master_cols))

  # Export to CSV
  file_path <- file.path(output_folder, paste0(station_id, "_hads_output.csv"))
  write_csv(final_df, file_path)

  # Log entry
  cat(paste(now(), "-", station_id, "- Success:", nrow(final_df), "rows processed.\n"), file = log_file, append = TRUE)
  return(final_df)
}

# 3. Run
# HADS only supports up to 7 days of historical data. 
# We ignore CLI arguments and always use incremental logic with a 7-day cap.
last_ts <- get_last_timestamp()

since_days <- as.numeric(difftime(now(), last_ts, units = "days"))
# Round up and cap at 7 days
since_days_param <- min(7, max(1, ceiling(since_days)))

message(sprintf("Fetching HADS data for the last %s days (incremental sync)", since_days_param))

all_station_data <- map(stations, ~process_hads_to_wide(.x, sinceday = since_days_param))

# Update timestamp to now after a successful run
save_last_timestamp(now())
message("HADS sync complete.")
