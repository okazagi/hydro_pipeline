#!/usr/bin/env Rscript

# import_old_data.R
# Imports historical standardized data into the SQLite database with name unification.

suppressPackageStartupMessages({
  library(tidyverse)
  library(DBI)
  library(RSQLite)
  library(here)
  library(jsonlite)
})

# ---------------- CONFIGURATION ---------------- #
DB_PATH    <- here("hydro_data.db")
OLD_DATA_DIR <- here("old_data_clean")
NEW_DATA_DIR <- here("data_clean")

# ---------------- STATION MAPPING ---------------- #
# Maps various aliases to a canonical Station_Name
NAME_MAP <- c(
  "ASEC2_STATION" = "ASEC2",
  "CASTLE CREEK NEAR ASPEN" = "ASEC2",
  "IDWC2_STATION" = "IDWC2",
  "INDEPENDENCE PASS NEAR ASPEN 15SE" = "IDWC2",
  "Brush Creek" = "RFBRC",
  "Glassier Ranch" = "RFGLR",
  "Glenwood Springs" = "RFGLS",
  "Northstar Aspen Grove" = "RFNSA",
  "Northstar Transition Zone" = "RFNST",
  "Sky Mtn" = "RFSKM",
  "Smuggler Mtn" = "RFSMM",
  "Spring Valley" = "RFSPV"
)

# Numeric IDs for iRON stations from station_key.json
STID_PATH   <- here("config", "station_key.json")
station_key <- read_json(STID_PATH, simplifyVector = FALSE)
iron_id_map <- setNames(
  sapply(station_key, function(s) as.character(s$id)),
  names(station_key)
)

# ---------------- INITIALIZATION ---------------- #
if (file.exists(DB_PATH)) file.remove(DB_PATH)
con <- dbConnect(RSQLite::SQLite(), DB_PATH)

# Define Master Schema (27 columns)
master_cols <- c(
  "Date_UTC", "Time_UTC", "Station_ID", "Station_Name",
  "Battery_Voltage", "AirTemp_C", "RH", "Barometric_Pressure",
  "Epithermal_Neutron_counts", "Thermal_Neutron_counts", "Blw_Grnd_Epithermal_Neutron_counts",
  "SoilTemp_C_5cm", "SoilTemp_C_10cm", "SoilTemp_C_20cm", "SoilTemp_C_50cm", "SoilTemp_C_100cm",
  "WaterCont_5cm_m3m3", "WaterCont_10cm_m3m3", "WaterCont_20cm_m3m3", "WaterCont_50cm_m3m3", "WaterCont_100cm_m3m3",
  "Rain_cm", "Dewpoint_C", "DataFlag",
  "Wind_Speed", "Wind_Direction", "Solar_Radiation"
)

# Create table
cols_sql <- paste(sprintf('"%s"', master_cols), collapse = ", ")
create_query <- sprintf('
  CREATE TABLE measurements (%s, 
  UNIQUE(Station_Name, Date_UTC, Time_UTC) ON CONFLICT REPLACE)', 
  cols_sql)
dbExecute(con, create_query)
dbExecute(con, 'CREATE INDEX idx_station ON measurements (Station_Name)')
dbExecute(con, 'CREATE INDEX idx_date ON measurements (Date_UTC)')

# ---------------- IMPORT FUNCTION ---------------- #

import_directory <- function(dir_path, recursive = FALSE) {
  files <- list.files(dir_path, pattern = "\\.csv$", full.names = TRUE, recursive = recursive)
  message(sprintf("Importing %d files from %s...", length(files), basename(dir_path)))
  
  for (f in files) {
    # Read everything as character to avoid automatic date/time numeric conversion
    df <- read_csv(f, show_col_types = FALSE, col_types = cols(.default = "c"))
    if (nrow(df) == 0) next
    
    # Unify names
    if (!"Station_Name" %in% names(df)) {
      # Infer from filename
      fname <- tools::file_path_sans_ext(basename(f))
      df$Station_Name <- fname
    }
    
    df <- df %>%
      mutate(Station_Name = ifelse(Station_Name %in% names(NAME_MAP), NAME_MAP[Station_Name], Station_Name))
    
    # Use numeric ID for iRON stations; fall back to station name for others (USGS, NWCC, HADS)
    df$Station_ID <- ifelse(
      df$Station_Name %in% names(iron_id_map),
      iron_id_map[df$Station_Name],
      df$Station_Name
    )
    
    # 2. STANDARDIZE DATE AND TIME
    # Some files might have full timestamps in Time_UTC or different formats
    # We combine them then split them back to ensure consistency
    df <- df %>%
      mutate(
        # Attempt to parse the combined datetime
        # We try multiple common formats found in old data
        temp_dt = parse_date_time(paste(Date_UTC, Time_UTC), orders = c("Ymd HMS", "Ymd HM", "mdy HMS", "mdy HM")),
        Date_UTC = as.character(as.Date(temp_dt)),
        Time_UTC = format(temp_dt, "%H:%M:%S")
      ) %>%
      filter(!is.na(Date_UTC)) %>%
      select(-temp_dt)

    # 3. ALIGN TO SCHEMA
    missing_cols <- setdiff(master_cols, names(df))
    for (mc in missing_cols) df[[mc]] <- NA_character_
    
    final_df <- df %>%
      select(all_of(master_cols)) %>%
      # Important: Only convert known numeric columns to numeric
      mutate(across(!any_of(c("Date_UTC", "Time_UTC", "Station_ID", "Station_Name", "DataFlag")), as.numeric)) %>%
      # Filter out any header repeats or invalid dates
      filter(!is.na(Date_UTC) & Date_UTC != "Date_UTC")
    
    dbWriteTable(con, "staging", final_df, overwrite = TRUE)
    cols_list <- paste(sprintf('"%s"', master_cols), collapse = ", ")
    upsert_query <- sprintf('INSERT OR REPLACE INTO measurements (%s) SELECT %s FROM staging', 
                            cols_list, cols_list)
    dbExecute(con, upsert_query)
  }
}

# ---------------- EXECUTION ---------------- #

# 1. Import new data first (so old data can be replaced if overlapping and preferred, 
# or vice-versa. Usually we want the *best* data to win.
# I'll import OLD data first, then NEW data to ensure NEW data wins conflicts.)
import_directory(OLD_DATA_DIR, recursive = TRUE)
import_directory(NEW_DATA_DIR, recursive = FALSE)

if (dbExistsTable(con, "staging")) dbRemoveTable(con, "staging")

# ---------------- VERIFICATION ---------------- #

total_rows <- dbGetQuery(con, "SELECT count(*) FROM measurements")[1,1]
unique_stations <- dbGetQuery(con, "SELECT count(distinct Station_Name) FROM measurements")[1,1]

message("\nSync complete.")
message(sprintf("Total Records: %d", total_rows))
message(sprintf("Total Unique Stations: %d", unique_stations))

# Sample check
sample_station <- dbGetQuery(con, "SELECT Station_Name, count(*) as count FROM measurements GROUP BY Station_Name ORDER BY count DESC LIMIT 5")
print(sample_station)

dbDisconnect(con)
