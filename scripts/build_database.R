#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(tidyverse)
  library(DBI)
  library(RSQLite)
  library(here)
  library(jsonlite)
})

DB_PATH   <- here("hydro_data.db")
INPUT_DIR <- here("data_clean")

# --- Canonical station registry (single source of truth per network) ---
# Any Station_Name not found here is rejected rather than silently inserted.
# This is what would have caught the "ASEC2_STATION" naming-drift bug before
# it ever reached the database (see hydro_data.db.bak_20260716_pre_asec2_repair).
licor_stations <- names(read_json(here("config", "station_key.json"), simplifyVector = FALSE))
nwcc_stations  <- names(read_json(here("config", "nwcc_stations.json"), simplifyVector = FALSE))
usgs_stations  <- read_csv(here("config", "usgs_sites.csv"), show_col_types = FALSE)$station_name
hads_stations  <- c("ASEC2", "IDWC2")  # matches scripts/hads_retrieval.R

CANONICAL_STATIONS <- c(licor_stations, nwcc_stations, usgs_stations, hads_stations)

con <- dbConnect(RSQLite::SQLite(), DB_PATH)

message("Synchronizing data_clean/ with database...")

files <- list.files(INPUT_DIR, pattern = "\\.csv$", full.names = TRUE)

for (f in files) {
  df <- read_csv(f, show_col_types = FALSE,
    col_types = cols(Station_ID = "c", Date_UTC = "c", Time_UTC = "c"))

  if (nrow(df) == 0) next

  # Guard: reject rows with malformed dates (e.g. epoch day numbers from a past transform bug)
  valid_date_mask <- grepl("^\\d{4}-\\d{2}-\\d{2}$", df$Date_UTC)
  n_invalid <- sum(!valid_date_mask)
  if (n_invalid > 0) {
    warning(sprintf("[build_database] Dropping %d rows with non-ISO Date_UTC in %s",
                    n_invalid, basename(f)))
    df <- df[valid_date_mask, ]
  }
  if (nrow(df) == 0) next

  station_name <- unique(df$Station_Name)[1]

  if (!(station_name %in% CANONICAL_STATIONS)) {
    warning(sprintf(
      "[build_database] Skipping %s: unrecognized Station_Name '%s' not in canonical station registry.",
      basename(f), station_name))
    next
  }

  message(sprintf("  -> Processing station: %s", station_name))

  if (!dbExistsTable(con, "measurements")) {
    cols <- paste(sprintf('"%s"', names(df)), collapse = ", ")
    dbExecute(con, sprintf(
      'CREATE TABLE measurements (%s,
       UNIQUE(Station_Name, Date_UTC, Time_UTC) ON CONFLICT REPLACE)', cols))
    dbExecute(con, 'CREATE INDEX idx_station ON measurements (Station_Name)')
    dbExecute(con, 'CREATE INDEX idx_date    ON measurements (Date_UTC)')
    message("  -> Created table 'measurements'.")
  }

  # Schema migration: add any new columns the CSV has that the DB doesn't
  existing_cols <- dbListFields(con, "measurements")
  new_cols <- setdiff(names(df), existing_cols)
  for (col in new_cols) {
    dbExecute(con, sprintf('ALTER TABLE measurements ADD COLUMN "%s" REAL', col))
    message(sprintf("  -> Added missing column: %s", col))
  }

  dbWriteTable(con, "staging", df, overwrite = TRUE)

  cols_list <- paste(sprintf('"%s"', names(df)), collapse = ", ")
  dbExecute(con, sprintf(
    'INSERT OR REPLACE INTO measurements (%s) SELECT %s FROM staging',
    cols_list, cols_list))
}

if (dbExistsTable(con, "staging")) dbRemoveTable(con, "staging")

total_rows       <- dbGetQuery(con, "SELECT count(*) FROM measurements")[1,1]
unique_stations  <- dbGetQuery(con, "SELECT count(distinct Station_Name) FROM measurements")[1,1]

message(sprintf("\nDatabase sync complete: %s", DB_PATH))
message(sprintf("Total Records: %d", total_rows))
message(sprintf("Total Stations: %d", unique_stations))

dbDisconnect(con)
