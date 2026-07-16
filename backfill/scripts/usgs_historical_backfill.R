#!/usr/bin/env Rscript

# usgs_historical_backfill.R
# Retrieves full historical records for all USGS stations and syncs to database.
# Chunks requests by year and includes rate-limiting to avoid API errors.

suppressPackageStartupMessages({
  library(tidyverse)
  library(dataRetrieval)
  library(DBI)
  library(RSQLite)
  library(here)
  library(lubridate)
})

# 1. Source existing pipeline functions
source(here("scripts", "usgs_transform.R"))

# 2. Configuration
DB_PATH <- here("hydro_data.db")
METADATA_DIR <- here("metadata")
usgs_sites <- read_csv(here("config", "usgs_sites.csv"), show_col_types = FALSE)
p_codes <- c("72253", "74207", "72431", "00052", "00020", "75969")

# 3. Helper: Get earliest start date from metadata file
get_start_date_from_meta <- function(site_id) {
  meta_file <- file.path(METADATA_DIR, paste0(site_id, "_meta.csv"))
  if (!file.exists(meta_file)) return("2023-01-01")
  meta_df <- read_csv(meta_file, show_col_types = FALSE)
  if (nrow(meta_df) == 0) return("2023-01-01")
  return(format(as.Date(min(meta_df$begin, na.rm = TRUE)), "%Y-%m-%d"))
}

# 4. Helper: Sync cleaned file to Database
sync_to_db <- function(clean_file) {
  con <- dbConnect(RSQLite::SQLite(), DB_PATH)
  df <- read_csv(clean_file, show_col_types = FALSE, col_types = cols(Station_ID = "c"))
  df <- df %>%
    mutate(
      temp_dt = parse_date_time(paste(Date_UTC, Time_UTC), orders = c("Ymd HMS", "Ymd HM")),
      Date_UTC = as.character(as.Date(temp_dt)),
      Time_UTC = format(temp_dt, "%H:%M:%S")
    ) %>%
    filter(!is.na(Date_UTC)) %>%
    select(-temp_dt)
  dbWriteTable(con, "staging", df, overwrite = TRUE)
  master_cols <- dbListFields(con, "measurements")
  cols_list <- paste(sprintf('"%s"', master_cols), collapse = ", ")
  upsert_query <- sprintf('INSERT OR REPLACE INTO measurements (%s) SELECT %s FROM staging', cols_list, cols_list)
  dbExecute(con, upsert_query)
  dbRemoveTable(con, "staging")
  dbDisconnect(con)
}

# 5. Main Loop
message("--- Starting USGS Historical Backfill (Stabilized API Calls) ---")

for (i in 1:nrow(usgs_sites)) {
  site_id_full <- usgs_sites$site_id[i]
  clean_id <- gsub("USGS-", "", site_id_full)
  station_name <- usgs_sites$station_name[i]
  
  start_date_obj <- as.Date(get_start_date_from_meta(site_id_full))
  end_date_obj   <- today()
  
  message(sprintf("\n[%d/%d] Processing %s", i, nrow(usgs_sites), station_name))
  years <- seq(year(start_date_obj), year(end_date_obj))
  
  for (yr in years) {
    chunk_start <- max(start_date_obj, as.Date(sprintf("%d-01-01", yr)))
    chunk_end   <- min(end_date_obj, as.Date(sprintf("%d-12-31", yr)))
    if (chunk_start > chunk_end) next
    
    message(sprintf("  -> Chunk %d: %s to %s", yr, chunk_start, chunk_end))
    
    tryCatch({
      # Direct NWIS call with standardized date strings
      raw_data <- readNWISuv(siteNumbers = clean_id, 
                             parameterCd = p_codes,
                             startDate = format(chunk_start, "%Y-%m-%d"),
                             endDate = format(chunk_end, "%Y-%m-%d"))
      
      if (nrow(raw_data) > 0) {
        raw_file <- here("data_raw", paste0("USGS_", clean_id, "_historical.csv"))
        write_csv(renameNWISColumns(raw_data), raw_file)
        process_usgs_data(raw_file)
        sync_to_db(here("data_clean", paste0(station_name, ".csv")))
        message(sprintf("  -> Successfully synced %d data.", yr))
      } else {
        message(sprintf("  -> No data for %d.", yr))
      }
      
      # Rate limiting to prevent 400/connection errors
      Sys.sleep(3)
      
    }, error = function(e) {
      message(sprintf("  !! Error in %d: %s", yr, e$message))
      Sys.sleep(5) # Back off more on error
    })
  }
}
message("\n--- Historical Backfill Complete ---")
