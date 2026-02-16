#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(lubridate)
})

# ---------------------------------------------------------
# SET YOUR PATHS HERE
# ---------------------------------------------------------
LEGACY_CSV <- "old_data/RFGLR_migrated.csv"
NEW_CSV    <- "data_clean/synoptic_feb/RFGLR_master.csv"
FINAL_OUT  <- "data_clean/RFGLR_combined_final.csv"
# ---------------------------------------------------------

combine_station_data <- function(old_path, new_path, out_path) {

  if(!file.exists(old_path)) stop("Legacy file not found.")
  if(!file.exists(new_path)) stop("New master file not found.")

  # 1. Load both files
  df_old <- read_csv(old_path, show_col_types = FALSE)
  df_new <- read_csv(new_path, show_col_types = FALSE)

  cat(sprintf("Merging %s (%d rows) with %s (%d rows)...\n",
              basename(old_path), nrow(df_old), basename(new_path), nrow(df_new)))

  # 2. Combine and Deduplicate
  # We use bind_rows, then arrange by time.
  # distinct() with .keep_all = TRUE will keep the FIRST occurrence it sees.
  # By putting df_new first, we ensure that if a timestamp exists in both,
  # the "New" data (with better metadata/QC) is the one preserved.

  df_combined <- bind_rows(df_new, df_old) %>%
    # Convert to POSIXct temporarily for accurate sorting
    mutate(temp_ts = ymd_hms(paste(Date_UTC, Time_UTC))) %>%
    arrange(temp_ts) %>%
    # Keep the version from the 'New' file if timestamps collide
    distinct(temp_ts, .keep_all = TRUE) %>%
    select(-temp_ts) # Remove helper column

  # 3. Final sanity check: sort one last time by Date and Time
  df_combined <- df_combined %>%
    arrange(Date_UTC, Time_UTC)

  # 4. Write to disk
  write_csv(df_combined, out_path, na = "")

  cat(sprintf("Success! Combined file created: %s (%d total rows)\n",
              out_path, nrow(df_combined)))
}

combine_station_data(LEGACY_CSV, NEW_CSV, FINAL_OUT)
