#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(lubridate)
})

# ---------------------------------------------------------
# FILE PATHS
# ---------------------------------------------------------
FILE_V1_LEGACY <- "old_data/RFGLR_2014_2023_migrated_v1.csv"
FILE_V2_2024   <- "old_data/RFGLR_2024_migrated_v1.csv"
FILE_V3_2025   <- "old_data/RFGLR_2025.csv"
FINAL_OUT      <- "data_clean/RFGLR_combined.csv"

# ---------------------------------------------------------
# HELPER: Get clean POSIXct from our schema
# ---------------------------------------------------------
get_ts <- function(df) {
  ymd_hms(paste(df$Date_UTC, df$Time_UTC), tz = "UTC")
}

# 1. Load the foundation files
cat("Loading Foundation (2014-2023) and 2024 data...\n")
df_v1 <- read_csv(FILE_V1_LEGACY, show_col_types = FALSE)
df_v2 <- read_csv(FILE_V2_2024, show_col_types = FALSE)

# 2. Find the "Cutoff" from the end of 2024
# We find the absolute last observation in the 2024 file
v2_timestamps <- get_ts(df_v2)
cutoff_time <- max(v2_timestamps, na.rm = TRUE)

cat(sprintf("Cutoff point found: %s\n", cutoff_time))

# 3. Load 2025 data and filter
cat("Loading 2025 data and applying cutoff filter...\n")
df_v3_raw <- read_csv(FILE_V3_2025, show_col_types = FALSE)

# Create timestamps for the new data to compare
df_v3_filtered <- df_v3_raw %>%
  mutate(temp_ts = get_ts(.)) %>%
  filter(temp_ts > cutoff_time) %>%  # Keep only data AFTER the 2024 end
  select(-temp_ts)

cat(sprintf("Removed %d overlapping rows from 2025 data.\n",
            nrow(df_v3_raw) - nrow(df_v3_filtered)))

# 4. Stack all three in order
cat("Stacking files into final combined master...\n")
df_final <- bind_rows(df_v1, df_v2, df_v3_filtered) %>%
  arrange(Date_UTC, Time_UTC) %>%
  # Final safety check for any remaining duplicates
  distinct(Date_UTC, Time_UTC, .keep_all = TRUE)

# 5. Write to disk
write_csv(df_final, FINAL_OUT, na = "")

cat(sprintf("Success! Created %s (%d total rows)\n",
            FINAL_OUT, nrow(df_final)))
