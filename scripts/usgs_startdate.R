library(tidyverse)
library(fs)

# --- Configuration ---
config_path <- "config/usgs_sites.csv"
metadata_dir <- "metadata"
output_path <- "config/station_request_dates.csv"

# --- 1. Load Site List ---
sites_df <- read_csv(config_path, show_col_types = FALSE)

# --- 2. Function to Process Each Station ---
get_common_start_date <- function(site_id) {

  # Ensure site_id is a character string
  sid <- as.character(site_id)

  # INTELLIGENT FILENAME CREATION:
  # Check if the site_id already has "USGS-" at the start.
  # If it does, don't add it again. If it doesn't, add it.
  if (startsWith(sid, "USGS-")) {
    file_name <- paste0(sid, "_meta.csv")
  } else {
    file_name <- paste0("USGS-", sid, "_meta.csv")
  }

  file_path <- file.path(metadata_dir, file_name)

  if (file_exists(file_path)) {
    meta_data <- read_csv(file_path, show_col_types = FALSE)

    if ("begin" %in% names(meta_data)) {
      # Return the LATEST start date (maximum of the 'begin' column)
      return(max(as.Date(meta_data$begin), na.rm = TRUE))
    }
  }
  return(NA)
}

# --- 3. Execute Loop ---
processed_df <- sites_df %>%
  mutate(
    begin = map_vec(site_id, get_common_start_date)
  ) %>%
  filter(!is.na(begin)) %>%
  select(site_no = site_id, station_name, begin)

# --- 4. Save Output ---
write_csv(processed_df, output_path)

print(paste("Processing complete. Saved:", output_path))
# Check the first few rows to confirm it worked
print(head(processed_df))
