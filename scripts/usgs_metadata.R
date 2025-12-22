library(tidyverse)
library(fs)

# 1. Read the Site List
# We use unique() to avoid downloading the same metadata twice if there are duplicates
site_list <- read_csv("config/usgs_sites.csv", show_col_types = FALSE)
unique_sites <- unique(site_list$site_id)

# 2. Define the Retrieval Function
retrieve_station_metadata <- function(site_id) {

  # Clean up the ID for the filename (remove special chars if any)
  safe_name <- gsub("[:/]", "_", site_id)
  file_path <- file.path("metadata", paste0(safe_name, "_meta.csv"))

  message(paste("Fetching metadata for:", site_id, "..."))

  # Use tryCatch to handle network errors or bad IDs gracefully
  tryCatch({

    # --- YOUR FUNCTION CALL ---
    # Assuming the library containing this function is already loaded
    meta_df <- read_waterdata_ts_meta(monitoring_location_id = site_id)

    # Check if we actually got data back
    if (nrow(meta_df) > 0) {
      write_csv(meta_df, file_path)
      message(paste("   -> Saved to:", file_path))
    } else {
      warning(paste("   -> No metadata found for:", site_id))
    }

  }, error = function(e) {
    # If it fails, print the error but don't stop the script
    message(paste("   -> ERROR for site", site_id, ":", e$message))
  })
}

# 3. Execute the Loop
# Walk loops through the list and runs the function for each item
walk(unique_sites, retrieve_station_metadata)

message("--- Metadata retrieval complete ---")
