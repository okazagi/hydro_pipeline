library(dataRetrieval)
library(dplyr)
library(readr)

retrieve_station_data <- function(site_id, start_date, end_date) {

  # 1. Clean the Site ID
  clean_site_id <- gsub("USGS-", "", site_id)

  # 2. Define parameters
  p_codes <- c("72253", "74207", "72431", "00052", "00020", "75969")

  message(paste("Retrieving data for site:", clean_site_id))

  # 3. Fetch Data
  raw_data <- readNWISuv(siteNumbers = clean_site_id,
                         parameterCd = p_codes,
                         startDate = start_date,
                         endDate = end_date)

  # 4. Rename columns
  latest_usgs_data <- renameNWISColumns(raw_data)

  # 5. Save Raw Data
  # FIXED: Removed 'hydro_pipeline/' prefix.
  # This saves to: ~/hydro_pipeline/data_raw/USGS_12345_raw.csv
  file_path <- paste0("data_raw/USGS_", clean_site_id, "_raw.csv")

  # Safety check: Create data_raw only if it somehow doesn't exist
  if(!dir.exists("data_raw")) dir.create("data_raw")

  write_csv(latest_usgs_data, file_path)
  message(paste("Raw data saved to:", file_path))

  return(file_path)
}

