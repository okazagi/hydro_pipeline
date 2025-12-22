library(tidyverse)

# 1. Load your functions
source("scripts/usgs_retrieve.R")
source("scripts/usgs_transform.R")

# 2. Read the generated list with start dates
#    (We use the file generated in the previous step)
request_list <- read_csv("config/station_request_dates.csv", show_col_types = FALSE)

# 3. Set the End Date for this run
#    Start date is now dynamic (per station), but you set the end date here.
manual_end_date <- "2025-12-20"

# 4. Loop through each site
for(i in 1:nrow(request_list)) {

  # Extract variables for the current row
  # Note: We use 'site_no' because that is what we named the column in the previous script
  current_site <- request_list$site_no[i]

  # Ensure the date is formatted as a string (YYYY-MM-DD) for the API
  current_start <- as.character(request_list$begin[i])

  message(paste("\n--- Starting Batch for:", current_site, "---"))
  message(paste("    Range:", current_start, "to", manual_end_date))

  # A. TRY to Retrieve
  tryCatch({

    # Run Retrieval
    # We pass the STATION SPECIFIC start date and the MANUAL end date
    raw_file <- retrieve_station_data(current_site, current_start, manual_end_date)

    # B. Run Processing immediately on that file
    process_usgs_data(raw_file)

  }, error = function(e) {
    message(paste("SKIPPING", current_site, "- Error:", e$message))
  })
}

message("\nBatch run complete!")
