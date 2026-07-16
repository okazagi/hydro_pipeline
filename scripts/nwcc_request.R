library(httr)
library(jsonlite)
library(lubridate)
library(here)
library(purrr)

# 1. Define Output Directory and Timestamp File
output_dir <- here("data_raw")
if (!dir.exists(output_dir)) {
  dir.create(output_dir)
  message(paste("Created directory:", output_dir))
}
TIMESTAMP_FILE <- here("config", "nwcc_last_timestamp.txt")

# 2. Date Logic
get_start_date <- function() {
  if (file.exists(TIMESTAMP_FILE)) {
    return(readLines(TIMESTAMP_FILE, n = 1, warn = FALSE))
  }
  return("2023-07-14") # Default if no file exists
}

save_last_date <- function(date_str) {
  if (!dir.exists(dirname(TIMESTAMP_FILE))) dir.create(dirname(TIMESTAMP_FILE), recursive = TRUE)
  writeLines(as.character(date_str), TIMESTAMP_FILE)
}

# 3. Define Station Metadata (single source of truth, shared with nwcc_transform.R)
STATION_KEY_PATH <- here("config", "nwcc_stations.json")
station_key <- read_json(STATION_KEY_PATH, simplifyVector = FALSE)
stations <- imap(station_key, function(meta, name) list(id = meta$id, name = name))

# 4. Global API Parameters
base_url <- "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data"
elements_param <- "TOBS,SMS:*,PRES,DPTP,PRCP,RHUM,STO:*"

# Check for manual start date from CLI
cli_args <- commandArgs(trailingOnly = TRUE)
start_date <- if (length(cli_args) > 0) cli_args[1] else get_start_date()

end_date_param <- format(today(), "%Y-%m-%d")

message(paste("Fetching data from", start_date, "to", end_date_param))

# 5. Loop, Fetch, and Save Raw JSON
for (station in stations) {
  message(paste("Fetching raw JSON for:", station$name, "..."))

  query_params <- list(
    stationTriplets = station$id,
    elements = elements_param,
    duration = "HOURLY",
    beginDate = start_date,
    endDate = end_date_param,
    returnFlags = "false",
    returnOriginalValues = "false",
    returnSuspectData = "false"
  )

  # Make the API Request
  response <- GET(url = base_url, query = query_params)

  # Check status
  if (status_code(response) != 200) {
    warning(paste("Error fetching", station$name, "- Status Code:", status_code(response)))
    next
  }

  # Get the raw text content (the JSON string)
  json_text <- content(response, "text", encoding = "UTF-8")

  # Save directly to file
  file_path <- file.path(output_dir, paste0(station$name, ".json"))
  writeLines(json_text, file_path)

  message(paste("Saved:", file_path))
}

# Update the last timestamp to today
save_last_date(end_date_param)
message(paste("Updated last timestamp to", end_date_param))
message("All downloads complete.")
