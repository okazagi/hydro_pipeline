library(httr)
library(jsonlite)

# 1. Define Output Directory
output_dir <- "data_raw"
if (!dir.exists(output_dir)) {
  dir.create(output_dir)
  message(paste("Created directory:", output_dir))
}

# 2. Define Station Metadata
stations <- list(
  list(id = "1101:CO:SNTL", name = "Chapman Tunnel", start_date = "2023-07-14"),
  list(id = "556:CO:SNTL",  name = "Kiln",           start_date = "2023-07-14"),
  list(id = "1326:CO:SNTL", name = "Castle Peak",    start_date = "2024-09-16")
)

# 3. Global API Parameters
base_url <- "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data"
# Note: Using the corrected comma separator
elements_param <- "TOBS,SMS:*,PRES,DPTP,PRCP,RHUM,STO:*"
end_date_param <- "2024-10-1"

# 4. Loop, Fetch, and Save Raw JSON
for (station in stations) {
  message(paste("Fetching raw JSON for:", station$name, "..."))

  query_params <- list(
    stationTriplets = station$id,
    elements = elements_param,
    duration = "HOURLY",
    beginDate = station$start_date,
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

message("All downloads complete.")
