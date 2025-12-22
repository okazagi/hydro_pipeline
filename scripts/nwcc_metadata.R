library(httr)
library(jsonlite)

# 1. Define the base URL
base_url <- "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/stations"

# 2. Define your list of station triplets
station_triplets <- c(
  "1101:CO:SNTL",  # Chapman Tunnel
  "556:CO:SNTL",   # Kiln
  "1326:CO:SNTL"   # Castle Peak
)

# 3. Set up the parameters
# httr will automatically handle the repeated 'stationTriplets' parameter
params <- list(
  returnForecastPointMetadata = "false",
  returnReservoirMetadata = "false",
  returnStationElements = "true",
  activeOnly = "true"
)

# Add the triplets to the params list.
# We interpret the vector as multiple parameters with the same name.
params <- c(params, setNames(as.list(station_triplets), rep("stationTriplets", length(station_triplets))))

# 4. Make the GET request
response <- GET(url = base_url, query = params)

# 5. Check for errors and parse
if (status_code(response) == 200) {

  # Parse JSON content
  content_text <- content(response, "text", encoding = "UTF-8")
  data <- fromJSON(content_text, flatten = TRUE)

  # Print the data
  print(data)

  # Example: Accessing specific columns if 'data' is a data frame
  if (is.data.frame(data)) {
    cat("\n--- Summary ---\n")
    subset_data <- data[, c("name", "stationTriplet", "latitude", "longitude")]
    print(subset_data)
  }

} else {
  stop(paste("API request failed with status:", status_code(response)))
}
