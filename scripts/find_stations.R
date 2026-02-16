library(httr)
library(jsonlite)
library(dplyr)

# ---------------- CONFIG ---------------- #
API_TOKEN <- Sys.getenv("SYNOPTIC_TOKEN")

# Coordinates for "Center of RFV" (approx near Aspen/Snowmass)
# You can adjust this if you need to look further down valley
LAT <- 39.27
LON <- -106.88
RADIUS_MILES <- 100  # Look within 20 miles of this point

# ---------------- SEARCH ---------------- #

message(sprintf("Searching for stations within %s miles of %.4f, %.4f...", RADIUS_MILES, LAT, LON))

url <- "https://api.synopticdata.com/v2/stations/metadata"

# 1. Search by Radius
resp <- GET(url, query = list(
  token = API_TOKEN,
  radius = paste(LAT, LON, RADIUS_MILES, sep = ","),
  limit = 100,      # Show us the top 100 closest stations
  fields = "stid,name,status,network_id" # Only get what we need
))

if (status_code(resp) == 200) {
  content_text <- content(resp, as = "text", encoding = "UTF-8")
  json <- fromJSON(content_text)

  if (!is.null(json$STATION) && length(json$STATION) > 0) {
    # Convert to a clean table
    stations <- json$STATION %>%
      select(STID, NAME, STATUS, MNET_ID) %>%
      as_tibble()

    # Print the list clearly
    print(stations, n = 100)

    message("\n------------------------------------------------")
    message("Look at the list above.")
    message("1. Find 'Aspen Airport'. Is the ID 'KASE' or something else?")
    message("2. Look for 'USGS' stations. Their IDs are likely just numbers (e.g., '1234').")
    message("3. Check for your missing IDs (CO094, etc).")

  } else {
    message("No stations found in this radius. Try increasing the radius.")
  }
} else {
  message(sprintf("API Error: %s", status_code(resp)))
}

# ---------------- SPECIFIC CHECK ---------------- #
# Let's double check if "KASE" exists explicitly but under a different filter
message("\n--- Double Checking KASE ---")
resp_kase <- GET(url, query = list(token=API_TOKEN, stid="KASE"))
if (status_code(resp_kase) == 200 && length(fromJSON(content(resp_kase, "text"))$STATION) > 0) {
  message("Wait! 'KASE' WAS found when queried directly here. If this prints, your previous script might have had a hidden typo.")
} else {
  message("'KASE' definitely not found by ID. The token might be restricted to specific networks.")
}
