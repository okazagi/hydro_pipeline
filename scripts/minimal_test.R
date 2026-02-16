library(httr)
library(jsonlite)

# 1. Get Token
token <- Sys.getenv("SYNOPTIC_TOKEN")

# Print the first few chars of the token to ensure it's actually loaded
# (Don't share the full token)
message(sprintf("Using Token starting with: %s...", substr(token, 1, 5)))

# 2. Build the URL EXACTLY as the Request Builder did
# (Removed 'sensorvars=1' which was in our previous attempts)
url <- "https://api.synopticdata.com/v2/stations/metadata"

message("Sending request for CO094...")

resp <- GET(url, query = list(
  token = token,
  stid = "CO094",
  complete = "1"
))

# 3. Check Raw Output
if (status_code(resp) == 200) {
  raw_text <- content(resp, as = "text", encoding = "UTF-8")

  # Print the raw text first to see if ANYTHING came back
  message("\n--- RAW JSON RESPONSE ---")
  cat(substr(raw_text, 1, 500)) # Print first 500 chars
  message("\n-------------------------")

  # Try to parse
  json <- fromJSON(raw_text, simplifyVector = FALSE)

  if (!is.null(json$STATION) && length(json$STATION) > 0) {
    message("\nSUCCESS: R found the station!")
    message(paste("Name:", json$STATION[[1]]$NAME))
  } else {
    message("\nFAILURE: API returned 200 OK, but STATION list is empty.")
  }

} else {
  message(sprintf("\nAPI ERROR: %s", status_code(resp)))
  message(content(resp, as = "text"))
}
