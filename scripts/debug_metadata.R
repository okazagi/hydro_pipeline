library(httr)
library(jsonlite)
library(lubridate)

# ---------------- CONFIG ---------------- #
problem_stations <- c("CO094", "IDPC2", "CAAXT", "APXC2", "CO100", "USGSHY19869", "KASE", "CALOG")
API_TOKEN <- Sys.getenv("SYNOPTIC_TOKEN")

# Define the two API endpoints to test
endpoints <- list(
  "Standard" = "https://api.synopticdata.com/v2/stations/metadata",
  "Secure (S)" = "https://api.s.synopticdata.com/v2/stations/metadata"
)

# ---------------- HELPER ---------------- #
try_fetch_metadata <- function(url, stid, token) {
  resp <- GET(url, query = list(
    token = token,
    stid = stid,
    complete = "1",
    sensorvars = "1"
  ))

  if (status_code(resp) == 200) {
    # safe parsing
    content_txt <- content(resp, as = "text", encoding = "UTF-8")
    json <- fromJSON(content_txt, simplifyVector = FALSE)
    return(json)
  }
  return(NULL)
}

# ---------------- MAIN LOOP ---------------- #

for (stid in problem_stations) {
  message(paste0("\n------------------------------------------------"))
  message(sprintf("INSPECTING: %s", stid))

  found <- FALSE

  # Try both URLs
  for (api_name in names(endpoints)) {
    base_url <- endpoints[[api_name]]

    # message(sprintf("   ...trying %s endpoint...", api_name))
    data <- try_fetch_metadata(base_url, stid, API_TOKEN)

    if (!is.null(data$STATION) && length(data$STATION) > 0) {
      st <- data$STATION[[1]]

      message(sprintf("   [SUCCESS via %s API]", api_name))
      message(sprintf("   Name:   %s", st$NAME))
      message(sprintf("   Status: %s", st$STATUS))

      # Check Period of Record
      if (!is.null(st$PERIOD_OF_RECORD)) {
        start_date <- if(is.list(st$PERIOD_OF_RECORD)) st$PERIOD_OF_RECORD$start else st$PERIOD_OF_RECORD
        message(sprintf("   Start:  %s", start_date))
      } else {
        message("   Start:  (Not listed)")
      }

      # Check Vars
      if (!is.null(st$SENSOR_VARIABLES)) {
        vars <- names(st$SENSOR_VARIABLES)
        message(paste("   Vars:  ", paste(head(vars, 5), collapse = ", "), "..."))
      }

      found <- TRUE
      break # Stop checking other URLs if we found it
    }
  }

  if (!found) {
    message("   [FAILED] Station not found on either Standard or '.s' API.")
  }
}
