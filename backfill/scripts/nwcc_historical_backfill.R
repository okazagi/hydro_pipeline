#!/usr/bin/env Rscript

# nwcc_historical_backfill.R
#
# Fetches full historical SNOTEL/NWCC records from the AWDB REST API
# for each station, starting from its documented begin date.
# Chunks requests by year to stay within API limits.
# Transforms inline and syncs directly to hydro_data.db.
#
# Station coverage after backfill:
#   Kiln          (556:CO:SNTL)  : 1979-10-01 -> present
#   Chapman Tunnel(1101:CO:SNTL) : 2007-09-01 -> present
#   Castle Peak   (1326:CO:SNTL) : 2024-09-18 -> present

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(purrr)
  library(tidyr)
  library(readr)
  library(lubridate)
  library(DBI)
  library(RSQLite)
  library(here)
})

# ------ Configuration -------------------------------------------------

DB_PATH  <- here("hydro_data.db")
BASE_URL <- "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data"
ELEMENTS <- "TOBS,RHUM,DPTP,PRCP,PRES"   # air temp, RH, dewpoint, precip, pressure

MASTER_COLS <- c(
  "Date_UTC", "Time_UTC", "Station_ID", "Station_Name",
  "Battery_Voltage", "AirTemp_C", "RH", "Barometric_Pressure",
  "Epithermal_Neutron_counts", "Thermal_Neutron_counts", "Blw_Grnd_Epithermal_Neutron_counts",
  "SoilTemp_C_5cm", "SoilTemp_C_10cm", "SoilTemp_C_20cm", "SoilTemp_C_50cm", "SoilTemp_C_100cm",
  "WaterCont_5cm_m3m3", "WaterCont_10cm_m3m3", "WaterCont_20cm_m3m3",
  "WaterCont_50cm_m3m3", "WaterCont_100cm_m3m3",
  "Rain_cm", "Dewpoint_C", "DataFlag",
  "Wind_Speed", "Wind_Direction", "Solar_Radiation"
)

# Unit helpers
f_to_c    <- function(f) round((as.numeric(f) - 32) * (5/9), 4)
in_to_cm  <- function(i) round(as.numeric(i) * 2.54, 4)

# Station list: use begin dates from metadata/nwcc_station_metadata.csv
stations <- list(
  list(id = "556:CO:SNTL",  name = "Kiln",           begin = as.Date("1979-10-01")),
  list(id = "1101:CO:SNTL", name = "Chapman Tunnel",  begin = as.Date("2007-09-01")),
  list(id = "1326:CO:SNTL", name = "Castle Peak",     begin = as.Date("2024-09-18"))
)

# ------ DB sync helper ------------------------------------------------

sync_df_to_db <- function(df, con) {
  if (nrow(df) == 0) return(invisible(NULL))
  master_cols <- dbListFields(con, "measurements")
  # Add any missing columns as NA
  for (col in master_cols) {
    if (!col %in% names(df)) df[[col]] <- NA
  }
  df <- df[, master_cols]
  dbWriteTable(con, "staging", df, overwrite = TRUE)
  cols_list <- paste(sprintf('"%s"', master_cols), collapse = ", ")
  dbExecute(con, sprintf(
    'INSERT OR REPLACE INTO measurements (%s) SELECT %s FROM staging',
    cols_list, cols_list
  ))
  dbRemoveTable(con, "staging")
}

# ------ API fetch helper for one station + one year chunk -------------

fetch_year_chunk <- function(station_id, start_dt, end_dt) {
  resp <- tryCatch(
    GET(url = BASE_URL, query = list(
      stationTriplets       = station_id,
      elements              = ELEMENTS,
      duration              = "HOURLY",
      beginDate             = format(start_dt, "%Y-%m-%d"),
      endDate               = format(end_dt,   "%Y-%m-%d"),
      returnFlags           = "false",
      returnOriginalValues  = "false",
      returnSuspectData     = "false"
    ), timeout(60)),
    error = function(e) NULL
  )
  if (is.null(resp) || status_code(resp) != 200) return(NULL)
  content(resp, "text", encoding = "UTF-8")
}

# ------ Transform one JSON response into master-schema data frame -----

transform_chunk <- function(json_text, station_id, station_name) {
  json_res <- tryCatch(fromJSON(json_text, flatten = TRUE), error = function(e) NULL)
  if (is.null(json_res) || !is.data.frame(json_res) || !"data" %in% names(json_res)) return(NULL)

  raw_data <- tryCatch(json_res$data[[1]], error = function(e) NULL)
  if (is.null(raw_data) || nrow(raw_data) == 0) return(NULL)

  # Parse each element into a named data frame, then wide-join
  frames <- list()
  for (i in seq_len(nrow(raw_data))) {
    elem_code <- raw_data$stationElement.elementCode[i]
    vals      <- raw_data$values[[i]]
    if (!is.null(vals) && nrow(vals) > 0 && "value" %in% names(vals)) {
      frames[[elem_code]] <- vals %>%
        select(date, value) %>%
        rename(!!elem_code := value) %>%
        mutate(!!elem_code := as.numeric(.data[[elem_code]]))
    }
  }

  if (length(frames) == 0) return(NULL)

  merged <- reduce(frames, full_join, by = "date") %>% arrange(date)

  get_col <- function(df, col) if (col %in% names(df)) df[[col]] else NA_real_

  final <- merged %>%
    mutate(temp_dt = ymd_hm(date)) %>%
    filter(!is.na(temp_dt)) %>%
    mutate(
      Date_UTC     = as.character(as_date(temp_dt)),
      Time_UTC     = format(temp_dt, "%H:%M:%S"),
      Station_ID   = station_id,
      Station_Name = station_name,
      AirTemp_C    = f_to_c(get_col(., "TOBS")),
      RH           = get_col(., "RHUM"),
      Dewpoint_C   = f_to_c(get_col(., "DPTP")),
      Rain_cm      = in_to_cm(get_col(., "PRCP")),
      Barometric_Pressure              = get_col(., "PRES"),
      Battery_Voltage                  = NA_real_,
      Epithermal_Neutron_counts        = NA_real_,
      Thermal_Neutron_counts           = NA_real_,
      Blw_Grnd_Epithermal_Neutron_counts = NA_real_,
      SoilTemp_C_5cm   = NA_real_, SoilTemp_C_10cm  = NA_real_,
      SoilTemp_C_20cm  = NA_real_, SoilTemp_C_50cm  = NA_real_,
      SoilTemp_C_100cm = NA_real_,
      WaterCont_5cm_m3m3   = NA_real_, WaterCont_10cm_m3m3  = NA_real_,
      WaterCont_20cm_m3m3  = NA_real_, WaterCont_50cm_m3m3  = NA_real_,
      WaterCont_100cm_m3m3 = NA_real_,
      Wind_Speed      = NA_real_,
      Wind_Direction  = NA_real_,
      Solar_Radiation = NA_real_,
      DataFlag        = "Normal"
    ) %>%
    select(all_of(MASTER_COLS))

  return(final)
}

# ------ Main loop -----------------------------------------------------

message("=== NWCC Historical Backfill ===")
message(sprintf("Started: %s\n", Sys.time()))

con <- dbConnect(SQLite(), DB_PATH)

for (stn in stations) {
  message(sprintf("--- Station: %s (%s) ---", stn$name, stn$id))
  message(sprintf("    Begin date: %s", stn$begin))

  # Find what we already have so we don't re-fetch
  existing <- dbGetQuery(con, sprintf(
    "SELECT MIN(Date_UTC) as min_d, MAX(Date_UTC) as max_d, COUNT(*) as n
     FROM measurements WHERE Station_ID = '%s'", stn$id
  ))
  message(sprintf("    In DB now : %s to %s (%d rows)",
                  existing$min_d, existing$max_d, existing$n))

  # Determine fetch range — start from station begin, end today
  fetch_start <- stn$begin
  fetch_end   <- today()

  years <- seq(year(fetch_start), year(fetch_end))
  total_synced <- 0L

  for (yr in years) {
    chunk_start <- max(fetch_start, as.Date(sprintf("%d-01-01", yr)))
    chunk_end   <- min(fetch_end,   as.Date(sprintf("%d-12-31", yr)))
    if (chunk_start > chunk_end) next

    message(sprintf("  -> %d: %s to %s ...", yr, chunk_start, chunk_end), appendLF = FALSE)

    json_text <- fetch_year_chunk(stn$id, chunk_start, chunk_end)

    if (is.null(json_text)) {
      message(" [no response / error]")
      Sys.sleep(5)
      next
    }

    df <- transform_chunk(json_text, stn$id, stn$name)

    if (is.null(df) || nrow(df) == 0) {
      message(" [no data]")
      Sys.sleep(2)
      next
    }

    sync_df_to_db(df, con)
    total_synced <- total_synced + nrow(df)
    message(sprintf(" synced %d rows", nrow(df)))

    # Polite pause between requests — AWDB has rate limits
    Sys.sleep(2)
  }

  message(sprintf("    Done. Total rows synced for %s: %d\n", stn$name, total_synced))
}

dbDisconnect(con)

message(sprintf("=== Backfill complete: %s ===", Sys.time()))
message("Run rfv_air_temp_2025.R to regenerate the comparison plot.")
