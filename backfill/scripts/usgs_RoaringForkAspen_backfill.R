#!/usr/bin/env Rscript

# usgs_RoaringForkAspen_backfill.R
#
# Standalone historical backfill for USGS-09073400 (Roaring Fork River near Aspen, CO).
#   - Discharge   (00060): 2015-01-01 -> today
#   - Gage Height (00065): 2020-10-08 -> today
#
# Requests directly from NWIS in yearly chunks, transforms via the existing
# current_pipeline/usgs_transform.R, and upserts into hydro_data_April.db.
#
# At the end, writes config/usgs_timestamps/09073400_last_timestamp.txt so the
# regular incremental pipeline resumes from the backfill's max timestamp instead
# of defaulting to 2023-01-01.

suppressPackageStartupMessages({
  library(tidyverse)
  library(dataRetrieval)
  library(DBI)
  library(RSQLite)
  library(here)
  library(lubridate)
})

# Pull in process_usgs_data() from the pipeline transform WITHOUT triggering its
# `if (!interactive())` main block (which would re-transform every raw file in
# data_raw/). We evaluate the file in an isolated env where interactive() is
# masked to TRUE, then lift out the function. process_usgs_data() keeps this env
# as its closure, so its references to MASTER_COLS / site_config still resolve.
.transform_env <- new.env(parent = globalenv())
.transform_env$interactive <- function() TRUE
sys.source(here("current_pipeline", "usgs_transform.R"), envir = .transform_env)
process_usgs_data <- .transform_env$process_usgs_data

# ---------------- CONFIG ---------------- #
SITE_ID       <- "USGS-09073400"
CLEAN_SITE_ID <- "09073400"
STATION_NAME  <- "usgs_RoaringForkAspen"
DB_PATH       <- here("hydro_data_April.db")
TIMESTAMP_DIR <- here("config", "usgs_timestamps")

DISCH_START <- as.Date("2015-01-01")
GH_START    <- as.Date("2020-10-08")
END_DATE    <- today()

# ---------------- HELPERS ---------------- #
sync_to_db <- function(clean_file) {
  con <- dbConnect(RSQLite::SQLite(), DB_PATH)
  on.exit(dbDisconnect(con), add = TRUE)

  df <- read_csv(clean_file, show_col_types = FALSE,
                 col_types = cols(Station_ID = "c", Date_UTC = "c", Time_UTC = "c"))
  if (nrow(df) == 0) return(invisible(0L))

  df <- df %>%
    mutate(temp_dt = parse_date_time(paste(Date_UTC, Time_UTC),
                                     orders = c("Ymd HMS", "Ymd HM"))) %>%
    filter(!is.na(temp_dt)) %>%
    mutate(Date_UTC = as.character(as.Date(temp_dt)),
           Time_UTC = format(temp_dt, "%H:%M:%S")) %>%
    select(-temp_dt)

  # Align CSV to DB schema (DB is the source of truth for column order/set)
  master_cols <- dbListFields(con, "measurements")
  for (col in setdiff(master_cols, names(df))) df[[col]] <- NA
  df <- df[, master_cols]

  dbWriteTable(con, "staging", df, overwrite = TRUE)
  cols_list <- paste(sprintf('"%s"', master_cols), collapse = ", ")
  n <- dbExecute(con, sprintf(
    'INSERT OR REPLACE INTO measurements (%s) SELECT %s FROM staging',
    cols_list, cols_list))
  dbRemoveTable(con, "staging")
  invisible(n)
}

# ---------------- MAIN ---------------- #
message(sprintf("--- Backfilling %s (%s) ---", STATION_NAME, SITE_ID))
message(sprintf("Discharge:   %s -> %s", DISCH_START, END_DATE))
message(sprintf("Gage Height: %s -> %s", GH_START, END_DATE))
message(sprintf("DB:          %s", DB_PATH))

# Sanity check: the transform looks up station_name via usgs_sites.csv
sites <- read_csv(here("config", "usgs_sites.csv"), show_col_types = FALSE)
if (!SITE_ID %in% sites$site_id) {
  stop(sprintf("Station %s not in config/usgs_sites.csv; add it before running.", SITE_ID))
}

years  <- seq(year(DISCH_START), year(END_DATE))
max_ts <- NULL

for (yr in years) {
  chunk_start <- max(DISCH_START, as.Date(sprintf("%d-01-01", yr)))
  chunk_end   <- min(END_DATE,   as.Date(sprintf("%d-12-31", yr)))
  if (chunk_start > chunk_end) next

  p_codes <- "00060"
  if (chunk_end >= GH_START) p_codes <- c(p_codes, "00065")

  message(sprintf("\n[%d] %s -> %s (params: %s)",
                  yr, chunk_start, chunk_end, paste(p_codes, collapse = ",")))

  tryCatch({
    raw_data <- readNWISuv(
      siteNumbers = CLEAN_SITE_ID,
      parameterCd = p_codes,
      startDate   = format(chunk_start, "%Y-%m-%d"),
      endDate     = format(chunk_end, "%Y-%m-%d")
    )

    if (nrow(raw_data) == 0) {
      message("  -> No data")
      Sys.sleep(2)
      next
    }

    if (!dir.exists(here("data_raw"))) dir.create(here("data_raw"), recursive = TRUE)
    raw_file <- here("data_raw",
                     sprintf("USGS_%s_%d_backfill.csv", CLEAN_SITE_ID, yr))
    write_csv(renameNWISColumns(raw_data), raw_file)

    process_usgs_data(raw_file)
    n_synced <- sync_to_db(here("data_clean", paste0(STATION_NAME, ".csv")))

    yr_max <- max(raw_data$dateTime, na.rm = TRUE)
    if (is.null(max_ts) || yr_max > max_ts) max_ts <- yr_max

    message(sprintf("  -> Fetched %d rows, upserted %d", nrow(raw_data), n_synced))
    Sys.sleep(3)
  }, error = function(e) {
    message(sprintf("  !! Error in %d: %s", yr, e$message))
    Sys.sleep(5)
  })
}

# Hand the baton to the incremental pipeline
if (!is.null(max_ts)) {
  if (!dir.exists(TIMESTAMP_DIR)) dir.create(TIMESTAMP_DIR, recursive = TRUE)
  ts_file <- file.path(TIMESTAMP_DIR, paste0(CLEAN_SITE_ID, "_last_timestamp.txt"))
  writeLines(format(max_ts, "%Y-%m-%dT%H:%M:%SZ"), ts_file)
  message(sprintf("\nLast timestamp written: %s", ts_file))
}

message("\n--- Backfill complete ---")
