#!/usr/bin/env Rscript
# Combine yearly RFSPV_backfill CSVs into one timestamp-keyed wide tibble.
# Retains both canonical (a-set) and _B (b-set) columns so Phase 5 can pick.

suppressPackageStartupMessages({
  library(readr); library(dplyr); library(tidyr); library(stringr)
  library(purrr); library(lubridate); library(jsonlite); library(tibble)
})

BACKFILL_DIR <- "RFSPV_backfill"
OUT_PATH     <- file.path(BACKFILL_DIR, "rfspv_backfill_combined.csv")
SENSOR_KEY   <- jsonlite::read_json("config/sensor_key.json", simplifyVector = TRUE)

f_to_c  <- function(x) (as.numeric(x) - 32) * (5 / 9)
in_to_cm <- function(x) as.numeric(x) * 2.54

# Parse one HOBOware header token into structured fields.
parse_token <- function(raw) {
  m <- str_match(
    raw,
    "^([^()]+?)\\s*\\(([^ ]+)\\s+(\\d+):(\\d+)-(\\d+)\\),([^,]*),([^,]*)(?:,(.*))?$"
  )
  list(
    measure = str_trim(m[, 2]),
    serial  = m[, 5],
    channel = m[, 6],
    units   = str_trim(m[, 7]),
    label   = str_trim(ifelse(is.na(m[, 9]), "", m[, 9])),
    key     = paste0(m[, 5], "-", m[, 6])
  )
}

# Decide canonical column name for a given header token.
canonical_name <- function(tok) {
  mapped <- SENSOR_KEY[[tok$key]]
  if (is.null(mapped) || is.na(mapped) || mapped == "") {
    return(NA_character_)
  }
  mapped
}

read_year_file <- function(f) {
  yr <- str_match(basename(f), "^([0-9]{4})_")[, 2]
  message(sprintf("Reading %s (year %s)", basename(f), yr))

  hdr_line <- read_lines(f, n_max = 1)
  hdr <- scan(text = hdr_line, what = character(), sep = ",", quote = "\"", quiet = TRUE)
  n_cols <- length(hdr)

  # All columns read as character; we coerce per-column after rename.
  raw <- read_csv(f, col_types = cols(.default = col_character()), progress = FALSE)
  if (ncol(raw) != n_cols) {
    stop(sprintf("Column count mismatch in %s: header=%d data=%d", f, n_cols, ncol(raw)))
  }

  # Column 1 = Line#, column 2 = Date. Skip Line#, parse Date.
  date_col_idx <- 2
  ts <- raw[[date_col_idx]] %>%
    str_replace("\\s*\\+0000$", "") %>%
    parse_date_time(orders = "mdy HMS", tz = "UTC")

  if (any(is.na(ts))) {
    n_bad <- sum(is.na(ts))
    warning(sprintf("  %d unparseable timestamps in %s", n_bad, basename(f)))
  }

  out <- tibble(timestamp = ts)

  meas_idx <- setdiff(seq_len(n_cols), c(1, date_col_idx))
  per_col <- list()  # canonical_name -> list of numeric vectors

  for (i in meas_idx) {
    tok <- parse_token(hdr[i])
    canon <- canonical_name(tok)
    if (is.na(canon)) {
      message(sprintf("  [skip] no sensor_key mapping for col %d (%s)", i, hdr[i]))
      next
    }
    # Drop columns where HOBOware exported with units == "units" (raw-voltage placeholder).
    if (identical(tok$units, "units")) {
      message(sprintf("  [skip] %s col %d has units='units' (placeholder), dropping", canon, i))
      next
    }
    vals <- suppressWarnings(as.numeric(raw[[i]]))
    # Per-column unit conversion (based on the units string in the header).
    if (tok$units == "°F") {
      vals <- f_to_c(vals)
    } else if (tok$units == "in") {
      vals <- in_to_cm(vals)
    } else if (tok$units %in% c("m³/m³", "%", "V", "mV", "")) {
      # leave as-is
    } else {
      message(sprintf("  [warn] unrecognised units '%s' on %s; leaving values unchanged",
                      tok$units, canon))
    }
    per_col[[canon]] <- c(per_col[[canon]], list(vals))
  }

  # Collapse duplicates that mapped to the same canonical name (row-wise mean, na.rm=TRUE).
  collapsed <- imap(per_col, function(vlist, name) {
    if (length(vlist) == 1L) return(vlist[[1]])
    M <- do.call(cbind, vlist)
    apply(M, 1, function(r) {
      r2 <- r[!is.na(r)]
      if (length(r2) == 0) NA_real_ else mean(r2)
    })
  })

  out <- bind_cols(out, as_tibble(collapsed))
  out <- out %>% filter(!is.na(timestamp))
  out
}

files <- list.files(BACKFILL_DIR, pattern = "^[0-9]{4}_RFSPV.*\\.csv$", full.names = TRUE)
all_years <- map(files, read_year_file)

# bind_rows fills missing columns with NA across years with different sensor sets.
combined <- bind_rows(all_years) %>% arrange(timestamp)

# Cross-year boundary timestamps: collapse same-timestamp rows (prefer non-NA, mean if both).
combined <- combined %>%
  group_by(timestamp) %>%
  summarise(across(everything(), function(v) {
    v2 <- v[!is.na(v)]
    if (length(v2) == 0) NA_real_ else mean(v2)
  }), .groups = "drop")

# Build final schema.
combined <- combined %>%
  mutate(
    Date_UTC = as.character(as.Date(timestamp)),
    Time_UTC = format(timestamp, "%H:%M:%S"),
    Station_ID = "RFSPV",
    Station_Name = "RFSPV"
  ) %>%
  select(Date_UTC, Time_UTC, Station_ID, Station_Name, everything(), -timestamp)

write_csv(combined, OUT_PATH)
message(sprintf("\nWrote %s  (%d rows, %d cols)", OUT_PATH, nrow(combined), ncol(combined)))

# Quick coverage report by year x column.
cov <- combined %>%
  mutate(year = substr(Date_UTC, 1, 4)) %>%
  pivot_longer(-c(Date_UTC, Time_UTC, Station_ID, Station_Name, year),
               names_to = "col", values_to = "v") %>%
  group_by(year, col) %>%
  summarise(n_nonnull = sum(!is.na(v)), n = n(), .groups = "drop") %>%
  mutate(pct = round(100 * n_nonnull / n, 1)) %>%
  arrange(col, year)
write_csv(cov, "metadata/rfspv_backfill_combined_coverage.csv")
message("Wrote metadata/rfspv_backfill_combined_coverage.csv")
