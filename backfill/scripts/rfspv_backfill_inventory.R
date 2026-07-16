suppressPackageStartupMessages({
  library(readr); library(dplyr); library(tidyr); library(stringr); library(purrr); library(jsonlite)
})

backfill_dir <- "RFSPV_backfill"
files <- list.files(backfill_dir, pattern = "^[0-9]{4}_RFSPV.*\\.csv$", full.names = TRUE)

parse_header_token <- function(year, idx, raw) {
  # Examples:
  #  Water Content (S-SMC 10955846:10928526-1),m³/m³,Spring Valley,2in b
  #  Temperature (S-TMB 21535701:10935206-1),°F,Spring Valley 2022,Soil Temperature
  #  Snow Depth (RAW-V-5 21535701:21519379-1),in,Spring Valley 2022
  m <- str_match(
    raw,
    "^([^()]+?)\\s*\\(([^ ]+)\\s+(\\d+):(\\d+)-(\\d+)\\),([^,]*),([^,]*)(?:,(.*))?$"
  )
  # Columns: 1=full, 2=measure, 3=sensor_type, 4=logger, 5=serial, 6=channel, 7=units, 8=location, 9=label
  measure  <- str_trim(m[, 2])
  sensor_type <- m[, 3]
  logger   <- m[, 4]
  serial   <- m[, 5]
  channel  <- m[, 6]
  units    <- str_trim(m[, 7])
  location <- str_trim(m[, 8])
  label    <- str_trim(ifelse(is.na(m[, 9]), "", m[, 9]))
  label_lc <- str_to_lower(label)

  ab <- case_when(
    str_detect(label_lc, "(^|[^a-z])b([^a-z]|$)") | str_detect(label_lc, "_b\\b") | str_detect(label_lc, "\\(b\\)") ~ "b",
    str_detect(label_lc, "(^|[^a-z])a([^a-z]|$)") | str_detect(label_lc, "_a\\b") | str_detect(label_lc, "\\(a\\)") ~ "a",
    TRUE ~ NA_character_
  )

  depth_in <- str_match(label_lc, "(\\d+)\\s*in")[, 2]
  depth_cm <- suppressWarnings(round(as.numeric(depth_in) * 2.54))

  role <- case_when(
    measure == "Water Content" & !is.na(depth_cm) ~ paste0("WC_", depth_cm, "cm"),
    measure == "Water Content" ~ "WC_unknown",
    measure == "Temperature" & str_detect(label_lc, "soil") ~ "SoilTemp",
    measure == "Temperature" & str_detect(label_lc, "air") ~ "AirTemp",
    measure == "Temperature" ~ "Temp_unknown",
    measure == "RH" ~ "RH",
    measure == "Dew Point" ~ "Dewpoint",
    measure == "Rain" ~ "Rain",
    measure == "Snow Depth" ~ "SnowDepth",
    measure == "Battery" | str_detect(measure, regex("battery", ignore_case = TRUE)) ~ "Battery",
    TRUE ~ paste0("OTHER:", measure)
  )

  tibble(
    year = year, col_idx = idx, raw_header = raw,
    measure = measure, sensor_type = sensor_type,
    logger_serial = logger, sensor_serial = serial,
    channel = channel, units = units, location = location,
    label = label, role = role, depth_cm = depth_cm, ab = ab
  )
}

inv <- map_dfr(files, function(f) {
  yr  <- str_match(basename(f), "^([0-9]{4})_")[, 2]
  hdr <- read_lines(f, n_max = 1)
  toks <- scan(text = hdr, what = character(), sep = ",", quote = "\"", quiet = TRUE)
  # Skip Line# and Date
  data_idx <- seq_along(toks)[-c(1, 2)]
  map_dfr(data_idx, ~ parse_header_token(yr, .x, toks[.x]))
})

dir.create("metadata", showWarnings = FALSE)
write_csv(inv, "metadata/rfspv_backfill_sensor_inventory.csv")

# Summary by (role, ab, sensor_serial) per year
summary_tbl <- inv %>%
  count(year, role, ab, sensor_serial, units, name = "n_cols") %>%
  arrange(role, ab, sensor_serial, year)
write_csv(summary_tbl, "metadata/rfspv_backfill_sensor_summary.csv")

# Cross check against existing sensor_key.json
sk <- jsonlite::read_json("config/sensor_key.json")
sk_df <- tibble(key = names(sk), mapped_to = unlist(sk))
inv_keys <- inv %>%
  mutate(key = paste0(sensor_serial, "-", channel)) %>%
  distinct(sensor_serial, channel, key, role, ab, units) %>%
  left_join(sk_df, by = "key")
write_csv(inv_keys, "metadata/rfspv_backfill_sensor_key_crosscheck.csv")

cat("\n==== HEADER COLUMN COUNT BY YEAR ====\n")
print(inv %>% count(year, name = "header_cols"))

cat("\n==== UNIQUE SENSOR_SERIAL x ROLE x AB ====\n")
print(inv %>% distinct(sensor_serial, channel, role, ab, units) %>% arrange(role, ab, sensor_serial), n = 100)

cat("\n==== ROLE x AB x YEAR (number of header columns; >1 means duplicates within file) ====\n")
print(
  inv %>% count(role, ab, year) %>% pivot_wider(names_from = year, values_from = n, values_fill = 0),
  n = 100
)

cat("\n==== SENSOR_KEY CROSSCHECK (mapped_to == NA means missing from config/sensor_key.json) ====\n")
print(inv_keys %>% arrange(role, ab, sensor_serial), n = 100)
