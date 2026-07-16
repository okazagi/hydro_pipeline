library(dplyr)
library(readr)
library(lubridate)
library(here)
library(tidyr)

in_file <- here("independence_pass", "IndePass_munged_1_28_24_schema.csv")
df <- read_csv(in_file, show_col_types = FALSE)

cat("================================================================\n")
cat("INDEPENDENCE PASS — DATA INTEGRITY REPORT\n")
cat("Source:", basename(in_file), "\n")
cat("Rows:", nrow(df), " Cols:", ncol(df), "\n")
cat("================================================================\n\n")

# Reconstruct timestamp
df <- df %>%
  mutate(ts = ymd_hms(paste(Date_UTC, Time_UTC), tz = "UTC")) %>%
  arrange(ts)

# ---------------- 1. TIME COVERAGE & CONTINUITY ----------------
cat("## 1. Time coverage & continuity\n")
cat(sprintf("Range:          %s  ->  %s\n", min(df$ts), max(df$ts)))
cat(sprintf("Span:           %.1f years\n",
            as.numeric(difftime(max(df$ts), min(df$ts), units = "days")) / 365.25))

# Duplicate timestamps
dups <- df %>% count(ts) %>% filter(n > 1)
cat(sprintf("Duplicate ts:   %d distinct timestamps repeated\n", nrow(dups)))
if (nrow(dups) > 0) {
  cat("  First 5 dupes:\n")
  print(head(dups, 5))
}

# Gap analysis
gaps <- df %>%
  mutate(delta_hr = as.numeric(difftime(ts, lag(ts), units = "hours"))) %>%
  filter(!is.na(delta_hr))

cat(sprintf("\nTimestep summary (hours between consecutive records):\n"))
print(summary(gaps$delta_hr))

cat("\nModal step:\n")
print(gaps %>% count(delta_hr, sort = TRUE) %>% head(6))

# Gaps > 3h — assume nominal step is 1–2h
big_gaps <- gaps %>%
  filter(delta_hr > 3) %>%
  mutate(gap_days = round(delta_hr / 24, 2)) %>%
  select(prev_end = ts, delta_hr, gap_days) %>%
  arrange(desc(delta_hr))

cat(sprintf("\nGaps > 3 hours: %d\n", nrow(big_gaps)))
if (nrow(big_gaps) > 0) {
  cat("Top 15 largest gaps:\n")
  print(head(big_gaps, 15))
}

# ---------------- 2. MISSING-VALUE AUDIT ----------------
cat("\n## 2. Missing values per column\n")
miss <- df %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(everything(), names_to = "col", values_to = "n_missing") %>%
  mutate(pct_missing = round(100 * n_missing / nrow(df), 2)) %>%
  arrange(desc(n_missing))
print(miss, n = Inf)

# Fully-missing rows (no sensor data at all)
sensor_cols <- c("AirTemp_C", "RH", "Dewpoint_C",
                 "WaterCont_5cm_m3m3", "WaterCont_20cm_m3m3", "WaterCont_50cm_m3m3",
                 "SoilTemp_C_20cm", "Wind_Speed", "Wind_Direction",
                 "Rain_cm", "Wind_Gust_ms", "SnowDepth_cm")
all_na <- df %>%
  rowwise() %>%
  mutate(n_na = sum(is.na(c_across(all_of(sensor_cols))))) %>%
  ungroup() %>%
  filter(n_na == length(sensor_cols))
cat(sprintf("\nRows with EVERY sensor column NA: %d\n", nrow(all_na)))

# ---------------- 3. OUT-OF-RANGE / PHYSICALLY IMPLAUSIBLE ----------------
cat("\n## 3. Out-of-range / implausible values\n")
# Independence Pass, CO — elev ~3,687 m. Realistic sensor ranges:
limits <- tribble(
  ~col,                   ~lo,   ~hi,
  "AirTemp_C",            -40,   30,
  "RH",                   0,     100,
  "Dewpoint_C",           -50,   25,
  "SoilTemp_C_20cm",      -20,   30,
  "WaterCont_5cm_m3m3",   0,     0.6,
  "WaterCont_20cm_m3m3",  0,     0.6,
  "WaterCont_50cm_m3m3",  0,     0.6,
  "Wind_Speed",           0,     50,
  "Wind_Gust_ms",         0,     70,
  "Wind_Direction",       0,     360,
  "Rain_cm",              0,     15,
  "SnowDepth_cm",         0,     600
)

oor_rows <- list()
for (i in seq_len(nrow(limits))) {
  cc <- limits$col[i]; lo <- limits$lo[i]; hi <- limits$hi[i]
  v <- df[[cc]]
  n_lo <- sum(v < lo, na.rm = TRUE)
  n_hi <- sum(v > hi, na.rm = TRUE)
  if (n_lo + n_hi > 0) {
    mn <- suppressWarnings(min(v, na.rm = TRUE))
    mx <- suppressWarnings(max(v, na.rm = TRUE))
    oor_rows[[cc]] <- tibble(column = cc, limit_lo = lo, limit_hi = hi,
                             n_below = n_lo, n_above = n_hi,
                             observed_min = mn, observed_max = mx)
  }
}
oor <- bind_rows(oor_rows)
if (nrow(oor) == 0) {
  cat("No values outside expected physical ranges.\n")
} else {
  cat("Columns with values outside expected sensor ranges:\n")
  print(oor, n = Inf)
}

# Dewpoint > AirTemp is physically impossible
bad_dp <- df %>%
  filter(!is.na(Dewpoint_C) & !is.na(AirTemp_C) & Dewpoint_C > AirTemp_C + 0.5)
cat(sprintf("\nDewpoint > AirTemp (+0.5C tolerance): %d rows\n", nrow(bad_dp)))
if (nrow(bad_dp) > 0) {
  cat("Sample:\n")
  print(bad_dp %>% select(ts, AirTemp_C, Dewpoint_C, RH) %>% head(10))
}

# ---------------- 4. CONSTANT / STUCK-SENSOR DETECTION ----------------
cat("\n## 4. Stuck-sensor runs (same value >= 24 consecutive records)\n")
stuck_runs <- function(x, min_len = 24) {
  r <- rle(x)
  idx <- which(r$lengths >= min_len & !is.na(r$values))
  if (length(idx) == 0) return(tibble(value = numeric(0), length = integer(0)))
  tibble(value = r$values[idx], length = r$lengths[idx]) %>% arrange(desc(length))
}
for (cc in sensor_cols) {
  s <- stuck_runs(df[[cc]])
  if (nrow(s) > 0) {
    cat(sprintf("  %s: %d runs, longest = %d records at value %.4g\n",
                cc, nrow(s), s$length[1], s$value[1]))
  }
}

# ---------------- 5. PER-COLUMN SUMMARY ----------------
cat("\n## 5. Column summaries (non-NA)\n")
num_cols <- sensor_cols
stats <- lapply(num_cols, function(cc) {
  v <- df[[cc]]
  v <- v[!is.na(v)]
  if (length(v) == 0) return(NULL)
  tibble(col = cc, n = length(v),
         min = min(v), p01 = quantile(v, .01),
         median = median(v), mean = mean(v),
         p99 = quantile(v, .99), max = max(v))
}) %>% bind_rows()
print(stats, n = Inf)

cat("\n================ END REPORT ================\n")
