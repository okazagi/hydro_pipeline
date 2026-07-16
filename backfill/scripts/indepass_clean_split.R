library(dplyr)
library(readr)
library(lubridate)
library(here)
library(zoo)

in_file <- here("independence_pass", "IndePass_munged_1_28_24_schema.csv")
out_dir <- here("independence_pass")

df <- read_csv(in_file, show_col_types = FALSE) %>%
  mutate(ts = ymd_hms(paste(Date_UTC, Time_UTC), tz = "UTC")) %>%
  arrange(ts)

# ----- 1. Sentinel values (from QC report) -----
# These exact magic numbers appeared as long stuck runs in the source data.
sentinel_equal <- list(
  AirTemp_C       = -97.44,
  SoilTemp_C_20cm = -94.79,
  SnowDepth_cm    = -888.9
)

flag <- rep("", nrow(df))

for (cc in names(sentinel_equal)) {
  hit <- !is.na(df[[cc]]) & abs(df[[cc]] - sentinel_equal[[cc]]) < 0.01
  df[[cc]][hit] <- NA_real_
  flag[hit] <- paste0(flag[hit], ";sentinel:", cc)
}

# ----- 2. Physical-range clipping (out-of-range -> NA) -----
limits <- list(
  AirTemp_C            = c(-40, 30),
  RH                   = c(0, 100),
  Dewpoint_C           = c(-50, 25),
  SoilTemp_C_20cm      = c(-20, 30),
  WaterCont_5cm_m3m3   = c(0, 0.6),
  WaterCont_20cm_m3m3  = c(0, 0.6),
  WaterCont_50cm_m3m3  = c(0, 0.6),
  Wind_Speed           = c(0, 50),
  Wind_Gust_ms         = c(0, 70),
  Wind_Direction       = c(0, 360),
  Rain_cm              = c(0, 15),
  SnowDepth_cm         = c(0, 600)
)
for (cc in names(limits)) {
  lo <- limits[[cc]][1]; hi <- limits[[cc]][2]
  v <- df[[cc]]
  bad <- !is.na(v) & (v < lo | v > hi)
  df[[cc]][bad] <- NA_real_
  flag[bad] <- paste0(flag[bad], ";oor:", cc)
}

# ----- 3. Stuck RH=0 runs (>= 24 consecutive) are sensor failures -----
rh <- df$RH
r <- rle(!is.na(rh) & rh == 0)
ends <- cumsum(r$lengths)
starts <- ends - r$lengths + 1
for (i in seq_along(r$lengths)) {
  if (r$values[i] && r$lengths[i] >= 24) {
    idx <- starts[i]:ends[i]
    df$RH[idx] <- NA_real_
    flag[idx] <- paste0(flag[idx], ";stuck:RH0")
  }
}

df$DataFlag <- ifelse(flag == "", NA_character_, sub("^;", "", flag))

n_flag <- sum(!is.na(df$DataFlag))
message(sprintf("Rows with cleaning action: %d of %d (%.1f%%)",
                n_flag, nrow(df), 100 * n_flag / nrow(df)))

# ----- 4. Calendar split at 2019-01-01 -----
# Chosen as a clean, unambiguous calendar boundary. Note: the largest data gap
# (216 days) starts 2019-07-02; the "post_2019" segment therefore contains a
# short Jan–Jul 2019 chunk before that outage.
split_ts <- ymd_hms("2019-01-01 00:00:00", tz = "UTC")
pre  <- df %>% filter(ts <  split_ts) %>% select(-ts)
post <- df %>% filter(ts >= split_ts) %>% select(-ts)

pre_path  <- file.path(out_dir, "indepass_pre_2019.csv")
post_path <- file.path(out_dir, "indepass_post_2019.csv")
write_csv(pre,  pre_path)
write_csv(post, post_path)
message(sprintf("pre_2019:  %d rows -> %s", nrow(pre), pre_path))
message(sprintf("post_2019: %d rows -> %s", nrow(post), post_path))

# ----- 5. Interpolated version -----
# Method: time-indexed linear interpolation with a 6-hour maximum gap.
# This is the WMO / USGS-recommended default for hourly meteorological series:
# defensible, non-oscillating, preserves observed values exactly, and refuses
# to invent long stretches of data. Gaps > 6 h remain NA.
#
# Variable-specific handling:
#   - Wind_Direction: interpolated as sin/cos components (circular variable).
#   - Rain_cm: left as-is (no interpolation). Episodic precip cannot be
#     reconstructed from neighboring values — standard hydromet practice.
#   - All-NA columns: skipped.

interp_df <- df %>% arrange(ts)
t_sec <- as.numeric(interp_df$ts)
max_gap_sec <- 6 * 3600  # 6 hours

interp_linear <- function(x, t, maxgap_sec) {
  # Time-aware linear interpolation. zoo::na.approx's `maxgap` is a count of
  # consecutive NAs, which is meaningless for this series (irregular cadence,
  # long all-NA sentinel runs). We interpolate freely, then re-NA every
  # position whose bounding observations are more than maxgap_sec apart.
  if (all(is.na(x))) return(x)
  filled <- suppressWarnings(na.approx(x, x = t, na.rm = FALSE, rule = 1))

  obs <- !is.na(x)
  if (sum(obs) < 2) return(x)

  n <- length(x)
  prev_idx <- rep(NA_integer_, n); nxt_idx <- rep(NA_integer_, n)
  last <- NA_integer_
  for (i in seq_len(n)) { if (obs[i]) last <- i; prev_idx[i] <- last }
  nxt <- NA_integer_
  for (i in seq.int(n, 1L)) { if (obs[i]) nxt <- i; nxt_idx[i] <- nxt }

  na_pos <- which(!obs)
  span <- rep(Inf, length(na_pos))
  ok <- !is.na(prev_idx[na_pos]) & !is.na(nxt_idx[na_pos])
  span[ok] <- t[nxt_idx[na_pos[ok]]] - t[prev_idx[na_pos[ok]]]
  filled[na_pos[span > maxgap_sec]] <- NA_real_
  filled
}

cols_linear <- c("AirTemp_C", "RH", "Dewpoint_C",
                 "SoilTemp_C_20cm",
                 "WaterCont_5cm_m3m3", "WaterCont_20cm_m3m3", "WaterCont_50cm_m3m3",
                 "Wind_Speed", "Wind_Gust_ms",
                 "SnowDepth_cm")

for (cc in cols_linear) {
  interp_df[[cc]] <- interp_linear(interp_df[[cc]], t_sec, max_gap_sec)
}

# Circular interpolation for wind direction via sin/cos components.
if (!all(is.na(interp_df$Wind_Direction))) {
  rad <- interp_df$Wind_Direction * pi / 180
  s <- interp_linear(sin(rad), t_sec, max_gap_sec)
  c_ <- interp_linear(cos(rad), t_sec, max_gap_sec)
  wd <- (atan2(s, c_) * 180 / pi) %% 360
  # Only fill positions that were originally NA and both components interpolated
  na_mask <- is.na(interp_df$Wind_Direction) & !is.na(s) & !is.na(c_)
  interp_df$Wind_Direction[na_mask] <- wd[na_mask]
}

# Clip soil-moisture interpolation back to [0, 0.6] defensive bound.
for (cc in c("WaterCont_5cm_m3m3", "WaterCont_20cm_m3m3", "WaterCont_50cm_m3m3")) {
  v <- interp_df[[cc]]
  interp_df[[cc]] <- ifelse(!is.na(v) & v < 0, 0,
                    ifelse(!is.na(v) & v > 0.6, 0.6, v))
}

# Mark interpolated rows per column in DataFlag (coarse: any-col interpolated)
new_filled <- rowSums(
  !is.na(interp_df[, cols_linear]) & is.na(df[, cols_linear])
) > 0
interp_df$DataFlag <- ifelse(
  new_filled,
  ifelse(is.na(interp_df$DataFlag), "interpolated",
         paste0(interp_df$DataFlag, ";interpolated")),
  interp_df$DataFlag
)

interp_path <- file.path(out_dir, "indepass_interpolated.csv")
write_csv(interp_df %>% select(-ts), interp_path)

# Report fill rates
cat("\nInterpolation fill summary (rows newly filled vs originally NA):\n")
fill_report <- lapply(cols_linear, function(cc) {
  orig_na <- sum(is.na(df[[cc]]))
  new_na  <- sum(is.na(interp_df[[cc]]))
  tibble(col = cc, orig_NA = orig_na, after_NA = new_na,
         filled = orig_na - new_na,
         pct_filled_of_NA = round(100 * (orig_na - new_na) / pmax(orig_na, 1), 1))
}) %>% bind_rows()
print(fill_report)

message(sprintf("interpolated: %d rows -> %s", nrow(interp_df), interp_path))
