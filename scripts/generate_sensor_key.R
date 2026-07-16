#!/usr/bin/env Rscript
# Derive flat config/sensor_key.json from the nested config/master_key.json.
# Run after any edit to master_key.json so the live pipeline (which reads the
# flat form) stays in sync.

suppressPackageStartupMessages({ library(jsonlite); library(here) })

# NOTE: master_key.json's top-level keys are human-readable display names
# ("Brush Creek", "Glassier Ranch", ...) — a THIRD naming scheme for the same
# 8 LI-COR stations, alongside their canonical Station_Name shorthand codes
# (RFBRC, RFGLR, ... in config/station_key.json, matched here via each
# station's "shorthand" field). Do not use these display names as
# Station_Name anywhere in the pipeline or DB — they exist for documentation
# purposes only.
mk <- read_json(here("config", "master_key.json"), simplifyVector = FALSE)
flat <- list()
conflicts <- c()

for (station in names(mk)) {
  for (logger_id in names(mk[[station]]$loggers)) {
    sensors <- mk[[station]]$loggers[[logger_id]]$sensors
    for (sid in names(sensors)) {
      v <- sensors[[sid]]$variable
      if (is.null(v) || is.na(v)) next
      if (!is.null(flat[[sid]]) && flat[[sid]] != v) {
        conflicts <- c(conflicts,
          sprintf("%s: %s (existing) vs %s (%s/%s)", sid, flat[[sid]], v, station, logger_id))
      }
      flat[[sid]] <- as.character(v)
    }
  }
}

if (length(conflicts) > 0) {
  message("WARNING: same serial mapped to different variables across stations:")
  for (c in conflicts) message("  ", c)
}

write_json(flat, here("config", "sensor_key.json"),
           pretty = TRUE, auto_unbox = TRUE)
message(sprintf("Wrote %d entries to config/sensor_key.json", length(flat)))
