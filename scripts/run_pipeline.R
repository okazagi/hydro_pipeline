#!/usr/bin/env Rscript

# master_pipeline.R
# Orchestrates the retrieval and transformation of hydrological data.
# Designed for use with cron or manual runs with a start date.

suppressPackageStartupMessages({
  library(here)
  library(lubridate)
  library(purrr)
  library(readr)
})

# 1. Setup Logging and Arguments
args <- commandArgs(trailingOnly = TRUE)
manual_start <- if (length(args) > 0) args[1] else NULL

LOG_FILE <- here("pipeline_execution.log")
log_msg <- function(msg) {
  formatted_msg <- sprintf("[%s] %s", now(), msg)
  cat(formatted_msg, "\n", file = LOG_FILE, append = TRUE)
  message(formatted_msg)
}

if (!is.null(manual_start)) {
  log_msg(sprintf("--- Starting Pipeline (MANUAL START: %s) ---", manual_start))
} else {
  log_msg("--- Starting Pipeline (INCREMENTAL MODE) ---")
}

# 2. Helper to run R scripts via system call
run_script <- function(script_path, extra_args = NULL) {
  full_path <- here(script_path)
  log_msg(sprintf("Running script: %s %s", script_path, if(is.null(extra_args)) "" else extra_args))
  
  # Pass the script path followed by any arguments
  args_to_pass <- c(full_path, extra_args)
  result <- system2("Rscript", args = args_to_pass, stdout = TRUE, stderr = TRUE)
  
  if (!is.null(attr(result, "status")) && attr(result, "status") != 0) {
    log_msg(sprintf("ERROR in %s: %s", script_path, paste(result, collapse = "\n")))
    return(FALSE)
  }
  
  log_msg(sprintf("Successfully completed: %s", script_path))
  return(TRUE)
}

# 3. PHASE 1: RETRIEVAL
log_msg("PHASE 1: Data Retrieval")

# A. LI-COR
licor_req_success <- run_script("scripts/licor_request.R", manual_start)

# B. NWCC
nwcc_req_success <- run_script("scripts/nwcc_request.R", manual_start)

# C. HADS (Note: HADS only has 7 days of data, ignoring manual start)
hads_req_success <- run_script("scripts/hads_retrieval.R")

# D. USGS
usgs_req_success <- run_script("scripts/usgs_retrieve.R", manual_start)

# 4. PHASE 2: TRANSFORMATION
log_msg("PHASE 2: Data Transformation")

# A. NWCC Transform
if (nwcc_req_success) {
  run_script("scripts/nwcc_transform.R")
} else {
  log_msg("Skipping NWCC Transform due to retrieval failure.")
}

# B. LI-COR Transform
if (licor_req_success) {
  run_script("scripts/licor_transform.R")
} else {
  log_msg("Skipping LI-COR Transform due to retrieval failure.")
}

# C. USGS Transform
if (usgs_req_success) {
  run_script("scripts/usgs_transform.R")
} else {
  log_msg("Skipping USGS Transform due to retrieval failure.")
}

# 5. PHASE 3: QUALITY CONTROL
log_msg("PHASE 3: Quality Control")
run_script("scripts/quality_control.R")

# 6. PHASE 4: DATABASE SYNCHRONIZATION
log_msg("PHASE 4: Database Sync")
run_script("scripts/build_database.R")

# 7. VALIDATION SUMMARY
log_msg("PHASE 5: Final Validation")

clean_files <- list.files(here("data_clean"), pattern = "\\.csv$", full.names = TRUE)
recent_files <- clean_files[file.mtime(clean_files) > (now() - hours(1))]

if (length(recent_files) > 0) {
  log_msg(sprintf("Validation Success: %d files updated in data_clean/ in the last hour.", length(recent_files)))
  for (f in recent_files) {
    file_info <- file.info(f)
    log_msg(sprintf("  - %s (%d bytes)", basename(f), file_info$size))
  }
} else {
  log_msg("WARNING: No files were updated in data_clean/ during this run.")
}


# 8. PHASE 6: R2 BACKUP
log_msg("PHASE 6: R2 Backup")
backup_result <- system2(here("scripts/backup_to_r2.sh"), stdout = TRUE, stderr = TRUE)
if (!is.null(attr(backup_result, "status")) && attr(backup_result, "status") != 0) {
  log_msg(sprintf("WARNING: R2 backup failed: %s", paste(backup_result, collapse = "\n")))
} else {
  log_msg("R2 backup of hydro_data.db completed successfully.")
}

log_msg("--- Pipeline Execution Complete ---")
