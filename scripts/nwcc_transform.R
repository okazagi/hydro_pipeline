library(jsonlite)
library(dplyr)
library(purrr)
library(readr)
library(tidyr)
library(stringr)

# 1. Setup Directories
raw_dir   <- "data_raw"
clean_dir <- "data_clean"

if (!dir.exists(clean_dir)) dir.create(clean_dir)

# Get list of JSON files
json_files <- list.files(raw_dir, pattern = "\\.json$", full.names = TRUE)

message(paste("Found", length(json_files), "files to process."))

# 2. Process Loop
for (file in json_files) {

  # Extract station name from filename for logging
  station_name <- tools::file_path_sans_ext(basename(file))
  message(paste("\nProcessing:", station_name, "..."))

  # Load the specific JSON file
  json_res <- fromJSON(file, flatten = TRUE)

  # Validation: Ensure it has data
  if (!is.data.frame(json_res) || !"data" %in% names(json_res)) {
    warning(paste("Skipping", station_name, "- Invalid structure"))
    next
  }

  raw_data <- json_res$data[[1]]

  if (is.null(raw_data) || length(raw_data) == 0) {
    warning(paste("Skipping", station_name, "- Data list is empty"))
    next
  }

  data_frames_list <- list()
  used_names <- c()

  # 3. Iterate through sensors
  for (i in 1:nrow(raw_data)) {

    # --- FIX: DIRECT ACCESS TO FLATTENED COLUMNS ---

    # Get Element Code (e.g., SMS, TOBS)
    elem_code <- raw_data$stationElement.elementCode[i]

    # Get Ordinal (Unique ID)
    ordinal <- raw_data$stationElement.ordinal[i]

    # Get Depth (Directly check the column)
    depth_val <- NULL
    if ("stationElement.heightDepth" %in% names(raw_data)) {
      val <- raw_data$stationElement.heightDepth[i]
      if (!is.na(val)) {
        depth_val <- val
      }
    }

    # Build Column Name
    col_name <- elem_code

    # Append Depth (e.g., SMS_-2)
    # We add "in" (inches) implicitly because this API usually implies it for depth
    if (!is.null(depth_val)) {
      col_name <- paste0(col_name, "_", depth_val, "in")
    }

    # Duplicate Check (Use ordinal if name exists)
    if (col_name %in% used_names) {
      col_name <- paste0(col_name, "_ord", ordinal)
    }
    used_names <- c(used_names, col_name)

    # --- EXTRACT VALUES ---
    values_df <- raw_data$values[[i]]

    if (!is.null(values_df) && nrow(values_df) > 0) {
      clean_df <- values_df %>%
        select(date, value) %>%
        rename(!!col_name := value) %>%
        mutate(!!col_name := as.numeric(!!sym(col_name)))

      data_frames_list[[length(data_frames_list) + 1]] <- clean_df
    }
  }

  # 4. Merge and Save
  if (length(data_frames_list) > 0) {
    final_df <- data_frames_list %>%
      reduce(full_join, by = "date") %>%
      arrange(date)

    save_path <- file.path(clean_dir, paste0(station_name, ".csv"))
    write_csv(final_df, save_path)

    message(paste("Success! Saved to:", save_path))
    print(names(final_df)) # Show columns to verify
  } else {
    warning(paste("No usable data found in", station_name))
  }
}

message("\nAll processing complete.")
