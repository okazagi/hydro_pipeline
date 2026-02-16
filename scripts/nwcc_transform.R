library(jsonlite)
library(dplyr)
library(purrr)
library(readr)
library(tidyr)
library(stringr)
library(lubridate) # Added for robust date/time splitting

# --- NEW: Define Station Metadata ---
stations <- list(
  list(id = "1101:CO:SNTL", name = "Chapman Tunnel", start_date = "2023-07-14"),
  list(id = "556:CO:SNTL",  name = "Kiln",            start_date = "2023-07-14"),
  list(id = "1326:CO:SNTL", name = "Castle Peak",    start_date = "2024-09-16")
)

# Get station name and id
station_lookup <- setNames(
  map_chr(stations, "id"),
  map_chr(stations, "name")
)

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

  # If name matches the list, get the ID. If not found, defaults to NA.
  real_station_id <- station_lookup[station_name]

  # Fallback: If filename doesn't match list, keep the filename as ID or warn
  if (is.na(real_station_id)) {
    warning(paste("ID not found for station:", station_name, "- Using filename as ID"))
    real_station_id <- station_name
  }

  message(paste("\nProcessing:", station_name, "(ID:", real_station_id, ")..."))

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

  # 4. Merge, Map to Schema, and Save
  if (length(data_frames_list) > 0) {

    # helper function to safely get column or return NA
    get_col <- function(df, col_name) {
      if (col_name %in% names(df)) return(df[[col_name]])
      return(NA_real_)
    }

    # Merge all sensor data frames
    merged_df <- data_frames_list %>%
      reduce(full_join, by = "date") %>%
      arrange(date)

    # --- APPLY MASTER SCHEMA MAPPING ---
    # We parse the date string first to ensure we can split it correctly
    final_df <- merged_df %>%
      mutate(temp_datetime = ymd_hm(date)) %>% # Parses "2024-01-01 12:00"
      transmute(
        # Split Date and Time, remove original 'date'
        Date_UTC = as.character(as_date(temp_datetime)),
        Time_UTC = format(temp_datetime, "%H:%M:%S"),

        # Identifiers
        Station_ID   = real_station_id,
        Station_Name = station_name,

        # Air / Atmos
        AirTemp_C           = get_col(., "TOBS"),
        RH                  = get_col(., "RHUM"),
        Barometric_Pressure = NA_real_,

        # Neutron Counts
        Epithermal_Neutron_counts          = NA_real_,
        Thermal_Neutron_counts             = NA_real_,
        Blw_Grnd_Epithermal_Neutron_counts = NA_real_,

        # Soil Temperature
        SoilTemp_C_5cm   = get_col(., "STO_-2in"),
        SoilTemp_C_10cm  = get_col(., "STO_-4in"),
        SoilTemp_C_20cm  = get_col(., "STO_-8in"),
        SoilTemp_C_50cm  = get_col(., "STO_-20in"),
        SoilTemp_C_100cm = get_col(., "STO_-40in"),

        # Water Content
        WaterCont_5cm_m3m3   = get_col(., "SMS_-2in"),
        WaterCont_10cm_m3m3  = get_col(., "SMS_-4in"),
        WaterCont_20cm_m3m3  = get_col(., "SMS_-8in"),
        WaterCont_50cm_m3m3  = get_col(., "SMS_-20in"),
        WaterCont_100cm_m3m3 = get_col(., "SMS_-40in"),

        # Other / Secondary
        Rain_cm               = NA_real_,
        Dewpoint_C            = NA_real_,
        DataFlag              = NA_character_,
        WaterCont_5cm_m3m3_2  = NA_real_,
        WaterCont_20cm_m3m3_2 = NA_real_,
        WaterCont_50cm_m3m3_2 = NA_real_
      )

    save_path <- file.path(clean_dir, paste0(station_name, ".csv"))
    write_csv(final_df, save_path)

    message(paste("Success! Saved to:", save_path))
  } else {
    warning(paste("No usable data found in", station_name))
  }
}

message("\nAll processing complete.")
