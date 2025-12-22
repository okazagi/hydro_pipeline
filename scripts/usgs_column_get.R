library(tidyverse)
library(fs)

# 1. Define the 'Standard' columns (Right Hand Side of your map)
current_map_cols <- c(
  "dateTime",
  "site_no",
  "X_00020_Inst",
  "X_00052_Inst",
  "X_75969_Inst",
  "X_.Epi.thermal.neutron.counts._72431_Inst",
  "X_.Thermal.neutron.counts._72431_Inst",
  "X_.0.05.m.depth.CS655._72253_Inst",
  "X_.0.10.m.depth.CS655._72253_Inst",
  "X_.0.20.m.depth.CS655._72253_Inst",
  "X_.0.50.m.depth.CS655._72253_Inst",
  "X_.0.05.m.depth.CS655._74207_Inst",
  "X_.0.10.m.depth.CS655._74207_Inst",
  "X_.0.20.m.depth.CS655._74207_Inst",
  "X_.0.50.m.depth.CS655._74207_Inst"
)

# 2. Get list of files
files <- dir_ls("data_raw", glob = "*.csv")

# 3. Loop and collect data
checks <- map_dfr(files, function(f) {

  # Read header only
  file_cols <- names(read_csv(f, n_max = 0, show_col_types = FALSE))

  # Find what is different
  new_cols <- setdiff(file_cols, current_map_cols)
  # Filter for sensor columns (starting with X_)
  new_sensor_cols <- new_cols[str_starts(new_cols, "X_")]

  # If we found new stuff, return a tibble with a LIST column
  if(length(new_sensor_cols) > 0) {
    tibble(
      file = basename(f),
      # We wrap the vector in a list() so it stays compact for now
      new_sensors = list(new_sensor_cols)
    )
  } else {
    return(NULL)
  }
})

# 4. "Spread" the list column into separate cells
# unnest_wider will automatically create columns: Sensor_1, Sensor_2, etc.
formatted_checks <- checks %>%
  unnest_wider(new_sensors, names_sep = "_")

# 5. View and Save
print(formatted_checks)
write_csv(formatted_checks, "header_mismatches_wide.csv")
