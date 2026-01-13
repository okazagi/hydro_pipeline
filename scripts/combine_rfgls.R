library(tidyverse)
library(lubridate)
library(here)

# 1. Define file paths
path_uncalibrated <- here("data_clean", "Uncalibrated", "RFGLS_uncalibrated.csv")
path_master       <- here("data_clean", "synoptic", "RFGLS_master.csv")
output_path       <- here("data_clean", "Uncalibrated", "RFGLS_combined.csv")

# 2. Read the files
df_uncalibrated <- read_csv(path_uncalibrated, show_col_types = FALSE)
df_master       <- read_csv(path_master, show_col_types = FALSE)

# 3. Process Uncalibrated Data
df_uncalibrated_clean <- df_uncalibrated %>%
  mutate(Station_ID = as.character(Station_ID)) %>%
  rename(SoilTemp_C_20cm = SoilTemp_C)

# 4. Process Master Data
df_master_clean <- df_master %>%
  mutate(Station_ID = as.character(Station_ID))

# 5. Concatenate and Remove Duplicates
df_combined <- bind_rows(df_uncalibrated_clean, df_master_clean) %>%
  # distinct() removes rows where Station, Date, and Time are identical.
  # .keep_all = TRUE ensures we keep the other columns for the retained row.
  distinct(Date_UTC, Time_UTC, .keep_all = TRUE)

# 6. Sort and Reorder
df_final <- df_combined %>%
  mutate(datetime_temp = ymd_hms(paste(Date_UTC, Time_UTC))) %>%
  arrange(datetime_temp) %>%
  select(-datetime_temp) %>%
  # Ensure column order matches the master file
  select(names(df_master))

# 7. Write the result
write_csv(df_final, output_path)

message("Process complete.")
message("Duplicates removed based on Station_ID, Date_UTC, and Time_UTC.")
message("Merged file saved to: ", output_path)
