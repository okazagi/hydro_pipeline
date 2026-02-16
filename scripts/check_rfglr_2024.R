library(readr)
library(ggplot2)
library(tidyr)
library(dplyr)
library(lubridate)

# --- Configuration ---
INPUT_FILE <- "old_data/RFGLR_2024_migrated_v1.csv"
OUTPUT_PDF <- "old_data/RFGLR_2024_visual_inspection.pdf"

# 1. Load Data
df <- read_csv(INPUT_FILE, show_col_types = FALSE) %>%
  mutate(Timestamp = ymd_hms(paste(Date_UTC, Time_UTC)))

# 2. Reshape for plotting
df_long <- df %>%
  select(-Date_UTC, -Time_UTC, -Station_ID, -Station_Name, -DataFlag) %>%
  pivot_longer(cols = -Timestamp, names_to = "Variable", values_to = "Value") %>%
  filter(!is.na(Value))

# 3. Create the Plot
# We use facet_wrap with free scales so we can see the detail in each sensor
p <- ggplot(df_long, aes(x = Timestamp, y = Value)) +
  geom_line(color = "firebrick", size = 0.2) +
  # Adding points helps see if data is "stacked" at midnight
  geom_point(size = 0.1, alpha = 0.3) +
  facet_wrap(~Variable, scales = "free_y", ncol = 1) +
  theme_minimal() +
  labs(title = "Visual Inspection: RFGLR 2024 Migration",
       subtitle = "Check for vertical lines at midnight or long flatlines.",
       x = "2024 Timeline",
       y = "Sensor Value") +
  theme(strip.background = element_rect(fill = "gray95"),
        strip.text = element_text(face = "bold"))

# 4. Save to PDF (Tall format is best for many variables)
ggsave(OUTPUT_PDF, plot = p, width = 10, height = 30, limitsize = FALSE)

message(paste("Visual check complete. Open:", OUTPUT_PDF))
