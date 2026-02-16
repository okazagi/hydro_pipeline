#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(readr)
  library(ggplot2)
  library(tidyr)
  library(dplyr)
  library(lubridate)
})

# ---------------------------------------------------------
# SET YOUR PATH HERE
# ---------------------------------------------------------
INPUT_CSV <- "data_clean/RFGLR_combined_final.csv"
OUTPUT_PDF <- "data_clean/RFGLR_visual_check.pdf"
# ---------------------------------------------------------

dir.create(dirname(OUTPUT_PDF), recursive = TRUE, showWarnings = FALSE)

# Load and prepare data
df <- read_csv(INPUT_CSV, show_col_types = FALSE) %>%
  mutate(Timestamp = ymd_hms(paste(Date_UTC, Time_UTC)))

# Pivot long for easier faceting
# We exclude metadata columns from the pivoting
df_long <- df %>%
  select(-Date_UTC, -Time_UTC, -Station_ID, -Station_Name, -DataFlag) %>%
  pivot_longer(cols = -Timestamp, names_to = "Variable", values_to = "Value") %>%
  filter(!is.na(Value)) # Don't plot missing data gaps

# Create the plot
p <- ggplot(df_long, aes(x = Timestamp, y = Value)) +
  geom_line(color = "steelblue", size = 0.3) +
  facet_wrap(~Variable, scales = "free_y", ncol = 2) +
  theme_minimal() +
  labs(title = paste("Visual Data Check:", basename(INPUT_CSV)),
       subtitle = "Faceted by Variable (Free Y-Scales)",
       x = "Date",
       y = "Value") +
  theme(strip.text = element_text(size = 8),
        axis.text = element_text(size = 7))

# Save as a tall PDF to ensure clarity
ggsave(OUTPUT_PDF, plot = p, width = 12, height = 20)

cat(sprintf("Success! Visual check saved to: %s\n", OUTPUT_PDF))
