library(needs)

needs(tidyverse)


##############
# area chart #
##############

categories <- c(
  "Interpretation services",
  "Non-scheduled air transport services",
  "Software",
  "Surveillance services",
  "Transport services",
  "Travel agency"
)

data_wide <- read_csv("../data/frontex_contracts_clean.csv")%>%
  mutate(cpv_clean = case_when(
    cpv_clean %in% categories ~ cpv_clean,
    TRUE ~ "Other"
  ))%>%
  group_by(year, cpv_clean)%>%
  summarize(object_total_value_clean = sum(object_total_value_clean, na.rm = TRUE)) %>%
  spread(key = cpv_clean, value = object_total_value_clean)%>%
  mutate(across(`Interpretation services`:`Travel agency`, ~replace_na(., 0)))

write_csv(data_wide, "../data/frontex_categories_years.csv")


categories <- c(
  "Business services",
  "IT services",
  "Computer network services",
  "Security services"
)

data_wide <- read_csv("../data/eulisa_contracts_clean.csv")%>%
  mutate(cpv_clean = case_when(
    cpv_clean %in% categories ~ cpv_clean,
    TRUE ~ "Other"
  ))%>%
  group_by(year, cpv_clean)%>%
  summarize(object_total_value_clean = sum(object_total_value_clean, na.rm = TRUE)) %>%
  spread(key = cpv_clean, value = object_total_value_clean)%>%
  mutate(across(`Business services`:`Security services`, ~replace_na(., 0)))

write_csv(data_wide, "../data/eulisa_categories_years.csv")
