library(needs)

needs(tidyverse,
      ggplot2,
      readxl,
      glue,
      sf)



#################
# map addresses #
#################

eulisa <- read_csv("../data/eulisa_contractors_clean.csv")%>%
  mutate(unique_id = str_c(id, contractors_clean))%>%
  select(unique_id, 
         object_title, 
         object_total_value_clean, 
         contractors, 
         contractors_countries, 
         award_of_contracts_address,
         award_of_contracts_town,
         contractors_total_value_clean)%>%
  # one contract was split into two contracts for same company - causes problems later
  # so putting it together now
  mutate(contractors_total_value_clean = case_when(
    unique_id == "371708-2018Pomilio Blumm Srl" ~ 15400000,
    unique_id == "221695-2015ManpowerGroup Solutions Belgium SA" ~ 9487116,
    grepl("221695-2015Consortium U2 — Unisys SA", unique_id) == TRUE ~ 47114595,
    TRUE ~ contractors_total_value_clean))%>%
  unique()

eulisa_contractors <- eulisa %>%
  gather(-unique_id, key = key, value = value)%>%
  # fix wrongly formatted addresses
  mutate(value = str_replace_all(value, '" 2 rue d', "' 2 rue d"))%>%
  mutate(value = str_replace_all(value, '" avenue d', "' avenue d"))%>%
  mutate(value = str_replace_all(value, '" chaussée d', "' chaussée d"))%>%
  mutate(value = str_replace_all(value, '1001"', "1001'"))%>%
  mutate(value = str_replace_all(value, "\\[' ", ""))%>%
  mutate(value = str_replace_all(value, "'\\]", ""))%>%
  separate(value, c("A","B","C"), sep = "', ' ")

# test <- read_csv("https://raw.githubusercontent.com/anavaldi/eulisa-frontex-contracts/main/data/eulisa_contractors_clean.csv")%>%
#   mutate(unique_id = str_c(id, contractors_clean))%>%
#   group_by(unique_id)%>%
#   summarize(n = n())%>%
#   filter(n > 1)

firsts <- eulisa_contractors  %>%
  select(unique_id, key, A)%>%
  rename(value = A)%>%
  filter(!is.na(value))%>%
  filter(key %in% c("contractors",               
                    "contractors_countries",   
                    "award_of_contracts_address",
                    "award_of_contracts_town"))%>%
  mutate(unique_id = str_c(unique_id, "_1"))%>%
  unique()%>%
  spread(key = key, value=value)

seconds <- eulisa_contractors  %>%
  select(unique_id, key, B)%>%
  rename(value = B)%>%
  filter(!is.na(value))%>%
  mutate(unique_id = str_c(unique_id, "_2"))%>%
  unique()%>%
  spread(key = key, value=value)

thirds <- eulisa_contractors  %>%
  select(unique_id, key, C)%>%
  rename(value = C)%>%
  filter(!is.na(value))%>%
  mutate(unique_id = str_c(unique_id, "_3"))%>%
  unique()%>%
  spread(key = key, value=value)


all <- bind_rows(firsts,
                 seconds,
                 thirds)%>%
  # clean names
  mutate(contractors_clean = case_when(
    grepl("ATOS Belgium SA/NV \\(leader\\)", contractors) == TRUE ~ "Atos Belgium NV",
    grepl("Civitta Eesti AS \\(group leader\\)", contractors) == TRUE ~ "Civitta Eesti AS",
    grepl("Unisys SA \\(group leader\\)", contractors) == TRUE ~ "Unisys Belgium SA",
    grepl("Steria Benelux SA/NV \\(group leader\\)", contractors) == TRUE ~ "Sopra Steria Benelux SA/NV",
    grepl("Sopra Steria Benelux", contractors) == TRUE ~ "Sopra Steria Benelux SA/NV",
    grepl("Group Leader IBM Belgium BVBA", contractors) == TRUE ~ "IBM Belgium BVBA",
    contractors == "AS G4S EESTI" ~ "AS G4S Eesti",
    grepl("Atos Belgium", contractors) == TRUE ~ "Atos Belgium NV",
    contractors == "INFEUROPE S.A." ~ "Infeurope SA",
    contractors == "Axima Concept S.A." ~ "Axima Concept",
    grepl("3M Belgium BVBA", contractors) == TRUE ~ "3M Belgium BVBA",
    grepl("BULL SAS", contractors, ignore.case=TRUE) == TRUE ~ "Bull SAS",
    grepl("Morpho SAS", contractors, ignore.case=TRUE) == TRUE ~ "Morpho SAS",
    grepl("Accenture", contractors, ignore.case=TRUE) == TRUE ~ "Accenture NV/SA",
    grepl("Propager", contractors, ignore.case=TRUE) == TRUE ~ "ProPager SARL",
    grepl("ELIN GmbH", contractors, ignore.case=TRUE) == TRUE ~ "ELIN GmbH",
    grepl("infeurope SA", contractors, ignore.case=TRUE) == TRUE ~ "Infeurope SA",
    grepl("Securitas France SARL", contractors, ignore.case=TRUE) == TRUE ~ "Securitas France SARL",
    grepl("Everis Spain SLU succursale en Belgique \\(group leader\\)", contractors, ignore.case=TRUE) == TRUE ~ "Everis Spain SLU succursale en Belgique",
    grepl("Unisys SA \\(group leader\\)", contractors, ignore.case=TRUE) == TRUE ~ "Unisys SA",
    TRUE ~ contractors
  ))%>%
  mutate(contractors_clean = str_replace_all(contractors_clean, "\\(Group Leader\\)", ""))%>%
  mutate(contractors_clean = str_replace_all(contractors_clean, "\\(Leader\\)", ""))%>%
  mutate(contractors_clean = str_replace_all(contractors_clean, "Consortium ", ""))%>%
  mutate(contractors_clean = trimws(contractors_clean))

# export addresses for geocoding and filtering out new ones
write_csv(all, "temp/addresses_eulisa_contracts.csv")

# manually delete non-valid addresses and keep only one per company (for matching later, when geo-coded)
# attention: coordinates need to be with points not commas

# then import again
addresses <- read_csv("temp/addresses_eulisa_contracts_manuallyEdited.csv")

test <- all %>% select(contractors_clean)%>%unique()%>% full_join(addresses, by = "contractors_clean")

replace_vector = c("_1"="",
                   "_2"="",
                   "_3"="",
                   "_4"="",
                   "_5"="",
                   "_6"=""
  
)

fulldata <- all %>%
  mutate(id = str_replace_all(unique_id,replace_vector))%>%
  full_join(eulisa %>%
              select(unique_id, object_title, contractors_total_value_clean),
            by = c("id" = "unique_id")) %>%
  group_by(contractors_clean)%>%
  summarize(value_contracts_sum = sum(contractors_total_value_clean),
            number_contracts = n())%>%
  mutate(contractors_clean = trimws(contractors_clean))%>%
  left_join(addresses %>% select(contractors_clean, latitude, longitude, `address_concat`),
            by = "contractors_clean")%>%
  mutate(`address_concat` = str_replace_all(address_concat, "NA, ", ""))%>%
  rename(address = `address_concat`)

write_csv(fulldata, "../data/eulisa_contracts_coords.csv")
  




##############
# FRONTEX

replace_names <- c(
  '"' = "'",
  "' CAE Aviation', ' DEA Aviation Ltd', ' EASP Air BV'" = 
  "' CAE Aviation', ' DEA Aviation', ' EASP Air BV'"
)

frontex <- read_csv("../data/frontex_contractors_clean.csv")%>%
  mutate(unique_id = str_c(id, contractors_clean))%>%
  select(unique_id, 
         contractors, 
         contractors_countries, 
         award_of_contracts_address,
         award_of_contracts_town,
         contractors_total_value_clean)%>%
  # fix a name so it is added up correctly
  mutate(contractors = str_replace_all(contractors,replace_names))%>%
  # add values of contracts together if same id and same contractors
  group_by(unique_id, 
           contractors, 
           contractors_countries, 
           award_of_contracts_address,
           award_of_contracts_town)%>%
  summarize(contractors_total_value_clean = sum(contractors_total_value_clean))%>%
  ungroup()

frontex_contractors <- frontex %>%
  gather(-unique_id, key = key, value = value)%>%
  mutate(value = str_replace_all(value, "\\[' ", ""))%>%
  mutate(value = str_replace_all(value, "'\\]", ""))%>%
  separate(value, c("A","B","C", "D", "E", "F"), sep = "', ' ")


firsts <- frontex_contractors %>%
  select(unique_id, key, A)%>%
  rename(value = A)%>%
  filter(!is.na(value))%>%
  filter(key %in% c("contractors",               
                    "contractors_countries",   
                    "award_of_contracts_address",
                    "award_of_contracts_town"))%>%
  mutate(new_id = str_c(unique_id, "_1"))%>%
  unique()%>%
  spread(key = key, value=value)

seconds <- frontex_contractors %>%
  select(unique_id, key, B)%>%
  rename(value = B)%>%
  filter(!is.na(value))%>%
  mutate(new_id = str_c(unique_id, "_2"))%>%
  unique()%>%
  spread(key = key, value=value)

thirds <- frontex_contractors %>%
  select(unique_id, key, C)%>%
  rename(value = C)%>%
  filter(!is.na(value))%>%
  mutate(new_id = str_c(unique_id, "_3"))%>%
  unique()%>%
  spread(key = key, value=value)

fourths <- frontex_contractors %>%
  select(unique_id, key, D)%>%
  rename(value = D)%>%
  filter(!is.na(value))%>%
  mutate(new_id = str_c(unique_id, "_4"))%>%
  unique()%>%
  spread(key = key, value=value)

fifths <- frontex_contractors %>%
  select(unique_id, key, E)%>%
  rename(value = E)%>%
  filter(!is.na(value))%>%
  mutate(new_id = str_c(unique_id, "_5"))%>%
  unique()%>%
  spread(key = key, value=value)

sixths <- frontex_contractors %>%
  select(unique_id, key, F)%>%
  rename(value = F)%>%
  filter(!is.na(value))%>%
  mutate(new_id = str_c(unique_id, "_6"))%>%
  unique()%>%
  spread(key = key, value=value)


all <- bind_rows(firsts,
                 seconds,
                 thirds,
                 fourths,
                 fifths,
                 sixths)%>%
  # clean names
  mutate(contractors_clean = case_when(
    grepl("Limited", contractors) == TRUE ~ str_replace_all(contractors, "Limited", "Ltd"),
    contractors == "CAE Aviation Sarl" ~ "CAE Aviation",
    contractors == "Car Master 2 Sp. z o.o. Sp.k" ~ "Car Master 2 sp. z o.o. sp.k.",
    grepl("Consortium of Oktagon", contractors) == TRUE ~ "Oktagon Polska Paweł Toffel",
    contractors == "DEA Aviation Ltd" ~ "DEA Aviation",
    grepl("ESRI Polska", contractors) == TRUE ~ "ESRI Polska",
    grepl("Opticoelectron Group", contractors) == TRUE ~ "Opticoelectron Group",
    grepl("Optix", contractors,ignore.case=TRUE) == TRUE ~ "Optix SA",
    contractors == "Diamond-Executive Aviation Ltd" ~ "DEA Aviation",
    grepl("Impression Catering", contractors,ignore.case=TRUE) == TRUE ~ "Art'Impression Catering Sp. z o.o.",
    grepl("Espes Office Sp", contractors,ignore.case=TRUE) == TRUE ~ "Espes Office Sp. z o.o.",
    grepl("Events Center Creative Agency", contractors,ignore.case=TRUE) == TRUE ~ "Events Center Creative Agency Krzysztof Koczur",
    grepl("GMV", contractors,ignore.case=TRUE) == TRUE ~ "GMV Aerospace and Defence SAU",
    grepl("Grafton Recruitment", contractors,ignore.case=TRUE) == TRUE ~ "Grafton Recruitment Sp z oo",
    grepl("Asseco Poland", contractors,ignore.case=TRUE) == TRUE ~ "Asseco Poland SA",
    	
    TRUE ~ contractors
  ))

# export addresses for geocoding and filtering out new ones
write_csv(all, "temp/addresses_frontex_contracts.csv")

# manually delete non-valid addresses and keep only one per company (for matching later, when geo-coded)
# then import again
addresses <- read_csv("temp/addresses_frontex_contracts_manuallyEdited.csv")


# test <- all %>% select(contractors_clean)%>%unique()%>% full_join(addresses, by = "contractors_clean")

fulldata <- all %>%
  mutate(id = str_replace_all(unique_id,replace_vector))%>%
  left_join(frontex %>%
              select(unique_id, contractors_total_value_clean),
            by = c("id" = "unique_id")) %>%
  group_by(contractors_clean)%>%
  summarize(value_contracts_sum = sum(contractors_total_value_clean),
            number_contracts = n())%>%
  mutate(contractors_clean = trimws(contractors_clean))%>%
  left_join(addresses %>% select(contractors_clean, latitude, longitude, `address_concat`),
            by = "contractors_clean")%>%
  mutate(`address_concat` = str_replace_all(address_concat, "NA, ", ""))%>%
  rename(address = `address_concat`)
  

write_csv(fulldata, "../data/frontex_contracts_coords.csv")
