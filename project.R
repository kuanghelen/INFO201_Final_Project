library(stringr)
library(dplyr)

# IMPORTANT NOTES ON DATA JOINING:
#   - We are joining the two datasets by borough name (there are a total of 5 boroughs).
#   - Because the borough is not a unique identifier of each property, we chose to aggregate 
#       borough information from the (huge) crime dataset BEFORE joining with the property data.
#   - We created a new summarized dataframe (borough_df) of the crime dataframe (crime_df),
#       which we used to join with the property dataframe (property_df).
#   - This process was necessary to maintain less than 25,000 rows of data.

# ADDITIONAL COLUMNS + SUMMARY DATA FRAME
# Note that the final dataframe is named df
#   - Categorical columns: market_increase, assessed_increase, taxable_increase
#   - Continuous/Numerical columns: market_change, assessed_change, taxable_change
#   - Summarization data frame: summary_df


# read the csv files
property_df <- read.csv("data/Property_Value_2016.csv")
crime_df <- read.csv("data/NYPD_Crimes_2015.csv")

# remove columns that do not have the joining feature-- the borough information
property_df <- filter(property_df, str_detect(City..State..Zip., "NY"), Borough != "")
crime_df <- filter(crime_df, BORO_NM != "", BORO_NM != "(null)")

# rename the borough: "STATEN IS" in property_df to "STATEN ISLAND". This will help with joining.
property_df[property_df$Borough == "STATEN IS", "Borough"] <- "STATEN ISLAND"

# remove irrelevant columns
property_df <- select(
  property_df, 
  -c(
    MAILED.DATE, 
    EASE, 
    TAX.CLASS, 
    BOROCODE,
    Country, 
    BLD.Class, 
    RC.1, 
    RC2, 
    RC3, 
    RC4, 
    RC5, 
    Community.Board, 
    Council.District, 
    Census.Tract, 
    BIN, 
    BBL, 
    NTA,
    ORIGINAL.TRANSITIONAL..ASSESSED.VALUE,
    REVISED.TRANSITIONAL.ASSESSED.VALUE,
    ORIGINAL.TRANSITIONAL.EXEMPTION,
    REVISED.TRANSITIONAL.EXEMPTION,
    ORIGINAL.EXEMPTION,
    REVISED..EXEMPTION
  )
)
crime_df <- select(
  crime_df, 
  -c(
    CMPLNT_NUM, 
    CMPLNT_FR_DT, 
    RPT_DT, 
    KY_CD, 
    VIC_AGE_GROUP
  )
)

# Aggregate the crime dataset. These are used to make a new dataframe for joining.
temp_df <- filter(crime_df, LAW_CAT_CD == "FELONY")
group_df <- group_by(temp_df, BORO_NM)
fel_df <- summarize(group_df, felony = n())
fel_df <- arrange(fel_df, BORO_NM)

temp_df <- filter(crime_df, LAW_CAT_CD == "MISDEMEANOR")
group_df <- group_by(temp_df, BORO_NM)
mis_df <- summarize(group_df, misdemeanor = n())
mis_df <- arrange(mis_df, BORO_NM)

temp_df <- filter(crime_df, LAW_CAT_CD == "VIOLATION")
group_df <- group_by(temp_df, BORO_NM)
vio_df <- summarize(group_df, violation = n())
vio_df <- arrange(vio_df, BORO_NM)

temp_df <- filter(crime_df, VIC_SEX == "F")
group_df <- group_by(temp_df, BORO_NM)
fem_df <- summarize(group_df, female = n())
fem_df <- arrange(fem_df, BORO_NM)

temp_df <- filter(crime_df, VIC_SEX == "M")
group_df <- group_by(temp_df, BORO_NM)
mal_df <- summarize(group_df, male = n())
mal_df <- arrange(mal_df, BORO_NM)

temp_df <- filter(crime_df, VIC_RACE == "UNKNOWN")
group_df <- group_by(temp_df, BORO_NM)
race_unknown_df <- summarize(group_df, race_unknown = n())
race_unknown_df<- arrange(race_unknown_df, BORO_NM)

temp_df <- filter(crime_df, VIC_RACE == "WHITE HISPANIC")
group_df <- group_by(temp_df, BORO_NM)
race_whitehis_df <- summarize(group_df, race_white_hispanic= n())
race_whitehis_df<- arrange(race_whitehis_df, BORO_NM)

temp_df <- filter(crime_df, VIC_RACE == "WHITE")
group_df <- group_by(temp_df, BORO_NM)
race_white_df <- summarize(group_df, race_white = n())
race_white_df<- arrange(race_white_df, BORO_NM)

temp_df <- filter(crime_df, VIC_RACE == "BLACK")
group_df <- group_by(temp_df, BORO_NM)
race_black_df <- summarize(group_df, race_black = n())
race_black_df<- arrange(race_black_df, BORO_NM)

temp_df <- filter(crime_df, VIC_RACE == "BLACK HISPANIC")
group_df <- group_by(temp_df, BORO_NM)
race_blackhis_df <- summarize(group_df, race_black_hispanic = n())
race_blackhis_df<- arrange(race_blackhis_df, BORO_NM)

temp_df <- filter(crime_df, VIC_RACE == "ASIAN / PACIFIC ISLANDER")
group_df <- group_by(temp_df, BORO_NM)
race_asianp_df <- summarize(group_df, race_asian_pacific_islander = n())
race_asianp_df<- arrange(race_asianp_df, BORO_NM)

temp_df <- filter(crime_df, VIC_RACE == "AMERICAN INDIAN/ALASKAN NATIVE")
group_df <- group_by(temp_df, BORO_NM)
race_native_df <- summarize(group_df, race_american_indian_alaskan_native = n())
race_native_df<- arrange(race_native_df, BORO_NM)

# Combine the crime data aggregations to form a new dataframe.
borough_df <- data.frame(
  Borough = fel_df$BORO_NM, 
  felony = fel_df$felony,
  misdemeanor = mis_df$misdemeanor,
  violation = vio_df$violation,
  vic_female = fem_df$female,
  vic_male = mal_df$male,
  vic_race_unknown = race_unknown_df$race_unknown,
  vic_race_white_hispanic = race_whitehis_df$race_white_hispanic,
  vic_race_white = race_white_df$race_white,
  vic_race_black = race_black_df$race_black,
  vic_race_asian_pacific_islander = race_asianp_df$race_asian_pacific_islander,
  vic_race_american_indian_alaskan_native = race_native_df$race_american_indian_alaskan_native
)

# Join the property data (property_df) and crime data (borough_df).
df <- merge(x = property_df, y = borough_df, by = "Borough", all.x = TRUE)

# Add new continuous/numerical & categorical columns
df <- mutate(
  df, 
  market_change = REVISED.MARKET.VALUE - ORIGINAL.MARKET.VALUE,
  assessed_change = REVISED.ASSESSED.VALUE - ORIGINAL.ASSESSED.VALUE,
  taxable_change = REVISED.TAXABLE.VALUE - ORIGINAL.TAXABLE.VALUE,
  market_increase = ifelse(market_change > 0, TRUE, FALSE),
  assessed_increase = ifelse(assessed_change > 0, TRUE, FALSE),
  taxable_increase = ifelse(taxable_change > 0, TRUE, FALSE)
)

# code to find max data (used for summary df)
max_pt <- summarise_all(df, max)
max_pt <- select(
  max_pt, 
  c(
    market_change,
    assessed_change,
    taxable_change,
    felony,
    misdemeanor,
    violation,
    vic_female,
    vic_male,
    vic_race_unknown,
    vic_race_white_hispanic,
    vic_race_white,
    vic_race_black,
    vic_race_asian_pacific_islander,
    vic_race_american_indian_alaskan_native
  )
)

# code to find min data (used for summary df)
min_pt <- summarise_all(df, min)
min_pt <- select(
  min_pt, 
  c(
    market_change,
    assessed_change,
    taxable_change,
    felony,
    misdemeanor,
    violation,
    vic_female,
    vic_male,
    vic_race_unknown,
    vic_race_white_hispanic,
    vic_race_white,
    vic_race_black,
    vic_race_asian_pacific_islander,
    vic_race_american_indian_alaskan_native
  )
)

# create a summary table that store the max and min of several important columns
summary_df <- do.call("rbind", list(max_pt, min_pt))

# export the final df into a new csv file that will be in the data folder.
write.csv(df, "data/clean.csv", row.names=FALSE)

