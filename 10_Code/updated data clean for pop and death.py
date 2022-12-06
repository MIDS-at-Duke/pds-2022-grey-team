""" data cleaning for population and drug overdose death datasets"""

# Read mortality dataset in 2003 as an example
import pandas as pd
import numpy as np

df2003 = pd.read_csv("US_VitalStatistics/Underlying Cause of Death, 2003.txt", sep="\t")

# Filter specific comlums and check reasons of drug death
cause_of_death = df2003[
    [
        "County",
        "Year",
        "Drug/Alcohol Induced Cause",
        "Drug/Alcohol Induced Cause Code",
        "Deaths",
    ]
]

# Filter the drug overdose death
death_code = ["D1", "D2", "D4"]
death_2003 = cause_of_death[
    cause_of_death["Drug/Alcohol Induced Cause Code"].isin(death_code)
]

# Filter years from 2003 to 2015
years = [
    "2003",
    "2004",
    "2005",
    "2006",
    "2007",
    "2008",
    "2009",
    "2010",
    "2011",
    "2012",
    "2013",
    "2014",
    "2015",
]

# Combine mortality of drug overdose datasets from all years into one csv.file
dfs = []
for year in years:
    df = pd.read_csv(
        "US_VitalStatistics/Underlying Cause of Death, " + year + ".txt", sep="\t"
    )
    cause_of_death = df[
        [
            "County",
            "Year",
            "Drug/Alcohol Induced Cause",
            "Drug/Alcohol Induced Cause Code",
            "Deaths",
        ]
    ]
    death_code = ["D1", "D2", "D4"]
    drug_overdose = cause_of_death[
        cause_of_death["Drug/Alcohol Induced Cause Code"].isin(death_code)
    ]
    dfs.append(drug_overdose)

allyear_death = pd.concat(dfs)
allyear_death.to_csv("death_drug_overdose_all_years.csv", index=False)

# Split state and county within County column
allyear_death["State"] = allyear_death["County"].str.split(",").str[1].str.strip()

# Only choose 12 states we decided
state_need = ["MI", "NC", "OH", "MO", "GA", "AZ", "PA", "VA", "WA", "MA", "FL", "TX"]
needed_state_death = allyear_death[allyear_death["State"].isin(state_need)]

# Read population2000-2010 data and see column names
pop2000_2010 = pd.read_csv("Population/population2000-2010.csv", encoding="latin-1")

# Filter population data for each state and county from 2000-2009, convert it into pop_across-12-states(2000-2009).csv
df_pop2000_2009 = pop2000_2010[
    [
        "STNAME",
        "CTYNAME",
        "POPESTIMATE2000",
        "POPESTIMATE2001",
        "POPESTIMATE2002",
        "POPESTIMATE2003",
        "POPESTIMATE2004",
        "POPESTIMATE2005",
        "POPESTIMATE2006",
        "POPESTIMATE2007",
        "POPESTIMATE2008",
        "POPESTIMATE2009",
    ]
]
state = [
    "Texas",
    "Michigan",
    "North Carolina",
    "Ohio",
    "Missouri",
    "Pennsylvania",
    "Georgia",
    "Virginia",
    "Washington",
    "Arizona",
    "Florida",
    "Massachusetts",
]
cleaned_pop2000_2009 = df_pop2000_2009[df_pop2000_2009["STNAME"].isin(state)]
cleaned_pop2000_2009.to_csv("pop_across-12-states(2000-2009).csv", index=False)

# Read population2010-2020 data and see column names
pop2010_2020 = pd.read_csv("Population/population2010-2020.csv", encoding="latin-1")

# Filter population data for each state and county from 2010-2020, convert it into pop_across-12-states(2010-2020).csv
df_pop2010_2020 = pop2010_2020[
    [
        "STNAME",
        "CTYNAME",
        "CENSUS2010POP",
        "POPESTIMATE2010",
        "POPESTIMATE2011",
        "POPESTIMATE2012",
        "POPESTIMATE2013",
        "POPESTIMATE2014",
        "POPESTIMATE2015",
        "POPESTIMATE2016",
        "POPESTIMATE2017",
        "POPESTIMATE2018",
        "POPESTIMATE2019",
        "POPESTIMATE2020",
    ]
]
state = [
    "Texas",
    "Michigan",
    "North Carolina",
    "Ohio",
    "Missouri",
    "Pennsylvania",
    "Georgia",
    "Virginia",
    "Washington",
    "Arizona",
    "Florida",
    "Massachusetts",
]
cleaned_pop2010_2020 = df_pop2010_2020[df_pop2010_2020["STNAME"].isin(state)]
cleaned_pop2010_2020.to_csv("pop_across-12-states(2010-2020).csv", index=False)

# Reset index for cleaned population data
cleaned_pop2000_2009.reset_index(drop=True, inplace=True)
cleaned_pop2010_2020.reset_index(drop=True, inplace=True)

# Merge 2000-2020 population data into one dataframe
population_2000_2020 = cleaned_pop2000_2009.merge(cleaned_pop2010_2020, how="outer")

# Change year type for death data
needed_state_death["Year"] = needed_state_death["Year"].astype(float)
needed_state_death["Year"] = needed_state_death["Year"].astype(int)
needed_state_death["Year"] = needed_state_death["Year"].astype(str)

# Split state and county within County column
needed_state_death["County"] = (
    needed_state_death["County"].str.split(",").str[0].str.strip()
)

# Reset index for death data across 12 states
needed_state_death.reset_index(drop=True, inplace=True)

# Change Deaths type
needed_state_death["Deaths"] = needed_state_death["Deaths"].astype(float)

# Create a new drug overdose death dataframe
overdose = (
    needed_state_death[["County", "Year", "Deaths", "State"]]
    .groupby(["County", "Year", "State"], as_index=False)
    .sum()
)

# Drop data in other years and keep only 2003-2015 and rename population, year, county, and state columns
pop2003_2015 = population_2000_2020.drop(
    [
        "POPESTIMATE2010",
        "POPESTIMATE2000",
        "POPESTIMATE2001",
        "POPESTIMATE2002",
        "POPESTIMATE2016",
        "POPESTIMATE2017",
        "POPESTIMATE2018",
        "POPESTIMATE2019",
        "POPESTIMATE2020",
    ],
    axis=1,
)
pop2003_2015.rename(
    columns={
        "STNAME": "State",
        "CTYNAME": "County",
        "POPESTIMATE2003": "2003",
        "POPESTIMATE2004": "2004",
        "POPESTIMATE2005": "2005",
        "POPESTIMATE2006": "2006",
        "POPESTIMATE2007": "2007",
        "POPESTIMATE2008": "2008",
        "POPESTIMATE2009": "2009",
        "CENSUS2010POP": "2010",
        "POPESTIMATE2011": "2011",
        "POPESTIMATE2012": "2012",
        "POPESTIMATE2013": "2013",
        "POPESTIMATE2014": "2014",
        "POPESTIMATE2015": "2015",
    },
    inplace=True,
)

# Add a new column State_Name
pop2003_2015["State_Name"] = pop2003_2015["State"]

# Rename states to their abbreviation and filter state, county, year and population
states = {
    "Texas": "TX",
    "North Carolina": "NC",
    "Missouri": "MO",
    "Virginia": "VA",
    "Florida": "FL",
    "Pennsylvania": "PA",
    "Ohio": "OH",
    "Georgia": "GA",
    "Washington": "WA",
    "Arizona": "AZ",
    "Michigan": "MI",
    "Massachusetts": "MA",
}
pop2003_2015["State"].replace(states, inplace=True)
pop_each_year = pop2003_2015.melt(
    id_vars=["State", "County", "State_Name"], var_name="Year", value_name="Population"
)

# Merge population 2003-2015 data and death of drug overdose data across 12 states
overdose_pop = overdose.merge(pop_each_year, how="inner")

# Create a new column named Death_Ratio_per_100000
overdose_pop["Death_Ratio_per_100000"] = (
    overdose_pop["Deaths"].astype(float) / overdose_pop["Population"].astype(float)
) * 100000

# Convert new dataset into final merged csv.file
overdose_pop.to_csv("merged_pop_drug_death.csv", index=False)
