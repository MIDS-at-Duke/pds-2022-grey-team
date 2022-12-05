# read mortality dataset
import pandas as pd
import numpy as np
df = pd.read_csv('US_VitalStatistics/Underlying Cause of Death, 2003.txt',sep='\t')

# filter specific comlums and check reasons of drug death
cause_of_death = df[['County','Year','Drug/Alcohol Induced Cause','Drug/Alcohol Induced Cause Code','Deaths']]
cause_of_death[['Drug/Alcohol Induced Cause','Drug/Alcohol Induced Cause Code']].value_counts()

# filter the drug overdose death and covert new dataset into new csv.file
death_code = ['D1','D2', 'D4']
new=cause_of_death[cause_of_death['Drug/Alcohol Induced Cause Code'].isin(death_code)]
new.to_csv('death_drug_overdose.csv',index=False)

# filter years from 2003 to 2015
years = ['2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015']

# combine datasets from all years into one csv.file
dfs = [] 
for year in years:
    df = pd.read_csv('US_VitalStatistics/Underlying Cause of Death, '+year+'.txt',sep='\t')
    cause_of_death = df[['County','Year','Drug/Alcohol Induced Cause','Drug/Alcohol Induced Cause Code','Deaths']]
    death_code = ['D1','D2', 'D4']
    new=cause_of_death[cause_of_death['Drug/Alcohol Induced Cause Code'].isin(death_code)]
    dfs.append(new)
    
all = pd.concat(dfs)
all.to_csv('death_drug_overdose_all_years.csv',index=False)

# Only choose 12 states we decided
state_need = ['MI','NC', 'OH','MO','GA','AZ','PA','VA','WA','MA','FL','TX']
NEW=all[all['State'].isin(state_need)]

# read population2000-2010 data and see column names
df1 = pd.read_csv('Population/population2000-2010.csv',encoding='latin-1')
for i in df1.columns:
    print (i)

# filter population data for each state and county from 2000-2009, convert it into pop_across-12-states(2000-2009).csv
need1 = df1[['STNAME','CTYNAME','POPESTIMATE2000','POPESTIMATE2001','POPESTIMATE2002','POPESTIMATE2003','POPESTIMATE2004','POPESTIMATE2005','POPESTIMATE2006','POPESTIMATE2007','POPESTIMATE2008','POPESTIMATE2009']]
state = ['Texas','Michigan', 'North Carolina','Ohio','Missouri','Pennsylvania','Georgia','Virginia','Washington','Arizona','Florida','Massachusetts']
new1=need1[need1['STNAME'].isin(state)]
new1.to_csv('pop_across-12-states(2000-2009).csv',index=False)

# read population2010-2020 data and see column names
df2 = pd.read_csv('Population/population2010-2020.csv',encoding='latin-1')
for i in df2.columns:
    print (i)

# filter population data for each state and county from 2010-2020, convert it into pop_across-12-states(2010-2020).csv
need2 = df2[['STNAME','CTYNAME','CENSUS2010POP','POPESTIMATE2010','POPESTIMATE2011','POPESTIMATE2012','POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018','POPESTIMATE2019','POPESTIMATE2020']]
state = ['Texas','Michigan', 'North Carolina','Ohio','Missouri','Pennsylvania','Georgia','Virginia','Washington','Arizona','Florida','Massachusetts']
new2=need2[need2['STNAME'].isin(state)]
new2.to_csv('pop_across-12-states(2010-2020).csv',index=False)

# reset index
new1.reset_index(drop=True, inplace=True)
new2.reset_index(drop=True, inplace=True)

# merge 2000-2020 population data and change year type
population_2000_2020 = new1.merge(new2,how='outer')
NEW['Year'] = NEW['Year'].astype(float)
NEW['Year'] = NEW['Year'].astype(int)
NEW['Year'] = NEW['Year'].astype(str)

# split county and reset index
NEW['County'] = NEW['County'].str.split(',').str[0].str.strip()
NEW.reset_index(drop=True, inplace=True)

# replace all Missing into NA and drop all NA
NEW.replace({'Missing':np.nan},inplace=True)
NEW.dropna(inplace=True)

# change deaths type
NEW['Deaths'] = NEW['Deaths'].astype(float)

# create new drug overdose death dataframe
overdose = NEW[['County','Year','Deaths','State']].groupby(['County','Year','State'],as_index=False).sum()
overdose

# drop data in other years and keep only 2003-2015 and rename year
pop=population_2000_2020.drop(['POPESTIMATE2010','POPESTIMATE2000','POPESTIMATE2001', 'POPESTIMATE2002','POPESTIMATE2016', 'POPESTIMATE2017', 'POPESTIMATE2018','POPESTIMATE2019', 'POPESTIMATE2020'], axis=1)
pop.rename(columns={'STNAME': 'State', 'CTYNAME': 'County',
                    'POPESTIMATE2003': '2003', 'POPESTIMATE2004': '2004','POPESTIMATE2005': '2005','POPESTIMATE2006': '2006','POPESTIMATE2007': '2007',
                    'POPESTIMATE2008': '2008', 'POPESTIMATE2009': '2009','CENSUS2010POP': '2010',
                    'POPESTIMATE2011': '2011','POPESTIMATE2012': '2012','POPESTIMATE2013': '2013',
                    'POPESTIMATE2014': '2014','POPESTIMATE2015': '2015'}, inplace=True)

# rename state and filter state, county, year and population
states = {'Texas':'TX','North Carolina':'NC', 'Missouri':'MO','Virginia':'VA','Florida':'FL','Pennsylvania':'PA','Ohio':'OH','Georgia':'GA','Washington':'WA','Arizona':'AZ','Michigan':'MI','Massachusetts':'MA'}
pop['State'].replace(states, inplace=True)
pop_each_year = pop.melt(id_vars=['State','County'],var_name='Year',value_name='Population')
pop_each_year

# merge overdose and pop_each_year 
overdose_pop = overdose.merge(pop_each_year,how='inner')
overdose_pop.dropna(inplace=True)

# create new column Death_Rate_per_100000
overdose_pop['Death_Rate_per_100000'] = (overdose_pop['Deaths'].astype(float)/overdose_pop['Population'].astype(float))*100000

# convert new dataset into final merged csv.file
overdose_pop.to_csv('merged_pop_drug_death.csv',index=False)
