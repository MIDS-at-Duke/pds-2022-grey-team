#!/usr/bin/env python
# coding: utf-8

# In[47]:


import pandas as pd
import numpy as np


# In[48]:


# Read in the data
df = pd.read_csv("../00_Source/merged_pop_drug_death.csv")

# number of counties per state
df.groupby("State")["County"].nunique()


# In[49]:


# total number of counties
df["County"].nunique()


# In[50]:


# mean death_rate
df["Deaths"].describe()


# In[51]:


# mean of deaths per county
df.groupby(["State", "County"])["Deaths"].mean().sort_values(ascending=False)


# In[52]:


# number of counties with a mean death rate == 10, the minimum
df.groupby(["State", "County"])["Deaths"].mean().sort_values(ascending=False).loc[
    lambda x: x == 10
].count()


# In[53]:


# counties with highest drug death rates by county and state
df.groupby(["State", "County"])["Death_Rate_per_100000"].mean().sort_values(
    ascending=False
).head(10)


# In[54]:


# counties with highest drug death rates by county and state
df.groupby(["State", "County"])["Deaths"].mean().sort_values(ascending=False).head(10)


# In[55]:


# subset for each state and their controls
flor_cont = df[df["State"].isin(["FL", "MI", "NC", "OH"])].copy()
wash_cont = df[df["State"].isin(["WA", "MO", "GA", "AZ"])].copy()
texas_cont = df[df["State"].isin(["TX", "PA", "VA", "MA"])].copy()

# make sure the shapes from the 3 groups add up to the total in df
assert len(flor_cont) + len(wash_cont) + len(texas_cont) == len(df)


flor_cont["State"].unique()
wash_cont["State"].unique()
texas_cont["State"].unique()


# In[56]:


# indicator for each state depending on whether they had a policy enacted or not
flor_cont["case"] = np.where(flor_cont["State"] == "FL", "Policy", "No policy")
wash_cont["case"] = np.where(wash_cont["State"] == "WA", "Policy", "No policy")
texas_cont["case"] = np.where(texas_cont["State"] == "TX", "Policy", "No policy")

# merge the 3 groups into one dataframe
merged_test = pd.concat([flor_cont, wash_cont, texas_cont])


# In[57]:


# another indicator for when policy took effect(pre/post)

flor_cont["policy"] = np.where(flor_cont["Year"] >= 2010, "post", "pre")
wash_cont["policy"] = np.where(wash_cont["Year"] >= 2011, "post", "pre")
texas_cont["policy"] = np.where(texas_cont["Year"] >= 2007, "post", "pre")


# In[58]:


flor_cont.head()


# In[59]:


flor_cont.groupby(["case", "policy"])["Death_Rate_per_100000"].describe()


# In[60]:


wash_cont.groupby(["case", "policy"])["Death_Rate_per_100000"].describe()
# seing the same trend for washington


# In[61]:


texas_cont.groupby(["case", "policy"])["Death_Rate_per_100000"].describe()


# In[62]:


# states death trends over time
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid")
sns.set_context("talk")

# death rate over time for all states depending on whether they had a policy or not

merged_test.groupby(["Year", "case"])["Death_Rate_per_100000"].mean().unstack().plot()
plt.title("Mortality Rate Per 100,000 Persons Over Time")
plt.ylabel("Mean death rate")
plt.xlabel("Year")
plt.show()

