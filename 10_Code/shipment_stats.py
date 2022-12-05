#!/usr/bin/env python
# coding: utf-8

# In[104]:


import pandas as pd
import numpy as np
shipment = pd.read_parquet('../20_Intermediate_Files/mergedshipment.parquet', engine = 'fastparquet')
shipment.head()


# In[105]:


# subset the data to only include the states of interest
shipment_sub = shipment[shipment['BUYER_STATE'].isin(["FL", "MI", "NC", "OH", "WA", "AZ", "MO", "GA"])].copy()

flor_ship= shipment_sub[shipment_sub["BUYER_STATE"].isin(["FL", "MI", "NC", "OH"])].copy()
wash_ship = shipment_sub[shipment_sub["BUYER_STATE"].isin(["WA", "MO", "GA", "AZ"])].copy()

flor_ship["case"] = np.where(flor_ship["BUYER_STATE"] == "FL", "Policy", "No policy")
wash_ship["case"] = np.where(wash_ship["BUYER_STATE"] == "WA", "Policy", "No policy")

flor_ship["policy"] = np.where(flor_ship["Year"] >= 2010, "post", "pre")
wash_ship["policy"] = np.where(wash_ship["Year"] >= 2011, "post", "pre")




# In[106]:


# summary statistics for the shipment data
flor_ship.groupby(["case", "policy"]).agg({"Opioids_per_Capita": ["mean", "median", "std"]})


# In[107]:


# number of counties in florida
flor_ship.groupby(["case", "policy"]).agg({"BUYER_COUNTY": "nunique"})


# In[108]:


wash_ship.groupby(["case", "policy"]).agg({"BUYER_COUNTY": "nunique"})


# In[109]:


# opioid shipment per capita for florida and control states
flor_ship.groupby(["case"]).agg({"Opioids_per_Capita": ["mean", "median", "min", "max"]})


# In[125]:


#washington and the control states
wash_ship.groupby(["case"]).agg({"Opioids_per_Capita": ["mean", "median", "std", "count"]})


# In[127]:


flor_ship.head(3)


# In[112]:


# total quantity of opioids shipped to Florida pre and post policy
flor_ship.groupby(["case", "policy"]).agg({"Opioids_per_Capita": ["mean","median", "min", "max"]})


# In[113]:


wash_ship.groupby(["case", "policy"]).agg({"Opioids_per_Capita": ["mean", "median", "min", "max"]})
#washington_opioid_stats.to_csv("../30_Results/washington_opioid_stats.csv", index = False)


# In[114]:


# county with the highest opioid shipment per capita
flor_ship.groupby(["State","BUYER_COUNTY"]).agg({"Opioids_per_Capita": ["mean", "median", "min", "max"]}).sort_values(by = [("Opioids_per_Capita", "mean")], ascending = False).head(10)


# In[124]:


# subset for florida only
flor_ship_plot = flor_ship[flor_ship["BUYER_STATE"] == "FL"].copy()
# states opiod shipments over  over time
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid")
sns.set_context("talk")




# death rate over time for all counties in florida depending on whether they had a policy or not

flor_ship_plot.groupby(["Year","BUYER_COUNTY"])["Opioids_per_Capita"].mean().unstack().plot(legend=False, figsize = (10,5))
# show the first 5 countries on the in the legenf with the highest Opoid shipment per capita
plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), fancybox=True, shadow=True, ncol=5)

plt.title("Opioid shipment per capita over time for all counties in Florida")
plt.ylabel("Opioid shipments per capita")
plt.xlabel("Year")
plt.show()



# In[117]:


# states opiod shipments over  over time
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid")
sns.set_context("talk")

wash_ship.groupby(["Year", "case"])["Opioids_per_Capita"].mean().unstack().plot()
plt.title("Opiod Shipments in Washington")
plt.ylabel("Opioids per Capita")
plt.xlabel("Year")
plt.show()

