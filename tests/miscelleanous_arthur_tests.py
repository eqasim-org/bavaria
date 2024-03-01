# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 10:46:15 2024

@author: arthur.burianne
"""

import pandas as pd

tmp_path = ("C:/Users/arthur.burianne/Documents/tum/simulations/fake_munich_10_tmp/")

# df_filtered = pd.read_pickle(tmp_path + "data.census.filtered__bae49205a1e9b09da2324345552c5d19.p")

# df_cleaned = pd.read_pickle(tmp_path + "data.census.cleaned__bae49205a1e9b09da2324345552c5d19.p")

# df_raw = pd.read_pickle(tmp_path + "data.census.raw__bae49205a1e9b09da2324345552c5d19.p")

# df_adresses = pd.read_pickle(tmp_path + "synthesis.locations.home.locations__2b7751b4e24cc2bc3921da0807c1854d.p")

# df_homes = pd.read_pickle(tmp_path + "synthesis.population.spatial.home.zones__bacfbfd409f8e06d3fec7a51f32b9045.p")

# df_candidates = pd.read_pickle(tmp_path + "synthesis.population.spatial.primary.candidates__78451717d3f191e5664d400aea866a48.p")


df_records = []
requested_departements = ["091"]

f = "C:/Users/arthur.burianne/Documents/sources_data/fake_data_munich/rp_2019/13111-004r.xlsx"
print(f)


col_names = ["department_id","department_name","age_class","all_total","all_male",
             "all_female","national_all","national_male","national_female","foreign_all","foreign_male,","foreign_female"]
df_census = pd.read_excel(f,skiprows=6,names=col_names)

df_census = df_census.loc[df_census["all_total"].notna()].copy()

df_census["department_id"] = df_census["department_id"].ffill()

df_census["department_name"] = df_census["department_name"].ffill()

df_census = df_census.loc[df_census["department_id"].str.startswith("091")].copy()

df_census = df_census.loc[df_census["age_class"] != "Insgesamt"].copy()
df_census.loc[df_census["age_class"] == "unter 20","age_class"] = "0"
df_census["age_class"] = df_census["age_class"].str[:2]

col_filter = df_census.columns.tolist()
col_filter.remove("department_id")
col_filter.remove("department_name")

for col in col_filter:
    df_census[col] = df_census[col].replace(".",0)
    df_census[col] = df_census[col].astype("int32")


# %%
# read excel raw file
col_names = ["CANTVILLE","name","total","male","foreign","6","10","15","18","20","25","30","40","50","65","75","75+","gemeinde_id_2","strangers"]
df_census = pd.read_excel(f, sheet_name="Gemeinden",skiprows=5,names=col_names)
df_census = df_census.drop(columns=["gemeinde_id_2","strangers"])

# exclude empty sections separator rows and landers or other administrative levels
df_census = df_census.loc[df_census["gender"].notna()].copy()
df_census = df_census.reset_index(drop=True)

# fill data structure gaps
df_census['CANTVILLE'] = df_census['CANTVILLE'].ffill()
df_census['optional_admin_id'] = df_census['optional_admin_id'].ffill()
df_census['name'] = df_census['name'].ffill()

# cast data types and deal with 0 values expressed as "-"
col_filter = df_census.columns.tolist()
col_filter.remove("CANTVILLE")
col_filter.remove("optional_admin_id")
col_filter.remove("name")
col_filter.remove("gender")

for col in col_filter:
    df_census[col] = df_census[col].replace("-",0)
    df_census[col] = df_census[col].astype("int32")


# replace total / female values by male / female values
idxs_total = range(0,len(df_census),2)
idxs_male = idxs_total
idxs_female = range(1,len(df_census),2)

for col in col_filter:
    df_census.loc[idxs_male,col] = df_census.loc[idxs_total,col] - df_census.loc[idxs_female,col].values

df_census.loc[idxs_male,"gender"] = "male"
df_census.loc[idxs_female,"gender"] = "female"

# filter locations
df_census['CANTVILLE'] = "09" + df_census['CANTVILLE']
df_census = df_census.loc[df_census["CANTVILLE"].str.len()>3] # removes departments from list keeps states city (4 digits)

df_census_cat = pd.DataFrame()
for dep in requested_departements:
    df_census_ex = df_census.loc[df_census["CANTVILLE"].str.startswith(dep)]

    df_census_cat = pd.concat([df_census_cat,df_census_ex])


# return df_census_cat.reset_index(drop=True)


# def validate(context):
#     if not os.path.exists("{}/{}".format(context.config("data_path"), context.config("census_path"))):
#         raise RuntimeError("RP 2019 data is not available")

#     return os.path.getsize("{}/{}".format(context.config("data_path"), context.config("census_path")))
