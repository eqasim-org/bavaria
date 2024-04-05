"""
Created on Mon Apr  1 21:15:38 2024

This stage prepare datasets to merge employees from district level with inhabitants from municipality level
"""

import pandas as pd

def configure(context):
    context.config("data_path")
    context.stage("data.census.raw_employment")

def execute(context):
    print("test : ipf_format")
#     df_census = context.stage("data.census.raw")
#     df_employment = context.stage("data.census.raw_employment")

    
#     # %% preparation
#     age_cols = ['0','3', '6', '10', '15', '18', '20', '25', '30', '40', '50', '65', '75']
#     new_age_cols = ['3', '6', '10', '15', '18', '20', '25', '30', '40', '50', '65', '75',"120"]
    
    
#     df_census = df_census.loc[df_census["CANTVILLE"].str.len()>=5]
    
#     df_census["department_id"] = df_census["CANTVILLE"].str[:5]
#     df_census["municipality_id"] = df_census["CANTVILLE"] 
#     df_census["sex"] = df_census["gender"] 
    
#     df_census_all = pd.DataFrame()
#     df_model = df_census[['department_id',"municipality_id","sex"]].copy()
    
#     for col in age_cols:
#         df_model["age_class"] = col
#         df_model["weight"] = df_census[col]
#         df_census_all = pd.concat([df_census_all,df_model],ignore_index=True)
    
#     df_census_all.loc[df_census_all["sex"]== 'male','sex'] = 1
#     df_census_all.loc[df_census_all["sex"]== 'female','sex'] = 2
#     df_census_all["sex"] = df_census_all["sex"].astype(int)
    
#     df_census_all["new_age_class"] = ""
#     for idx,i in enumerate(age_cols):
#         df_census_all.loc[df_census_all["age_class"]==i,"new_age_class"] = new_age_cols[idx]
    
#     df_census_all["age_class_municipality"] = df_census_all["new_age_class"].astype(int)


# # %% employee preparation

#     df_model = df_employment[["department_id","age_class"]]
    
#     df_male = df_model.copy()
#     df_male["sex"] = 1
#     df_male["weight"] = df_employment["all_male"]
    
#     df_female = df_model.copy()
#     df_female["sex"] = 2
#     df_female["weight"] = df_employment["all_female"]
    
#     df_employment_all = pd.concat([df_male,df_female],ignore_index=True)
#     df_employment_all
#     df_employment_all["employed"] = True
#     df_employment_all.dtypes
    
#     df_employment_all = df_employment_all.loc[df_employment_all["department_id"].str.len()==5]
    
    
#     old_age_classes = [ 0, 20, 25, 30, 50, 60, 65]
#     new_age_classes = [ 20, 25, 30, 50, 60, 65,120]
    
#     df_employment_all["new_age_class"] = 0
#     for idx,i in enumerate(old_age_classes):
#         df_employment_all.loc[df_employment_all["age_class"]==i,"new_age_class"] = new_age_classes[idx]
    
#     df_employment_all["age_class_department"] = df_employment_all["new_age_class"].astype(int)
#     df_employment_all
    

#     return df_census_all #, df_employment_all
