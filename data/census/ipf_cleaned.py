# -*- coding: utf-8 -*-
"""
Created on Sat Apr 13 22:16:45 2024

@author: arthur.burianne
"""

from tqdm import tqdm
import pandas as pd
import numpy as np

"""
This stage filters out census observations which live or work outside of
Île-de-France.
"""

def configure(context):
    context.stage("data.census.ipf_merge")

def execute(context):
    df = context.stage("data.census.ipf_merge")
    
    df = df.reset_index()

    # simplified fake values
    df["person_id"] = df.index
    df["household_id"] = df.index
    df["iris_id"] = "undefined"
    df["commune_id"] =  df["municipality_id"]
    df["departement_id"] =  df["department_id"]
    df = df.drop(columns=["municipality_id","department_id"])
    df["work_outside_region"] =  False
    df["education_outside_region"] =  False
    df["consumption_units"] = 1.2
    df["household_size"] = 1

    
    # random fake values
    df["age"] =  0
    df.loc[df["age_class_municipality"] == 120,"age_class_municipality"] = 99
    age_classes = df["age_class_municipality"].unique()
    
    old_age = 0
    for age in age_classes:    
        df.loc[(df["age_class_municipality"]>old_age)&(df["age_class_municipality"]<=age),"age"] = np.random.randint(old_age,age,len(df.loc[(df["age_class_municipality"]>old_age)&(df["age_class_municipality"]<=age),"age"]))
        

    # df.loc[df["sex"] == "1", "sex"] = "male"
    # df.loc[df["sex"] == "2", "sex"] = "female"
    # df["sex"] = df["sex"].astype("category")

    df["couple"] = False   
    df.loc[np.random.randint(0,2,len(df))==1,"couple"] = True

    df["commute_mode"] = "not_assigned"
    df["TRANS"] = np.random.randint(1,6,len(df))
    df.loc[df["TRANS"] == 1, "commute_mode"] = np.nan
    df.loc[df["TRANS"] == 2, "commute_mode"] = "walk"
    df.loc[df["TRANS"] == 3, "commute_mode"] = "bike"
    df.loc[df["TRANS"] == 4, "commute_mode"] = "car"
    df.loc[df["TRANS"] == 5, "commute_mode"] = "pt"
    df["commute_mode"] = df["commute_mode"].astype("category")

    df["studies"] = False   
    df.loc[np.random.randint(0,6,len(df))==1,"studies"] = True
    df.loc[df["employed"],"studies"] = True

    df["number_of_vehicles"] = np.random.randint(0,3,len(df))

    df["socioprofessional_class"] = np.random.randint(0,8,len(df))

    return df
    
