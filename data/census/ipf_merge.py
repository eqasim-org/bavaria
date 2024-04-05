# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 21:15:38 2024

@author: arthur.burianne
"""
import pandas as pd
import numpy as np
import itertools


"""
This stage merge prepared datasets of employees from district level with inhabitants from municipality level
"""

def configure(context):
    context.stage("data.census.ipf_format")
    context.config("data_path")

 
def execute(context):
    df_municipalities,df_departments = context.stage("data.census.ipf_format")

        
    # Create a map that holds aset of municipalities (value) per department (key)
    municipalities_by_department = {
        department_id: set(municipality_ids)
        for department_id, municipality_ids in df_municipalities[[
            "department_id", "municipality_id"]].groupby("department_id")["municipality_id"]
    }
    
    # Construct a combined age category
    age_values = np.array(sorted(list(set(df_municipalities["age_class_municipality"]) | set(df_departments["age_class_department"]))))
    age_20_values = sorted(df_municipalities["age_class_municipality"].unique())
    age_15_values = sorted(df_departments["age_class_department"].unique())
    
    age_20_mapping = {}
    age_15_mapping = {}
    
    for age_value in age_values:
        age_20_mapping[age_value] = age_20_values[np.count_nonzero(age_value > age_20_values)]
        age_15_mapping[age_value] = age_15_values[np.count_nonzero(age_value > age_15_values)]
        
    # Construct other unique values
    sex_values = sorted(list(set(df_municipalities["sex"].unique()) | set(df_departments["sex"].unique())))    
    employed_values = [True,False]#sorted(df_departments["employed"].unique())
    municipality_values = sorted(df_municipalities["municipality_id"].unique())
    department_values = sorted(df_municipalities["department_id"].unique())


    # Initialize the data set with all combinations of values
    
    index = pd.MultiIndex.from_product([
        municipality_values,
        sex_values, age_values,
        employed_values
    ], names = [
        "municipality_id", 
        "sex", "age_combined", "employed"
    ])
    
    df_population = pd.DataFrame(index = index).reset_index()
    df_population["weight"] = 1.0
    
    # Write back department ID into the data set for weighting
    df_population["department_id"] = ""
    for department_id, municipality_ids in municipalities_by_department.items():
        df_population.loc[
            df_population["municipality_id"].isin(municipality_ids), "department_id"] = department_id
    
    df_population["age_class_department"] = df_population["age_combined"].replace(age_15_mapping)
    df_population["age_class_municipality"] = df_population["age_combined"].replace(age_20_mapping)
    
    df_population.head(8)


    # Prepare filters
    # First, construct municipality filters
    municipality_population_filters = []
    municipality_targets = []
    
    for combination in list(itertools.product(municipality_values, sex_values, age_20_values)):
        # Special IDF: Don't create filters for municipalities where we don't have data
        if np.count_nonzero(df_municipalities["municipality_id"] == combination[0]) == 0:
            continue # otherwise they would be scaled down to zero
    
        f_reference = df_municipalities["municipality_id"] == combination[0]
        f_reference &= df_municipalities["sex"] == combination[1]
        f_reference &= df_municipalities["age_class_municipality"] == combination[2] 
    
        f_population = df_population["municipality_id"] == combination[0]
        f_population &= df_population["sex"] == combination[1]
        f_population &= df_population["age_class_municipality"] == combination[2]
        municipality_population_filters.append(f_population)
    
        target_weight = df_municipalities.loc[f_reference, "weight"].sum()
        municipality_targets.append(target_weight)
    
    department_population_filters = []
    department_targets = []
    
    for combination in list(itertools.product(department_values, sex_values, age_15_values,[True])):
        f_reference = df_departments["department_id"] == combination[0]
        f_reference &= df_departments["sex"] == combination[1]
        f_reference &= df_departments["age_class_department"] == combination[2] 
        f_reference &= df_departments["employed"] == combination[3]
    
        f_population = df_population["department_id"] == combination[0]
        f_population &= df_population["sex"] == combination[1]
        f_population &= df_population["age_class_department"] == combination[2]
        f_population &= df_population["employed"] == combination[3]
        department_population_filters.append(f_population)
    
        target_weight = df_departments.loc[f_reference, "weight"].sum()
        department_targets.append(target_weight)

    
    # Initialize IPF
    iteration = 0
    ipf_factors = []

    
    # Perform IPF
    
    while iteration < 1000:
        print("Iteration", iteration)
        iteration_factors = []
    
        # First, run all municipality constraints
        for f, target_weight in zip(municipality_population_filters, municipality_targets):
            current_weight = df_population.loc[f, "weight"].sum()
    
            if current_weight > 0:
                update_factor = target_weight / current_weight
                df_population.loc[f, "weight"] *= update_factor
                iteration_factors.append(update_factor)
    
        # Second, run all department constraints
        for f, target_weight in zip(department_population_filters, department_targets):
            current_weight = df_population.loc[f, "weight"].sum()
    
            if current_weight > 0:
                update_factor = target_weight / current_weight
                df_population.loc[f, "weight"] *= update_factor
                iteration_factors.append(update_factor)
    
        print({
            "n": len(iteration_factors),
            "mean": np.mean(iteration_factors),
            "min": np.min(iteration_factors),
            "max": np.max(iteration_factors)
        })
    
        iteration_factors = np.array(iteration_factors)
        ipf_factors.append(iteration_factors)
    
        if np.max(iteration_factors) - 1 < 1e-2:
            if np.min(iteration_factors) > 1 - 1e-2:
                break
    
        iteration += 1

    return df_population
