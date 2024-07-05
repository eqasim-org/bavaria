import pandas as pd
import numpy as np

"""
This stage updates the formatting of the population and employment census data sets such
that they can be procesed by the IPF algorithm.
"""

def configure(context):
    context.stage("germany.data.census.population")
    context.stage("germany.data.census.employment")

def execute(context):
    # Load data
    df_population = context.stage("germany.data.census.population")
    df_employment = context.stage("germany.data.census.employment")

    # Generate upper bounded age classes
    lower_age = np.sort(df_population["age_class"].unique())
    upper_age = np.array(list(lower_age[1:]) + [9999])
    df_population["age_class"] = df_population["age_class"].replace(lower_age, upper_age)

    # Generate upper bounded age classes
    lower_age = np.sort(df_employment["age_class"].unique())
    upper_age = np.array(list(lower_age[1:]) + [9999])
    df_employment["age_class"] = df_employment["age_class"].replace(lower_age, upper_age)

    # Generate numeric sex
    df_population["sex"] = df_population["sex"].replace({ "male": 1, "female": 2 })
    df_employment["sex"] = df_employment["sex"].replace({ "male": 1, "female": 2 })

    # Validation
    unique_population_kreis = set(df_population["commune_id"].str[:5].unique())
    unique_employment_kreis = set(df_employment["departement_id"].unique())
    assert unique_population_kreis == unique_employment_kreis

    # Departement ID
    df_population["departement_id"] = df_population["commune_id"].str[:5].astype("category")

    return df_population, df_employment
