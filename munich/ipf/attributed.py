from tqdm import tqdm
import pandas as pd
import numpy as np

"""
This stage adds additional attributes to the generated synthetic population from IPF.
"""

def configure(context):
    context.stage("bavaria.ipf.model")
    context.config("random_seed")

def execute(context):
    df = context.stage("bavaria.ipf.model")

    # Identifiers
    df["person_id"] = np.arange(len(df))
    df["household_id"] = np.arange(len(df))

    # Spatial
    df["commune_id"] = df["commune_id"].astype(str)
    df["iris_id"] = df["commune_id"] + "0000"
    df["iris_id"] = df["iris_id"].astype("category")

    # Fixed attributes
    df["work_outside_region"] = False
    df["education_outside_region"] = False
    df["consumption_units"] = 1.0
    df["household_size"] = 1
    df["couple"] = False
    df["studies"] = False
    df["socioprofessional_class"] = 0

    # License
    df["has_license"] = df["license"]

    # Don't consider vehicle availability
    df["number_of_cars"] = 1
    df["number_of_bicycles"] = 1

    # Ignore PT subscription
    df["has_pt_subscription"] = False

    # Commute mode (is this important?)
    df["commute_mode"] = np.nan

    # Age distribution (we inflate the categories and distribute the ages uniformly in each group)
    initial_weight = df["weight"].sum()

    age_values = np.sort(df["age_class"].unique())
    MAXIMUM_AGE = 100

    df_age = []
    for k in range(len(age_values)):
        lower = age_values[k]
        upper = MAXIMUM_AGE if k == len(age_values) - 1 else age_values[k + 1]
        count = upper - lower

        df_age.append(pd.DataFrame({ 
            "age_class": [lower] * count,
            "age": lower + np.arange(count),
            "age_factor": [1.0 / count] * count
        }))

    df_age = pd.concat(df_age)

    df = pd.merge(df, df_age, on = "age_class")
    df["weight"] *= df["age_factor"]
    df = df.drop(columns = ["age_class", "age_factor"])
    
    df["person_id"] = np.arange(len(df))
    df["household_id"] = np.arange(len(df))

    final_weight = df["weight"].sum()
    assert np.abs(initial_weight - final_weight) < 1e-6

    return df
