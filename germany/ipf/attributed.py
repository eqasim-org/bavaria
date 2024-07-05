from tqdm import tqdm
import pandas as pd
import numpy as np

"""
This stage adds additional attributes to the generated synthetic population from IPF.
"""

def configure(context):
    context.stage("germany.ipf.model")
    context.config("random_seed")

def execute(context):
    df = context.stage("germany.ipf.model")

    # Identifiers
    df["person_id"] = np.arange(len(df))
    df["household_id"] = np.arange(len(df))

    # Spatial
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

    # Vehicles (attention, may be tricky for choice model)
    df["number_of_vehicles"] = 0

    # Commute mode (is this important?)
    df["commute_mode"] = np.nan

    # Age distribution
    random = np.random.RandomState(context.config("random_seed"))
    
    age_values = np.sort(df["age_class"].unique())
    MAXIMUM_AGE = 100

    for k in range(len(age_values)):
        lower = 0 if k == 0 else age_values[k - 1]
        upper = age_values[k]

        f = df["age_class"] == upper
        upper = min(upper, MAXIMUM_AGE + 1)
                    
        df.loc[f, "age"] = random.randint(lower, upper, np.count_nonzero(f))

    df["age"] = df["age"].astype(int)
    return df
