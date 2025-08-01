import pandas as pd
import numpy as np
import os

"""
This stage loads the raw household size data for Bavaria.
"""

def configure(context):
    context.config("data_path")
    context.config("bavaria.household_size_path", "bavaria/12211-105.xlsx")

def lower_age(age):
    if age.startswith("unter"):
        return 0
    elif "bis unter" in age:
        age = age.split(" bis unter ")
        return int(age[0])
    elif age.endswith("oder älter"):
        return int(age.split(" ")[0])
    
def upper_age(age):
    if age.startswith("unter"):
        return int(age.split(" ")[-1])
    elif "bis unter" in age:
        age = age.split(" bis unter ")
        return int(age[1])
    elif age.endswith("oder älter"):
        return np.inf

def execute(context):
    df = pd.read_excel("{}/{}".format(
        context.config("data_path"), context.config("bavaria.household_size_path")
    ), skiprows = 5, index_col = None)

    df.columns = [
        "sex", "age", "unused0", "p1", "unused1", "p2", "p3", "p4", "p5+"
    ]

    df["sex"] = df["sex"].ffill()
    df = df[df["sex"].eq("weiblich") | df["sex"].eq("männlich")]
    df = df[df["age"].ne("Insgesamt")]
    df = df[["sex", "age", "p1", "p2", "p3", "p4", "p5+"]]

    for column in ("p1", "p2", "p3", "p4", "p5+"):
        df[column] = pd.to_numeric(df[column], errors = "coerce").fillna(0).astype(int) * 1000

    df = pd.melt(df, ["sex", "age"], ["p1", "p2", "p3", "p4", "p5+"], var_name = "household_size", value_name = "weight")
    df["household_size"] = df["household_size"].str.replace("p", "").astype("category")
    df["lower_age"] = df["age"].apply(lower_age)
    df["upper_age"] = df["age"].apply(upper_age)
    df["sex"] = df["sex"].replace({
        "männlich": "male", "weiblich": "female"
    }).astype("category")

    df = df[["lower_age", "upper_age", "sex", "household_size", "weight"]]
    return df

def validate(context):
    if not os.path.exists("{}/{}".format(context.config("data_path"), context.config("bavaria.household_size_path"))):
        raise RuntimeError("Bavarian household size data is not available")

    return os.path.getsize("{}/{}".format(context.config("data_path"), context.config("bavaria.household_size_path")))
