import pandas as pd
import numpy as np
import os

"""
This stage loads the raw household income data for Bavaria.
"""

def configure(context):
    context.config("data_path")
    context.config("bavaria.household_income_path", "bavaria/12211-101.xlsx")

def transform_income(income):
    if income.startswith("unter"):
        return "0-500"
    elif "bis unter" in income:
        lower, upper = int(income.split(" ")[0]), int(income.split(" ")[3])
        return "{}-{}".format(lower, upper)
    elif income.endswith("und mehr"):
        return "5000+"
    else:
        raise RuntimeError()

def execute(context):
    df = pd.read_excel("{}/{}".format(
        context.config("data_path"), context.config("bavaria.household_income_path")
    ), skiprows = 10, index_col = None)

    df.columns = [
        "income_class", "unused0", "p1", "unsued1", "p2", "p3", "p4", "p5+", "unused2", "unused3"
    ]

    df = df[["income_class", "p1", "p2", "p3", "p4", "p5+"]]
    df = df[df["income_class"].str.contains("unter") | df["income_class"].str.contains("und mehr")]

    for column in ("p1", "p2", "p3", "p4", "p5+"):
        df[column] = pd.to_numeric(df[column], errors = "coerce").fillna(0).astype(int) * 1000

    df = pd.melt(df, "income_class", ["p1", "p2", "p3", "p4", "p5+"], var_name = "household_size", value_name = "weight")
    df["household_size"] = df["household_size"].str.replace("p", "").astype("category")
    df["income_class"] = df["income_class"].apply(transform_income).astype("category")

    df = df[["household_size", "income_class", "weight"]]
    return df

def validate(context):
    if not os.path.exists("{}/{}".format(context.config("data_path"), context.config("bavaria.household_income_path"))):
        raise RuntimeError("Bavarian household income data is not available")

    return os.path.getsize("{}/{}".format(context.config("data_path"), context.config("bavaria.household_income_path")))
