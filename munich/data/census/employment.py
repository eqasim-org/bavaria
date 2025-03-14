import pandas as pd
import numpy as np
import os

"""
This stage loads the raw employment data for Bavaria

TODO: Can this be replaced by a Germany-wide extract from GENESIS?
"""

def configure(context):
    context.stage("bavaria.data.spatial.codes")

    context.config("data_path")
    context.config("bavaria.employment_path", "bavaria/13111-004r.xlsx")

def execute(context):
    # Load data
    df_employment = pd.read_excel("{}/{}".format(
        context.config("data_path"), context.config("bavaria.employment_path")
    ), skiprows = 6, names = [
        "departement_id", "department_name", "age_class", 
        "all_total", "all_male", "all_female", 
        "national_all", "national_male", "national_female",
        "foreign_all", "foreign_male", "foreign_female"
    ])

    # Remove text at the end
    index = np.argmax(df_employment["departement_id"] == "______________")
    df_employment = df_employment.iloc[:index]

    # Filter for full Kreis entries
    df_employment["departement_id"] = df_employment["departement_id"].ffill()

    df_employment = df_employment[
        df_employment["departement_id"].str.len() == 5
    ].copy()

    # Remove totals
    df_employment = df_employment[
        df_employment["age_class"] != "Insgesamt"
    ].copy()

    # Format age class
    df_employment.loc[df_employment["age_class"] == "unter 20", "age_class"] = "0"
    df_employment["age_class"] = df_employment["age_class"].str[:2]
    df_employment["age_class"] = df_employment["age_class"].astype(int)

    # Format data frame
    df_employment = df_employment[[
        "departement_id", "age_class", "all_male", "all_female"
    ]]

    # Bring into long format
    df_employment = pd.melt(df_employment, 
        ["departement_id", "age_class"], ["all_male", "all_female"], 
        var_name = "sex", value_name = "weight")
    
    # Format sex
    df_employment["sex"] = df_employment["sex"].str[4:].astype("category")

    # Filter for requested codes
    df_codes = context.stage("bavaria.data.spatial.codes")
    df_employment = df_employment[df_employment["departement_id"].isin(df_codes["departement_id"])]

    return df_employment[["departement_id", "age_class", "sex", "weight"]]

def validate(context):
    if not os.path.exists("{}/{}".format(context.config("data_path"), context.config("bavaria.employment_path"))):
        raise RuntimeError("Bavarian employment data is not available")

    return os.path.getsize("{}/{}".format(context.config("data_path"), context.config("bavaria.employment_path")))
