import pandas as pd
import numpy as np
import os

"""
This stage loads the raw census data for Bavaria.

TODO: This could be replaced with a Germany-wide extract from GENESIS
"""

def configure(context):
    context.stage("germany.data.spatial.codes")

    context.config("data_path")
    context.config("bavaria.population_path", "bavaria/a1310c_202200.xla")

def construct_municipality_id(municipality_code, association_code):
    if len(municipality_code) == 3:
        # a city without a Kreis, pad with zeros
        return "09" + municipality_code + "0000000" 
    elif len(municipality_code) == 6:
        # a regular Gemeinde with a Kreis

        if association_code == "-":
            # the Gemeinde is not in an association (Verbund)
            return "".join([
                "09", # Bavaria
                municipality_code[0:3], # First digit (Bezirk) + two digits (Kreis)
                "0", # indicating that it is not in a Verbund
                municipality_code[3:], # Repeat last three digits (Gemeinde)
                municipality_code[3:], # Repeat last three digits (Gemeinde)
            ])
        
        else:
            # the Gemeinde is in an association (Verbund)
            return "".join([
                "09", # Bavaria
                municipality_code[0:3], # First digit (Bezirk) + two digits (Kreis)
                "5", # indicating that it is in a Verbund
                str(association_code), # the association code
                municipality_code[3:], # Repeat last three digits (Gemeinde)
            ])

    raise RuntimeError("Invalid format")

def execute(context):
    df_census = pd.read_excel("{}/{}".format(
        context.config("data_path"), context.config("bavaria.population_path")
    ), sheet_name = "Gemeinden", skiprows = 5, names = [
        "municipality_code", "association_code", "name", "sex", "total", 
        "age_0", "age_3", "age_6","age_10", "age_15", "age_18", "age_20", "age_25", "age_30", "age_40", "age_50", "age_65", "age_75", 
        "municipality_code_copy", "association_code_copy"
    ])

    # Only keep rows where we have a value
    df_census = df_census[~df_census["total"].isna()].copy()
    
    # Padding of identifiers, only one following line
    df_census["municipality_code"] = df_census["municipality_code"].ffill(limit = 1)
    df_census["association_code"] = df_census["association_code"].ffill(limit = 1)
    
    # Only keep rows where we have 6 digits (Bezirk + Kreis + Gemeinde) or 3 digits (city without Kreis)
    df_census = df_census[
        (df_census["municipality_code"].str.len() == 6) | 
        (df_census["municipality_code"].str.len() == 3)
    ].copy()

    # Now reconstruct the municipality code (ARS, the first column gives the AGS!)
    # All municipalities that are without a Kreis get a 0 suffix
    df_census["commune_id"] = [
        construct_municipality_id(*codes) for codes in zip(
            df_census["municipality_code"], df_census["association_code"]
        ) 
    ]

    df_census["commune_id"] = df_census["commune_id"].astype("category")

    # Clean up age structure
    df_census = pd.melt(df_census, ["commune_id", "sex"], [
        "age_0", "age_3", "age_6","age_10", "age_15", "age_18", "age_20", "age_25", "age_30", "age_40", "age_50", "age_65", "age_75"
    ], var_name = "age_class", value_name = "population")

    df_census["age_class"] = df_census["age_class"].str.replace("age_", "").astype(int)

    # Clean counts
    df_census["population"] = df_census["population"].replace({ "-": 0 }).astype(int)

    # Cleanup gender
    df_census["sex"] = df_census["sex"].replace({
        "  insgesamt": "total", "  weiblich": "female"
    })

    df_census = pd.merge(
        df_census[df_census["sex"] == "total"].rename(columns = { "population": "total_population" }).drop(columns = ["sex"]),
        df_census[df_census["sex"] == "female"].rename(columns = { "population": "female_population" }).drop(columns = ["sex"]),
        on = ["commune_id", "age_class"]
    )
    
    df_census["male_population"] = df_census["total_population"] - df_census["female_population"]

    df_male = df_census[["commune_id", "age_class", "male_population"]].rename(columns = {
        "male_population": "weight"
    })
    df_male["sex"] = "male"

    df_female = df_census[["commune_id", "age_class", "female_population"]].rename(columns = {
        "female_population": "weight"
    })
    df_female["sex"] = "female"

    df_census = pd.concat([df_male, df_female])
    df_census["sex"] = df_census["sex"].astype("category")

    # Filter for requested codes
    df_codes = context.stage("germany.data.spatial.codes")
    df_census = df_census[df_census["commune_id"].isin(df_codes["commune_id"])]

    return df_census[["commune_id", "sex", "age_class", "weight"]]

def validate(context):
    if not os.path.exists("{}/{}".format(context.config("data_path"), context.config("bavaria.population_path"))):
        raise RuntimeError("Bavarian census data is not available")

    return os.path.getsize("{}/{}".format(context.config("data_path"), context.config("bavaria.population_path")))
