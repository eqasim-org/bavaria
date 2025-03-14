import pandas as pd
import os
import numpy as np

"""
This stage loads the driving license ownership information for Germany.
"""

def configure(context):
    context.config("data_path")
    context.config("bavaria.licenses_path", "germany/fe4_2024.xlsx")

    context.stage("bavaria.data.spatial.codes")
    context.stage("bavaria.data.census.population")

COUNT_COLUMN = "Fahrerlaubnisse bzw. Führerscheine"
# COUNT_COLUMN = "Zusammen"

def execute(context):
    # Load country-wide data
    df_country = pd.read_excel("{}/{}".format(context.config("data_path"), context.config("bavaria.licenses_path")),
        sheet_name = "FE4.2", skiprows = 8)
    
    # Select columns
    df_country = df_country[["Geschlecht und\nLebensalter (in Jahren)", COUNT_COLUMN]]
    df_country.columns = ["age_class", "relative_weight"]

    # Construct sex column
    f_sex = df_country["age_class"].str.contains("Männer")
    f_sex |= df_country["age_class"].str.contains("Frauen")
    f_sex |= df_country["age_class"].str.contains("Zusammen")

    df_country.loc[f_sex, "sex"] = df_country.loc[f_sex, "age_class"]
    df_country["sex"] = df_country["sex"].ffill()

    df_country = df_country[~df_country["sex"].str.contains("Zusammen") & ~f_sex]

    df_country.loc[df_country["sex"].str.contains("Männer"), "sex"] = "male"
    df_country.loc[df_country["sex"].str.contains("Frauen"), "sex"] = "female"
    df_country["sex"] = df_country["sex"].astype("category")

    # Weight
    df_country["relative_weight"] = df_country["relative_weight"] / df_country["relative_weight"].sum()

    # Clean age column
    df_country["age_class"] = df_country["age_class"].apply(clean_age_class).astype(int)

    # Load Bundesland-specific data
    df_land = pd.read_excel("{}/{}".format(context.config("data_path"), context.config("bavaria.licenses_path")),
        sheet_name = "FE4.3", skiprows = 8)
    
    # Select columns
    df_land = df_land[["Geschlecht und Land", COUNT_COLUMN]]
    df_land.columns = ["land", "relative_weight"]

    # Construct sex column
    f_sex = df_land["land"].str.contains("Männer")
    f_sex |= df_land["land"].str.contains("Frauen")
    f_sex |= df_land["land"].str.contains("Zusammen")

    df_land.loc[f_sex, "sex"] = df_land.loc[f_sex, "land"]
    df_land["sex"] = df_land["sex"].ffill()

    df_land = df_land[~df_land["sex"].str.contains("Zusammen") & ~f_sex]

    df_land.loc[df_land["sex"].str.contains("Männer"), "sex"] = "male"
    df_land.loc[df_land["sex"].str.contains("Frauen"), "sex"] = "female"
    df_land["sex"] = df_land["sex"].astype("category")

    # Select Bavarian for the time being
    df_land = df_land[df_land["land"] == "Bayern"]

    # Weight
    df_land["relative_weight"] = df_land["relative_weight"] / df_land["relative_weight"].sum()

    # Load Kreis-specific data
    df_kreis = pd.read_excel("{}/{}".format(context.config("data_path"), context.config("bavaria.licenses_path")),
        sheet_name = "FE4.4", skiprows = 7)
    
    assert df_kreis.columns[1].startswith("Amtlicher")
    assert df_kreis.columns[5].startswith("Pkw")

    df_kreis = df_kreis[["Amtlicher Gemeindeschlüssel", COUNT_COLUMN]]
    df_kreis.columns = ["kreis_code", "weight"]

    # Select columns
    df_kreis = df_kreis[df_kreis["kreis_code"].str.len() == 5]
    
    # Formatting
    df_kreis["departement_id"] = df_kreis["kreis_code"].astype("str").astype("category")
    df_kreis = df_kreis[["departement_id", "weight"]]

    # Selection of districts
    df_codes = context.stage("bavaria.data.spatial.codes")
    df_kreis = df_kreis[df_kreis["departement_id"].isin(df_codes["departement_id"])]

    # Consolidation with population data
    df_population = context.stage("bavaria.data.census.population")

    required_kreis = set(df_population["commune_id"].str[:5].unique())
    available_kreis = set(df_kreis["departement_id"].unique())

    # Fix some missing information. Mostly, these are cases in which there is an independent city
    # (Kreisfreie Stadt) handles the licenses for the surrounding attached (homonymous) Kreis.
    missing_kreis = required_kreis - available_kreis

    if "09187" in missing_kreis:
        # Rosenheim (the Kreis 09187, not the city itself, 09163) is missing
        # We assume that both are aggregated as 09163, so we distribute by population ratio
        city_population = df_population.loc[df_population["commune_id"].str[:5] == "09163", "weight"].sum()
        kreis_population = df_population.loc[df_population["commune_id"].str[:5] == "09187", "weight"].sum()
        total_licenses = df_kreis.loc[df_kreis["departement_id"] == "09163", "weight"].sum() 
        city_ratio = city_population / (city_population + kreis_population)

        df_kreis.loc[df_kreis["departement_id"] == "09163", "weight"] = total_licenses * city_ratio

        df_kreis = pd.concat([df_kreis, pd.DataFrame({
            "departement_id": ["09187"], "weight": [total_licenses * (1 - city_ratio)]
        })])

        missing_kreis.remove("09187")

    if "09274" in missing_kreis:
        # Landshut (the Kreis 09274, not the city itself, 09261) is missing
        # We assume that both are aggregated as 09261, so we distribute by population ratio
        city_population = df_population.loc[df_population["commune_id"].str[:5] == "09261", "weight"].sum()
        kreis_population = df_population.loc[df_population["commune_id"].str[:5] == "09274", "weight"].sum()
        total_licenses = df_kreis.loc[df_kreis["departement_id"] == "09261", "weight"].sum() 
        city_ratio = city_population / (city_population + kreis_population)

        df_kreis.loc[df_kreis["departement_id"] == "09261", "weight"] = total_licenses * city_ratio

        df_kreis = pd.concat([df_kreis, pd.DataFrame({
            "departement_id": ["09274"], "weight": [total_licenses * (1 - city_ratio)]
        })])

        missing_kreis.remove("09274")

    if "09278" in missing_kreis:
        # Straubing (the Kreis 09278, not the city itself, 09263) is missing
        # We assume that both are aggregated as 09263, so we distribute by population ratio
        city_population = df_population.loc[df_population["commune_id"].str[:5] == "09263", "weight"].sum()
        kreis_population = df_population.loc[df_population["commune_id"].str[:5] == "09278", "weight"].sum()
        total_licenses = df_kreis.loc[df_kreis["departement_id"] == "09263", "weight"].sum() 
        city_ratio = city_population / (city_population + kreis_population)

        df_kreis.loc[df_kreis["departement_id"] == "09263", "weight"] = total_licenses * city_ratio

        df_kreis = pd.concat([df_kreis, pd.DataFrame({
            "departement_id": ["09278"], "weight": [total_licenses * (1 - city_ratio)]
        })])

        missing_kreis.remove("09278")

    # Check that we have fixed all Kreis
    assert len(missing_kreis) == 0

    return df_country, df_land, df_kreis

def clean_age_class(age_class):
    if age_class.startswith("Bis"):
        return 0
    elif age_class.endswith("mehr"):
        return int(age_class.split(" ")[0])
    else:
        return int(age_class.split(" ")[0])
