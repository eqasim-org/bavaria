from tqdm import tqdm
import pandas as pd
import numpy as np
import data.hts.hts as hts

"""
This stage cleans the French population census:
  - Assign new unique integer IDs to households and persons
  - Clean up spatial information and sociodemographic attributes
"""

def configure(context):
    context.stage("data.census.raw")
    context.stage("data.spatial.codes")


def execute(context):
    df = context.stage("data.census.raw")

    df_census = pd.DataFrame()
    cols_ages = ["0","3","6","10","15","18","20","25","30","40","50","65","75"]
    classes_ages = [2,5,9,14,17,19,24,29,39,49,64,74,76]

    cols_mains = ["CANTVILLE","optional_admin_id","name","gender"]

    for idx, row in df.iterrows():
        df_tmp = pd.DataFrame(columns=cols_mains)
        df_tmp["age"] =classes_ages
        df_tmp["IPONDI"] = row[cols_ages].values
        df_tmp[cols_mains]  = row[cols_mains]
        df_census = pd.concat([df_census,df_tmp])

    df  = df_census.reset_index(drop=True)
    df["NUMMI"] = df.index

    # Construct household IDs for persons with NUMMI != Z
    df_household_ids = df[["CANTVILLE", "NUMMI"]]
    df_household_ids = df_household_ids[df_household_ids["NUMMI"] != "Z"]
    df_household_ids["temporary"] = df_household_ids["CANTVILLE"].astype(int) + df_household_ids["NUMMI"]
    df_household_ids = df_household_ids.drop_duplicates("temporary")
    df_household_ids["household_id"] = np.arange(len(df_household_ids))
    df = pd.merge(df, df_household_ids, on = ["CANTVILLE", "NUMMI"], how = "left")

    # Fill up undefined household ids (those where NUMMI == Z)
    f = np.isnan(df["household_id"])
    df.loc[f, "household_id"] = np.arange(np.count_nonzero(f)) + df["household_id"].max()
    df["household_id"] = df["household_id"].astype(int)

    # Put person IDs
    df["person_id"] = np.arange(len(df))

    # Sorting
    df = df.sort_values(by = ["household_id", "person_id"])

    # Spatial information
    df["departement_id"] = "091"
    df["departement_id"] =  df["departement_id"].astype("category")
    # df["departement_id"] = df["DEPT"].astype("category")

    df["commune_id"] = df["CANTVILLE"]
    f_undefined = df["commune_id"].str.contains("Z")
    df.loc[f_undefined, "commune_id"] = "undefined"
    df["commune_id"] = df["commune_id"].astype("category")

    df["iris_id"] = "Z"
    f_undefined = df["iris_id"].str.contains("Z") | df["iris_id"].str.contains("X")
    df.loc[f_undefined, "iris_id"] = "undefined"
    df["iris_id"] = df["iris_id"].astype("category")

    # Verify with requested codes
    df_codes = context.stage("data.spatial.codes")

    df["commune_id"] =  df["commune_id"].astype(str)
    mask  = df["commune_id"].str.len()==5
    df.loc[mask,"commune_id"] = df.loc[mask,"commune_id"] + "000"
    df["commune_id"] =  df["commune_id"].astype("category")


    excess_communes = set(df["commune_id"].astype(str).unique()) - set(df_codes["commune_id"].astype(str).unique())


    # print(excess_communes)

    if not excess_communes == set():
        raise RuntimeError("Found additional communes: %s" % excess_communes)

    # excess_iris = set(df["iris_id"].unique()) - set(df_codes["iris_id"].unique())
    # if not excess_iris == set():
    #     raise RuntimeError("Found additional IRIS: %s" % len(excess_iris))

    # Age
    # df["age"] = df["AGED"].apply(lambda x: "0" if x == "000" else x.lstrip("0")).astype(int)
    df["age"] = np.random.randint(0,100,len(df))

    # Clean COUPLE
    df["COUPLE"] = np.random.randint(0,2,len(df))
    df["couple"] = df["COUPLE"] ==  1

    # Clean TRANS
    df["TRANS"] = np.random.randint(1,6,len(df))
    df.loc[df["TRANS"] == 1, "commute_mode"] = np.nan
    df.loc[df["TRANS"] == 2, "commute_mode"] = "walk"
    df.loc[df["TRANS"] == 3, "commute_mode"] = "bike"
    df.loc[df["TRANS"] == 4, "commute_mode"] = "car"
    df.loc[df["TRANS"] == 5, "commute_mode"] = "pt"
    df.loc[df["TRANS"] == "Z", "commute_mode"] = np.nan
    df["commute_mode"] = df["commute_mode"].astype("category")

    # Weight
    df["weight"] = df["IPONDI"]

    print(df["IPONDI"] )
    # df["weight"] = df["total"]

    # Clean SEXE
    # df.loc[df["SEXE"] == "1", "sex"] = "male"
    # df.loc[df["SEXE"] == "2", "sex"] = "female"
    df["sex"] = df["gender"].astype("category")

    # Clean employment
    df["TACT"] = np.random.randint(10,13,len(df))
    df["employed"] = df["TACT"] == 11

    # Studies
    df["ETUD"] = np.random.randint(0,7,len(df))

    df["studies"] = df["ETUD"] == 1

    df["number_of_vehicles"] = np.random.randint(0,3,len(df))

    # Household size
    df_size = df[["household_id"]].groupby("household_id").size().reset_index(name = "household_size")
    df = pd.merge(df, df_size)

    # Socioprofessional category
    df["CS1"] = np.random.randint(0,8,len(df))
    df["socioprofessional_class"] = df["CS1"].astype(int)

    # Place of work or education
    df["ILT"] = np.random.randint(0,20,len(df))
    df["ILETUD"] =  np.random.randint(0,20,len(df))

    df["work_outside_region"] = df["ILT"].isin((4, 5, 6))
    df["education_outside_region"] = df["ILETUD"].isin((4,5, 6))

    # Consumption units
    df = pd.merge(df, hts.calculate_consumption_units(df), on = "household_id")

    return df[[
        "person_id", "household_id", "weight",
        "iris_id", "commune_id", "departement_id",
        "age", "sex", "couple",
        "commute_mode", "employed",
        "studies", "number_of_vehicles", "household_size",
        "work_outside_region", "education_outside_region",
        "consumption_units", "socioprofessional_class"
    ]]

#
