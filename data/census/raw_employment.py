import pandas as pd
import os
"""
This stage loads the raw data from the French population census.
"""

def configure(context):
    context.stage("data.census.raw")
    context.config("data_path")
    context.config("census_path", "rp_2019")
    context.config("census_xla", "13111-004r.xlsx")


def execute(context):

    f = "{}/{}/{}".format(context.config("data_path"), context.config("census_path"),context.config("census_xla"))

    col_names = ["department_id","department_name","age_class","all_total","all_male",
                 "all_female","national_all","national_male","national_female","foreign_all","foreign_male,","foreign_female"]
    df_census = pd.read_excel(f,skiprows=6,names=col_names)

    df_census = df_census.loc[df_census["all_total"].notna()].copy()

    df_census["department_id"] = df_census["department_id"].ffill()

    df_census["department_name"] = df_census["department_name"].ffill()

    df_census = df_census.loc[df_census["department_id"].str.startswith("091")].copy()

    df_census = df_census.loc[df_census["age_class"] != "Insgesamt"].copy()
    df_census.loc[df_census["age_class"] == "unter 20","age_class"] = "0"
    df_census["age_class"] = df_census["age_class"].str[:2]

    col_filter = df_census.columns.tolist()
    col_filter.remove("department_id")
    col_filter.remove("department_name")

    for col in col_filter:
        df_census[col] = df_census[col].replace(".",0)
        df_census[col] = df_census[col].astype("int32")

    return df_census.reset_index(drop=True)


# def validate(context):
#     if not os.path.exists("{}/{}".format(context.config("data_path"), context.config("census_path"))):
#         raise RuntimeError("RP 2019 data is not available")

#     return os.path.getsize("{}/{}".format(context.config("data_path"), context.config("census_path")))
