import pandas as pd
import os
import zipfile

"""
This stage loads the raw data from the French population census.
"""

def configure(context):
    context.stage("data.spatial.codes")

    context.config("data_path")
    context.config("census_path", "rp_2019")
    context.config("census_xla", "a1310c_202200.xla")

COLUMNS_DTYPES = {
    "CANTVILLE":"str",
    "NUMMI":"str",
    "AGED":"str",
    "COUPLE":"str",
    "CS1":"str",
    "DEPT":"str",
    "ETUD":"str",
    "ILETUD":"str",
    "ILT":"str",
    "IPONDI":"str",
    "IRIS":"str",
    "REGION":"str",
    "SEXE":"str",
    "TACT":"str",
    "TRANS":"str",
    "VOIT":"str",
    "DEROU":"str"
}

def execute(context):
    df_records = []
    df_codes = context.stage("data.spatial.codes")

    requested_departements = df_codes["departement_id"].astype(str).unique()

    f = "{}/{}/{}".format(context.config("data_path"), context.config("census_path"),context.config("census_xla"))
    print(f)

    # read excel raw file
    col_names = ["CANTVILLE","optional_admin_id","name","gender","total","3","6","10","15","18","20","25","30","40","50","65","75","75+","gemeinde_id_2","strangers"]
    df_census = pd.read_excel(f, sheet_name="Gemeinden",skiprows=5,names=col_names)
    df_census = df_census.drop(columns=["gemeinde_id_2","strangers"])

    # exclude empty sections separator rows and landers or other administrative levels
    df_census = df_census.loc[df_census["gender"].notna()].copy()
    df_census = df_census.reset_index(drop=True)

    # fill data structure gaps
    df_census['CANTVILLE'] = df_census['CANTVILLE'].ffill()
    df_census['optional_admin_id'] = df_census['optional_admin_id'].ffill()
    df_census['name'] = df_census['name'].ffill()

    # cast data types and deal with 0 values expressed as "-"
    col_filter = df_census.columns.tolist()
    col_filter.remove("CANTVILLE")
    col_filter.remove("optional_admin_id")
    col_filter.remove("name")
    col_filter.remove("gender")

    for col in col_filter:
        df_census[col] = df_census[col].replace("-",0)
        df_census[col] = df_census[col].astype("int32")


    # replace total / female values by male / female values
    idxs_total = range(0,len(df_census),2)
    idxs_male = idxs_total
    idxs_female = range(1,len(df_census),2)

    for col in col_filter:
        df_census.loc[idxs_male,col] = df_census.loc[idxs_total,col] - df_census.loc[idxs_female,col].values

    df_census.loc[idxs_male,"gender"] = "male"
    df_census.loc[idxs_female,"gender"] = "female"

    # filter locations
    df_census['CANTVILLE'] = "09" + df_census['CANTVILLE']
    df_census = df_census.loc[df_census["CANTVILLE"].str.len()>3] # removes departments from list keeps states city (4 digits)

    df_census_cat = pd.DataFrame()
    for dep in requested_departements:
        df_census_ex = df_census.loc[df_census["CANTVILLE"].str.startswith(dep)]

        df_census_cat = pd.concat([df_census_cat,df_census_ex])


    return df_census_cat.reset_index(drop=True)


def validate(context):
    if not os.path.exists("{}/{}".format(context.config("data_path"), context.config("census_path"))):
        raise RuntimeError("RP 2019 data is not available")

    return os.path.getsize("{}/{}".format(context.config("data_path"), context.config("census_path")))
