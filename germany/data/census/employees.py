import pandas as pd
import os
import numpy as np

"""
This stage loads the work flow data (Pendlerstatistik) for Bavaria.
"""

def configure(context):
    context.config("data_path")
    context.config("bavaria.work_flow_path", "bavaria/a6502c_202200.xla")

    context.stage("germany.data.spatial.codes")

def execute(context):
    # Load data
    df = pd.read_excel("{}/{}".format(context.config("data_path"), context.config("bavaria.work_flow_path")),
        skiprows = 8, sheet_name = "Gemeinden", names = [
            "municipality_code", "municipality_name", "unknown1",
            "count", "male", "foreigners", 
            "sector4", "sector5", "sector6", "sector7", "sector8",
            "flow", "other"
        ])
    
    df["municipality_code"] = df["municipality_code"].astype(str)
    
    # Remove text at the end
    index = np.argmax(df["municipality_code"].str.startswith("*)"))
    df = df.iloc[:index].copy()

    # Remove totals
    df = df[df["municipality_name"] != "Insgesamt"].copy()

    # Obtain Kreis identifier from lines without count
    f = df["count"].isna() & (df["municipality_code"] != "nan")
    df.loc[f, "kreis"] = df.loc[f, "municipality_code"]
    df["kreis"] = df["kreis"].ffill()

    # Only rows with values
    df = df[~df["count"].isna() & (df["count"] != "•")].copy()

    # Only rows with valid identifiers
    df = df[df["municipality_code"].str.len() != "nan"]

    # Construct identifiers
    df_codes = context.stage("germany.data.spatial.codes")
    
    f_kreisfrei = df["kreis"].isna()
    df.loc[f_kreisfrei, "ags"] = "09" + df["municipality_code"] + "000"
    df.loc[~f_kreisfrei, "ags"] = "09" + df["kreis"] + df["municipality_code"]

    df = pd.merge(df, df_codes[["ags", "commune_id"]], on = "ags")

    missing = set(df_codes["ags"]) - set(df["ags"])
    #assert len(missing) == 59

    df = df[["commune_id", "count"]].rename(columns = {
        "count": "weight"
    })

    # Data type
    df["weight"] = df["weight"].astype(float)

    return df
