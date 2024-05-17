import pandas as pd
import os
import numpy as np

"""
"""

def configure(context):
    context.config("data_path")
    context.config("census_path", "rp_2019")
    context.config("census_xlc", "a6502c_202200.xla")


def execute(context):

    f = "{}/{}/{}".format(context.config("data_path"), context.config("census_path"),context.config("census_xlc"))

    col_names = ["municipality_id","municipality_name","test","employees","male",
                 "foreigners","4","5","6","7","8","local_employees","other"]
    df_census = pd.read_excel(f,skiprows=8,names=col_names,sheet_name="Gemeinden")
    
   
    # new departments index
    indices = df_census["municipality_name"] == "Insgesamt"
    idxs_total = df_census[indices].index
    idxs_starts = idxs_total+1
    idxs_starts = idxs_starts[:-1]
    
    # compose full municipality indexes
    df_census["department_id"] = ""
    df_census.loc[idxs_starts,"department_id"] = df_census.loc[idxs_starts,"municipality_id"] 
    df_census.loc[4,"department_id"] = "171"
    df_census.loc[df_census["department_id"] == "","department_id"] = np.nan 
    df_census["department_id"] = df_census["department_id"].ffill()
    df_census["municipality_id"] = "09" + df_census["department_id"].astype(str) + df_census["municipality_id"].astype(str)
    
    df_census = df_census.drop(idxs_starts)
    df_census = df_census.drop(idxs_total)
    df_census = df_census.loc[df_census["municipality_id"].notna()]

    df_census = df_census.drop(3)
    df_census = df_census.drop(4)
    # df_census = df_census.drop([2251,2252,2253,2254,2255,2256,2257])
    
    # deal with first 3 short length values
    df_census.loc[0,"municipality_id"] = "09161000"
    df_census.loc[1,"municipality_id"] = "09162000"
    df_census.loc[2,"municipality_id"] = "09163000"

    df_census["municipality_id"] = df_census["municipality_id"].astype(str)
    df_census = df_census.loc[df_census["municipality_id"].str.len()==8].copy()
    
    
    df_census = df_census.replace('•',0)
    
    df_census["destination_id"] = df_census["municipality_id"]
    
    df_census = df_census[["destination_id","employees","local_employees"]].reset_index(drop=True).copy()
    
    df_census = df_census.loc[~df_census["employees"].isna()].copy()
    
    df_census = df_census.loc[df_census["destination_id"].str.startswith("091")].copy()
    
    print(df_census.loc[df_census["employees"]<df_census["local_employees"]])
    exit()
    
    return df_census
