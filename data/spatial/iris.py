import pandas as pd
import geopandas as gpd
import os
import py7zr
import glob

"""
Loads the IRIS zoning system.
"""

def configure(context):
    context.config("data_path")
    context.config("iris_path", "iris_2021")
    context.stage("data.spatial.codes")

def execute(context):

    df_codes = context.stage("data.spatial.codes")

    df_iris = gpd.read_file("{}/{}/{}".format(context.config("data_path"), context.config("iris_path"),"DE_VG250.gpkg"),layer='VG250_GEM')[[
        "ARS", "geometry"
    ]].rename(columns = {
        "ARS": "commune_id"
    })

    df_iris.crs = "EPSG:25832"


    df_iris["iris_id"] =  df_iris["commune_id"].astype(str) + "0000"
    df_iris["iris_id"] = df_iris["iris_id"].astype("category")
    df_iris["commune_id"] = df_iris["commune_id"].astype("category")


    # Merge with requested codes and verify integrity
    df_iris = pd.merge(df_iris, df_codes, on = ["iris_id", "commune_id"])



    requested_iris = set(df_codes["iris_id"].unique())
    merged_iris = set(df_iris["iris_id"].unique())


    if requested_iris != merged_iris:
        raise RuntimeError("Some IRIS are missing: %s" % len(requested_iris - merged_iris,))

    return df_iris
