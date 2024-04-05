import fiona
import pandas as pd
import os
import shapely.geometry as geo
import geopandas as gpd
import zipfile
import re
import glob
import numpy as np

"""
This stage loads the raw data from the French building registry (BD-TOPO).
"""

def configure(context):
    context.config("data_path")
    context.config("bdtopo_path", "bdtopo_idf")
    context.confit("bdtopo_shp","091_Oberbayern_Hausumringe.zip")

    context.stage("data.spatial.departments")

def execute(context):
    df_departments = context.stage("data.spatial.departments")
    print("Expecting data for {} departments".format(len(df_departments)))

    with zipfile.ZipFile("{}/{}".format(context.config("data_path"), context.config("bdtopo_idf"))) as archive:
        with archive.open(context.config("bdtopo_shp")) as f:

            df_buildings = gpd.read_file(f,
                engine="pyogrio",
                use_arrow=True,
            )

    df_buildings["building_id"] = df_buildings.index
    df_buildings["housing"] = df_buildings.area.div(50).round()
    df_buildings.loc[(df_buildings["housing"] >0) & (df_buildings["housing"] <40)]

    return df_buildings[["building_id", "housing", "geometry"]]
