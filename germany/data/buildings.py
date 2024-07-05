import geopandas as gpd
import zipfile
import pyogrio
import numpy as np

"""
This stage loads the raw data from the Bavarian building registry.
"""

def configure(context):
    context.config("data_path")
    context.config("bavaria.buildings_path", "bavaria/091_Oberbayern_Hausumringe.zip")
    
    context.stage("germany.data.spatial.iris")

def execute(context):
    # Load buildings
    with zipfile.ZipFile("{}/{}".format(
        context.config("data_path"), context.config("bavaria.buildings_path"))) as archive:
        archive.extractall(context.path())

    df_buildings = pyogrio.read_dataframe("{}/hausumringe.shp".format(
        context.path()), columns = [])[["geometry"]]
    
    # Weighting by area
    df_buildings["weight"] = df_buildings.area

    # Attributes
    df_buildings["building_id"] = np.arange(len(df_buildings))
    df_buildings["geometry"] = df_buildings.centroid

    # Filter
    df_buildings = df_buildings[
        (df_buildings["weight"] >= 40) & (df_buildings["weight"] < 400)
    ].copy()

    # As a replacement for SIRENE
    df_buildings["location_id"] = df_buildings["building_id"]
    df_buildings["employees"] = df_buildings["weight"]
    df_buildings["fake"] = False

    df_zones = context.stage("germany.data.spatial.iris")
    df_buildings = gpd.sjoin(df_buildings, df_zones[["geometry", "commune_id", "iris_id"]], 
        how = "left", predicate = "within").reset_index(drop=True).drop(columns = ["index_right"])
    
    ## TODO: Maybe we need to fix missing municipalities?

    # As a replacement for BPE
    df_buildings["offers_leisure"] = False
    df_buildings["offers_shop"] = False
    df_buildings["offers_other"] = False

    return df_buildings[[
        "building_id", "weight", "commune_id", "iris_id", "geometry"
    ]]
