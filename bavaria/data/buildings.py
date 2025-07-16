import geopandas as gpd
import zipfile
import pyogrio
import numpy as np
import pandas as pd
import glob, os

"""
This stage loads the raw data from the Bavarian building registry.
"""

def configure(context):
    context.config("data_path")
    context.config("bavaria.buildings_path", "bavaria/buildings")
    
    context.stage("bavaria.data.spatial.iris")

def execute(context):
    df_zones = context.stage("bavaria.data.spatial.iris")
    df_combined = []
    
    start_index = 0
    for path in glob.glob("{}/{}/*_Hausumringe.zip".format(context.config("data_path"), context.config("bavaria.buildings_path"))):
        print("Processing", path.split("/")[-1].split("_Haus")[0])
        
        # Load buildings
        with zipfile.ZipFile(path) as archive:
            archive.extractall(context.path())

        # Find the .shp file in the extracted directory
        shp_files = glob.glob(f"{context.path()}/*/hausumringe.shp")
        if not shp_files:
            raise FileNotFoundError(f"hausumringe.shp not found after extracting {path}")
        shp_path = shp_files[0]

        df_buildings = pyogrio.read_dataframe(shp_path, columns = [])["geometry"].to_frame()
        
        # Weighting by area
        df_buildings["weight"] = df_buildings.area

        # Attributes
        df_buildings["building_id"] = np.arange(len(df_buildings)) + start_index
        start_index += len(df_buildings) + 1

        df_buildings["geometry"] = df_buildings.centroid

        # Filter
        df_buildings = df_buildings[
            (df_buildings["weight"] >= 40) & (df_buildings["weight"] < 400)
        ].copy()

        # Impute spatial identifiers
        df_buildings = gpd.sjoin(df_buildings, df_zones[["geometry", "commune_id", "iris_id"]], 
            how = "left", predicate = "within").reset_index(drop = True).drop(columns = ["index_right"])

        df_combined.append(df_buildings[[
            "building_id", "weight", "commune_id", "iris_id", "geometry"
        ]])

    df_combined = gpd.GeoDataFrame(pd.concat(df_combined), crs = df_combined[0].crs)

    required_zones = set(df_zones["commune_id"].unique())
    available_zones = set(df_combined["commune_id"].unique())
    missing_zones = required_zones - available_zones

    if len(missing_zones) > 0:
        print("Adding {} centroids as buildings for missing municipalities".format(len(missing_zones)))

        df_missing = df_zones[df_zones["commune_id"].isin(missing_zones)][["commune_id", "iris_id", "geometry"]].copy()
        df_missing["geometry"] = df_missing["geometry"].centroid
        df_missing["building_id"] = np.arange(len(df_missing)) + start_index
        df_missing["weight"] = 1.0

        df_combined = pd.concat([df_combined, df_missing])
    
    return df_combined

def validate(context):
    total_size = 0
    pattern = "{}/{}/*_Hausumringe.zip".format(context.config("data_path"), context.config("bavaria.buildings_path"))
    files = glob.glob(pattern)
    print("DEBUG: Looking for files in pattern:", pattern)
    print("DEBUG: Found files:", files)
    for path in files:
        total_size += os.path.getsize(path)

    if total_size == 0:
        raise RuntimeError("Did not find any building data for Bavaria")

    return total_size
