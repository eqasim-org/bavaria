import geopandas as gpd
import zipfile
import pyogrio
import numpy as np
import pandas as pd
import glob
import os
import logging

# Set up logging
logger = logging.getLogger(__name__)

"""
This stage loads the raw data from the Bavarian building registry.

The module processes building shapefiles from the Bavarian building registry,
extracts building footprints, calculates areas, and assigns geographic identifiers.
"""

# Constants for building filtering
MIN_BUILDING_AREA = 40
MAX_BUILDING_AREA = 400

def configure(context):
    """
    Configure the stage by declaring dependencies and configuration parameters.

    Args:
        context: The context object for the pipeline stage
    """
    context.config("data_path")
    context.config("bavaria.buildings_path", "bavaria/buildings")

    context.stage("bavaria.data.spatial.iris")

def execute(context):
    """
    Execute the stage to process building data from shapefiles.

    Args:
        context: The context object containing the pipeline data

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing processed building data

    Raises:
        FileNotFoundError: If required shapefiles are not found
        ValueError: If data validation fails
    """
    try:
        df_zones = context.stage("bavaria.data.spatial.iris")
        df_combined = []

        start_index = 0
        building_files = glob.glob("{}/{}/*_Hausumringe.zip".format(context.config("data_path"), context.config("bavaria.buildings_path")))

        if not building_files:
            raise FileNotFoundError("No building data files found in {}/{}".format(context.config("data_path"), context.config("bavaria.buildings_path")))

        for path in building_files:
            logger.info("Processing %s", path.split("/")[-1].split("_Haus")[0])

            # Load buildings
            with zipfile.ZipFile(path) as archive:
                archive.extractall(context.path())

            shapefile_path = "{}/hausumringe.shp".format(context.path())
            if not os.path.exists(shapefile_path):
                raise FileNotFoundError("Shapefile not found: {}".format(shapefile_path))

            df_buildings = pyogrio.read_dataframe(shapefile_path, columns = []) [["geometry"]]

            # Weighting by area
            df_buildings["weight"] = df_buildings.area

            # Attributes
            df_buildings["building_id"] = np.arange(len(df_buildings)) + start_index
            start_index += len(df_buildings) + 1

            df_buildings["geometry"] = df_buildings.centroid

            # Filter
            df_buildings = df_buildings[
                (df_buildings["weight"] >= MIN_BUILDING_AREA) & (df_buildings["weight"] < MAX_BUILDING_AREA)
            ].copy()

            # Impute spatial identifiers
            df_buildings = gpd.sjoin(df_buildings, df_zones[["geometry", "commune_id", "iris_id"]],
                how = "left", predicate = "within").reset_index(drop = True).drop(columns = ["index_right"])

            df_combined.append(df_buildings[[
                "building_id", "weight", "commune_id", "iris_id", "geometry"
            ]])

        if not df_combined:
            raise ValueError("No building data was processed successfully")

        df_combined = gpd.GeoDataFrame(pd.concat(df_combined), crs = df_combined[0].crs)

        required_zones = set(df_zones["commune_id"].unique())
        available_zones = set(df_combined["commune_id"].unique())
        missing_zones = required_zones - available_zones

        if len(missing_zones) > 0:
            logger.info("Adding %d centroids as buildings for missing municipalities", len(missing_zones))

            df_missing = df_zones[df_zones["commune_id"].isin(missing_zones)][["commune_id", "iris_id", "geometry"]].copy()
            df_missing["geometry"] = df_missing["geometry"].centroid
            df_missing["building_id"] = np.arange(len(df_missing)) + start_index
            df_missing["weight"] = 1.0

            df_combined = pd.concat([df_combined, df_missing])

        return df_combined
    except Exception as e:
        raise RuntimeError("Failed to process building data: {}".format(str(e)))

def validate(context):
    """
    Validate that building data files exist and are accessible.

    Args:
        context: The context object containing configuration data

    Returns:
        int: Total size of building data files

    Raises:
        RuntimeError: If no building data files are found
    """
    total_size = 0

    for path in glob.glob("{}/{}/*_Hausumringe.zip".format(context.config("data_path"), context.config("bavaria.buildings_path"))):
        total_size += os.path.getsize(path)

    if total_size == 0:
        raise RuntimeError("Did not find any building data for Bavaria")

    return total_size
