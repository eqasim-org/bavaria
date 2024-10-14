import numpy as np
import pandas as pd

"""
Yield education location candidates for Germany.
"""

def configure(context):
    context.stage("munich.data.osm.locations")
    context.stage("data.spatial.municipalities")

MINIMUM_AREA = 20

def execute(context):
    # Load data
    df = context.stage("munich.data.osm.locations")    
    df = df[df["location_type"] == "education"].copy()
    df["fake"] = False

    # Weight
    df["weight"] = np.maximum(df["area"], MINIMUM_AREA) * df["floors"]

    # Handle types
    for education_type in ["kindergarten", "school", "university"]:
        f = df["building"] == education_type
        df.loc[f, "education_type"] = education_type

    for education_type in ["kindergarten", "school", "university"]:
        f = df["amenity"] == education_type
        df.loc[f, "education_type"] = education_type

    df = df[~df["education_type"].isna()].copy()

    # Need this for the IDF logic, not for the Germany logic
    df_fake = context.stage("data.spatial.municipalities")
    df_fake = df_fake[~df_fake["commune_id"].isin(df["commune_id"])].copy()

    df_fake["geometry"] = df_fake["geometry"].centroid

    df_fake["iris_id"] = df_fake["commune_id"].astype(str) + "0000"
    df_fake["iris_id"] = df_fake["iris_id"].astype("category")

    df_fake["fake"] = True

    df_fake["education_type"] = "unknown"
    df_fake["weight"] = 1.0

    # Merge
    df = pd.concat([
        df[["fake", "commune_id", "iris_id", "education_type", "weight", "geometry"]], 
        df_fake[["fake", "commune_id", "iris_id", "education_type", "weight", "geometry"]]
    ])

    # Convert to category
    df["education_type"] = df["education_type"].astype("category")

    # Identifiers
    df["location_id"] = np.arange(len(df))
    df["location_id"] = "edu_" + df["location_id"].astype(str)

    return df
