import numpy as np
import pandas as pd

"""
Yield education location candidates for Germany.
"""

def configure(context):
    context.stage("germany.data.buildings")

def execute(context):
    # Load data
    df = context.stage("germany.data.buildings")

    df["location_id"] = "edu_" + df["building_id"].astype(str)
    df["fake"] = False

    return df[[
        "location_id", "fake", "commune_id", "iris_id", "geometry",
    ]]
