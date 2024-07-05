import numpy as np
import pandas as pd

"""
Yield work location candidates for Germany.
"""

def configure(context):
    context.stage("germany.data.buildings")

def execute(context):
    # Load data
    df = context.stage("germany.data.buildings")

    df["location_id"] = "sec_" + df["building_id"].astype(str)
    df["offers_leisure"] = True
    df["offers_shop"] = True
    df["offers_other"] = True

    return df[[
        "location_id", "commune_id", "iris_id", "geometry",
        "offers_leisure", "offers_shop", "offers_other"
    ]]
