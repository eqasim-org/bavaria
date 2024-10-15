import numpy as np
import pandas as pd

"""
Yield home location candidates for Germany.
"""

def configure(context):
    context.stage("munich.data.buildings")

def execute(context):
    # Load data
    df = context.stage("munich.data.buildings")
    df = df.rename(columns = { "building_id": "home_location_id" })

    return df[[
        "home_location_id", "weight", "commune_id", "iris_id", "geometry",
    ]]
