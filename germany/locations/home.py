import numpy as np
import pandas as pd

"""
Yield home location candidates for Germany.
"""

def configure(context):
    context.stage("germany.data.buildings")

def execute(context):
    # Load data
    df = context.stage("germany.data.buildings")

    return df[[
        "building_id", "weight", "commune_id", "iris_id", "geometry",
    ]]
