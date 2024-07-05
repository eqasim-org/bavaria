import numpy as np
import pandas as pd

"""
Yield home zones for Germany based on synthetic population data.
"""

def configure(context):
    context.stage("germany.data.buildings")

def execute(context):
    # Load data
    df = context.stage("germany.data.buildings")

    # Format data
    df = df.drop_duplicates("household_id")

    return df[["household_id", "departement_id", "commune_id", "iris_id"]]
