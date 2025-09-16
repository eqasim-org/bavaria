import numpy as np
import pandas as pd

"""
Yield home zones for Germany based on synthetic population data.

This module extracts unique household locations from the synthetic population
dataset, providing identifiers for households and their associated geographic zones.
"""

def configure(context):
    """
    Configure the stage by declaring dependencies.

    Args:
        context: The context object for the pipeline stage
    """
    context.stage("synthesis.population.sampled")

def execute(context):
    """
    Execute the stage to extract household location data.

    Args:
        context: The context object containing the pipeline data

    Returns:
        pd.DataFrame: A DataFrame containing household IDs and their geographic identifiers
    """
    # Load data
    df = context.stage("synthesis.population.sampled")

    # Format data
    df = df.drop_duplicates("household_id")

    return df[["household_id", "departement_id", "commune_id", "iris_id"]]
