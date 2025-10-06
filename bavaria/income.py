import numpy as np
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm

"""
This stage provides a zero household income for all households as it is needed in 
downstream stages for Germany.

The module creates a placeholder income value for all households in the 
synthetic population dataset.
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
    Execute the stage to create zero income values for households.

    Args:
        context: The context object containing the pipeline data

    Returns:
        pd.DataFrame: A DataFrame containing household IDs with zero income values
    """
    # Load data
    df = context.stage("synthesis.population.sampled")[["household_id"]]

    # Format
    df = df.drop_duplicates("household_id")
    df["household_income"] = 0.0

    return df
