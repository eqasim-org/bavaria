import numpy as np
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm

"""
This stage is used to short-circuit the filtering of activity chains by department for
the ENTD.

The function identifies all department IDs that appear in the ENTD dataset,
including those in household data, person data, and trip data.
"""

def configure(context):
    """
    Configure the stage by declaring dependencies.

    Args:
        context: The context object for the pipeline stage
    """
    context.stage("data.hts.entd.cleaned")

def execute(context):
    """
    Execute the stage to extract department IDs from the ENTD dataset.

    Args:
        context: The context object containing the pipeline data

    Returns:
        pd.DataFrame: A DataFrame containing sorted department IDs
    """
    df_households, df_persons, df_trips = context.stage("data.hts.entd.cleaned")

    values = set(df_persons["departement_id"])
    values |= set(df_trips["origin_departement_id"])
    values |= set(df_trips["destination_departement_id"])

    return pd.DataFrame({ "departement_id": sorted(values) })
