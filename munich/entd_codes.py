import numpy as np
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm

"""
This stage is used to short-circut the filtering of activity chains by department for
the ENTD.
"""

def configure(context):
    context.stage("data.hts.entd.cleaned")

def execute(context):
    df_households, df_persons, df_trips = context.stage("data.hts.entd.cleaned")

    values = set(df_persons["departement_id"])
    values |= set(df_trips["origin_departement_id"])
    values |= set(df_trips["destination_departement_id"])
    
    return pd.DataFrame({ "departement_id": sorted(values) })
