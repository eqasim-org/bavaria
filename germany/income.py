import numpy as np
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm

"""
This stage provides a zero household income for all households as it is needed in 
downstream stages for Germany.
"""

def configure(context):
    context.stage("germany.ipf.attributed")

def execute(context):
    # Load data
    df = context.stage("germany.ipf.attributed")[["household_id", "commune_id"]]
    
    # Format
    df = df.drop_duplicates("household_id")
    df["household_income"] = 0.0
    
    return df
