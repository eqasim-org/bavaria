# -*- coding: utf-8 -*-
"""
Created on Tue May 14 14:41:06 2024

@author: arthur.burianne
"""

from tqdm import tqdm
import pandas as pd
import numpy as np
import numpy.linalg as la

"""
Transforms absolute OD flows from French census into a weighted destination
matrix given a certain origin commune for work and education.

Potential TODO: Do this by mode of transport!
"""

def configure(context):
    context.stage("data.spatial.municipalities")

def execute(context):
    df_municipalities = context.stage("data.spatial.municipalities")
    
    municipalities = df_municipalities["commune_id"].values
    
        
    # Initialize matrix to zero
    distance_matrix = np.ones((len(municipalities), len(municipalities)))
    
    # Convert locations to (N,2)-array
    locations = np.array([
        df_municipalities["geometry"].centroid.x,
        df_municipalities["geometry"].centroid.y
    ]).T
    
    # Calculate Euclidean distances per row
    for k in tqdm(range(len(locations))):
        distance_matrix[k,:] = la.norm(locations[k] - locations, axis = 1)
    
    # Convert to km
    distance_matrix *= 1e-3
    
    # Formatting into a data frame
    df_distances = pd.DataFrame({ "distance_km": distance_matrix.reshape(-1) }, index = pd.MultiIndex.from_product([
    municipalities, municipalities
    ], names = ["origin_id", "destination_id"])).reset_index()
   
    return df_distances
