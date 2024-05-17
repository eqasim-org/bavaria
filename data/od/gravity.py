# -*- coding: utf-8 -*-
"""
Created on Tue May 14 16:46:44 2024

@author: arthur.burianne
"""

import pandas as pd
import os
import numpy as np
"""
This stage loads the raw data from the French population census.
"""

def configure(context):
    context.stage("data.spatial.distances_matrix")
    context.stage("data.census.gemeinde_employment")
    context.stage("data.census.ipf_merge")

def execute(context):
    
    df_distances = context.stage("data.spatial.distances_matrix")
    df_employment = context.stage("data.census.gemeinde_employment")
    df_population = context.stage("data.census.ipf_merge")
    
    # df_employment["employees"] = df_employment["employees"].astype(float)

    df_population["origin_id"] = df_population["municipality_id"]
    df_population = df_population.groupby("origin_id").sum("weight")
    df_population["population"] = df_population["weight"]
    df_population = df_population[["population"]].copy()
    df_population = df_population.reset_index()
   
    
    # Find the set of used municipalities (also taking into account zero flows)
    municipalities = set(df_population["origin_id"])
    municipalities |= set(df_employment["destination_id"])
    municipalities |= set(df_distances["origin_id"])
    municipalities |= set(df_distances["destination_id"])
    municipalities = sorted(list(municipalities))
    
    municipalities = municipalities



    # Make sure that we have every municipality in the population, the emloyment, and the distances, 
    # and make sure that they are in the same order if we perform array-based operations
    
    df_population = df_population.set_index("origin_id").reindex(municipalities).fillna(0.0)
    df_employment = df_employment.set_index("destination_id").reindex(municipalities).fillna(0.0)
    df_distances = df_distances.set_index(["origin_id", "destination_id"]).reindex(pd.MultiIndex.from_product([
        municipalities, municipalities
    ]))
    
    
    # Transform from a list into a matrix
    distances = df_distances["distance_km"].values.reshape((len(municipalities), len(municipalities)))

    
    def evaluate_gravity(population, employment, friction, verbose = True):
        # Initizlize production, attraction, and flow
        production = np.ones((len(municipalities),))
        attraction = np.ones((len(municipalities),))
        flow = np.ones((len(municipalities), len(municipalities)))
    
        # Perform maximum 100 iterations (but convergence will hopefully happen earlier)
        for iteration in range(100):
            if verbose:
                print("Iteration", iteration)
    
            # Backup to calculate change
            previous_production = np.copy(production)
            previous_attraction = np.copy(attraction)
            previous_flow = np.copy(flow)
    
            # Calculate production terms
            for k in range(len(municipalities)):
                production[k] = population[k] / np.sum(attraction * friction[k,:])
    
            # Calculate attraction terms
            for k in range(len(municipalities)):
                attraction[k] = employment[k] / np.sum(production * friction[:,k])
    
            # Initialize new flow matrix
            flow = np.copy(friction)
    
            # Apply production terms
            for i in range(len(municipalities)):
                flow[i,:] *= production[i]
    
            # Apply attraction terms
            for j in range(len(municipalities)):
                flow[:,j] *= attraction[j]
    
            # Calculate change to previous iteration
            production_delta = np.abs(production - previous_production)
            attraction_delta = np.abs(attraction - previous_attraction)
            flow_delta = np.abs(flow - previous_flow)
    
            if verbose:
                print("  Production", "max:", np.max(production_delta))
                print("  Attraction", "max:", np.max(attraction_delta))
                print("  Flow", "max:", np.max(flow_delta))
    
            # Stop if change is sufficiently small
            if np.max(production_delta) < 1e-3 and np.max(attraction_delta) < 1e-3 and np.max(flow_delta) < 1e-3:
                break
        
            return flow
        
        
        
        # Remove local employees from the marginals
    
    population = df_population["population"].values - df_employment["local_employees"].values
    employment = df_employment["employees"].values - df_employment["local_employees"].values
    
    population = df_population["population"] - df_employment["local_employees"]
    employment = df_employment["employees"] - df_employment["local_employees"]

    print(df_employment.loc[df_employment["employees"]>df_employment["local_employees"]])

    print(len(df_employment))
    # print(np.count_nonzero(population<0))
    # print(np.count_nonzero(employment<0))

    exit()    
    
    # Balancing of the remaining population and workplaces
    observations = min(np.sum(population), np.sum(employment))
    population *= observations / np.sum(population)
    employment *= observations / np.sum(employment)
    
    # Friction term (interaction term)
    friction = np.exp(-0.09 * distances - 2.2)
    
    # Remove diagonal, set to zero
    friction -= np.eye(len(municipalities)) * np.diag(friction)
    
    flow = evaluate_gravity(population, employment, friction)
    
    # Add back the local employees to the matrix
    flow += np.eye(len(municipalities)) * df_employment["local_employees"].values

    
    # Convert to data frame
    df_flow = pd.DataFrame({
        "flow": flow.reshape((-1,)),
    }, index = pd.MultiIndex.from_product([municipalities, municipalities], names = [
        "origin_id", "destination_id"
    ])).reset_index()
    
    df_flow["weight"] = df_flow["flow"]
    
    print(df_flow)
    exit()
    return df_flow,df_flow
