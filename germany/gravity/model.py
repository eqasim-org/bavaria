
import pandas as pd
import os
import numpy as np

"""
Apply gravity model to generate a distance matrix for Oberbayern.
"""

def configure(context):
    context.stage("germany.gravity.distance_matrix")
    context.stage("germany.ipf.attributed")
    context.stage("germany.data.census.employees")

def evaluate_gravity(population, employees, friction):
    # Initizlize production, attraction, and flow
    production = np.ones((len(population),))
    attraction = np.ones((len(population),))
    flow = np.ones((len(population), len(population)))
    converged = False

    # Perform maximum 100 iterations (but convergence will hopefully happen earlier)
    for iteration in range(1000):
        # Backup to calculate change
        previous_production = np.copy(production)
        previous_attraction = np.copy(attraction)
        previous_flow = np.copy(flow)

        # Calculate production terms
        for k in range(len(population)):
            production[k] = population[k] / np.sum(attraction * friction[k,:])

        # Calculate attraction terms
        for k in range(len(population)):
            attraction[k] = employees[k] / np.sum(production * friction[:,k])

        # Initialize new flow matrix
        flow = np.copy(friction)

        # Apply production terms
        for i in range(len(population)):
            flow[i,:] *= production[i]

        # Apply attraction terms
        for j in range(len(population)):
            flow[:,j] *= attraction[j]

        # Calculate change to previous iteration
        production_delta = np.abs(production - previous_production)
        attraction_delta = np.abs(attraction - previous_attraction)
        flow_delta = np.abs(flow - previous_flow)

        print("Gravity iteration", iteration, 
            "prod. max. Δ:", np.max(production_delta),
            "attr. max. Δ:", np.max(attraction_delta),
            "flow max. Δ:", np.max(flow_delta),
        )

        # Stop if change is sufficiently small
        if np.max(production_delta) < 1e-3 and np.max(attraction_delta) < 1e-3 and np.max(flow_delta) < 1e-3:
            converged = True
            break
    
    assert converged
    return flow

def execute(context):
    # Load data
    df_distances = context.stage("germany.gravity.distance_matrix")
    df_population = context.stage("germany.ipf.attributed")
    df_employees = context.stage("germany.data.census.employees")

    # Manage identifiers
    df_population = df_population.rename(columns = {
        "commune_id": "origin_id",
        "weight": "population"
    })[["origin_id", "population"]]

    df_employees = df_employees.rename(columns = {
        "commune_id": "destination_id",
        "weight": "employees"
    })[["destination_id", "employees"]]

    # Aggregate population
    df_population = df_population.groupby("origin_id")["population"].sum().reset_index()

    # Find the set of used municipalities (also taking into account zero flows)
    municipalities = set(df_population["origin_id"])
    municipalities |= set(df_employees["destination_id"])
    municipalities |= set(df_distances["origin_id"])
    municipalities |= set(df_distances["destination_id"])
    municipalities = sorted(list(municipalities))
    
    # Make sure we have all municipalities in all data sets
    df_population = df_population.set_index("origin_id").reindex(municipalities).fillna(0.0)
    df_employees = df_employees.set_index("destination_id").reindex(municipalities).fillna(0.0)
    df_distances = df_distances.set_index(["origin_id", "destination_id"]).reindex(pd.MultiIndex.from_product([
        municipalities, municipalities
    ]))

    # Transform from a list into a matrix
    distances = df_distances["distance_km"].values.reshape((len(municipalities), len(municipalities)))

    # Run model
    population = df_population["population"] 
    employees = df_employees["employees"]

    # Balancing of the remaining population and workplaces
    observations = min(np.sum(population), np.sum(employees))
    population *= observations / np.sum(population)
    employees *= observations / np.sum(employees)

    # Model parameters estimated from Île-de-France
    friction = np.exp(-0.09 * distances - 2.4) + np.eye(len(municipalities)) * 1.0
    flow = evaluate_gravity(population, employees, friction)

    # Convert to data frame
    df_matrix = pd.DataFrame({
        "weight": flow.reshape((-1,)),
    }, index = pd.MultiIndex.from_product([municipalities, municipalities], names = [
        "origin_id", "destination_id"
    ])).reset_index()

    # Convert to probability
    df_total = df_matrix[["origin_id", "weight"]].groupby("origin_id").sum().reset_index().rename({ "weight" : "total" }, axis = 1)
    df_matrix = pd.merge(df_matrix, df_total, on = "origin_id")
    df_matrix["weight"] = df_matrix["weight"] / df_matrix["total"]
    df_matrix = df_matrix[["origin_id", "destination_id", "weight"]]

    # One representing work, one representing education
    return df_matrix, df_matrix
