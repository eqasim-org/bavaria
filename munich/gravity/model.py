
import pandas as pd
import os
import numpy as np

"""
Apply gravity model to generate a distance matrix for Oberbayern.
"""

DEFAULT_SLOPE = -0.2 # -0.09 came from IDF, value -2.0 has been calibrated
DEFAULT_CONSTANT = -2.4
DEFAULT_DIAGONAL = 1.0

def configure(context):
    context.stage("munich.gravity.distance_matrix")
    context.stage("munich.ipf.attributed")
    context.stage("munich.data.census.employees")
    context.config("gravity_slope", DEFAULT_SLOPE)
    context.config("gravity_constant", DEFAULT_CONSTANT)
    context.config("gravity_diagonal", DEFAULT_DIAGONAL)

def evaluate_gravity(population, employees, friction):
    # Initizlize production, attraction, and flow
    production = np.ones((len(population),))
    attraction = np.ones((len(population),))
    flow = np.ones((len(population), len(population)))
    converged = False

    # Perform maximum 100 iterations (but convergence will hopefully happen earlier)
    for iteration in range(int(1e6)):
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
    df_distances = context.stage("munich.gravity.distance_matrix")
    df_population = context.stage("munich.ipf.attributed")
    df_employees = context.stage("munich.data.census.employees")

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
    slope = context.config("gravity_slope")
    constant = context.config("gravity_constant")
    diagonal = context.config("gravity_diagonal")

    friction = np.exp(slope * distances + constant) + np.eye(len(municipalities)) * diagonal
    flow = evaluate_gravity(population, employees, friction)

    # Convert to data frame
    df_matrix = pd.DataFrame({
        "weight": flow.reshape((-1,)),
    }, index = pd.MultiIndex.from_product([municipalities, municipalities], names = [
        "origin_id", "destination_id"
    ])).reset_index()

    # Calculate totals
    df_total = df_matrix[["origin_id", "weight"]].groupby("origin_id").sum().reset_index().rename({ "weight" : "total" }, axis = 1)
    df_matrix = pd.merge(df_matrix, df_total, on = "origin_id")

    # Fix missing flows
    f_missing_total = df_matrix["total"] == 0.0
    df_matrix.loc[f_missing_total & (df_matrix["origin_id"] == df_matrix["destination_id"]), "weight"] = 1.0
    df_matrix.loc[f_missing_total, "total"] = 1.0

    # Convert to probability
    df_matrix["weight"] = df_matrix["weight"] / df_matrix["total"]
    df_matrix = df_matrix[["origin_id", "destination_id", "weight"]]

    # One representing work, one representing education
    return df_matrix, df_matrix
