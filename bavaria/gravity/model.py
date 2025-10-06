
import pandas as pd
import os
import numpy as np
import logging

# Set up logging
logger = logging.getLogger(__name__)

"""
Apply gravity model to generate a distance matrix for Oberbayern.

The gravity model simulates spatial interaction patterns between municipalities
based on population, employment, and distance factors. It uses an iterative
algorithm to balance production and attraction terms until convergence.
"""

# Default model parameters
DEFAULT_SLOPE = -0.2  # -0.09 came from IDF, value -2.0 has been calibrated
DEFAULT_CONSTANT = -2.4
DEFAULT_DIAGONAL = 1.0
MAX_ITERATIONS = int(1e6)
CONVERGENCE_THRESHOLD = 1e-3

def configure(context):
    """
    Configure the stage by declaring dependencies and configuration parameters.

    Args:
        context: The context object for the pipeline stage
    """
    context.stage("bavaria.gravity.distance_matrix")
    context.stage("bavaria.ipf.attributed")
    context.stage("bavaria.data.census.employees")
    context.config("gravity_slope", DEFAULT_SLOPE)
    context.config("gravity_constant", DEFAULT_CONSTANT)
    context.config("gravity_diagonal", DEFAULT_DIAGONAL)

def _initialize_gravity_arrays(population_size):
    """
    Initialize production, attraction, and flow arrays for the gravity model.

    Args:
        population_size: Size of the population array

    Returns:
        tuple: Production, attraction, and flow arrays
    """
    production = np.ones((population_size,))
    attraction = np.ones((population_size,))
    flow = np.ones((population_size, population_size))
    return production, attraction, flow

def _calculate_production_terms(population, attraction, friction):
    """
    Calculate production terms for the gravity model.

    Args:
        population: Population array
        attraction: Attraction array
        friction: Friction matrix

    Returns:
        np.array: Updated production terms
    """
    production = np.copy(population)
    for k in range(len(population)):
        denominator = np.sum(attraction * friction[k, :])
        if denominator > 0:
            production[k] /= denominator
    return production

def _calculate_attraction_terms(employees, production, friction):
    """
    Calculate attraction terms for the gravity model.

    Args:
        employees: Employees array
        production: Production array
        friction: Friction matrix

    Returns:
        np.array: Updated attraction terms
    """
    attraction = np.copy(employees)
    for k in range(len(employees)):
        denominator = np.sum(production * friction[:, k])
        if denominator > 0:
            attraction[k] /= denominator
    return attraction

def _update_flow_matrix(friction, production, attraction):
    """
    Update the flow matrix with production and attraction terms.

    Args:
        friction: Friction matrix
        production: Production array
        attraction: Attraction array

    Returns:
        np.array: Updated flow matrix
    """
    flow = np.copy(friction)

    # Apply production terms
    for i in range(len(production)):
        flow[i, :] *= production[i]

    # Apply attraction terms
    for j in range(len(attraction)):
        flow[:, j] *= attraction[j]

    return flow

def _check_convergence(previous_production, previous_attraction, previous_flow,
                       production, attraction, flow, threshold):
    """
    Check if the gravity model has converged.

    Args:
        previous_production: Previous production array
        previous_attraction: Previous attraction array
        previous_flow: Previous flow matrix
        production: Current production array
        attraction: Current attraction array
        flow: Current flow matrix
        threshold: Convergence threshold

    Returns:
        bool: True if converged, False otherwise
    """
    production_delta = np.abs(production - previous_production)
    attraction_delta = np.abs(attraction - previous_attraction)
    flow_delta = np.abs(flow - previous_flow)

    max_production_delta = np.max(production_delta)
    max_attraction_delta = np.max(attraction_delta)
    max_flow_delta = np.max(flow_delta)

    logger.info("Gravity iteration - prod. max. Δ: %.6f, attr. max. Δ: %.6f, flow max. Δ: %.6f",
                max_production_delta, max_attraction_delta, max_flow_delta)

    return (max_production_delta < threshold and
            max_attraction_delta < threshold and
            max_flow_delta < threshold)

def evaluate_gravity(population, employees, friction):
    """
    Apply gravity model to generate flow matrix.

    Args:
        population: Population array
        employees: Employees array
        friction: Friction matrix

    Returns:
        np.array: Flow matrix

    Raises:
        RuntimeError: If the algorithm fails to converge
    """
    # Initialize production, attraction, and flow
    production, attraction, flow = _initialize_gravity_arrays(len(population))
    converged = False

    # Perform maximum iterations (but convergence will hopefully happen earlier)
    for iteration in range(MAX_ITERATIONS):
        # Backup to calculate change
        previous_production = np.copy(production)
        previous_attraction = np.copy(attraction)
        previous_flow = np.copy(flow)

        # Calculate production terms
        production = _calculate_production_terms(population, attraction, friction)

        # Calculate attraction terms
        attraction = _calculate_attraction_terms(employees, production, friction)

        # Update flow matrix
        flow = _update_flow_matrix(friction, production, attraction)

        # Check for convergence
        if _check_convergence(previous_production, previous_attraction, previous_flow,
                             production, attraction, flow, CONVERGENCE_THRESHOLD):
            converged = True
            break

    if not converged:
        raise RuntimeError("Gravity model failed to converge after {} iterations".format(MAX_ITERATIONS))

    return flow

def _prepare_data_frames(df_distances, df_population, df_employees, municipalities):
    """
    Prepare data frames for gravity model processing.

    Args:
        df_distances: Distance matrix DataFrame
        df_population: Population data DataFrame
        df_employees: Employees data DataFrame
        municipalities: List of municipality IDs

    Returns:
        tuple: Prepared population, employees, and distance matrices
    """
    # Manage identifiers
    df_population = df_population.rename(columns={
        "commune_id": "origin_id",
        "weight": "population"
    })[["origin_id", "population"]]

    df_employees = df_employees.rename(columns={
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

    return df_population, df_employees, distances, municipalities

def _balance_population_employees(population, employees):
    """
    Balance population and employees data.

    Args:
        population: Population array
        employees: Employees array

    Returns:
        tuple: Balanced population and employees arrays
    """
    # Balancing of the remaining population and workplaces
    observations = min(np.sum(population), np.sum(employees))
    population *= observations / np.sum(population)
    employees *= observations / np.sum(employees)

    return population, employees

def _convert_to_probability_matrix(df_matrix):
    """
    Convert flow matrix to probability matrix.

    Args:
        df_matrix: Flow matrix DataFrame

    Returns:
        pd.DataFrame: Probability matrix DataFrame
    """
    # Calculate totals
    df_total = df_matrix[["origin_id", "weight"]].groupby("origin_id").sum().reset_index().rename({"weight": "total"}, axis=1)
    df_matrix = pd.merge(df_matrix, df_total, on="origin_id")

    # Fix missing flows
    f_missing_total = df_matrix["total"] == 0.0
    df_matrix.loc[f_missing_total & (df_matrix["origin_id"] == df_matrix["destination_id"]), "weight"] = 1.0
    df_matrix.loc[f_missing_total, "total"] = 1.0

    # Convert to probability
    df_matrix["weight"] = df_matrix["weight"] / df_matrix["total"]
    df_matrix = df_matrix[["origin_id", "destination_id", "weight"]]

    return df_matrix

def execute(context):
    """
    Execute the gravity model stage.

    Args:
        context: The context object containing the pipeline data

    Returns:
        tuple: Two identical DataFrames containing origin-destination flow probabilities
    """
    # Load data
    df_distances = context.stage("bavaria.gravity.distance_matrix")
    df_population = context.stage("bavaria.ipf.attributed")
    df_employees = context.stage("bavaria.data.census.employees")

    # Prepare data frames
    df_population, df_employees, distances, municipalities = _prepare_data_frames(
        df_distances, df_population, df_employees, [])

    # Convert to arrays
    population = df_population["population"].values
    employees = df_employees["employees"].values

    # Balance population and employees
    population, employees = _balance_population_employees(population, employees)

    # Model parameters estimated from Île-de-France
    slope = context.config("gravity_slope")
    constant = context.config("gravity_constant")
    diagonal = context.config("gravity_diagonal")

    friction = np.exp(slope * distances + constant) + np.eye(len(municipalities)) * diagonal
    flow = evaluate_gravity(population, employees, friction)

    # Convert to data frame
    df_matrix = pd.DataFrame({
        "weight": flow.reshape((-1,)),
    }, index=pd.MultiIndex.from_product([municipalities, municipalities], names=[
        "origin_id", "destination_id"
    ])).reset_index()

    # Convert to probability matrix
    df_matrix = _convert_to_probability_matrix(df_matrix)

    # One representing work, one representing education
    return df_matrix, df_matrix
