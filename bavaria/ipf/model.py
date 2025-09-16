import pandas as pd
import numpy as np
import itertools
import logging

# Set up logging
logger = logging.getLogger(__name__)

"""
This stage merge prepared datasets of employees from Kreis level
with inhabitants from Gemeinde level using Iterative Proportional Fitting

The IPF algorithm adjusts an initial weight matrix to match known marginal totals
for population, employment, and license data across different demographic categories.
"""

# Constants for IPF algorithm
MAX_ITERATIONS = 1000
CONVERGENCE_THRESHOLD = 1e-2
MAX_AGE_CLASS = 9999
DEFAULT_MINIMUM_EMPLOYMENT_AGE = 16

def configure(context):
    """
    Configure the stage by declaring dependencies and configuration parameters.

    Args:
        context: The context object for the pipeline stage
    """
    context.stage("bavaria.ipf.prepare")
    context.config("bavaria.minimum_age.employment", DEFAULT_MINIMUM_EMPLOYMENT_AGE)

def _construct_age_classes(df_population, df_employment, df_licenses_country, df_licenses_kreis):
    """
    Construct combined age classes and mappings from different datasets.

    Args:
        df_population: Population data DataFrame
        df_employment: Employment data DataFrame
        df_licenses_country: Country-level license data DataFrame
        df_licenses_kreis: Kreis-level license data DataFrame

    Returns:
        tuple: Contains age classes and mappings for population, employment, and license data
    """
    # Construct a combined age class
    population_age_classes = np.sort(df_population["age_class"].unique())
    population_age_upper = list(population_age_classes[1:]) + [MAX_AGE_CLASS]

    employment_age_classes = np.sort(df_employment["age_class"].unique())
    employment_age_upper = list(employment_age_classes[1:]) + [MAX_AGE_CLASS]

    license_age_classes = np.sort(df_licenses_country["age_class"].unique())
    license_age_upper = list(license_age_classes[1:]) + [MAX_AGE_CLASS]

    combined_age_classes = np.array(np.sort(list(
        set(population_age_classes) |
        set(employment_age_classes) |
        set(license_age_classes) |
        set([DEFAULT_MINIMUM_EMPLOYMENT_AGE]))))  # minimum_employment_age

    population_age_mapping = {}
    employment_age_mapping = {}
    license_age_mapping = {}

    for age_class in combined_age_classes:
        population_age_mapping[age_class] = population_age_classes[np.count_nonzero(population_age_upper <= age_class)]
        employment_age_mapping[age_class] = employment_age_classes[np.count_nonzero(employment_age_upper <= age_class)]
        license_age_mapping[age_class] = license_age_classes[np.count_nonzero(license_age_upper <= age_class)]

    return (combined_age_classes, population_age_mapping, employment_age_mapping, 
            license_age_mapping, population_age_upper, employment_age_upper, license_age_upper)

def _generate_model_seed(unique_communes, unique_sexes, combined_age_classes, unique_employed, unique_license):
    """
    Generate the initial seed matrix with all combinations of values.

    Args:
        unique_communes: Array of unique commune indices
        unique_sexes: Array of unique sex values
        combined_age_classes: Array of combined age classes
        unique_employed: Array of employment status values
        unique_license: Array of license status values

    Returns:
        pd.DataFrame: Initial seed DataFrame with weights
    """
    # Initialize the seed with all combinations of values
    index = pd.MultiIndex.from_product([
        unique_communes, unique_sexes, combined_age_classes, unique_employed, unique_license
    ], names = ["commune_index", "sex", "combined_age_class", "employed", "license"])

    df_model = pd.DataFrame(index = index).reset_index()
    df_model["weight"] = 1.0

    # Provide a prior based on the size of the age classes
    combined_age_classes_sizes = {
        lower: upper - lower for
        lower, upper in zip(combined_age_classes[:-1], combined_age_classes[1:])
    }
    combined_age_classes_sizes[combined_age_classes[-1]] = 1.0
    df_model["weight"] *= df_model["combined_age_class"].apply(lambda c: combined_age_classes_sizes[c])

    return df_model

def _attach_spatial_identifiers(df_model, df_population):
    """
    Attach spatial identifiers (departement indices) to the model.

    Args:
        df_model: Model DataFrame
        df_population: Population data DataFrame

    Returns:
        pd.DataFrame: Model DataFrame with attached spatial identifiers
    """
    # Attach departement indices
    df_spatial = df_population[["commune_index", "departement_index"]].drop_duplicates()
    df_model["departement_index"] = df_model["commune_index"].replace(dict(zip(
        df_spatial["commune_index"], df_spatial["departement_index"]
    )))

    return df_model

def _attach_age_classes(df_model, population_age_mapping, employment_age_mapping, license_age_mapping):
    """
    Attach individual age classes to the model.

    Args:
        df_model: Model DataFrame
        population_age_mapping: Mapping for population age classes
        employment_age_mapping: Mapping for employment age classes
        license_age_mapping: Mapping for license age classes

    Returns:
        pd.DataFrame: Model DataFrame with attached age classes
    """
    df_model["age_class_population"] = df_model["combined_age_class"].replace(population_age_mapping)
    df_model["age_class_employment"] = df_model["combined_age_class"].replace(employment_age_mapping)
    df_model["age_class_license"] = df_model["combined_age_class"].replace(license_age_mapping)

    return df_model

def _generate_population_constraints(df_population, df_model, unique_communes, unique_sexes, population_age_classes, context):
    """
    Generate population constraints for the IPF algorithm.

    Args:
        df_population: Population data DataFrame
        df_model: Model DataFrame
        unique_communes: Array of unique commune indices
        unique_sexes: Array of unique sex values
        population_age_classes: Array of population age classes
        context: Context object for progress reporting

    Returns:
        tuple: Lists of selectors and targets for population constraints
    """
    selectors = []
    targets = []

    combinations = list(itertools.product(unique_communes, unique_sexes, population_age_classes))
    for combination in context.progress(combinations, total = len(combinations), label = "Generating population constraints"):    
        f_reference = df_population["commune_index"] == combination[0]
        f_reference &= df_population["sex"] == combination[1]
        f_reference &= df_population["age_class"] == combination[2]

        f_model = df_model["commune_index"] == combination[0]
        f_model &= df_model["sex"] == combination[1]
        f_model &= df_model["age_class_population"] == combination[2]
        selectors.append(f_model)

        target_weight = df_population.loc[f_reference, "weight"].sum()
        targets.append(target_weight)

    return selectors, targets

def _generate_employment_constraints(df_employment, df_model, unique_departements, unique_sexes, employment_age_classes, context):
    """
    Generate employment constraints for the IPF algorithm.

    Args:
        df_employment: Employment data DataFrame
        df_model: Model DataFrame
        unique_departements: Array of unique departement indices
        unique_sexes: Array of unique sex values
        employment_age_classes: Array of employment age classes
        context: Context object for progress reporting

    Returns:
        tuple: Lists of selectors and targets for employment constraints
    """
    selectors = []
    targets = []

    combinations = list(itertools.product(unique_departements, unique_sexes, employment_age_classes))
    for combination in context.progress(combinations, total = len(combinations), label = "Generating employment constraints"):
        f_reference = df_employment["departement_index"] == combination[0]
        f_reference &= df_employment["sex"] == combination[1]
        f_reference &= df_employment["age_class"] == combination[2]

        f_model = df_model["departement_index"] == combination[0]
        f_model &= df_model["sex"] == combination[1]
        f_model &= df_model["age_class_employment"] == combination[2]
        f_model &= df_model["employed"] # Only select employed!
        selectors.append(f_model)

        target_weight = df_employment.loc[f_reference, "weight"].sum()
        targets.append(target_weight)

    return selectors, targets

def _generate_license_constraints(df_licenses_country, df_licenses_kreis, df_model, unique_sexes, license_age_classes, unique_departements, context):
    """
    Generate license constraints for the IPF algorithm.

    Args:
        df_licenses_country: Country-level license data DataFrame
        df_licenses_kreis: Kreis-level license data DataFrame
        df_model: Model DataFrame
        unique_sexes: Array of unique sex values
        license_age_classes: Array of license age classes
        unique_departements: Array of unique departement indices
        context: Context object for progress reporting

    Returns:
        tuple: Lists of selectors and targets for license constraints
    """
    selectors = []
    targets = []

    # License country constraints
    combinations = list(itertools.product(unique_sexes, license_age_classes))
    for combination in context.progress(combinations, total = len(combinations), label = "Generating license constraints"):
        f_reference = df_licenses_country["sex"] == combination[0]
        f_reference &= df_licenses_country["age_class"] == combination[1] 

        f_model = df_model["sex"] == combination[0]
        f_model &= df_model["age_class_license"] == combination[1]
        f_model &= df_model["license"] # Only select license owners!
        selectors.append(f_model)

        target_weight = df_licenses_country.loc[f_reference, "weight"].sum()
        targets.append(target_weight)

    # License Kreis constraints
    for departement_index in context.progress(unique_departements, total = len(unique_departements), label = "Generating license constraints per Kreis"):
        f_reference = df_licenses_kreis["departement_index"] == departement_index

        f_model = df_model["departement_index"] == departement_index
        f_model &= df_model["license"] # Only select license owners!
        selectors.append(f_model)

        target_weight = df_licenses_kreis.loc[f_reference, "weight"].sum()
        targets.append(target_weight)

    return selectors, targets

def _perform_ipf(selectors, targets, weights, max_iterations=MAX_ITERATIONS, convergence_threshold=CONVERGENCE_THRESHOLD):
    """
    Perform the Iterative Proportional Fitting algorithm.

    Args:
        selectors: List of selector arrays
        targets: List of target weights
        weights: Initial weights array
        max_iterations: Maximum number of iterations
        convergence_threshold: Convergence threshold for stopping criteria

    Returns:
        tuple: Final weights array and convergence status

    Raises:
        RuntimeError: If the algorithm fails to converge
    """
    # Transform to index-based
    selectors = [np.nonzero(s.values) for s in selectors]

    # Perform IPF
    iteration = 0
    converged = False

    while iteration < max_iterations:
        iteration_factors = []

        for f, target_weight in zip(selectors, targets):
            current_weight = np.sum(weights[f])

            if current_weight > 0:
                update_factor = target_weight / current_weight
                weights[f] *= update_factor
                iteration_factors.append(update_factor)

        logger.info(
            "Iteration: %d, factors: %d, mean: %.6f, min: %.6f, max: %.6f",
            iteration, len(iteration_factors), np.mean(iteration_factors),
            np.min(iteration_factors), np.max(iteration_factors))

        if np.max(iteration_factors) - 1 < convergence_threshold:
            if np.min(iteration_factors) > 1 - convergence_threshold:
                converged = True
                break

        iteration += 1

    if not converged:
        raise RuntimeError("IPF algorithm failed to converge after {} iterations".format(max_iterations))

    return weights, converged

def execute(context):
    """
    Execute the IPF stage to merge population and employment data.

    Args:
        context: The context object containing the pipeline data

    Returns:
        pd.DataFrame: Weighted demographic data by commune, departement, sex, age class, employment, and license status

    Raises:
        RuntimeError: If the IPF algorithm fails to converge
    """
    df_population, df_employment, df_licenses_country, df_licenses_kreis = context.stage("bavaria.ipf.prepare")

    # Construct age classes and mappings
    (combined_age_classes, population_age_mapping, employment_age_mapping, 
     license_age_mapping, population_age_upper, employment_age_upper, license_age_upper) = _construct_age_classes(
        df_population, df_employment, df_licenses_country, df_licenses_kreis)

    # Construct other unique values
    unique_sexes = np.sort(list(set(df_population["sex"]) | set(df_employment["sex"])))
    unique_employed = [True, False]
    unique_communes = np.sort(df_population["commune_index"].unique())
    unique_departements = np.sort(df_employment["departement_index"].unique())
    unique_license = [True, False]

    # Generate the initial seed matrix
    df_model = _generate_model_seed(unique_communes, unique_sexes, combined_age_classes, unique_employed, unique_license)

    # Attach spatial identifiers
    df_model = _attach_spatial_identifiers(df_model, df_population)

    # Attach individual age classes
    df_model = _attach_age_classes(df_model, population_age_mapping, employment_age_mapping, license_age_mapping)

    # Initialize weighting selectors and targets
    selectors = []
    targets = []

    # Population constraints
    pop_selectors, pop_targets = _generate_population_constraints(
        df_population, df_model, unique_communes, unique_sexes,
        np.sort(df_population["age_class"].unique()), context)
    selectors.extend(pop_selectors)
    targets.extend(pop_targets)

    # Employment constraints
    emp_selectors, emp_targets = _generate_employment_constraints(
        df_employment, df_model, unique_departements, unique_sexes,
        np.sort(df_employment["age_class"].unique()), context)
    selectors.extend(emp_selectors)
    targets.extend(emp_targets)

    # Minimum employment age
    f_model = df_model["combined_age_class"] < DEFAULT_MINIMUM_EMPLOYMENT_AGE
    f_model &= df_model["employed"]
    selectors.append(f_model)
    targets.append(0.0)

    # License constraints
    lic_selectors, lic_targets = _generate_license_constraints(
        df_licenses_country, df_licenses_kreis, df_model, unique_sexes,
        np.sort(df_licenses_country["age_class"].unique()), unique_departements, context)
    selectors.extend(lic_selectors)
    targets.extend(lic_targets)

    # Perform IPF
    weights = df_model["weight"].values
    weights, converged = _perform_ipf(selectors, targets, weights)

    df_model["weight"] = weights

    # Reestablish sex categories
    df_model["sex"] = df_model["sex"].replace({ 1: "male", 2: "female" }).astype("category")

    # Add identifiers
    df_model = pd.merge(df_model, df_population[["commune_index", "commune_id"]].drop_duplicates(), on = "commune_index", how = "left")
    assert np.count_nonzero(df_model["commune_id"].isna()) == 0

    df_model = pd.merge(df_model, df_population[["departement_index", "departement_id"]].drop_duplicates(), on = "departement_index", how = "left")
    assert np.count_nonzero(df_model["departement_id"].isna()) == 0

    df_model = df_model.rename(columns = { "combined_age_class": "age_class" })
    return df_model[["commune_id", "departement_id", "sex", "age_class", "employed", "license", "weight"]]
