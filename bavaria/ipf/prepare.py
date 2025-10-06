import pandas as pd
import numpy as np
import logging

# Set up logging
logger = logging.getLogger(__name__)

"""
This stage updates the formatting of the population and employment census data sets such
that they can be processed by the IPF algorithm.

The module prepares demographic data by creating numeric indices, validating data consistency,
and adjusting license data to match population totals.
"""

# Constants
MAX_AGE_CLASS = 9999

def configure(context):
    """
    Configure the stage by declaring dependencies.

    Args:
        context: The context object for the pipeline stage
    """
    context.stage("bavaria.data.census.population")
    context.stage("bavaria.data.census.employment")
    context.stage("bavaria.data.census.licenses")

def _validate_data_consistency(df_population, df_employment, df_licenses_kreis):
    """
    Validate consistency between population, employment, and license data.

    Args:
        df_population: Population data DataFrame
        df_employment: Employment data DataFrame
        df_licenses_kreis: Kreis-level license data DataFrame

    Raises:
        AssertionError: If data consistency checks fail
    """
    unique_population_kreis = set(df_population["commune_id"].str[:5].unique())
    unique_employment_kreis = set(df_employment["departement_id"].unique())
    unique_licenses_kreis = set(df_licenses_kreis["departement_id"].unique())

    if unique_population_kreis != unique_employment_kreis:
        raise AssertionError("Population and employment Kreis codes do not match")

    if unique_population_kreis != unique_licenses_kreis:
        raise AssertionError("Population and license Kreis codes do not match")

def _create_numeric_indices(df_population, df_employment, df_licenses_kreis):
    """
    Create numeric indices for communes and departments.

    Args:
        df_population: Population data DataFrame
        df_employment: Employment data DataFrame
        df_licenses_kreis: Kreis-level license data DataFrame

    Returns:
        tuple: DataFrames with added numeric indices
    """
    # Generate numeric department index
    df_population["departement_id"] = df_population["commune_id"].str[:5].astype("category")

    unique_communes = np.sort(df_population["commune_id"].unique())
    unique_departements = np.sort(list(
        set(df_employment["departement_id"].unique()) |
        set(df_population["departement_id"].unique())))

    commune_mapping = {c: k for k, c in enumerate(unique_communes)}
    departement_mapping = {c: k for k, c in enumerate(unique_departements)}

    df_population["commune_index"] = df_population["commune_id"].replace(commune_mapping)
    df_population["departement_index"] = df_population["departement_id"].replace(departement_mapping)
    df_employment["departement_index"] = df_employment["departement_id"].replace(departement_mapping)
    df_licenses_kreis["departement_index"] = df_licenses_kreis["departement_id"].replace(departement_mapping)

    return df_population, df_employment, df_licenses_kreis, commune_mapping, departement_mapping

def _adjust_license_data(df_licenses_country, df_licenses_kreis, df_population):
    """
    Adjust license data to be consistent with population data.

    Args:
        df_licenses_country: Country-level license data DataFrame
        df_licenses_kreis: Kreis-level license data DataFrame
        df_population: Population data DataFrame

    Returns:
        tuple: Adjusted license data DataFrames
    """
    # Consolidate municipalities
    for department_id in df_licenses_kreis["departement_id"].unique():
        population = df_population.loc[df_population["commune_id"].str[:5] == department_id, "weight"].sum()
        licenses = df_licenses_kreis.loc[df_licenses_kreis["departement_id"] == department_id, "weight"].sum()

        if licenses > population:
            factor = population / licenses
            df_licenses_kreis.loc[df_licenses_kreis["departement_id"] == department_id, "weight"] *= factor
            logger.info("Adapting licenses for %s by factor %.4f", department_id, factor)

    # Scale up the sociodemographics for the study area
    df_licenses_country["weight"] = df_licenses_country["relative_weight"] * df_licenses_kreis["weight"].sum()

    return df_licenses_country, df_licenses_kreis

def _consolidate_demographics(df_population, df_licenses_country):
    """
    Consolidate demographics for sex and age groups.

    Args:
        df_population: Population data DataFrame
        df_licenses_country: Country-level license data DataFrame

    Returns:
        pd.DataFrame: Adjusted license data DataFrame
    """
    # Consolidate sex and age
    population_age_classes = np.sort(df_population["age_class"].unique())
    license_age_classes = np.sort(df_licenses_country["age_class"].unique())

    joint_age_classes = np.sort(list(set(population_age_classes) & set(license_age_classes)))
    joint_age_upper = list(joint_age_classes[1:]) + [MAX_AGE_CLASS]

    for sex in [1, 2]:
        for lower, upper in zip(joint_age_classes, joint_age_upper):
            f_population = df_population["sex"] == sex
            f_population &= df_population["age_class"] >= lower
            f_population &= df_population["age_class"] < upper

            f_license = df_licenses_country["sex"] == sex
            f_license &= df_licenses_country["age_class"] >= lower
            f_license &= df_licenses_country["age_class"] < upper

            population = df_population.loc[f_population, "weight"].sum()
            licenses = df_licenses_country.loc[f_license, "weight"].sum()

            if population < licenses:
                factor = population / licenses

                logger.info("Adapting sex:%s age:(%d, %d) by factor %.4f",
                           ["", "m", "f"][sex], lower, upper, factor)

                df_licenses_country.loc[f_license, "weight"] *= factor

    return df_licenses_country

def execute(context):
    """
    Execute the stage to prepare data for IPF processing.

    Args:
        context: The context object containing the pipeline data

    Returns:
        tuple: Prepared DataFrames for population, employment, and license data
    """
    # Load data
    df_population = context.stage("bavaria.data.census.population")
    df_employment = context.stage("bavaria.data.census.employment")

    df_licenses_country = context.stage("bavaria.data.census.licenses")[0]
    df_licenses_kreis = context.stage("bavaria.data.census.licenses")[2]

    # Generate numeric sex
    df_population["sex"] = df_population["sex"].replace({"male": 1, "female": 2})
    df_employment["sex"] = df_employment["sex"].replace({"male": 1, "female": 2})
    df_licenses_country["sex"] = df_licenses_country["sex"].replace({"male": 1, "female": 2})

    # Validate data consistency
    _validate_data_consistency(df_population, df_employment, df_licenses_kreis)

    # Create numeric indices
    df_population, df_employment, df_licenses_kreis, commune_mapping, departement_mapping = _create_numeric_indices(
        df_population, df_employment, df_licenses_kreis)

    # Adjust license data
    df_licenses_country, df_licenses_kreis = _adjust_license_data(
        df_licenses_country, df_licenses_kreis, df_population)

    # Consolidate demographics
    df_licenses_country = _consolidate_demographics(df_population, df_licenses_country)

    # Take into account updated total
    factor = df_licenses_country["weight"].sum() / df_licenses_kreis["weight"].sum()
    logger.info("Adapting total with correction factor %.4f", factor)
    df_licenses_kreis["weight"] *= factor

    return df_population, df_employment, df_licenses_country, df_licenses_kreis
