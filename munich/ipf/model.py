import pandas as pd
import numpy as np
import itertools

"""
This stage merge prepared datasets of employees from Kreis level 
with inhabitants from Gemeinde level using Iterative Proportional Fitting
"""

def configure(context):
    context.stage("bavaria.ipf.prepare")
 
def execute(context):
    df_population, df_employment, df_licenses_country, df_licenses_kreis = context.stage("bavaria.ipf.prepare")

    # Construct a combined age class
    population_age_classes = np.sort(df_population["age_class"].unique())
    population_age_upper = list(population_age_classes[1:]) + [9999]

    employment_age_classes = np.sort(df_employment["age_class"].unique())
    employment_age_upper = list(employment_age_classes[1:]) + [9999]

    license_age_classes = np.sort(df_licenses_country["age_class"].unique())
    license_age_upper = list(license_age_classes[1:]) + [9999]
    
    combined_age_classes = np.array(np.sort(list(
        set(population_age_classes) | 
        set(employment_age_classes) |
        set(license_age_classes))))
    
    population_age_mapping = {}
    employment_age_mapping = {}
    license_age_mapping = {}

    for age_class in combined_age_classes:
        population_age_mapping[age_class] = population_age_classes[np.count_nonzero(population_age_upper <= age_class)]
        employment_age_mapping[age_class] = employment_age_classes[np.count_nonzero(employment_age_upper <= age_class)]
        license_age_mapping[age_class] = license_age_classes[np.count_nonzero(license_age_upper <= age_class)]

    # Construct other unique values
    unique_sexes = np.sort(list(set(df_population["sex"]) | set(df_employment["sex"])))
    unique_employed = [True, False]
    unique_communes = np.sort(df_population["commune_index"].unique())
    unique_departements = np.sort(df_employment["departement_index"].unique())
    unique_license = [True, False]

    # Initialize the seed with all combinations of values
    index = pd.MultiIndex.from_product([
        unique_communes, unique_sexes, combined_age_classes, unique_employed, unique_license
    ], names = ["commune_index", "sex", "combined_age_class", "employed", "license"])

    df_model = pd.DataFrame(index = index).reset_index()
    df_model["weight"] = 1.0

    # Attach departement indices
    df_spatial = df_population[["commune_index", "departement_index"]].drop_duplicates()
    df_model["departement_index"] = df_model["commune_index"].replace(dict(zip(
        df_spatial["commune_index"], df_spatial["departement_index"]
    )))

    # Attach individual age classes
    df_model["age_class_population"] = df_model["combined_age_class"].replace(population_age_mapping)
    df_model["age_class_employment"] = df_model["combined_age_class"].replace(employment_age_mapping)
    df_model["age_class_license"] = df_model["combined_age_class"].replace(license_age_mapping)

    # Initialize weighting selectors and targets
    selectors = []
    targets = []
    
    # Population constraints
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

    # Employment constraints   
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

    # Transform to index-based
    selectors = [np.nonzero(s.values) for s in selectors]
    
    # Perform IPF
    iteration = 0
    converged = False
    weights = df_model["weight"].values

    while iteration < 1000:
        iteration_factors = []
    
        for f, target_weight in zip(selectors, targets):
            current_weight = np.sum(weights[f])
    
            if current_weight > 0:
                update_factor = target_weight / current_weight
                weights[f] *= update_factor
                iteration_factors.append(update_factor)

        print(
            "Iteration:", iteration,
            "factors:", len(iteration_factors),
            "mean:", np.mean(iteration_factors),
            "min:", np.min(iteration_factors),
            "max:", np.max(iteration_factors))
        
        if np.max(iteration_factors) - 1 < 1e-2:
            if np.min(iteration_factors) > 1 - 1e-2:
                converged = True
                break
    
        iteration += 1

    df_model["weight"] = weights

    assert converged

    # Reestablish sex categories
    df_model["sex"] = df_model["sex"].replace({ 1: "male", 2: "female" }).astype("category")

    # Add identifiers
    df_model = pd.merge(df_model, df_population[["commune_index", "commune_id"]].drop_duplicates(), on = "commune_index", how = "left")
    assert np.count_nonzero(df_model["commune_id"].isna()) == 0

    df_model = pd.merge(df_model, df_population[["departement_index", "departement_id"]].drop_duplicates(), on = "departement_index", how = "left")
    assert np.count_nonzero(df_model["departement_id"].isna()) == 0

    df_model = df_model.rename(columns = { "combined_age_class": "age_class" })
    return df_model[["commune_id", "departement_id", "sex", "age_class", "employed", "license", "weight"]]
