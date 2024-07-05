import pandas as pd
import numpy as np
import itertools

"""
This stage merge prepared datasets of employees from Kreis level 
with inhabitants from Gemeinde level using Iterative Proportional Fitting
"""

def configure(context):
    context.stage("germany.ipf.prepare")
 
def execute(context):
    df_population, df_employment = context.stage("germany.ipf.prepare")

    # Create a map that maps communes to the departement
    df_unique = df_population[["commune_id", "departement_id"]].drop_duplicates()
    departement_mapping = dict(zip(
        df_unique["commune_id"], df_unique["departement_id"]
    ))

    # Construct a combined age category
    df_population["age_class_population"] = df_population["age_class"]
    df_employment["age_class_employment"] = df_employment["age_class"]

    unique_age_classes = np.array(np.sort(list(
        set(df_population["age_class_population"]) | 
        set(df_employment["age_class_employment"]))))

    unique_population_age_classes = np.sort(df_population["age_class_population"].unique())
    unique_employment_age_classes = np.sort(df_employment["age_class_employment"].unique())

    population_age_mapping = {}
    employment_age_mapping = {}

    for age_class in unique_age_classes:
        population_age_mapping[age_class] = unique_population_age_classes[np.count_nonzero(age_class > unique_population_age_classes)]
        employment_age_mapping[age_class] = unique_employment_age_classes[np.count_nonzero(age_class > unique_employment_age_classes)]

    # Construct other unique values
    unique_sexes = np.sort(list(set(df_population["sex"]) | set(df_employment["sex"])))
    unique_employed = [True, False]
    unique_communes = np.sort(df_population["commune_id"].unique())
    unique_departements = np.sort(df_employment["departement_id"].unique())

    # Initialize the seed with all combinations of values
    index = pd.MultiIndex.from_product([
        unique_communes, unique_sexes, unique_age_classes, unique_employed
    ], names = ["commune_id", "sex", "age_class", "employed"])

    df_model = pd.DataFrame(index = index).reset_index()
    df_model["weight"] = 1.0

    # Attach hierachichal departement identifiers
    df_model["departement_id"] = df_model["commune_id"].replace(departement_mapping)

    # Attach individual age classes
    df_model["age_class_population"] = df_model["age_class"].replace(population_age_mapping)
    df_model["age_class_employment"] = df_model["age_class"].replace(employment_age_mapping)

    # Initialize weighting selectors and targets
    selectors = []
    targets = []
    
    # Population constraints
    combinations = list(itertools.product(unique_communes, unique_sexes, unique_population_age_classes))
    for combination in context.progress(combinations, total = len(combinations), label = "Generating population constraints"):    
        f_reference = df_population["commune_id"] == combination[0]
        f_reference &= df_population["sex"] == combination[1]
        f_reference &= df_population["age_class_population"] == combination[2] 
    
        f_model = df_model["commune_id"] == combination[0]
        f_model &= df_model["sex"] == combination[1]
        f_model &= df_model["age_class_population"] == combination[2]
        selectors.append(f_model)
    
        target_weight = df_population.loc[f_reference, "weight"].sum()
        targets.append(target_weight)
    
    # Employment constraints    
    combinations = list(itertools.product(unique_departements, unique_sexes, unique_employment_age_classes))
    for combination in context.progress(combinations, total = len(combinations), label = "Generating employment constraints"):
        f_reference = df_employment["departement_id"] == combination[0]
        f_reference &= df_employment["sex"] == combination[1]
        f_reference &= df_employment["age_class_employment"] == combination[2] 
    
        f_model = df_model["departement_id"] == combination[0]
        f_model &= df_model["sex"] == combination[1]
        f_model &= df_model["age_class_employment"] == combination[2]
        f_model &= df_model["employed"] # Only select employed!
        selectors.append(f_model)
    
        target_weight = df_employment.loc[f_reference, "weight"].sum()
        targets.append(target_weight)
    
    # Perform IPF
    iteration = 0
    converged = False
    
    while iteration < 1000:
        iteration_factors = []
    
        for f, target_weight in zip(selectors, targets):
            current_weight = df_model.loc[f, "weight"].sum()
    
            if current_weight > 0:
                update_factor = target_weight / current_weight
                df_model.loc[f, "weight"] *= update_factor
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

    assert converged

    # Reestablish sex categories
    df_model["sex"] = df_model["sex"].replace({ 1: "male", 2: "female" }).astype("category")

    return df_model[["commune_id", "departement_id", "sex", "age_class", "employed", "weight"]]
