import pandas as pd
import numpy as np

"""
This stage updates the formatting of the population and employment census data sets such
that they can be procesed by the IPF algorithm.
"""

def configure(context):
    context.stage("bavaria.data.census.population")
    context.stage("bavaria.data.census.employment")
    context.stage("bavaria.data.census.licenses")

def execute(context):
    # Load data
    df_population = context.stage("bavaria.data.census.population")
    df_employment = context.stage("bavaria.data.census.employment")

    df_licenses_country = context.stage("bavaria.data.census.licenses")[0]
    df_licenses_kreis = context.stage("bavaria.data.census.licenses")[2]

    # Generate numeric sex
    df_population["sex"] = df_population["sex"].replace({ "male": 1, "female": 2 })
    df_employment["sex"] = df_employment["sex"].replace({ "male": 1, "female": 2 })
    df_licenses_country["sex"] = df_licenses_country["sex"].replace({ "male": 1, "female": 2 })

    # Validation
    unique_population_kreis = set(df_population["commune_id"].str[:5].unique())
    unique_employment_kreis = set(df_employment["departement_id"].unique())
    unique_licenses_kreis = set(df_licenses_kreis["departement_id"].unique())
    assert unique_population_kreis == unique_employment_kreis
    assert unique_population_kreis == unique_licenses_kreis

    # Generate numeric department index
    df_population["departement_id"] = df_population["commune_id"].str[:5].astype("category")

    unique_communes = np.sort(df_population["commune_id"].unique())
    unique_departements = np.sort(list(
        set(df_employment["departement_id"].unique()) | 
        set(df_population["departement_id"].unique())))

    commune_mapping = { c: k for k, c in enumerate(unique_communes) }
    departement_mapping = { c: k for k, c in enumerate(unique_departements) }

    df_population["commune_index"] = df_population["commune_id"].replace(commune_mapping)
    df_population["departement_index"] = df_population["departement_id"].replace(departement_mapping)
    df_employment["departement_index"] = df_employment["departement_id"].replace(departement_mapping)
    df_licenses_kreis["departement_index"] = df_licenses_kreis["departement_id"].replace(departement_mapping)

    ## Licenses

    # Consolidate municipalities
    for department_id in df_licenses_kreis["departement_id"].unique():
        population = df_population.loc[df_population["commune_id"].str[:5] == department_id, "weight"].sum()
        licenses = df_licenses_kreis.loc[df_licenses_kreis["departement_id"] == department_id, "weight"].sum()

        if licenses > population:
            factor = population / licenses
            df_licenses_kreis.loc[df_licenses_kreis["departement_id"] == department_id, "weight"] *= factor
            print("Adapting licenses for {} by factor {}".format(department_id, factor))

    # Scale up the sociodemographics for the study area
    df_licenses_country["weight"] = df_licenses_country["relative_weight"] * df_licenses_kreis["weight"].sum()

    # Consolidate sex and age
    population_age_classes = np.sort(df_population["age_class"].unique())
    license_age_classes = np.sort(df_licenses_country["age_class"].unique())
    
    joint_age_classes = np.sort(list(set(population_age_classes) & set(license_age_classes)))
    joint_age_upper = list(joint_age_classes[1:]) + [9999]

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

                print("Adapting sex:{} age:({}, {}) by factor {}".format(
                    ["", "m", "f"][sex], lower, upper, factor
                ))

                df_licenses_country.loc[f_license, "weight"] *= factor
    
    # Take into account updated total
    factor = df_licenses_country["weight"].sum() / df_licenses_kreis["weight"].sum()
    print("Adapting total with correction factor {}".format(factor))
    df_licenses_kreis["weight"] *= factor

    return df_population, df_employment, df_licenses_country, df_licenses_kreis
