import synthesis.population.enriched as delegate

import pandas as pd
import geopandas as gpd
import numpy as np

def configure(context):
    delegate.configure(context)

    context.stage("synthesis.population.spatial.home.locations")

    context.stage("bavaria.data.mid.data")
    context.stage("bavaria.data.mid.zones")

    context.stage("bavaria.data.census.household_size")
    context.stage("bavaria.data.census.household_income")

    context.config("random_seed")

    context.config("bavaria.minimum_age.car_availability", 0)
    context.config("bavaria.minimum_age.bicycle_availability", 0)
    context.config("bavaria.minimum_age.pt_subscription", 0)

    context.config("bavaria.minimum_age.one_person_household", 16)

"""
This stage overrides car availability, bike availability and transit subscription based on MiD data
"""

def execute(context):
    # delegate population
    df_persons = delegate.execute(context)

    # require home locations
    df_homes = context.stage("synthesis.population.spatial.home.locations")[["household_id", "geometry"]].copy()

    # load MiD
    df_zones = context.stage("bavaria.data.mid.zones")
    mid = context.stage("bavaria.data.mid.data")

    # assign zone membership to each person
    f_covered = np.zeros(len(df_homes), dtype = bool)
    for zone in df_zones["name"].unique():
        df_query = gpd.sjoin(df_homes, df_zones[df_zones["name"] == zone], predicate = "within")
        df_homes["inside_{}".format(zone)] = df_homes["household_id"].isin(df_query["household_id"])
        f_covered |= df_homes["inside_{}".format(zone)]

    df_homes["inside_external"] = ~f_covered

    df_persons = gpd.GeoDataFrame(
        pd.merge(df_persons, df_homes, on = "household_id"),
        crs = df_homes.crs
    )

    # Run IPFs to impute availabilities
    iterations = 1000

    # CAR AVAILABILITY
    df_persons["car_availability"] = 1.0
    constraints = mid["car_availability_constraints"]

    constraints.append({ 
        "age": (-np.inf, context.config("bavaria.minimum_age.car_availability") - 1), 
        "target": 0.0 
    })

    filters = []
    targets = []

    for constraint in constraints:
        f = np.ones((len(df_persons),), dtype = bool)

        if "zone" in constraint:
            f &= df_persons["inside_{}".format(constraint["zone"])]
        
        if "sex" in constraint:
            f &= df_persons["sex"] == constraint["sex"]

        if "age" in constraint:
            f &= df_persons["age"].between(*constraint["age"])

        targets.append(constraint["target"] * np.count_nonzero(f))
        filters.append(f)

    for iteration in context.progress(range(iterations), label = "imputing car availability"):
        factors = []

        for f, target in zip(filters, targets):
            current = df_persons.loc[f, "car_availability"].sum()
            factor = target / current
            df_persons.loc[f, "car_availability"] *= factor
            factors.append(factor)

    print("Factors", "min:", min(factors), "max:", max(factors), "mean:", np.mean(factors))
    print(df_persons["car_availability"].min(), df_persons["car_availability"].max())
    
    # BIKE AVAILABILITY
    df_persons["bicycle_availability"] = 1.0
    constraints = mid["bicycle_availability_constraints"]

    constraints.append({ 
        "age": (-np.inf, context.config("bavaria.minimum_age.bicycle_availability") - 1), 
        "target": 0.0 
    })

    filters = []
    targets = []

    for constraint in constraints:
        f = np.ones((len(df_persons),), dtype = bool)

        if "zone" in constraint:
            if constraint["zone"].startswith("!"):
                f &= ~df_persons["inside_{}".format(constraint["zone"][1:])]
            else:
                f &= df_persons["inside_{}".format(constraint["zone"])]
        
        if "sex" in constraint:
            f &= df_persons["sex"] == constraint["sex"]

        if "age" in constraint:
            f &= df_persons["age"].between(*constraint["age"])

        targets.append(constraint["target"] * np.count_nonzero(f))
        filters.append(f)

    for iteration in context.progress(range(iterations), label = "imputing bike availability"):
        factors = []

        for f, target in zip(filters, targets):
            current = df_persons.loc[f, "bicycle_availability"].sum()
            factor = target / current
            df_persons.loc[f, "bicycle_availability"] *= factor
            factors.append(factor)

    print("Factors", "min:", min(factors), "max:", max(factors), "mean:", np.mean(factors))

    # PT SUBSCRIPTION
    df_persons["has_pt_subscription"] = 1.0
    constraints = mid["pt_subscription_constraints"]

    constraints.append({ 
        "age": (-np.inf, context.config("bavaria.minimum_age.pt_subscription") - 1), 
        "target": 0.0 
    })

    filters = []
    targets = []

    for constraint in constraints:
        f = np.ones((len(df_persons),), dtype = bool)

        if "zone" in constraint:
            if constraint["zone"].startswith("!"):
                f &= ~df_persons["inside_{}".format(constraint["zone"][1:])]
            else:
                f &= df_persons["inside_{}".format(constraint["zone"])]
        
        if "sex" in constraint:
            f &= df_persons["sex"] == constraint["sex"]

        if "age" in constraint:
            f &= df_persons["age"].between(*constraint["age"])

        targets.append(constraint["target"] * np.count_nonzero(f))
        filters.append(f)

    for iteration in context.progress(range(iterations), label = "imputing pt subscription"):
        factors = []

        for f, target in zip(filters, targets):
            current = df_persons.loc[f, "has_pt_subscription"].sum()
            factor = target / current
            df_persons.loc[f, "has_pt_subscription"] *= factor
            factors.append(factor)

    print("Factors", "min:", min(factors), "max:", max(factors), "mean:", np.mean(factors))

    # Sample values
    random = np.random.RandomState(context.config("random_seed") + 8572)

    u = random.random_sample(len(df_persons))
    selection = u < df_persons["car_availability"]
    df_persons["car_availability"] = "none"
    df_persons.loc[selection, "car_availability"] = "all"
    df_persons["car_availability"] = df_persons["car_availability"].astype("category")

    u = random.random_sample(len(df_persons))
    selection = u < df_persons["bicycle_availability"]
    df_persons["bicycle_availability"] = "none"
    df_persons.loc[selection, "bicycle_availability"] = "all"
    df_persons["bicycle_availability"] = df_persons["bicycle_availability"].astype("category")

    u = random.random_sample(len(df_persons))
    selection = u < df_persons["has_pt_subscription"]
    df_persons["has_pt_subscription"] = selection

    # Household size (overwrite)
    df_household_size = context.stage("bavaria.data.census.household_size")

    # Make sure that persons <16 are not in 1-person households
    minimum_age = context.config("bavaria.minimum_age.one_person_household")
    df_household_size["lower_age"] = df_household_size["lower_age"].replace({ 0: minimum_age })

    df_young = df_household_size[df_household_size["lower_age"] == minimum_age].copy()
    df_young["lower_age"] = 0
    df_young["upper_age"] = minimum_age
    df_young.loc[df_young["household_size"] == "1", "weight"] = 0

    df_household_size = pd.concat([df_household_size, df_young])

    for (lower_age, upper_age, sex), df in df_household_size.groupby(["lower_age", "upper_age", "sex"]):
        f = df_persons["age"].between(lower_age, upper_age, inclusive = "left")
        f &= df_persons["sex"] == sex ## TODO

        df = df.copy()
        df["weight"] /= df["weight"].sum()
        df = df.sample(n = np.count_nonzero(f), weights = "weight", replace = True)
        df_persons.loc[f, "household_size"] = df["household_size"].values

    df_persons["household_size"] = df_persons["household_size"].astype("category")

    # Household income (overwrite)
    df_income = context.stage("bavaria.data.census.household_income")

    for household_size, df in df_income.groupby("household_size"):
        f = df_persons["household_size"] == household_size

        df = df.copy()
        df["weight"] /= df["weight"].sum()
        df = df.sample(n = np.count_nonzero(f), weights = "weight", replace = True)
        df_persons.loc[f, "household_income"] = df["income_class"].values

    df_persons["high_income"] = df_persons["household_income"] == "5000+"
    return df_persons
