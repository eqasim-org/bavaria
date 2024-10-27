import synthesis.population.enriched as delegate

import pandas as pd
import geopandas as gpd
import numpy as np

def configure(context):
    delegate.configure(context)

    context.stage("synthesis.population.spatial.home.locations")

    context.stage("munich.data.mid.data")
    context.stage("munich.data.mid.zones")
    context.config("random_seed")

"""
This stage overrides car availability, bike availability and transit subscription based on MiD data
"""

def execute(context):
    # delegate population
    df_persons = delegate.execute(context)

    # require home locations
    df_homes = context.stage("synthesis.population.spatial.home.locations")[["household_id", "geometry"]].copy()

    # load MiD
    df_zones = context.stage("munich.data.mid.zones")
    mid = context.stage("munich.data.mid.data")

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

    filters = []
    targets = []

    for constraint in constraints:
        f = df_persons["inside_{}".format(constraint["zone"])]
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
    
    return df_persons
