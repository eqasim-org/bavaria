import numpy as np
import pandas as pd
import geopandas as gpd
from sklearn.neighbors import KDTree

"""
A stage that finds education locations for the individuals.
"""

def configure(context):
    context.stage("synthesis.population.spatial.home.locations")
    context.stage("synthesis.population.enriched")
    context.stage("synthesis.population.trips")
    context.stage("munich.locations.education")
    context.config("random_seed")

    context.stage("synthesis.population.spatial.primary.candidates")

ASSIGNMENT = [
    { "filter": lambda x: x["age"].between(0, 6), "education_type": "kindergarten", "distance": 2e3 }, # 5
    { "filter": lambda x: x["age"].between(7, 17), "education_type": "school", "distance": 2e3,  }, # 10
    { "filter": lambda x: x["age"].between(18, np.inf), "education_type": "university", "distance": 10e3 }, # 50
]

def execute(context):
    # Initialize RNG
    random = np.random.RandomState(context.config("random_seed"))

    # Obtain persons that need an education location
    df_persons = context.stage("synthesis.population.spatial.primary.candidates")["persons"]
    df_persons = df_persons[df_persons["has_education_trip"]].copy()
    households = set(df_persons["household_id"].unique())

    # Attach age
    df_age = context.stage("synthesis.population.enriched")[["person_id", "age"]]
    df_persons = pd.merge(df_persons, df_age)

    # Attach home location
    df_homes = context.stage("synthesis.population.spatial.home.locations")
    df_homes = df_homes[df_homes["household_id"].isin(households)].copy()
    df_homes = df_homes[["household_id", "geometry"]]

    df_persons = pd.merge(df_persons, df_homes)
    df_persons = gpd.GeoDataFrame(df_persons, crs = df_homes.crs)

    # Load locations
    df_locations = context.stage("munich.locations.education")
    df_locations = df_locations[~df_locations["fake"]] # Ignore fake ones

    # Perform assignment
    df_result = []

    for instruction in ASSIGNMENT:
        print("Processing", instruction["education_type"])

        # Find persons and their coordinates
        df_person_selection = df_persons[instruction["filter"](df_persons)].copy()
        person_coordinates = np.array([
            df_person_selection["geometry"].x,
            df_person_selection["geometry"].y
        ]).T

        # Find locations and their coordinates
        df_location_candidates = df_locations[df_locations["education_type"] == instruction["education_type"]].copy()
        location_coordinates = np.array([
            df_location_candidates["geometry"].x,
            df_location_candidates["geometry"].y
        ]).T

        # Set up spatial index for querying
        spatial_index = KDTree(location_coordinates)

        # Find the closest location as backup for every person
        closest_indices = spatial_index.query(person_coordinates, return_distance = False).flatten()

        # Find the locations within a radius for every person
        candidate_indices = spatial_index.query_radius(person_coordinates, instruction["distance"])

        # Fill empty queries with the closest one as backup
        for k in range(len(candidate_indices)):
            if len(candidate_indices[k]) == 0:
                candidate_indices[k] = [closest_indices[k]]

        # Solve the selection problems
        location_weights = df_location_candidates["weight"].values

        u = random.random_sample(size = len(candidate_indices))
        selection = []

        for k in range(len(candidate_indices)):
            selection_weights = location_weights[candidate_indices[k]]
            
            cdf = np.cumsum(selection_weights) / np.sum(selection_weights)
            selection.append(candidate_indices[k][np.count_nonzero(u[k] > cdf)])

        df_partial = df_person_selection[["person_id"]].copy()
        df_partial["location_id"] = df_location_candidates["location_id"].iloc[selection].values

        df_result.append(df_partial)
    
    # Merge results
    df_result = pd.concat(df_result)

    # Add location information
    df_result = pd.merge(df_result, df_locations, on = "location_id", how = "left")
    df_result = gpd.GeoDataFrame(df_result, crs = df_locations.crs)

    assert len(df_persons) == len(df_result)
    return df_result[["person_id", "commune_id", "location_id", "geometry"]]
