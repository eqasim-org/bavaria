import pyrosm
import pandas as pd
import numpy as np
import geopandas as gpd
import warnings

OSM_FILTERS = [
    { 
        "location_type": "education", 
        "filter": {
            "building": ["school", "university", "kindergarten"],
            "amenity": ["school", "university", "kindergarten"],
        }
    },
    { 
        "location_type": "shop", 
        "filter": {
            "building": ["retails", "apartments;commerical", "mixd_use", "mixed", "kiosk", "supermarket", "mixed_use", "mall", "commercial", "shop", "retail"],
            "amenity": ["pharmacy", "convenience_store", "commercial", "marketplace", "winery", "food_court", "convenience"],
        }
    },
    {
        "location_type": "leisure",
        "filter": {
            "amenity": ["social_facility", "theatre", "swimming_pool", "place_of_worship", "library", "science_park", "social_centre", "arts_centre", "community_centre", "restaurant", "events_centre", "pub", "cafe", "commercial", "cinema", "winery", "bar", "amphitheatre", "concert_hall", "studio", "nightclub", "food_court", "bbq", "music_venue", "senior_center", "pool", "casino", "events_venue", "spa", "boat_rental", "senior_centre", "music_venue;bar", "community_center", "ice_cream","church", "park", "stripclub", "swingerclub", "biergarten", "music_rehearsal_place", "cafeteria", "meditation_centre", "gym", "planetarium", "clubhouse", "dive_centre", "community_hall", "event_hall", "bicycle_rental", "club", "gambling"]
        }
    },
    {
        "location_type": "work", 
        "filter": {
            "building": ["hotel", "tower", "police_station", "retail", "shop", "arena", "transportation", "office", "commercial", "hangar", "industrial", "terminal", "mall", "warehouse", "multi_level_parking", "university", "dormitory", "museum", "theatre", "stadium", "fire_station", "control_tower", "manufacture", "sports_centre", "hospital", "train_station", "civic", "church",  "gymnasium", "temple", "mixed_use", "central_office", "amphitheatre", "business", "barn", "data_center", "cinema", "service", "supermarket",  "weapon_armory", "cathedral", "farm_auxiliary", "factory", "station", "library", "farm", "mosque","stable", "historic_building", "carousel", "synagogue", "convent", "mortuary", "prison", "brewery", "office", "monastery", "clinic", "kiosk", "carpark", "mixed", "mixd_use", "motel", "community_center", "research", "charity", "medical", "offices", "community_centre", "synogogue", "Athletic_field_house", "depot", "Laundry", "chapel", "lighthouse", "clubhouse", "guardhouse", "bungalow", "retails", "tech_cab", "commerical", "gasstation", "yes;offices", "castle"],
            "amenity": ["school", "bank", "hospital", "social_facility", "police", "pharmacy", "theatre", "university", "college", "swimming_pool", "place_of_worship", "library", "clinic", "science_park", "conference_centre", "trailer_park", "social_centre", "arts_centre", "courthouse", "post_office", "community_centre", "car_rental", "restaurant", "ranger_station", "events_centre", "convenience_store", "townhall", "mortuary", "fuel", "car_wash", "fast_food", "pub", "fire_station", "cafe", "doctors", "commercial", "nursing_home", "marketplace", "cinema", "public_building", "winery", "dentist", "bar", "amphitheatre", "ferry_terminal", "concert_hall", "studio", "nightclub", "kindergarten", "civic", "food_court", "childcare", "prison", "caravan_rental", "monastery", "dialysis", "veterinary", "music_venue", "senior_center", "pool", "casino", "events_venue", "preschool", "animal_shelter", "spa", "boat_rental", "senior_centre", "brokerage", "vehicle_inspection", "healthcare", "music_venue;bar", "community_center", "embassy", "ice_cream", "tailor", "coworking_space", "church", "storage_rental", "stripclub", "swingerclub", "office", "biergarten", "music_rehearsal_place", "cafeteria", "truck_rental", "sperm_bank", "meditation_centre", "funeral_parlor", "cruise_terminal", "crematorium", "gym", "planetarium", "clubhouse", "language_school", "convenience", "music_school", "dive_centre", "community_hall", "event_hall", "research_institute", "club",  "gambling", "retirement_village"],
        }
    }
]

DEFAULT_FLOORS = 2

def configure(context):
    context.stage("germany.data.osm.chunked")
    context.stage("data.spatial.municipalities")
    context.stage("data.spatial.iris")

def execute(context):
    df_zones = context.stage("data.spatial.iris")[["geometry", "commune_id", "iris_id"]]
    chunk_ids = context.stage("germany.data.osm.chunked")

    df_locations = []
    for osm_filter in OSM_FILTERS:
        print("Processing {} ...".format(osm_filter["location_type"]))

        for chunk in context.progress(chunk_ids, total = len(chunk_ids)):
            osm = pyrosm.OSM("{}/{}.osm.pbf".format(context.path("germany.data.osm.chunked"), chunk))
            
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category = UserWarning)
                    warnings.filterwarnings("ignore", category = FutureWarning)

                    df_selection = osm.get_buildings(osm_filter["filter"])
            except AttributeError:
                df_selection = None
            except KeyError:
                df_selection = None

            if df_selection is not None and len(df_selection) > 0:
                columns = ["geometry"] + [
                    optional for optional in ["building", "amenity", "building:levels"]
                    if optional in df_selection
                ]

                df_selection = df_selection[columns].to_crs(df_zones.crs)
                df_selection["area"] = df_selection["geometry"].area
                df_selection["geometry"] = df_selection["geometry"].centroid

                # Fix area (in one case negative)
                df_selection["area"] = np.abs(df_selection["area"])

                # Handle number of floors
                if "building:levels" in columns:
                    df_selection["floors"] = pd.to_numeric(df_selection["building:levels"], errors = "coerce")
                    df_selection = df_selection.drop(columns = ["building:levels"])
                else:
                    df_selection["floors"] = np.nan

                df_selection["floors"] = df_selection["floors"].fillna(DEFAULT_FLOORS)
                df_selection["floors"] = np.maximum(df_selection["floors"], 1) # avoid negative

                df_local = df_zones[df_zones["commune_id"] == chunk]
                df_selection = gpd.sjoin(df_selection, df_local[["commune_id", "iris_id", "geometry"]])
                df_selection = df_selection.drop(columns = ["index_right"])

                df_selection["location_type"] = osm_filter["location_type"]
                df_locations.append(df_selection)
    
    df_locations = pd.concat(df_locations)

    if not "floors" in df_locations: df_locations["floors"] = np.nan
    df_locations["floors"] = df_locations["floors"].fillna(DEFAULT_FLOORS).astype(int)

    return df_locations
