import shapely.geometry as geo
import numpy as np
import pandas as pd
import geopandas as gpd

def configure(context):
    context.stage("data.bdtopo.raw")
    context.stage("data.spatial.municipalities")

def execute(context):
    df_locations = context.stage("data.bdtopo.raw")


    df_locations["destination_id"] = np.arange(len(df_locations))

    # Attach attributes for activity types
    df_locations["offers_leisure"] = False
    df_locations["offers_shop"] = False
    df_locations["offers_other"] = False
    
    
    df_locations.loc[np.random.randint(0,len(df_locations)-1,10000),"offers_leisure"] = True
    df_locations.loc[np.random.randint(0,len(df_locations)-1,10000),"offers_shop"] = True
    df_locations.loc[np.random.randint(0,len(df_locations)-1,10000),"offers_other"] = True

    # Define new IDs
    df_locations["location_id"] = np.arange(len(df_locations))
    df_locations["location_id"] = "sec_" + df_locations["location_id"].astype(str)

    return df_locations
