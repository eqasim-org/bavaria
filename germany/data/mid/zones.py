import geopandas as gpd
import os

"""
This stage provides a zoning system that is used in the MiD 2017 report for Munich 
"""

def configure(context):
    pass

def execute(context):
    self_path = os.path.dirname(os.path.abspath(__file__))
    df_zones = gpd.read_file("{}/zones.gpkg".format(self_path))

    df_zones["name"] = df_zones["name"].replace({
        "mittlerer_ring": "mr",
        "mittlerer_ring_to_city_boundary": "mrs"
    })
    
    return df_zones
