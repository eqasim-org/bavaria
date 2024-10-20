import os, json
import geopandas as gpd
import pandas as pd
import shapely.geometry as sgeo

"""
Loads station data for MVG
https://www.mvg.de/.rest/zdm/stations
"""

def configure(context):
    context.config("data_path")
    context.config("mvg_stations_path", "mvg/stations")

def execute(context):
    # Load raw data
    with open("{}/{}".format(context.config("data_path"), context.config("mvg_stations_path"))) as f:
        data = json.load(f)

    # Extract all existing zones
    zones = set()

    for station in data:
        zones |= set(station["tariffZones"].split("|"))

    zones = set(z for z in zones if len(z) > 0)

    # Convert to GeoDataFrame
    df_stations = []

    for station in data:
        station_zones = set(station["tariffZones"].split("|"))
        record = { "geometry": sgeo.Point(station["longitude"], station["latitude"]) }

        for z in zones:
            record["zone_{}".format(z)] = z in station_zones

        df_stations.append(record)

    df_stations = gpd.GeoDataFrame(pd.DataFrame.from_records(df_stations), crs = "EPSG:4326")

    # Extract zone (buffered multi-point) geometries
    df_zones = []

    for zone in zones:
        df_partial = df_stations[df_stations["zone_{}".format(zone)]].copy()
        df_zones.append({ "zone": zone, "geometry": sgeo.MultiPoint(df_partial["geometry"].values) })

    df_zones = gpd.GeoDataFrame(pd.DataFrame.from_records(df_zones), crs = "EPSG:4326")
    df_zones = df_zones.to_crs("EPSG:25832")

    df_zones["geometry"] = df_zones["geometry"].buffer(400)
    return df_zones[["zone", "geometry"]]

def validate(context):
    if not os.path.exists("{}/{}".format(context.config("data_path"), context.config("mvg_stations_path"))):
        raise RuntimeError("MVG zone data is not available")

    return os.path.getsize("{}/{}".format(context.config("data_path"), context.config("mvg_stations_path")))
