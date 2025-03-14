import os
import geopandas as gpd
import zipfile
import numpy as np

"""
This stages loads a file containing population data for Germany including the adminstrative codes.
"""

def configure(context):
    context.config("data_path")
    context.config("bavaria.political_prefix", ["091", "092", "093", "094", "095", "096", "097"])
    context.config("germany.population_path", "germany/vg250-ew_12-31.utm32s.gpkg.ebenen.zip")
    context.config("germany.population_source", "vg250-ew_12-31.utm32s.gpkg.ebenen/vg250-ew_ebenen_1231/DE_VG250.gpkg")

def execute(context):
    # Load IRIS registry
    with zipfile.ZipFile(
        "{}/{}".format(context.config("data_path"), context.config("germany.population_path"))) as archive:
        with archive.open(context.config("germany.population_source")) as f:
            df_population = gpd.read_file(f, layer = "vg250_gem")[[
                "ARS", "EWZ", "geometry"
            ]]

    # Filter for prefix
    prefix = context.config("bavaria.political_prefix")

    if type(prefix) == str:
        df_population = df_population[df_population["ARS"].str.startswith(prefix)].copy()
    else:
        f = np.zeros((len(df_population,)), dtype = bool)

        for item in prefix:
            f |= df_population["ARS"].str.startswith(item)
        
        df_population = df_population[f].copy()

    # Rename
    df_population = df_population.rename(columns = { 
        "ARS": "municipality_code",
        "EWZ": "population"
    })
    
    return df_population

def validate(context):
    if not os.path.exists("%s/%s" % (context.config("data_path"), context.config("germany.population_path"))):
        raise RuntimeError("German population data is not available")

    return os.path.getsize("%s/%s" % (context.config("data_path"), context.config("germany.population_path")))
