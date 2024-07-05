import os
import geopandas as gpd
import zipfile

"""
This stages loads a file containing population data for Germany including the adminstrative codes.
"""

def configure(context):
    context.config("data_path")
    context.config("germany.political_prefix", "091") # Default: Oberbayern 091
    context.config("germany.population_path", "germany/vg250-ew_12-31.utm32s.gpkg.ebenen.zip")
    context.config("germany.population_source", "vg250-ew_12-31.utm32s.gpkg.ebenen/vg250-ew_ebenen_1231/DE_VG250.gpkg")

def execute(context):
    # Load IRIS registry
    with zipfile.ZipFile(
        "{}/{}".format(context.config("data_path"), context.config("germany.population_path"))) as archive:
        with archive.open(context.config("germany.population_source")) as f:
            df_population = gpd.read_file(f, layer = "v_vg250_gem")[[
                "Regionalschlüssel_ARS", "Einwohnerzahl_EWZ", "geometry"
            ]]

    # Filter for prefix
    prefix = context.config("germany.political_prefix")
    df_population = df_population[df_population["Regionalschlüssel_ARS"].str.startswith(prefix)].copy()

    # Rename
    df_population = df_population.rename(columns = { 
        "Regionalschlüssel_ARS": "municipality_code",
        "Einwohnerzahl_EWZ": "population"
    })
    
    return df_population

def validate(context):
    if not os.path.exists("%s/%s" % (context.config("data_path"), context.config("germany.population_path"))):
        raise RuntimeError("German population data is not available")

    return os.path.getsize("%s/%s" % (context.config("data_path"), context.config("germany.population_path")))
