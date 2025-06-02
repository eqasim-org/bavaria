import bavaria.data.osm.osmconvert
import os

"""
The purpose of this stage is to cut the OSM data into smaller chunks so we can process
it more easily later on.
"""

def configure(context):
    context.stage("data.spatial.municipalities")
    context.stage("bavaria.data.osm.osmconvert")

    context.config("processes")

    context.config("data_path")
    context.config("osm_path_bavaria", "osm/bayern-latest.osm.pbf")

def process_municipality(context, zone_id):
    input_path = context.data("input_path")
    local_path = context.data("local_path")

    root_dir = os.getcwd() # happens to be repository root
    print(f"DEBUG processing municipality {zone_id}. Input path: {input_path}, local path: {local_path} cwd: {root_dir}")
    bavaria.data.osm.osmconvert.run(context, [input_path,
        "-B={}".format("{}/{}.poly".format(local_path, zone_id)),
        "-o={}".format("{}/{}.osm.pbf".format(local_path, zone_id))], cwd = root_dir)
    
    return zone_id
    
def execute(context):
    # Load zones and convert to polyfiles
    df_zones = context.stage("data.spatial.municipalities")[["commune_id", "geometry"]]
    df_zones = df_zones.to_crs("EPSG:4326")

    for zone_id, geometry in df_zones.itertuples(index = False):
        if not hasattr(geometry, "exterior"):
            geometry = geometry.convex_hull

        data = []
        data.append("polyfile")
        data.append("polygon")

        for coordinate in geometry.exterior.coords:
            data.append("    %e    %e" % coordinate[:2])

        data.append("END")
        data.append("END")

        with open("{}/{}.poly".format(context.path(), zone_id), "w+") as f:
            f.write("\n".join(data))
    
    # Cut into chunks
    with context.progress(label = "Chunking OSM data ...", total = len(df_zones)) as progress:
        with context.parallel({
            "input_path": "{}/{}".format(context.config("data_path"), context.config("osm_path_bavaria")),
            "local_path": context.path()
        }) as parallel:
            for item in parallel.imap(process_municipality, df_zones["commune_id"].values):
                progress.update()

    return df_zones["commune_id"].values

def validate(context):
    return os.path.getsize("{}/{}".format(context.config("data_path"), context.config("osm_path_bavaria")))