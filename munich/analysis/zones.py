import pyogrio

def configure(context):
    context.stage("munich.data.spatial.iris")

    context.config("output_path")
    context.config("output_prefix")

def execute(context):
    df = context.stage("munich.data.spatial.iris")

    df["municipality_id"] = df["commune_id"].astype(str)
    df["kreis_id"] = df["departement_id"].astype(str)

    df = df[["municipality_id", "kreis_id", "geometry"]]
    df = df.dissolve("municipality_id").reset_index()

    pyogrio.write_dataframe(df, "{}/{}zones.gpkg".format(
        context.config("output_path"), context.config("output_prefix")))
