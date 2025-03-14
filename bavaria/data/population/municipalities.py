"""
Generates a data frame with population count per municipality in Germany.
"""

def configure(context):
    context.stage("bavaria.data.population.raw")

def execute(context):
    # Load shapes
    df = context.stage("bavaria.data.population.raw")[["municipality_code", "population"]]

    # Clean up identifiers
    df["region_id"] = df["municipality_code"].str[:2].astype("category")
    df["departement_id"] = df["municipality_code"].str[:5].astype("category")
    df["commune_id"] = df["municipality_code"].astype("category")

    # Fake IRIS
    df["iris_id"] = df["commune_id"].astype(str) + "0000"
    df["iris_id"] = df["iris_id"].astype("category")

    return df[["region_id", "departement_id", "commune_id", "iris_id", "population"]]
