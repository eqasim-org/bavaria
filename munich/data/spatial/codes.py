"""
The codes (Amtlichen Regionalschlüssel - ARS)
are hierarchichally structred as follows:

- 2 digits: Bundesland (or city state)
- 1 digit: Regierungsbezirk / Bezirk
- 2 digits: Landkreis or Kreisfreie Stadt (city without "Landkreis / Kreis")
- 4 digits: Gemeindeverband (municipality associations)
- 3 digits: Gemeinde (municipality)

The correspondance to the code initially developed for France is as follows:

- Bundesland -> région
- Regierungsbezirk -> no correspondance
- Landkreis -> département
- Gemeindeverband -> no correspondance (theoretically communauté de communes)
- Gemeinde -> commune 
- The French statistical unit (IRIS) does not exist
"""

def configure(context):
    context.stage("bavaria.data.population.raw")

def execute(context):
    # Load codes
    df_codes = context.stage("bavaria.data.population.raw")[["municipality_code"]]

    # Clean up identifiers
    df_codes["region_id"] = df_codes["municipality_code"].str[:2].astype("category")
    df_codes["departement_id"] = df_codes["municipality_code"].str[:5].astype("category")
    df_codes["commune_id"] = df_codes["municipality_code"].astype("category")

    # Fake IRIS
    df_codes["iris_id"] = df_codes["commune_id"].astype(str) + "0000"
    df_codes["iris_id"] = df_codes["iris_id"].astype("category")

    # Track outdated AGS code for conversion
    df_codes["ags"] = df_codes["commune_id"].str[:5] + df_codes["commune_id"].str[9:]
    df_codes["ags"] = df_codes["ags"].astype("category")

    return df_codes[["region_id", "departement_id", "commune_id", "iris_id", "ags"]]
