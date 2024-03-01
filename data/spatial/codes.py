import os
import pandas as pd
import zipfile

"""
This stages loads a file containing all spatial codes in Gernamy and how
they can be translated into each other. These are mainly:

    - Länder (federal states), => regions in the code
    - Regierungsbezirke (administrative districts) => covers only a part of Germany
    - Kreise (districts/counties),
    - Verwaltungsgemeinschaften (administrative associations),
    - Gemeinden (municipalities),

Länder and Gemeinden are national coverage administrative boundaries,
it is not the case for the other levels, to simplify re use on other Germany
territories we simplify this way :

    - Länder = regions (in the code)
    - other required level = departments (in the code) this need to be declared
    in this script
"""

def configure(context):
    context.config("data_path")
    context.config("regions", [])
    context.config("departments", ["091"])
    context.config("codes_path", "codes_2021/vg250-ew_12-31.ee.excel.ebenen.zip")
    context.config("codes_xlsx", "vg250-ew_12-31.ee.excel.ebenen/vg250-ew_ebenen_1231/verwaltungsgebiete.xlsx")

def execute(context):
    # Load IRIS registry
    with zipfile.ZipFile(
        "{}/{}".format(context.config("data_path"), context.config("codes_path"))) as archive:
        with archive.open(context.config("codes_xlsx")) as f:
            df_codes = pd.read_excel(f,dtype=object,
                skiprows = 0, sheet_name = "VGTB_VZ_GEM"
            )[["AGS_G", "ARS_R", "ARS_L", "GEN_G"]].rename(columns = {
                "AGS_G": "commune_id",
                "ARS_R": "departement_id",
                "ARS_L": "region_id"
            })

# dtype={ "ARS_G": "object",
#  "ARS_R": "object",
#  "ARS_L": "object"}

    df_codes["commune_id"] = df_codes["commune_id"].astype("category")
    df_codes["departement_id"] = df_codes["departement_id"].astype("category")
    df_codes["region_id"] = df_codes["region_id"].astype("category")


    # Filter zones
    requested_regions = list(map(int, context.config("regions")))
    requested_departments = list(map(str, context.config("departments")))

    if len(requested_regions) > 0:
        df_codes = df_codes[df_codes["region_id"].isin(requested_regions)]

    if len(requested_departments) > 0:
        df_codes = df_codes[df_codes["departement_id"].isin(requested_departments)]

    df_codes["commune_id"] = df_codes["commune_id"].cat.remove_unused_categories()
    df_codes["departement_id"] = df_codes["departement_id"].cat.remove_unused_categories()

    # fake IRIS not divided
    df_codes["iris_id"] = df_codes["commune_id"].astype(str) + "0000"
    df_codes["iris_id"] = df_codes["iris_id"].astype("category")

    return df_codes[["region_id","departement_id","commune_id","iris_id"]]

def validate(context):
    if not os.path.exists("%s/%s" % (context.config("data_path"), context.config("codes_path"))):
        raise RuntimeError("Spatial reference codes are not available")

    return os.path.getsize("%s/%s" % (context.config("data_path"), context.config("codes_path")))
