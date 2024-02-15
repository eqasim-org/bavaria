# -*- coding: utf-8 -*-
"""
Created on Tue Feb 13 09:11:46 2024

@author: arthur.burianne
"""

import geopandas as gpd
import pandas as pd
import shapely.geometry as geo
import numpy as np
import os, shutil
import py7zr, zipfile
import glob
import subprocess

output_path = "C:/Users/arthur.burianne/Documents/sources_data/fake_data _correze"


"""
This script creates test fixtures for the Île-de-France / France pipeline.

For that, we generate a couple of artificial data sets that have the same
structure as the initial French data. We deliberately do *not* base this script
on the actual data sets (e.g., to filter and reduce them), but generate them
from scratch. This way, we can extend and improve these artificial files step
by step to test specific features of the pipeline.

In this artificial France, we have two regions: 10 and 20.

    +---------+---------+
    |         |         |
    |   10    |   20    | 50km
    |         |         |
    +---------+---------+
       50km       50km

Both regions are divided in four departments 1A, 1B, 1C, 1D and 2A, 2B, 2C, 2D:

    +----+----+              +----+----+
    | 1A | 1B | 25km         | 2A | 2B | 25km
    +----+----+              +----+----+
    | 1C | 1D | 25km         | 2C | 2D | 25km
    +----+----+              +----+----+
     25km 25km                25km 25km

Each department is divided in 25 municipalities, e.g. 1A001 to 1A025, which are boxes
of 5km x 5km:

    001 002 003 004 005
    006 007 008 009 010
    011 012 013 014 015
    016 017 018 019 020
    021 022 023 024 025

The municipalities are furthermore divided into IRIS of size 500m x 500m. This
gives 10x10 = 100 IRIS per municipality, e.g. 1A00250001 to 1A00250100. Only
few municipalities are covered by IRIS:
- 1B013, 1B014, 1B018, 1B019
- 2D007, 2D008, 2D012, 2D013
"""

##########################################################################

# reset fake data folders (9)
shutil.rmtree("%s/bpe_2021" % output_path,ignore_errors=True)
shutil.rmtree("%s/rp_2019" % output_path,ignore_errors=True)
shutil.rmtree("%s/bpe_2021" % output_path,ignore_errors=True)
shutil.rmtree("%s/filosofi_2019" % output_path,ignore_errors=True)
shutil.rmtree("{}/bdtopo_idf".format(output_path),ignore_errors=True)
shutil.rmtree("%s/ban_idf" % output_path,ignore_errors=True)
shutil.rmtree("%s/sirene" % output_path,ignore_errors=True)

# make real data folders (4)
# os.mkdir("%s/osm_idf" % output_path)
# os.mkdir("%s/gtfs_idf" % output_path)
# os.mkdir("%s/codes_2021" % output_path)
# os.mkdir("%s/iris_2021" % output_path)
# os.mkdir("%s/entd_2008" % output_path)
# os.mkdir("%s/egt_2010" % output_path)


# load geographical data
local_name=  output_path + "/idf_pickles/"
region = "75"
df_codes = pd.read_pickle(local_name + "data.spatial.codes.p")

df_iris = pd.read_pickle(local_name + "data.spatial.iris.p")
df = df_iris.copy()
df = df.rename(columns={'iris_id':'iris', 'commune_id':'municipality', 'geometry':'geometry', 'departement_id':'department', 'region_id':'region'})
df_iris["iris_id"] = df_iris["iris_id"].astype("category")
df_iris["commune_id"] = df_iris["commune_id"].astype("category")
df_iris["departement_id"] = df_iris["departement_id"].astype("category")

BPE_OBSERVATIONS = 5000
HTS_HOUSEHOLDS = 3000
HTS_HOUSEHOLD_MEMBERS = 3

CENSUS_HOUSEHOLDS = len(df_iris)*20
CENSUS_HOUSEHOLD_MEMBERS = 3

COMMUTE_FLOW_OBSERVATIONS = 5000
ADDRESS_OBSERVATIONS = 20000
SIRENE_OBSERVATIONS = 20000

random = np.random.RandomState(0)



# %% Dataset: Aggregate census
# Required attributes: IRIS, COM, DEP, REG, P15_POP
print("Creating aggregate census ...")

df_population = df.copy()
df_population = df_population[["iris", "municipality", "department", "region"]].rename(columns = dict(
    iris = "IRIS", municipality = "COM", department = "DEP", region = "REG"
))

# Set all population to fixed number
df_population["P19_POP"] = 120.0

os.mkdir("%s/rp_2019" % output_path)

with zipfile.ZipFile("%s/rp_2019/base-ic-evol-struct-pop-2019.zip" % output_path, "w") as archive:
    with archive.open("base-ic-evol-struct-pop-2019.xlsx", "w") as f:
        df_population.to_excel(
            f, sheet_name = "IRIS", startrow = 5, index = False
        )

# %% Dataset: BPE
# Required attributes: DCIRIS, LAMBERT_X, LAMBERT_Y, TYPEQU, DEPCOM, DEP
print("Creating BPE ...")

# We put enterprises at the centroid of the shapes
observations = BPE_OBSERVATIONS
categories = np.array(["A", "B", "C", "D", "E", "F", "G"])

df_selection = df.iloc[random.randint(0, len(df), size = observations)].copy()
df_selection["DCIRIS"] = df_selection["iris"]
df_selection["DEPCOM"] = df_selection["municipality"]
df_selection["DEP"] = df_selection["department"]
df_selection["LAMBERT_X"] = df_selection["geometry"].centroid.x
df_selection["LAMBERT_Y"] = df_selection["geometry"].centroid.y
df_selection["TYPEQU"] = categories[random.randint(0, len(categories), size = len(df_selection))]

# Deliberately set coordinates for some to NaN
df_selection.iloc[-10:, df_selection.columns.get_loc("LAMBERT_X")] = np.nan
df_selection.iloc[-10:, df_selection.columns.get_loc("LAMBERT_Y")] = np.nan

columns = ["DCIRIS", "LAMBERT_X", "LAMBERT_Y", "TYPEQU", "DEPCOM", "DEP"]

os.mkdir("%s/bpe_2021" % output_path)

with zipfile.ZipFile("%s/bpe_2021/bpe21_ensemble_xy_csv.zip" % output_path, "w") as archive:
    with archive.open("bpe21_ensemble_xy.csv", "w") as f:
        df_selection[columns].to_csv(f,
            sep = ";", index = False)


# %% Dataset: Tax data
# Required attributes: CODGEO, D115, ..., D915
print("Creating FILOSOFI ...")


df_income = df.drop_duplicates("municipality")[["municipality"]].rename(columns = dict(municipality = "CODGEO"))
df_income["D119"] = 9122.0
df_income["D219"] = 11874.0
df_income["D319"] = 14430.0
df_income["D419"] = 16907.0
df_income["Q219"] = 22240.0
df_income["D619"] = 22827.0
df_income["D719"] = 25699.0
df_income["D819"] = 30094.0
df_income["D919"] = 32303.0

# # Deliberately remove some of them
# df_income = df_income[~df_income["CODGEO"].isin([
#     "77179", "95488"
# ])]

# # Deliberately only provide median for some
# f = df_income["CODGEO"].isin(["77874", "78498"])
# df_income.loc[f, "D215"] = np.nan

os.mkdir("%s/filosofi_2019" % output_path)

with zipfile.ZipFile("%s/filosofi_2019/indic-struct-distrib-revenu-2019-COMMUNES.zip" % output_path, "w") as archive:
    with archive.open("FILO2019_DISP_COM.xlsx", "w") as f:
        df_income.to_excel(
            f, sheet_name = "ENSEMBLE", startrow = 5, index = False
        )


# %% Data set: Census
print("Creating census ...")

persons = []

token = 0
for household_index in range(CENSUS_HOUSEHOLDS):
    household_id = household_index

    # iris = df["iris"].iloc[random.randint(len(df))]
    iris =  df.at[token,"iris"]
    token+=1
    if token > len(df)-1:
        token = 0

    department = iris[:2]
    # if iris.endswith("0000"): iris = iris[:-4] + "XXXX"

    # if random.random_sample() < 0.1: # For some, commune is not known
    #     iris = "ZZZZZZZZZ"

    destination_municipality = random.choice(df["municipality"].unique())
    destination_department = df[df["municipality"] == destination_municipality]["department"].values[0]

    for person_index in range(CENSUS_HOUSEHOLD_MEMBERS):
        persons.append(dict(
            CANTVILLE = "ABCE", NUMMI = household_id,
            AGED = "%03d" % random.randint(90), COUPLE = random.choice([1, 2]),
            CS1 = random.randint(9),
            DEPT = department, IRIS = iris, REGION = region, ETUD = random.choice([1, 2]),
            ILETUD = 4 if department != destination_department else 0,
            ILT = 4 if department != destination_department else 0,
            IPONDI = float(1.0),
            SEXE = random.choice([1, 2]),
            TACT = random.choice([1, 2]),
            TRANS = 4, VOIT = random.randint(3), DEROU = random.randint(2)
        ))

columns = [
    "CANTVILLE", "NUMMI", "AGED", "COUPLE", "CS1", "DEPT", "IRIS", "REGION",
    "ETUD", "ILETUD", "ILT", "IPONDI",
    "SEXE", "TACT", "TRANS", "VOIT", "DEROU"
]

df_persons = pd.DataFrame.from_records(persons)[columns]
df_persons.columns = columns

with zipfile.ZipFile("%s/rp_2019/RP2019_INDCVI_csv.zip" % output_path, "w") as archive:
    with archive.open("FD_INDCVI_2019.csv", "w") as f:
        df_persons.to_csv(f, sep = ";")


# %% Data set: commute flows
print("Creating commute flows ...")

municipalities = df["municipality"].unique()
observations = COMMUTE_FLOW_OBSERVATIONS

# ... work
df_work = pd.DataFrame(dict(
    COMMUNE = municipalities[random.randint(0, len(municipalities), observations)],
    DCLT = municipalities[random.randint(0, len(municipalities), observations)],
    TRANS = random.randint(1, 6, size = (observations,))
))

df_work["ARM"] = "Z"
df_work["IPONDI"] = 1.0

columns = ["COMMUNE", "DCLT", "TRANS", "ARM", "IPONDI"]
df_work.columns = columns

with zipfile.ZipFile("%s/rp_2019/RP2019_MOBPRO_csv.zip" % output_path, "w") as archive:
    with archive.open("FD_MOBPRO_2019.csv", "w") as f:
        df_work.to_csv(f, sep = ";")

# ... education
df_education = pd.DataFrame(dict(
    COMMUNE = municipalities[random.randint(0, len(municipalities), observations)],
    DCETUF = municipalities[random.randint(0, len(municipalities), observations)]
))
df_education["ARM"] = "Z"
df_education["IPONDI"] = 1.0

columns = ["COMMUNE", "DCETUF", "ARM", "IPONDI"]
df_education.columns = columns

with zipfile.ZipFile("%s/rp_2019/RP2019_MOBSCO_csv.zip" % output_path, "w") as archive:
    with archive.open("FD_MOBSCO_2019.csv", "w") as f:
        df_education.to_csv(f, sep = ";")


# %% Data set: BD-TOPO
print("Creating BD-TOPO ...")

observations = ADDRESS_OBSERVATIONS

df_selection = df_iris.iloc[random.randint(0, len(df_iris), observations)]

x = df_selection["geometry"].centroid.x.values
y = df_selection["geometry"].centroid.y.values
z = random.randint(100, 400, observations) # Not used but keeping unit test hashes constant

ids = [
    "BATIMENT{:016d}".format(n) for n in random.randint(1000, 1000000, observations)
]

ids[0] = ids[1] # setting multiple adresses for 1 building usecase

df_bdtopo = gpd.GeoDataFrame({
    "nombre_de_logements": random.randint(0, 10, observations),
    "cleabs": ids,
    "geometry": [
        geo.Point(x, y) for x, y in zip(x, y)
    ]
}, crs = "EPSG:2154")

# polygons as buildings from iris centroid points
df_bdtopo.set_geometry(df_bdtopo.buffer(40),inplace=True,drop=True,crs="EPSG:2154")

os.mkdir("{}/bdtopo_idf".format(output_path))
df_bdtopo.to_file("{}/bdtopo_idf/content.gpkg".format(output_path), layer = "batiment")

bdtopo_date = "2022-03-15"
bdtopo_departments = ["1A", "1B", "1C", "1D", "2A", "2B", "2C", "2D"]

with py7zr.SevenZipFile("{}/bdtopo_idf/bdtopo.7z".format(output_path), "w") as archive:
    archive.write("{}/bdtopo_idf/content.gpkg".format(output_path), "content/content.gpkg")
    os.remove("{}/bdtopo_idf/content.gpkg".format(output_path))

for department in bdtopo_departments:
    shutil.copyfile(
        "{}/bdtopo_idf/bdtopo.7z".format(output_path),
        "{}/bdtopo_idf/BDTOPO_3-0_TOUSTHEMES_GPKG_LAMB93_D0{}_{}.7z".format(
            output_path, department, bdtopo_date))

os.remove("{}/bdtopo_idf/bdtopo.7z".format(output_path))


# %% Data set: BAN
print("Creating BAN ...")

observations = ADDRESS_OBSERVATIONS

df_selection = df_iris.iloc[random.randint(0, len(df_iris), observations)]

x = df_selection["geometry"].centroid.x.values
y = df_selection["geometry"].centroid.y.values
municipality = df["municipality"].unique()

df_ban = pd.DataFrame({
    "code_insee": municipality[random.randint(0, len(municipality), observations)],
    "x": x,
    "y": y})

df_ban = df_ban[:round(len(x)*.8)]
os.mkdir("%s/ban_idf" % output_path)

for dep in df["department"].unique():
    df_ban.to_csv("%s/ban_idf/adresses-%s.csv.gz" % (output_path, dep),  compression='gzip', sep=";", index=False)

# %% Data set: SIRENE
print("Creating SIRENE ...")

observations = SIRENE_OBSERVATIONS

identifiers = random.randint(0, 99999999, observations)

df_sirene = pd.DataFrame({
    "siren": identifiers,
    "siret": identifiers,
    "codeCommuneEtablissement": municipalities[random.randint(0, len(municipalities), observations)],
    "etatAdministratifEtablissement": "A"
})

df_sirene["activitePrincipaleEtablissement"] = "52.1"
df_sirene["trancheEffectifsEtablissement"] = "03"


os.mkdir("%s/sirene" % output_path)
df_sirene.to_csv(output_path + "/sirene/StockEtablissement_utf8.zip", index = False, compression={'method': 'zip', 'archive_name': 'StockEtablissement_utf8.csv'})


df_sirene = df_sirene[["siren"]].copy()
df_sirene["categorieJuridiqueUniteLegale"] = "1000"

df_sirene.to_csv(output_path + "/sirene/StockUniteLegale_utf8.zip", index = False, compression={'method': 'zip', 'archive_name': 'StockUniteLegale_utf8.csv'})


# %% Data set: SIRENE GEOLOCATION
print("Creating SIRENE GEOLOCATION...")

df_selection = df_iris.iloc[random.randint(0, len(df_iris), observations)]
x = df_selection["geometry"].centroid.x.values
y = df_selection["geometry"].centroid.y.values

codes_com =  df_codes["commune_id"].iloc[random.randint(0, len(df_iris), observations)]

df_sirene_geoloc = pd.DataFrame({
    "siret": identifiers,
    "x": x,
    "y": y,
    "plg_code_commune":codes_com,
})

df_sirene_geoloc.to_csv("%s/sirene/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.zip" % output_path, index = False, sep=";", compression={'method': 'zip', 'archive_name': 'GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.csv'})


