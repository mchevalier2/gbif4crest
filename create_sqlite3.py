""" Put all the datasets together into a SQLite3 database """

import sqlite3 as db

import pandas as pd

DATA_FOLDER = "./data/"
TEMP_FOLDER = "./tmp/"
DATABASE_FOLDER = "./database_files/"


conn = db.connect(DATABASE_FOLDER + "gbif4crest_03.sqlite3")
cursor = conn.cursor()

geopolitical_units = pd.read_csv(DATABASE_FOLDER + "geopolitical_units.csv")
data_types = {
    "geopoid": "INTEGER",
    "continent": "VARCHAR(13)",
    "basin": "VARCHAR(13)",
    "name": "VARCHAR(59)",
    "countrycode": "VARCHAR(3)",
}
geopolitical_units = geopolitical_units.rename(columns={"geopoID": "geopoid"})
geopolitical_units.to_sql(
    "geopolitical_units",
    conn,
    index=False,
    index_label="geopoid",
    dtype=data_types,
    if_exists="replace",
)


biogeography = pd.read_csv(DATABASE_FOLDER + "biogeography.csv")
biogeography = biogeography.rename(columns={"ecoID": "ecoid"})
biogeography.to_sql(
    "biogeography", conn, index=False, index_label="ecoid", if_exists="replace"
)


typeofobservations = pd.DataFrame(
    {
        "type_of_obs": [1, 2, 3, 4, 5, 6, 7, 8],
        "name": [
            x.lower()
            for x in [
                "HUMAN_OBSERVATION",
                "OBSERVATION",
                "LIVING_SPECIMEN",
                "FOSSIL_SPECIMEN",
                "MATERIAL_SAMPLE",
                "MACHINE_OBSERVATION",
                "LITERATURE",
                "UNKNOWN",
            ]
        ],
    }
)
typeofobservations.to_sql(
    "typeofobservations",
    conn,
    index=False,
    index_label="type_of_obs",
    if_exists="replace",
)


taxalist = pd.read_csv(DATABASE_FOLDER + "taxalist.csv")
taxalist = taxalist.rename(columns={"taxonID": "taxonid"})
taxalist[
    [
        "taxonid",
        "kingdom",
        "phylum",
        "class_name",
        "order_name",
        "family",
        "genus",
        "species",
        "gbifID",
        "nb_occ",
        "nb_occ_qdgc",
    ]
].to_sql("taxa", conn, index=False, index_label="taxonid", if_exists="replace")


data_qdgc = pd.read_csv(
    DATABASE_FOLDER + "data_qdgc.csv",
    dtype={
        "mari_ecoID": "Int64",
        "oceanID": "Int64",
        "continentID": "Int64",
        "terr_ecoID": "Int64",
    },
)
data_qdgc = data_qdgc.rename(
    columns={
        "locID": "locid",
        "oceanID": "oceanid",
        "continentID": "continentid",
        "terr_ecoID": "terr_ecoid",
        "mari_ecoID": "mari_ecoid",
        "countryID": "countryid",
    }
)

data_qdgc.drop("index", axis=1, inplace=True)
# data_qdgc.columns = ['locid', 'longitude', 'latitude', 'mari_ecoid', 'oceanid']
##> Test for oceanid
test = data_qdgc[data_qdgc["oceanid"].notna()].merge(
    geopolitical_units, how="inner", left_on="oceanid", right_on="geopoid"
)
assert test.shape[0] == data_qdgc[data_qdgc["oceanid"].notna()].shape[0]
##> Test for continentid
test = data_qdgc[data_qdgc["countryid"].notna()].merge(
    geopolitical_units, how="inner", left_on="countryid", right_on="geopoid"
)
assert test.shape[0] == data_qdgc[data_qdgc["countryid"].notna()].shape[0]
##> Test for terr_ecoid
test = data_qdgc[data_qdgc["terr_ecoid"].notna()].merge(
    biogeography, how="inner", left_on="terr_ecoid", right_on="ecoid"
)
assert test.shape[0] == data_qdgc[data_qdgc["terr_ecoid"].notna()].shape[0]
##> Test for mari_ecoid
test = data_qdgc[data_qdgc["mari_ecoid"].notna()].merge(
    biogeography, how="inner", left_on="mari_ecoid", right_on="ecoid"
)
assert test.shape[0] == data_qdgc[data_qdgc["mari_ecoid"].notna()].shape[0]
data_qdgc.to_sql(
    "data_qdgc", conn, index=False, index_label="taxonid", if_exists="replace"
)


distrib_qdgc = pd.read_csv(
    DATABASE_FOLDER + "distrib_qdgc.csv",
    dtype={"first_occ": "Int64", "last_occ": "Int64"},
)
distrib_qdgc = distrib_qdgc.rename(columns={"locID": "locid", "taxonID": "taxonid"})

test = distrib_qdgc.merge(taxalist, how="inner", on="taxonid")
assert test.shape[0] == distrib_qdgc.shape[0]
test = distrib_qdgc.merge(data_qdgc, how="inner", on="locid")
assert test.shape[0] == distrib_qdgc.shape[0]
distrib_qdgc.to_sql(
    "distrib_qdgc", conn, index=False, index_label="taxonid", if_exists="replace"
)


params = pd.DataFrame({"version": "gbif4crest_03", "resol": [1 / 12]})
params.to_sql("params", conn, index=True, if_exists="replace")


cursor.execute("CREATE INDEX kingdom_idx ON taxa (kingdom);")
cursor.execute("CREATE INDEX phylum_idx ON taxa (phylum);")
cursor.execute("CREATE INDEX class_idx ON taxa (class_name);")
cursor.execute("CREATE INDEX order_idx ON taxa (order_name);")
cursor.execute("CREATE INDEX family_idx ON taxa (family);")
cursor.execute("CREATE INDEX genus_idx ON taxa (genus);")
cursor.execute("CREATE INDEX species_idx ON taxa (species);")


cursor.execute("CREATE INDEX realm_idx ON biogeography (realm);")
cursor.execute("CREATE INDEX biome_idx ON biogeography (biome);")
cursor.execute("CREATE INDEX ecoregion_idx ON biogeography (ecoregion);")


cursor.execute("CREATE INDEX taxonid_idx ON distrib_qdgc (taxonid);")
cursor.execute("CREATE INDEX locid_idx ON distrib_qdgc (locid);")
cursor.execute("CREATE INDEX firstocc_idx ON distrib_qdgc (first_occ);")
cursor.execute("CREATE INDEX lastocc_idx ON distrib_qdgc (last_occ);")


cursor.execute("CREATE INDEX long_idx ON data_qdgc (longitude);")
cursor.execute("CREATE INDEX lat_idx ON data_qdgc (latitude);")
cursor.execute("CREATE INDEX countryid_idx ON data_qdgc (countryid);")
cursor.execute("CREATE INDEX oceanid_idx ON data_qdgc (oceanid);")
cursor.execute("CREATE INDEX terr_ecoid_idx ON data_qdgc (terr_ecoid);")
cursor.execute("CREATE INDEX mari_ecoid_idx ON data_qdgc (mari_ecoid);")


cursor.execute("CREATE INDEX continent_idx ON geopolitical_units (continent);")
cursor.execute("CREATE INDEX name_idx ON geopolitical_units (name);")
cursor.execute("CREATE INDEX basin_idx ON geopolitical_units (basin);")


conn.close()

##-;
