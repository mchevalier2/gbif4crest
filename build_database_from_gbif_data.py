""" This script downloads and processes the results of the API requests """

import os
import time

import pandas as pd
from pygbif import occurrences as occ

DATA_FOLDER = "./data/"
TEMP_FOLDER = "./tmp/"
DATABASE_FOLDER = "./database_files/"


try:
    os.mkdir(DATA_FOLDER)
except FileExistsError:
    pass


try:
    os.mkdir(TEMP_FOLDER)
except FileExistsError:
    pass


## CREATING TAXALIST TABLE ====================================================>
print("\n\n\nCREATING TAXALIST TABLE")


def create_ID_from_taxonomy(x: []) -> int:
    """Creates a database-specific ID based on the taxonomy and existing ID"""
    a, b, d = x
    ## Plants
    if a in ["Tracheophyta", "Anthocerotophyta", "Bryophyta", "Marchantiophyta"]:
        return 1000001 + d
    ## Forams
    if a in ["Foraminifera"]:
        return 4000001 + d
    ## Diatoms
    if b in ["Bacillariophyceae"]:
        return 5000001 + d
    ## Mammals, including rodents
    if b in ["Mammalia"]:
        return 6000001 + d
    return -1


LIST_OF_TAXALIST_FILES = [
    x for x in os.listdir(DATA_FOLDER) if x.startswith("taxalist_")
]
taxalist = (
    pd.concat(
        (pd.read_csv(DATA_FOLDER + f, low_memory=False) for f in LIST_OF_TAXALIST_FILES),
        ignore_index=True,
    )
    .drop_duplicates()
    .sort_values(
        ["kingdom", "phylum", "class_name", "order_name", "family", "genus", "species"]
    )
)
taxalist["gbifID"] = taxalist["taxonID"]
taxalist["newID"] = taxalist.groupby(["kingdom"]).agg({"taxonID": "cumcount"})
taxalist["taxonID"] = taxalist[["phylum", "class_name", "newID"]].apply(
    create_ID_from_taxonomy, axis=1
)
taxalist = taxalist.sort_values("taxonID")
taxalist["gbifID"] = taxalist["gbifID"].astype("Int64")
taxalist = taxalist.drop("newID", axis=1).reset_index(drop=True)

assert taxalist[taxalist["taxonID"] == -1].shape[0] == 0, [
    "\nSome taxa did not get a proper taxonID.",
    taxalist.query("taxonID==-1"),
]
taxalist.to_csv(DATABASE_FOLDER + "taxalist.csv", mode="w", header=True, index=False)
print(taxalist)
# <============================================================================


## CREATING DISTRIB TABLE =====================================================>
print("\n\n\nCREATING DISTRIB TABLE")
# Loading the list of files to download from GBIF
with open(DATA_FOLDER + "download_list.txt", encoding="utf-8") as f:
    LIST_OF_DATA_FILES = [x[:-1] for x in f.readlines()]

# For each file in that list, do
for datafile in LIST_OF_DATA_FILES:
    print("Analysing: " + datafile)
    keeptrying = datafile + ".zip" not in os.listdir(DATA_FOLDER)
    while keeptrying:
        try:
            occ.download_get(datafile, path=DATA_FOLDER)
            keeptrying = False
        except:
            time.sleep(600)
    gbif_data = pd.DataFrame({'speciesKey':[-1], "decimalLongitude":[-1.0], "decimalLatitude":[-1.0], "year":[-1], "basisOfRecord":['']})
    ## Reading the occurrence data by chunks to not overload laptop.
    for idx, temp_df in enumerate(pd.read_csv(DATA_FOLDER + datafile + ".zip", sep="\t", chunksize=2000000, low_memory=False)):
        print('chunk:', idx)
        temp_df = temp_df[
            ["speciesKey", "decimalLongitude", "decimalLatitude", "year", "basisOfRecord"]
        ].drop_duplicates()
        temp_df = temp_df[temp_df["decimalLongitude"].notna()]
        gbif_data = pd.concat([gbif_data, temp_df], ignore_index=True)
    gbif_data = (
        gbif_data
        .drop(index=0) # Excluding the fake row I added to circumvent the warning
        .drop_duplicates()
        .merge(taxalist, left_on="speciesKey", right_on="gbifID", how="left")
    )
    gbif_data = gbif_data[
        ["taxonID", "decimalLongitude", "decimalLatitude", "year", "basisOfRecord"]
    ]
    gbif_data["year"] = pd.to_numeric(gbif_data["year"]).astype("Int64")
    gbif_data["taxonID"] = gbif_data["taxonID"].astype("Int64")
    gbif_data = gbif_data[gbif_data["taxonID"].notna()]
    gbif_data.to_csv(
        TEMP_FOLDER + "distrib_" + datafile + ".useless",
        mode="w",
        header=True,
        index=False,
    )


# Merging everything and exporting as one clean database file
distrib = (
    pd.concat(
        (
            pd.read_csv(TEMP_FOLDER + "distrib_" + f + ".useless")
            for f in LIST_OF_DATA_FILES
        ),
        ignore_index=True,
    )
    .drop_duplicates()
    .sort_values(["taxonID"])
)
distrib["year"] = distrib["year"].astype("Int64")
print("\n## distrib.head()\n", distrib.head())
print("\n## distrib.describe()\n", distrib.describe())
print(
    "\n## distrib['basisOfRecord'].value_counts()\n",
    distrib["basisOfRecord"].value_counts(),
)

assert distrib["taxonID"].isna().sum() == 0, "NAs in taxonID"
assert distrib["decimalLongitude"].isna().sum() == 0, "NAs in decimalLongitude"
assert distrib["decimalLatitude"].isna().sum() == 0, "NAs in decimalLatitude"

distrib.to_csv(DATABASE_FOLDER + "distrib.csv", mode="w", header=True, index=False)

os.system(f"rm {TEMP_FOLDER}distrib_*.useless")

# <============================================================================


##-;
