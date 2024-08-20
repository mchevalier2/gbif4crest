""" This script creates the distrib_QDGC table """

import os

import pandas as pd

DATA_FOLDER = "./data/"
TEMP_FOLDER = "./tmp/"
DATABASE_FOLDER = "./database_files/"


def nearest_quarter(x: float) -> float:
    """This function returns the nearing centroid of the selected grid"""
    return round((x - 1.0 / 24.0 + 0.00000001) * 12) / 12 + 1.0 / 24.0


def f_locid(x: []) -> int:
    """Creates a unique idenfied for a set of coordinates"""
    x0, x1 = x
    if x0 >= 180:
        x0 = 180 - 1.0 / 24.0
    if x1 >= 90:
        x1 = 90 - 1.0 / 24.0
    return int((180 + x0) // (1.0 / 12.0)) + 360 * 12 * int((90 + x1) // (1.0 / 12.0))


distrib = pd.read_csv(DATABASE_FOLDER + "distrib.csv")

distrib["longitude"] = distrib["decimalLongitude"].apply(nearest_quarter)
distrib["latitude"] = distrib["decimalLatitude"].apply(nearest_quarter)
distrib["locid"] = distrib[["longitude", "latitude"]].apply(f_locid, axis=1)

distrib_qdgc = distrib[
    ["taxonID", "longitude", "latitude", "locid", "year", "basisOfRecord"]
].drop_duplicates()
print(distrib_qdgc)


def groupby_year(x: pd.DataFrame) -> []:
    """Summarises the date information in x into three cells:
    - youngest observation,
    - oldest information,
    - TRUE/FALSE if some observations are undated
    """
    x = x.drop_duplicates()
    res = ["NULL", "NULL", "NULL"]
    if x.isna().sum():
        res[2] = "TRUE"
    x = x[[not y for y in x.isna()]]
    if x.shape[0] > 0:
        res[0] = int(x.min())
        res[1] = int(x.max())
    return res


def groupby_basisofrecord(x: pd.DataFrame) -> []:
    """Returns a list of unique types of observations."""
    return list(x.drop_duplicates().sort_values())


def get_yr(x: float, lvl: int) -> float:
    """Returns the min, max, no_obs value based on lvl."""
    if lvl == 0:
        return x[0]
    if lvl == 1:
        return x[1]
    if lvl == 2:
        return 0 if x[2] == "NULL" else 1


def get_basisOfRecord(x: [], bOR: str) -> int:
    """Returns 1 if bOR in x"""
    return 1 if bOR in x else 0


distrib_qdgc_groupbys = distrib_qdgc.groupby(["taxonID", "locid"])[
    ["year", "basisOfRecord", "longitude"]
].agg(
    {"year": groupby_year, "basisOfRecord": groupby_basisofrecord, "longitude": "count"}
)
distrib_qdgc_groupbys.columns = ["year", "basisOfRecord", "n_occ"]
distrib_qdgc_groupbys = distrib_qdgc_groupbys.reset_index()
print(distrib_qdgc_groupbys)

## Opening up thelist of years
distrib_qdgc_groupbys["first_occ"] = distrib_qdgc_groupbys["year"].apply(get_yr, lvl=0)
distrib_qdgc_groupbys["last_occ"] = distrib_qdgc_groupbys["year"].apply(get_yr, lvl=1)
distrib_qdgc_groupbys["no_date"] = distrib_qdgc_groupbys["year"].apply(get_yr, lvl=2)

## Opening up the list of basisOfRecords
LIST_OF_BORS = [
    "LIVING_SPECIMEN",
    "OBSERVATION",
    "HUMAN_OBSERVATION",
    "MACHINE_OBSERVATION",
    "MATERIAL_SAMPLE",
    "MATERIAL_CITATION",
    "OCCURRENCE",
]
for b in LIST_OF_BORS:
    distrib_qdgc_groupbys[b.lower()] = distrib_qdgc_groupbys["basisOfRecord"].apply(
        get_basisOfRecord, bOR=b
    )

distrib_qdgc_groupbys.columns = [
    "taxonID",
    "locid",
    "year",
    "basisOfRecord",
    "n_occ",
    "first_occ",
    "last_occ",
    "no_date",
    "living_specimen",
    "observation",
    "human_observation",
    "machine_observation",
    "material_sample",
    "literature",
    "unknown",
]

distrib_qdgc_groupbys["fossil_specimen"] = 0
print(distrib_qdgc_groupbys)


distrib_qdgc_groupbys = distrib_qdgc_groupbys.drop(
    ["year", "basisOfRecord"], axis=1
)
distrib_qdgc_groupbys.to_csv(DATABASE_FOLDER + "distrib_qdgc.csv", mode="w", header=True, index=False)

print("\nCompressing distrib.csv")
os.system(f"zip {DATABASE_FOLDER}distrib.zip {DATABASE_FOLDER}distrib.csv")
os.system(f"rm {DATABASE_FOLDER}distrib.csv")


# ADAPTING DISTRIB TABLE WITH OBSERVATION COUNTS =============================>
occ_counts = distrib_qdgc_groupbys.groupby('taxonID')[['n_occ', 'locid']].agg({'n_occ':'sum', 'locid': 'count'})
taxalist = pd.read_csv(DATABASE_FOLDER + "taxalist.csv")
colnames=list(taxalist.columns)
taxalist = taxalist.merge(occ_counts, on='taxonID', how='inner')
taxalist.columns = colnames + ['nb_occ', 'nb_occ_qdgc']
print(taxalist)
taxalist.to_csv(DATABASE_FOLDER + "taxalist.csv", mode="w", header=True, index=False)
# <============================================================================





##-;
