""" This script extract taxonomic and distribution data for taxa from GBIF. """

import os
import subprocess
import time

import pandas as pd
import requests
from pygbif import occurrences as occ
from pygbif import species

LIST_OF_CLASSES = (
    [  ## Plants
        "Liliopsida",
        "Magnoliopsida",
        "Polypodiopsida",
        "Pinopsida",
        "Lycopodiopsida",
        "Gnetopsida",
        "Ginkgoopsida",
        "Cycadopsida",
    ]
    + ["Anthocerotopsida"]  ## More plants
    + ["Mammalia"]  ## mammals including rodents
    # + ["Bacillariophyceae"]  ## diatoms
    + [  ## More plants
        "Bryopsida",
        "Sphagnopsida",
        "Polytrichopsida",
        "Andreaeopsida",
        "Takakiopsida",
        "Andreaeobryopsida",
    ]
    + ["Jungermanniopsida", "Marchantiopsida", "Haplomitriopsida"]  ## More plants
    # + ["Globothalamea", "Tubothalamea"]  ## forams
)


DATA_FOLDER = "./data/"
TEMP_FOLDER = "./tmp/"

OFFSET_STEP = 1000
SP_OCC_THRESHOLD = 10
TREE_OF_LIFE = [
    "speciesKey",
    "kingdom",
    "phylum",
    "class",
    "order",
    "family",
    "genus",
    "species",
]

with open("gbif_pwd.txt", encoding="utf-8") as file:
    GBIF_USERNAME, GBIF_PASSWORD, GBIF_EMAIL = [x[:-1] for x in file.readlines()]


try:
    os.mkdir(DATA_FOLDER)
except FileExistsError:
    pass


try:
    os.mkdir(TEMP_FOLDER)
except FileExistsError:
    pass


def api_request_name_usage(key: int, limit: int, offset: int) -> {}:
    """Queries the GBIF API to get the list of children of a taxon"""
    keepgoing = True
    idx = 0
    while keepgoing and idx < 10:
        try:
            request = species.name_usage(
                key=key,
                data="children",
                rank="SPECIES",
                limit=limit,
                offset=offset,
                timeout=300,
            )
            keepgoing = False
        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.HTTPError,
            requests.exceptions.ChunkedEncodingError,
            ConnectionResetError,
            requests.exceptions.ConnectionError,
        ) as e:
            print("name_usage sleeping", e)
            time.sleep(60)
            idx += 1
    if idx == 10:
        print("The request has failed 10 times. Returning empty result.")
        return {"results": [], "endOfRecords": False}
    return request


def api_request_count(taxon_id: int):
    """Queries the GBIF API to get the number of occurrences of a taxon"""
    keepgoing = True
    idx = 0
    while keepgoing and idx < 10:
        try:
            nb_occ = occ.count(
                taxonKey=int(taxon_id), isGeoreferenced=True, timeout=300
            )
            keepgoing = False
        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.HTTPError,
            requests.exceptions.ChunkedEncodingError,
            ConnectionResetError,
            requests.exceptions.ConnectionError,
        ) as e:
            print("count sleeping", e)
            time.sleep(60)
            idx += 1
    if idx == 10:
        print("The request has failed 10 times. Returning empty result.")
        return -1
    return nb_occ


def api_request_download(splist_str: str) -> None:
    """Triggers the download from GBIF and saves the requests to local files"""
    res = occ.download(
        [
            f'speciesKey in ["{splist_str}"]',
            "decimalLatitude !Null",
            "decimalLongitude !Null",
            'basisOfRecord in ["LIVING_SPECIMEN", "OBSERVATION", '
            + '"HUMAN_OBSERVATION", "MACHINE_OBSERVATION", "MATERIAL_SAMPLE", '
            + '"MATERIAL_CITATION", "OCCURRENCE"]',
        ],
        "SIMPLE_CSV",
        user=GBIF_USERNAME,
        pwd=GBIF_PASSWORD,
        email=GBIF_EMAIL,
    )
    with open(DATA_FOLDER + "requests_list.txt", "a", encoding="utf-8") as f:
        f.write(str(res) + "\n")
    with open(DATA_FOLDER + "download_list.txt", "a", encoding="utf-8") as f:
        f.write(str(res[0]) + "\n")


def from_classes_to_orders(ll: list) -> list:
    """Queries the GBIF API to return the list of orders corresponding to a class"""
    list_of_orders = []
    for classes in ll:
        dic = species.name_backbone(name=classes, verbose=True, timeout=300)
        cla = pd.DataFrame(api_request_name_usage(dic["usageKey"], 1000, 0)["results"])
        list_of_orders += list(cla[["order", "class"]].dropna().loc[:, "order"])
    return list_of_orders


LIST_OF_ORDERS = from_classes_to_orders(LIST_OF_CLASSES)
LIST_OF_ORDERS = LIST_OF_ORDERS[LIST_OF_ORDERS.index("Macroscelidea") :]
print(LIST_OF_ORDERS)
# LIST_OF_CLASSES = ["Rotaliida", "Ericales", "Asterales"]

count_occurrences = 0
list_of_sp = []

for order in LIST_OF_ORDERS:
    filename = f"{DATA_FOLDER}taxalist_{order}.csv"
    dic = species.name_backbone(name=order, kingdom="plants", verbose=True)
    keep_going_order, offset_order = True, 0
    if "usageKey" in dic.keys():
        usageKey = dic["usageKey"]
    else:
        if "alternatives" in dic.keys():
            usageKey = dic["alternatives"][0]["usageKey"]
        else:
            keep_going_order = False
            print(f"\nWARNING: I couldn't find a match in GBIF backbone for {order}.")
            print(dic)
    with open(filename, "w", encoding="utf-8") as file:
        file.write(
            "taxonID,kingdom,phylum,class_name,order_name,family,genus,species\n"
        )
    while keep_going_order:
        order_name = api_request_name_usage(usageKey, OFFSET_STEP, offset_order)
        print(f"\n\n\nOffset loop on orders: {offset_order} with order {order}")
        keep_going_order = not order_name["endOfRecords"]
        offset_order += OFFSET_STEP
        df_order = pd.DataFrame(order_name["results"])
        if (df_order.shape[0] > 0) and ("family" in df_order.columns):
            df_order = df_order[df_order["family"].notna()]
            print(list(df_order["family"]))
            for i, vali in df_order.iterrows():
                keep_going_fam, offset_fam = True, 0
                while keep_going_fam:
                    print(
                        f'\n\n\nOffset loop on families "{vali["family"]}" with offset {offset_fam}'
                    )
                    fam = api_request_name_usage(vali["key"], OFFSET_STEP, offset_fam)
                    keep_going_fam = not fam["endOfRecords"]
                    offset_fam += OFFSET_STEP
                    df_fam = pd.DataFrame(fam["results"])
                    if "genus" in df_fam.columns:
                        for j, valj in df_fam.iterrows():
                            keep_going_gen, offset_gen = True, 0
                            while keep_going_gen:
                                gen = api_request_name_usage(
                                    valj["key"], OFFSET_STEP, offset_gen
                                )
                                keep_going_gen = not gen["endOfRecords"]
                                print(
                                    f'Genus: "{valj["genus"]}" | Offset: {offset_gen} | Keepgoing: {keep_going_gen} | NSP: {len(gen["results"])} | Occ bef. process: {count_occurrences}'
                                )
                                offset_gen += OFFSET_STEP
                                if len(gen["results"]) > 0:
                                    df_gen = pd.DataFrame(gen["results"])
                                    df_gen = df_gen[df_gen["genus"].notna()]
                                    df_gen = (
                                        df_gen.query("taxonomicStatus == 'ACCEPTED'")
                                        .query("rank == 'SPECIES'")
                                        .query("nameType == 'SCIENTIFIC'")
                                        .assign(tokeep=True)
                                        .reset_index()
                                    )
                                    if "speciesKey" in df_gen.columns:
                                        for k, valk in df_gen.iterrows():
                                            nbocc = api_request_count(
                                                int(valk["speciesKey"])
                                            )
                                            if nbocc < SP_OCC_THRESHOLD:
                                                df_gen.iloc[k, -1] = False
                                            else:
                                                count_occurrences += nbocc
                                        df_gen = df_gen[df_gen["tokeep"]]
                                        if df_gen.shape[0]:
                                            df_gen[TREE_OF_LIFE].to_csv(
                                                filename,
                                                mode="a",
                                                header=False,
                                                index=False,
                                            )
                                            list_of_sp += list(df_gen["speciesKey"])
    if count_occurrences > 40000000:
        print("\n\n\nCreating download request with:")
        splist = '", "'.join([str(int(x)) for x in list_of_sp])
        api_request_download(splist)
        count_occurrences = 0
        list_of_sp = []


splist = '", "'.join([str(int(x)) for x in list_of_sp])
api_request_download(splist)

## Some quick cleaning of the created files
## If no taxa names were added to a order files, the file is removed.
for file in [x for x in os.listdir(DATA_FOLDER) if x.startswith("taxalist_")]:
    wc = int(
        subprocess.check_output(["wc", "-l", DATA_FOLDER + file])
        .decode("utf8")
        .split()[0]
    )
    print(file, wc)
    if wc == 1:
        os.remove(DATA_FOLDER + file)


##-;

""" Accepted pylint errors *************
    213:0: C0301: Line too long (185/100) (line-too-long)
    154:8: W0621: Redefining name 'dic' from outer scope (line 168) (redefined-outer-name)
    165:0: C0103: Constant name "count_occurrences" doesn't conform to UPPER_CASE naming style (invalid-name)
    178:12: C0103: Constant name "keep_going_order" doesn't conform to UPPER_CASE naming style (invalid-name)
    186:44: E0606: Possibly using variable 'usageKey' before assignment (possibly-used-before-assignment)
    246:8: C0103: Constant name "splist" doesn't conform to UPPER_CASE naming style (invalid-name)
    270:0: W0105: String statement has no effect (pointless-string-statement)
"""
