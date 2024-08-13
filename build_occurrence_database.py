""" This script extract taxonomic and distribution data for taxa from GBIF. """

import os

import numpy as np
import pandas as pd
from pygbif import occurrences as occ
from pygbif import species

LIST_OF_CLASSES = ["Asterales", "Ericales"]
OFFSET_STEP = 1000

if "data" not in os.listdir():
    os.mkdir("data")

for new_class in LIST_OF_CLASSES:
    filename = f"./data/{new_class}.csv"
    filename2 = f"./data/{new_class}_distribs.csv"
    dic = species.name_backbone(name=new_class, kingdom="plants", verbose=True)
    with open(filename, "w", encoding="utf-8") as file:
        file.write(
            "taxonID,kingdom,phylum,class_name,order_name,family,genus,species\n"
        )
    with open(filename2, "w", encoding="utf-8") as file:
        file.write("taxonID,longitude,latitude,type_of_obs,year\n")
    keep_going_class, offset_class = True, 0
    while keep_going_class:
        cla = species.name_usage(
            key=dic["usageKey"],
            data="children",
            rank="SPECIES",
            limit=OFFSET_STEP,
            offset=offset_class,
        )
        print(f"\n\n\nOffset loop on classes: {offset_class}")
        keep_going_class = not cla["endOfRecords"]
        offset_class += OFFSET_STEP
        df_class = pd.DataFrame(cla["results"])
        df_class = df_class[df_class["family"].notna()]
        for i, vali in df_class.iterrows():
            keep_going_fam, offset_fam = True, 0
            while keep_going_fam:
                print(
                    f'\n\n\nOffset loop on families "{vali['family']}" with offset {offset_fam}'
                )
                fam = species.name_usage(
                    key=vali["key"],
                    data="children",
                    rank="SPECIES",
                    limit=OFFSET_STEP,
                    offset=offset_fam,
                )
                keep_going_fam = not fam["endOfRecords"]
                offset_fam += OFFSET_STEP
                df_fam = pd.DataFrame(fam["results"])
                for j, valj in df_fam.iterrows():
                    keep_going_gen, offset_gen = True, 0
                    while keep_going_gen:
                        print(
                            f'Offset loop on genus "{valj['genus']}" with offset {offset_gen}'
                        )
                        gen = species.name_usage(
                            key=valj["key"],
                            data="children",
                            rank="SPECIES",
                            limit=OFFSET_STEP,
                            offset=offset_gen,
                        )
                        keep_going_gen = not gen["endOfRecords"]
                        offset_gen += OFFSET_STEP
                        if len(gen["results"]):
                            df_gen = pd.DataFrame(gen["results"])
                            df_gen = df_gen[df_gen["genus"].notna()]
                            df_gen = (
                                df_gen.query("taxonomicStatus == 'ACCEPTED'")
                                .query("rank == 'SPECIES'")
                                .query("nameType == 'SCIENTIFIC'")
                            )
                            if df_gen.shape[0]:
                                df_gen[
                                    [
                                        "key",
                                        "kingdom",
                                        "phylum",
                                        "class",
                                        "order",
                                        "family",
                                        "genus",
                                        "species",
                                    ]
                                ].to_csv(filename, mode="a", header=False, index=False)
                                for sp_key in df_gen["key"]:
                                    keep_going_sp, offset_sp = True, 0
                                    while keep_going_sp:
                                        sp_occ = occ.search(
                                            taxonKey=sp_key,
                                            limit=300,
                                            offset=offset_sp,
                                            hasGeospatialIssue=False,
                                            hasCoordinate=True,
                                            basisOfRecord=[
                                                "LIVING_SPECIMEN",
                                                "OBSERVATION",
                                                "HUMAN_OBSERVATION",
                                                "MACHINE_OBSERVATION",
                                                "MATERIAL_SAMPLE",
                                                "MATERIAL_CITATION",
                                                "OCCURRENCE",
                                            ],
                                        )
                                        keep_going_sp = not sp_occ["endOfRecords"]
                                        offset_sp += 300
                                        if len(sp_occ["results"]):
                                            df_sp = pd.DataFrame(sp_occ["results"])
                                            df_sp = df_sp.query(
                                                "occurrenceStatus=='PRESENT'"
                                            )
                                            # df_sp = df_sp[df_sp['decimalLatitude'].notna()]
                                            if "year" not in df_sp.columns:
                                                df_sp.insert(0, "year", value=np.nan)
                                            if df_sp.shape[0]:
                                                df_sp[
                                                    [
                                                        "speciesKey",
                                                        "decimalLongitude",
                                                        "decimalLatitude",
                                                        "basisOfRecord",
                                                        "year",
                                                    ]
                                                ].to_csv(
                                                    filename2,
                                                    mode="a",
                                                    header=False,
                                                    index=False,
                                                )


##-;
