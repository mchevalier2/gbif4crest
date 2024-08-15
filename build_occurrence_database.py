""" This script extract taxonomic and distribution data for taxa from GBIF. """

import os

import requests
import time
import numpy as np
import pandas as pd
from pygbif import occurrences as occ
from pygbif import species


'''

WHAT_I_WANT = {
    'plants':{
        'Tracheophyta':['Liliopsida', 'Magnoliopsida', 'Polypodiopsida', 'Pinopsida', 'Lycopodiopsida', 'Gnetopsida', 'Ginkgoopsida', 'Cycadopsida'],
        'Anthocerotopsida':['Anthocerotopsida']
    },
    'rodents':{
        'chordata':{
            'Mammalia':['Rodentia']
        }
    },
    'beetles_and_chiro':{
        'Arthropoda':{
            'Insecta':['Coleoptera', 'Diptera']
        }
    },
    'Bryophyta':['Bryophyta'],
    'Marchantiophyta':['Marchantiophyta'],
    'diatoms':{
        'Ochrophyta':['Bacillariophyceae']
    },
    'forams':['Foraminifera']
}

'''

LIST_OF_CLASSES = ['Liliopsida', 'Magnoliopsida', 'Polypodiopsida', 'Pinopsida', 'Lycopodiopsida', 'Gnetopsida', 'Ginkgoopsida', 'Cycadopsida'] + ['Anthocerotopsida'] + ['Mammalia'] + ['Bacillariophyceae'] + ['Bryopsida', 'Sphagnopsida', 'Polytrichopsida', 'Andreaeopsida', 'Takakiopsida', 'Andreaeobryopsida'] + ['Jungermanniopsida', 'Marchantiopsida', 'Haplomitriopsida'] + ['Globothalamea', 'Tubothalamea']





DATA_FOLDER = './data/'
TEMP_FOLDER = './tmp/'

OFFSET_STEP = 1000
SP_OCC_THRESHOLD = 10
TREE_OF_LIFE = ["speciesKey", "kingdom", "phylum", "class", "order", "family", "genus", "species"]

with open('gbif_pwd.txt') as f:
    GBIF_USERNAME, GBIF_PASSWORD, GBIF_EMAIL = [x[:-1] for x in f.readlines()]



if DATA_FOLDER not in os.listdir():
    os.mkdir(DATA_FOLDER)

if TEMP_FOLDER not in os.listdir():
    os.mkdir(TEMP_FOLDER)



def species_name_usage(key, limit, offset):
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
                        )
            keepgoing=False
        except (requests.exceptions.HTTPError, requests.exceptions.ChunkedEncodingError, ConnectionResetError, requests.exceptions.ConnectionError) as e:
            print(e)
            time.sleep(60)
            idx += 1
    if idx == 10:
        print("The request has failed 10 times. Returning empty result.")
        return {"results": [], "endOfRecords": False}
    return request






def from_classes_to_orders(ll):
    res=[]
    for classes in ll:
        dic = species.name_backbone(name=classes, verbose=True)
        cla = pd.DataFrame(species_name_usage(dic["usageKey"], 1000, 0)['results'])
        res += list(cla[['order', 'class']].dropna().loc[:, 'order'])
        #cla = species_name_usage(dic["usageKey"], OFFSET_STEP, offset_class)
    return res



LIST_OF_ORDERS = from_classes_to_orders(LIST_OF_CLASSES)
LIST_OF_ORDERS






#LIST_OF_CLASSES = ["Rotaliida", "Ericales", "Asterales"]




count_occurrences = 0
list_of_sp = []

for order in LIST_OF_ORDERS:
    filename = f"{DATA_FOLDER}taxalist_{order}.csv"
    dic = species.name_backbone(name=order, kingdom="plants", verbose=True)
    with open(filename, "w", encoding="utf-8") as file:
        file.write(
            "taxonID,kingdom,phylum,class_name,order_name,family,genus,species\n"
        )
    keep_going_order, offset_order = True, 0
    while keep_going_order:
        ord = species_name_usage(dic["usageKey"], OFFSET_STEP, offset_order)
        print(f"\n\n\nOffset loop on orders: {offset_order}")
        keep_going_order = not ord["endOfRecords"]
        offset_order += OFFSET_STEP
        df_order = pd.DataFrame(ord["results"])
        df_order = df_order[df_order["family"].notna()]
        print([x for x in df_order["family"]])
        for i, vali in df_order.iterrows():
            keep_going_fam, offset_fam = True, 0
            if vali['family'] in ['Asteraceae', 'Calyceraceae', 'Campanulaceae']:
                keep_going_fam = False
            while keep_going_fam:
                print(
                    f'\n\n\nOffset loop on families "{vali['family']}" with offset {offset_fam}'
                )
                fam = species_name_usage(vali["key"], OFFSET_STEP, offset_fam)
                keep_going_fam = not fam["endOfRecords"]
                offset_fam += OFFSET_STEP
                df_fam = pd.DataFrame(fam["results"])
                for j, valj in df_fam.iterrows():
                    keep_going_gen, offset_gen = True, 0
                    while keep_going_gen:
                        gen = species_name_usage(valj["key"], OFFSET_STEP, offset_gen)
                        keep_going_gen = not gen["endOfRecords"]
                        print(
                            f'Genus: "{valj['genus']}" | Offset: {offset_gen} | Keepgoing: {keep_going_gen} | NSP: {len(gen["results"])} | Occ bef. process: {count_occurrences}'
                        )
                        offset_gen += OFFSET_STEP
                        if len(gen["results"]):
                            df_gen = pd.DataFrame(gen["results"])
                            df_gen = df_gen[df_gen["genus"].notna()]
                            df_gen = (
                                df_gen.query("taxonomicStatus == 'ACCEPTED'")
                                .query("rank == 'SPECIES'")
                                .query("nameType == 'SCIENTIFIC'")
                                .assign(tokeep=True)
                                .reset_index()
                            )
                            for k, valk in df_gen.iterrows():
                                continue_request = True
                                while continue_request:
                                    try:
                                        nbocc = occ.count(taxonKey = int(valk['speciesKey']), isGeoreferenced=True)
                                        continue_request = False
                                    except requests.exceptions.ReadTimeout:
                                        time.wait(60)
                                        print("waiting to download")
                                if nbocc < SP_OCC_THRESHOLD:
                                    df_gen.iloc[k, -1] = False
                                else:
                                    count_occurrences += nbocc
                            df_gen = df_gen[df_gen['tokeep']]
                            if df_gen.shape[0]:
                                df_gen[TREE_OF_LIFE].to_csv(filename, mode="a", header=False, index=False)
                                list_of_sp += list(df_gen['speciesKey'])
    if count_occurrences > 40000000:
        print("\n\n\nCreating download request with:")
        res = occ.download([f'speciesKey in ["{'", "'.join([str(x) for x in list_of_sp])}"]', 'decimalLatitude !Null', 'decimalLongitude !Null', 'basisOfRecord in ["LIVING_SPECIMEN", "OBSERVATION", "HUMAN_OBSERVATION", "MACHINE_OBSERVATION", "MATERIAL_SAMPLE", "MATERIAL_CITATION", "OCCURRENCE"]'], "SIMPLE_CSV", user=GBIF_USERNAME, pwd=GBIF_PASSWORD, email=GBIF_EMAIL)
        print(res)
        with open(DATA_FOLDER+'requests_list.txt', 'a', encoding='utf-8') as f:
            f.write(str(res)+'\n')
        with open(DATA_FOLDER+'download_list.txt', 'a', encoding='utf-8') as f:
            f.write(str(res[0])+'\n')
        count_occurrences = 0
        list_of_sp = []










##-;
