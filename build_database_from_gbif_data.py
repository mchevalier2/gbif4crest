import os
import time

import pandas as pd
from pygbif import occurrences as occ



DATA_FOLDER = './data/'
TEMP_FOLDER = './tmp/'
DATABASE_FOLDER = './database_files/'



if DATA_FOLDER not in os.listdir():
    os.mkdir(DATA_FOLDER)

if TEMP_FOLDER not in os.listdir():
    os.mkdir(TEMP_FOLDER)



## CREATING TAXALIST TABLE ====================================================>

LIST_OF_TAXALIST_FILES = [x for x in os.listdir(DATA_FOLDER) if x.startswith('taxalist_')]
#taxalist = pd.concat((pd.read_csv(DATA_FOLDER+f) for f in LIST_OF_TAXALIST_FILES), ignore_index=True)
taxalist = (
            pd.concat(
                    (pd.read_csv(DATA_FOLDER+f) for f in LIST_OF_TAXALIST_FILES),
                    ignore_index=True
                )
               .drop_duplicates()
               .sort_values(['kingdom','phylum','class_name', \
                             'order_name','family','genus','species'])
)
taxalist['newID'] = taxalist.groupby(['kingdom']).agg({'taxonID':'cumcount'})
print("\n\nWARNING: I still need to 1) rename newID to something better and 2) to encode the type of taxon in the newID.\n\n")
taxalist = taxalist.sort_values('newID')
taxalist.to_csv(DATABASE_FOLDER+'taxalist.csv', mode="w", header=True, index=False)

# <============================================================================





## CREATING DISTRIB TABLE =====================================================>

# Loading the list of files to download from GBIF
with open(DATA_FOLDER+'download_list.txt') as f:
    LIST_OF_DATA_FILES = [x[:-1] for x in f.readlines()]

# For each file in that list, do
for datafile in LIST_OF_DATA_FILES:
    keeptrying=True
    while keeptrying:
        try:
            occ.download_get(datafile, path=DATA_FOLDER)
            keeptrying=False
        except:
            time.sleep(600)
    gbif_data = pd.read_csv(DATA_FOLDER + datafile+'.zip', sep='\t')
    gbif_data = gbif_data[['speciesKey', 'decimalLongitude', 'decimalLatitude', 'year', 'basisOfRecord']].drop_duplicates()
    gbif_data = gbif_data.merge(taxalist, left_on='speciesKey', right_on='taxonID', how='left')
    gbif_data = gbif_data[['newID', 'decimalLongitude', 'decimalLatitude', 'year', 'basisOfRecord']]
    gbif_data['year'] = gbif_data['year'].astype('Int64')
    gbif_data.to_csv(TEMP_FOLDER+'distrib_'+datafile+'.useless', mode="w", header=True, index=False)


# Merging everything and exporting as one clean database file
distrib = (
            pd.concat(
                    (pd.read_csv(TEMP_FOLDER+'distrib_'+f+'.useless') for f in LIST_OF_DATA_FILES),
                    ignore_index=True
                )
               .drop_duplicates()
               .sort_values(['newID'])
)
distrib.to_csv(DATABASE_FOLDER+'distrib.csv', mode="w", header=True, index=False)


# <============================================================================


##-;
