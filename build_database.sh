cd /Users/palaeosaurus/Research/GBIF/gbif4crest
source venv/bin/activate

## Query the GBIF API to obtain data
python build_occurrence_database.py

## Download and process the raw GBIF data
python build_database_from_gbif_data.py

## Create the distrib table at the specified resolution
python build_distrib_qdgc.py

## Download all the necessary datasets
python download_geophysical_datasets.py
