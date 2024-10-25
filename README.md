# gbif4crest
A data engineering pipeline to process observations from the [GBIF database](https://www.gbif.org) into a relational database compatible with the [crestr R package](https://www.manuelchevalier.com/crestr/index.html).


    [x] Create venv
    [x] Install basic packages
    [x] Get gbif packages
    [x] Find a way to extract a list of species
    [x] Find a way to extract a distribution from a species ID
    [x] Make sure the script is robust to lots of conditions (run overnight)
    [x] Rename class to order.
    [x] Identify the unit where to make the primary for loop
    [x] Get the necessary order names
    [x] Rename newID to the correct names
    [x] Create correct taxonIDs
    [x] Create distrib_qdgc from distrib
    [x] Add nb_occ and nb_occ_qdgc to taxalist
    [x] Delete distrib (compress for now)
    [x] Clean taxa from taxalist that are not in distrib_qdgc
    [x] Download all necessary geophysical datasets
    [x] Rasterize_shp
    [x] Create biogeography and geopolitical_units tables
    [ ] Create data_qdgc tables
        [x] Terrestrial variables
        [x] Marine variables
        [ ] Biogeography variables
        [x] Fix cells with missing values
    [ ] Create SQLite3 database with all fastenning indexes (postprocess.sql)
    [ ] Clean unnecessary downloads from GBIF
