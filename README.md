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
    [ ] Create biogeography and geopolitical_units tables
    [ ] Create data_qdgc tables
    [ ] Clean taxa from taxalist that are not in distrib_qdgc
    [ ] Clean unnecessary downloads from GBIF
