# gbif4crest - A relational database combining modern biological observations with climate

This data engineering pipeline retrieves and organizes observations from the [GBIF database](https://www.gbif.org) into a structured relational database [[accessible here]](https://figshare.com/articles/GBIF_for_CREST_database/6743207). This standardized database includes calibration data essential for implementing the CREST climate reconstruction method using fossil bioindicators, and is compatible with the [crestr R package](https://www.manuelchevalier.com/crestr/index.html). It integrates information from multiple sources, resulting in a simple SQL database illustrated below. For a comprehensive overview of the various tables, refer to [here](https://www.manuelchevalier.com/crestr/articles/calibration-data.html).

![The relationship diagram of the gbif4crest database](jungle/fig-gbiftables_v1.png?raw=true "The relationship diagram of the gbif4crest database")



## The Data Engineering Pipeline

This pipeline is composed of two data engineering tracks that eventually converge to create the database:
- **Track 1** (in blue): Geolocalized observations of various biological entities, such as plants and mammals, are sourced from the [GBIF database](https://www.gbif.org), then curated and standardized. This track generates the TAXA, DISTRIB, and DISTRIB_QDGC tables as illustrated in the SQL diagram above.
- **Track 2** (in green): The biophysical and biogeographical variables defining the biological observations are retrieved, processed, and exported at the specified spatial resolution. This track generates the DATA_QDGC table based on the SQL diagram mentioned above.

The two tracks are ultimately combined to form the gbif4crest_03 database. It is exported in a SQLite3 format, making it directly compatible with the [crestr R package](https://www.manuelchevalier.com/crestr/index.html).

![The data analysis pipeline of this project](jungle/pipeline_diagram/pipeline_diagram.001.png?raw=true "The data analysis pipeline of this project")


## How to use

For safe usage of the various scripts in this repository, please input the commands listed below.


### Creating a virtual environment

While using `venv` (_virtual environments_) is optional, it is strongly recommended to ensure that this program does not affect your local system. Any installations or deletions within a venv will not impact your machine.


```
    cd /path/to/folder
    git clone https://github.com/mchevalier2/gbif4crest.git
    cd gbif4crest
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
```


Alternatively, you can download the following packages (and their dependencies) manually:  `duckdb`, `netCDF4`, `numpy`, `osgeo`, `pandas`, `pycountry`, `pycountry_convert`, `pygbif`, `requests`, `scipy`, `sqlite3`.


### Step 1: Extracting data from the GBIF API

**What it does**: Queries the GBIF-API to access all the modern observations of the biological taxa of interest. Only observations with a taxonomical resolution at the species level are kept.

**Required Interaction**: To download these datasets, you must have a valid GBIF account and include your credentials in a file named "gbif_pwd.txt". This three-line file should first list your GBIF username, followed by your GBIF password, and finally, a valid email address for data download notifications. An example is shown in the figure below — remember to leave an empty line at the end of the file.

![Illustration of the gbif_pwd.txt file](jungle/gbif_pwd.txt.png?raw=true "Illustration of the gbif_pwd.txt file")


**Possible interactions**:
1. Users can modify the taxa list extracted by appending or removing classes of organisms from the Python list called "LIST_OF_CLASSES". Only organism classes (_e.g._ mammalia) are accepted.

Once ready, you can start pulling data with the following command:. If you run this script with the default LIST_OF_CLASSES, it may take several days to compute.

```
python gbif_api_data_query.py
```



### Step 2: Processing the data as the TAXA and DISTRIB tables

**What it does**: Creates the TAXA and DISTRIB tables of the database. It will process all the 'taxalist_***.csv' files created in the previous step to generate the TAXA table. It will also download the raw observation data from GBIF (unless the archives are already available in the './tmp' folder and compile them in the DISTRIB table.

**Possible interactions**:
1. Speed up the process by directly downloading the observation data from the GBIF website. Retrieve the results of the various queries and store the files in the './tmp/' folder without altering their names.
2. If you modified the LIST_OF_CLASSES in the previous step, you will want to check that the IDing of your data remains operational. To do so, look at the function `create_id_from_taxonomy()` and adapt as necessary. By default, the gbif4crest_03 database contains information for four main types of biological entities: values between 1,000,000 and 1,999,999 represent plants, between 4,000,000 and 4,999,999 foraminifers, etc. These indexes connect the relational database’s TAXA, DISTRIB, and DISTRIB_QDGC tables.

```
python build_taxa_and_distrib.py
```

**PS**: During the execution of Step 2, you can jump to Step 4 to start downloading the next set of datasets for processing. However, remember to return to Step 3 once Step 2 is complete.


### Step 3: Building the DISTRIB_QDGC table

**What it does**: This script will post-process the DISTRIB table (long table), where the type of observations are mixed and their spatial resolution variable into a consolidated table at 1/12° spatial resolution (wide table). Depending on how many taxa you extracted from GBIF, this step might be hard to digest on a personal computer. This process will use a lot of computer memory, so it is better to run the script when you do not need to use it (meal, overnight, meeting, etc.).


**Possible interactions**: No interaction is expected at this stage.

```
python build_distrib_qdgc.py
```

Now, you have completed the first track: All the biological observations have been downloaded, curated, and post-processed. The following steps involve a similar processing of the biophysical and biogeographical datasets.


### Step 4: Downloading and processing the necessary biogeographical datasets

Download a series of datasets to process manually and save them in the `./tmp` folder created in Step 1.

This step is not automatised because some websites block such automatic downloads or require registration (all datasets are open access and free to use for academic purposes).

1. Elevation data: https://www.ngdc.noaa.gov/mgg/global/relief/ETOPO1/data/ice_surface/grid_registered/georeferenced_tiff/ETOPO1_Ice_g_geotiff.zip
2. Terrestrial climate data:
    - The WorldClim v2 data: https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_30s_bio.zip
    - The Aridity Index v3 data: https://figshare.com/ndownloader/files/34377245
3. Marine climate data:
    - Long-term monthly mean sea ice concentration data: https://www.psl.noaa.gov/data/gridded/data.noaa.oisst.v2.highres.html
    - Marine variables from the World Ocean Atlas 2018
        - Annual ('00') and Seasonal ('13', '14', '15', '16') variables:
            - temperature https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/temperature/netcdf/decav/0.25/
            - salinity https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/salinity/netcdf/decav/0.25/
        - Annual ('00') variables only:
            - nitrate https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/nitrate/netcdf/all/1.00/
            - oxygen https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/oxygen/netcdf/all/1.00/
            - phosphate https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/phosphate/netcdf/all/1.00/
            - silicate https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/silicate/netcdf/all/1.00/
4. Ecological data:
    - Ocean biomes: https://auckland.figshare.com/articles/dataset/GIS_shape_files_of_realm_maps/5596840?file=9737926
    - Ocean borders: http://geo.vliz.be:80/geoserver/wfs?request=getfeature&service=wfs&version=1.0.0&typename=MarineRegions:iho&outputformat=SHAPE-ZIP
    - Country borders: https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_admin_0_countries.zip
    - Terrestrial biomes: https://files.worldwildlife.org/wwfcmsprod/files/Publication/file/6kcchn7e3u_official_teow.zip



### Step 5: Build the biogeography and geopolitical tables


**What it does**: It rasterises (i.e., grids) all the datasets downloaded as shapefiles (vector format) at a predefined spatial resolution of 1/12°.


**Possible interactions**: No interaction is expected at this stage.

```
python rasterize_shapefile.py
```



### Step 6: Building the data_qdgc table


**What it does**: This script will process different geophysical biogeographical datasets to characterise each cell of our gridded world (1/12° resolution, corresponding to 9+M distinct grid cells). The first script creates two SQL tables (BIOGEOGRAPHY and GEOPOLITICAL_UNITS, not shown on the database diagram above) that optimise the information in the DISTRIB_QDGC table built in the next step. In detail, this means that biomes and country information will be represented as unique identifiers while the "real" data is preserved in the two tables created here. The second script will build the DISTRIB_QDGC table.

NOTE: This step will take some time but should not freeze your personal computer.

**Possible interactions**: No interaction is expected at this stage.


```
python build_biogeography_geopolitical_units.py
```
```
python build_data_qdgc.py
```


### Step 7: Putting everything together


**What it does**: This final step builds the database by pulling the products of the two tracks together and running some final data consistency checks. The SQLite3 file generated can now be fed to _crestr_ to analyse fossil datasets and perform quantitative climate reconstructions.


**Possible interactions**: No interaction is expected at this stage.


```
python create_sqlite3.py
```


### Cleaning up

While the different scripts will partly tidy up the working environments, many files remain in the `./data`, `./tmp` and `./database_files` folders. If all scripts were run successfully and you do not intend to re-run some of them, all files in these folders can be deleted, except naturally for the database file `gbif4crest_03.sqlite3`, for which you work so hard. This manual cleaning should save you several GB of hard drive space.
