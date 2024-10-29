# gbif4crest - A relational database combining modern biological observations with climate

This data engineering pipeline accesses and curates observations from the [GBIF database](https://www.gbif.org) into a relational database. This standardised database contains calibration data necessary to run the CREST climate reconstruction method from fossil bioindicators and is compatible with the [crestr R package](https://www.manuelchevalier.com/crestr/index.html). This database combines information from various sources and results in a simple SQL database illustrated below. For an exhaustive description of the different tables, have a read [here](https://www.manuelchevalier.com/crestr/articles/calibration-data.html).

![The relationship diagram of the gbif4crest database](jungle/fig-gbiftables_v1.png?raw=true "The relationship diagram of the gbif4crest database")



## The Data Engineering Pipeline

This pipeline is composed of two data engineering tracks that eventually converge to create the database:
- **Track 1** (in blue): The geolocalised observations of different biological entities (e.g. plants, mammals) are pulled from the [GBIF database](https://www.gbif.org), curated and homogenised. This track creates the TAXA, DISTRIB, and DISTRIB_QDGC tables from the above SQL diagram.
- **Track 2** (in green): The biophysical and biogeographical variables used to characterised the biological observations are downloaded, processed, and exported at the required spatial resolution. This track creates the DATA_QDGC table from the above SQL diagram.

The two tracks ar eventually merged to create the gbif4crest_03 database. It is exported as a SQLite3 file and it is, therefore, directly compatible with the [crestr R package](https://www.manuelchevalier.com/crestr/index.html).

![The data analysis pipeline of this project](jungle/pipeline_diagram/pipeline_diagram.001.png?raw=true "The data analysis pipeline of this project")


## How to use

To safely use the different scripts available in this repo, I recommend you type the different commands presented below.


### Creating a virtual environment

Although optional, the use of `venv` (i.e. _virtual environments_) is highly recommended to guarantee that this program does not interfere with your local system. Everything you install and delete from a venv does not impact your machine.


```
    cd /path/to/folder
    git clone https://github.com/mchevalier2/gbif4crest.git
    cd gbif4crest
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
```


Alternatively, you can download the followin packages (and their dependencies) manually:  `duckdb`, `netCDF4`, `numpy`, `osgeo`, `pandas`, `pycountry`, `pycountry_convert`, `pygbif`, `requests`, `scipy`, `sqlite3`


### Step 1: Extracting data from the GBIF API

**What it does**: Queries the GBIF-API to access all the modern observations of the biological taxa of interest. Only observations with a taxonomical resolution at the species level are kept.

**Mandatory interaction**: To download these datasets yourself, you need to have a valid gbif account and to add your credentials to a file called "gbif_pwd.txt". This 3-liner file must contain first your gbif login name, then your gbif password, and finally a valid email address where you'll receive notifications about your data downloads. An example is provided on the figure below -- please note the empty line at the end of the file.

![Illustration of the gbif_pwd.txt file](jungle/gbif_pwd.txt.png?raw=true "Illustration of the gbif_pwd.txt file")


**Possible interactions**:
1. Users can modify the list of taxa being extracted by appending or removing classes of organisms from the Python list calle "LIST_OF_CLASSES". Only organism classes (_e.g._ mammalia) are accepted.

Once ready, you can start pulling data with the following command. If you are running this script with the default LIST_OF_CLASSES, it may take several days to compute.

```
python gbif_api_data_query.py
```



### Step 2: Processing the data as the TAXA and DISTRIB tables

**What it does**: Creates the TAXA and DISTRIB tables of the database. It will process all the 'taxalist_***.csv' files that were created at the previous step to create the TAXA table. It will also download the raw observation data from GBIF (unless the archives are already available in the './tmp' folder and compile them in the DISTRIB table.

**Possible interactions**:
1. You can fasten the process by downloading the observation data directly from the GBIF website. Download the result of the different queries and place the archives in the './tmp/' folder without changing its name.
2. If you modified the LIST_OF_CLASSES at the previous step, you will want to check that the IDing of your data remains operational. To do so, look at the function `create_id_from_taxonomy()` and adapt as necessary. By default, the gbif4crest_03 database contains information for four main type of biological entities; values between 1,000,000 and 1,999,999 represent plants, between 4,000,000 and 4,999,999 foraminifers, etc. These indexes are used to connect different tables of the relational database (TAXA and DISTRIB/DISTRIB_QDGC).

```
python build_taxa_and_distrib.py
```

**PS**: While Step 2 is running, you can already jump to Step 4 to download the next batch of datasets to process. Don't forget to come back to Step 3 when Step 2 is done, though.


### Step 3: Building the DISTRIB_QDGC table

**What it does**: This script will post-process the DISTRIB table (long table), where the type of observations are mixed and their spatial resolution variable into a consolidated table at 1/12° spatial resolution (wide table). Depending on how many taxa you extracted from GBIF, this step might be a bit hard to digest on a personal computer. This process will a lot of computer memory, so better to run the script when you do not need to use it (meal, over night, meeting, etc.).


**Possible interactions**: No interaction expected at this stage.

```
python build_distrib_qdgc.py
```

At this stage, you have completed the first track: Al the biological observations have been downloaded, curated, and post-processed. The next steps will involve a similar processing of the biophysical and biogeographical datasets.


### Step 4: Downloading and processing the necessary biogeographical datasets

Manually download a series of datasets to process and to save them in the `./tmp` folder created at Step 1.

This step is not automatised because some websites block such automatic downloads or require a registration.

1. Elevation data: https://www.ngdc.noaa.gov/mgg/global/relief/ETOPO1/data/ice_surface/grid_registered/georeferenced_tiff/ETOPO1_Ice_g_geotiff.zip
2. Terrestrial climate data:
    - The WorldClim v2 data: https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_30s_bio.zip
    - The Aridity Index v3 data: https://figshare.com/ndownloader/files/34377245
3. Marine climate data:
    - Long-term monthly mean sea ice concentration data: https://www.psl.noaa.gov/data/gridded/data.noaa.oisst.v2.highres.html
    - Marine variables from the World Ocean atlas 2018
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


**What it does**: Tasterizes (i.e. grids) all the datasets downloaded as shapefile (vector format) at the predefined spatial resolution of 1/12°.


**Possible interactions**: No interaction expected at this stage.

```
python rasterize_shapefile.py
```



### Step 6: Building the data_qdgc table


**What it does**: This script will process different geophysical of biogeographical datasets to characterise to each cell of our gridded world (1/12° resolution, corresponding to 9+M distinct grid cells). The first script creates two SQL tables (BIOGEOGRAPHY and GEOPOLITICAL_UNITS, not shown on the database diagram above) that optimises the information contained in the DISTRIB_QDGC table built in the next step. In details, this means that biomes and country information will be represented as unique idenfiers while the "real" information is preserved in the two tables created here. The second script will build the DISTRIB_QDGC table.

NOTE: This step will take some time but should not freeze your personal computer.

**Possible interactions**: No interaction expected at this stage.


```
python build_biogeography_geopolitical_units.py
```
```
python build_data_qdgc.py
```


### Step 7: Putting everything together



### Cleaning up
