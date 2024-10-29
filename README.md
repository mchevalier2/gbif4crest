# gbif4crest - A relational database combining modern biological observations with climate

This data engineering pipeline accesses and curates observations from the [GBIF database](https://www.gbif.org) into a relational database. This standardised database contains calibration data necessary to run the CREST climate reconstruction method from fossil bioindicators and is compatible with the [crestr R package](https://www.manuelchevalier.com/crestr/index.html). This database combines information from various sources and results in a simple SQL database illustrated below. For an exhaustive description of the different tables, have a read [here](https://www.manuelchevalier.com/crestr/articles/calibration-data.html).

![The relationship diagram of the gbif4crest database](jungle/fig-gbiftables_v1.png?raw=true "The relationship diagram of the gbif4crest database")



## The Data Engineering Pipeline

![The data analysis pipeline of this project](jungle/pipeline_diagram/pipeline_diagram.001.png?raw=true "The data analysis pipeline of this project")


## How to use

To safely use the different scripts available in this repo, I recommend you type the different commands presented below.


### Creating a virtual environment

The use of `venv` (i.e. _virtual environments_) is key to guarantee that this program does not interfere with your local system. Everything you install and delete from a venv does not impact your machine.


```
    cd /path/to/folder
    git clone https://github.com/mchevalier2/gbif4crest.git
    cd gbif4crest
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
```


Alternatively, you can download the followin packages (and their dependencies) manually:  `duckdb`, `netCDF4`, `numpy`, `osgeo`, `pandas`, `pycountry`, `pycountry_convert`, `pygbif`, `requests`, `scipy`, `sqlite3`


### Extracting data from the GBIF API

Things to mention:
  - the gbif_pwd.txt file
  - The duration


### Processing the data as the taxa and distrib tables



### Building distrib_qdgc
this step is demanding in terms of memory for the machine.



### Downloading and processing the necessary biogeographical datasets



### Build the biogeography and geopolitical tables



### Building the data_qdgc table



### Putting everything together



### Cleaning up
