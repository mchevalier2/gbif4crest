""" This script rasterizes all the spatial datasets needed by data_qdgc """

import os

import geopandas as gpd
import pandas as pd
import pyogrio
import rasterio
from rasterio import features
from tifffile import imwrite

DATA_FOLDER = "./data/"
TEMP_FOLDER = "./tmp/"
DATABASE_FOLDER = "./database_files/"


out_shape = (21600, 43200)


print("\n\n>> Preparing realm data from ocean dataset")
try:
    os.system(
        f"unzip -o {TEMP_FOLDER}MarineRealmsShapeFile -d {TEMP_FOLDER}MarineRealms"
    )
    dat = gpd.read_file(f"{TEMP_FOLDER}MarineRealms/MarineRealms.shp")
    shapes = dat[["geometry", "Realm"]].values.tolist()
    road_zones_arr = rasterio.features.rasterize(
        shapes,
        fill=-1,
        out_shape=out_shape,
        transform=rasterio.transform.from_bounds(
            -180, -90, 180, 90, out_shape[1], out_shape[0]
        ),
    )
    imwrite(TEMP_FOLDER + "dat_realms.tif", road_zones_arr)
    os.system(f"rm -Rf {TEMP_FOLDER}MarineRealms")
except pyogrio.errors.DataSourceError as e:
    print(
        "> Download ocean biomes: https://auckland.figshare.com/articles/"
        + "dataset/GIS_shape_files_of_realm_maps/5596840?file=9737926"
    )


print("\n\n>> Preparing ocean name data from ocean dataset")
try:
    os.system(f"unzip -o {TEMP_FOLDER}iho -d {TEMP_FOLDER}iho")
    dat = gpd.read_file(f"{TEMP_FOLDER}iho/iho.shp")
    shapes = dat[["geometry", "mrgid"]].values.tolist()
    road_zones_arr = rasterio.features.rasterize(
        shapes,
        fill=-1,
        out_shape=out_shape,
        transform=rasterio.transform.from_bounds(
            -180, -90, 180, 90, out_shape[1], out_shape[0]
        ),
    )
    imwrite(TEMP_FOLDER + "dat.tif", road_zones_arr)
    pd.DataFrame(dat[["name", "mrgid"]]).to_csv(
        TEMP_FOLDER + "ocean_names_to_IDs.csv", index=False
    )
    os.system(f"rm -Rf {TEMP_FOLDER}iho")
except pyogrio.errors.DataSourceError as e:
    print(
        "> Download ocean borders: http://geo.vliz.be:80/geoserver/wfs?request="
        + "getfeature&service=wfs&version=1.0.0&typename=MarineRegions:"
        + "iho&outputformat=SHAPE-ZIP"
    )


print("\n\n>> Preparing country data from Natural Earth dataset")
try:
    os.system(
        f"unzip -o {TEMP_FOLDER}ne_10m_admin_0_countries_lakes -d {TEMP_FOLDER}NE_countries"
    )
    dat = gpd.read_file(f"{TEMP_FOLDER}NE_countries/ne_10m_admin_0_countries_lakes.shp")
    shapes = dat[["geometry", "NE_ID"]].values.tolist()
    road_zones_arr = rasterio.features.rasterize(
        shapes,
        fill=-1,
        out_shape=out_shape,
        transform=rasterio.transform.from_bounds(
            -180, -90, 180, 90, out_shape[1], out_shape[0]
        ),
    )
    imwrite(TEMP_FOLDER + "countries.tif", road_zones_arr)
    pd.DataFrame(dat[["NAME", "NE_ID"]]).to_csv(
        TEMP_FOLDER + "country_names_to_IDs.csv", index=False
    )
    os.system(f"rm -Rf {TEMP_FOLDER}NE_countries")
except pyogrio.errors.DataSourceError as e:
    print(
        "> Download country borders: https://www.naturalearthdata.com/http//www."
        + "naturalearthdata.com/download/10m/cultural/ne_10m_admin_0_countries.zip"
    )


print("\n\n>> Preparing vegetation data from WWF biome dataset")
try:
    os.system(f"unzip -o {TEMP_FOLDER}6kcchn7e3u_official_teow.zip -d {TEMP_FOLDER}biomes_wwf")
    dat = gpd.read_file(TEMP_FOLDER + "biomes_wwf/official/wwf_terr_ecos.shp")
    shapes = dat[["geometry", "BIOME"]].values.tolist()
    road_zones_arr = rasterio.features.rasterize(
        shapes,
        fill=-1,
        out_shape=out_shape,
        transform=rasterio.transform.from_bounds(
            -180, -90, 180, 90, out_shape[1], out_shape[0]
        ),
    )
    imwrite(TEMP_FOLDER + "BIOMES.tif", road_zones_arr)
    shapes = dat[["geometry", "ECO_ID"]].values.tolist()
    road_zones_arr = rasterio.features.rasterize(
        shapes,
        fill=-1,
        out_shape=out_shape,
        transform=rasterio.transform.from_bounds(
            -180, -90, 180, 90, out_shape[1], out_shape[0]
        ),
    )
    imwrite(TEMP_FOLDER + "ECOREGIONS.tif", road_zones_arr)
    pd.DataFrame(dat[["ECO_ID", "ECO_NAME", "REALM", "BIOME"]]).to_csv(
        TEMP_FOLDER + "ecoregions.csv", index=False
    )
    os.system(f"rm -Rf {TEMP_FOLDER}biomes_wwf")
except pyogrio.errors.DataSourceError as e:
    print(
        "> Download terrestrial biomes: https://files.worldwildlife.org/"
        + "wwfcmsprod/files/Publication/file/6kcchn7e3u_official_teow.zip"
    )

##-;

""" Accepted pylint errors *************
    133:0: W0105: String statement has no effect (pointless-string-statement)
    9:0: W0611: Unused features imported from rasterio (unused-import)
"""
