
import os
import glob
import numpy as np
import pandas as pd
from osgeo import gdal_array
import netCDF4 as nc
import duckdb
from scipy.stats import mstats


DATA_FOLDER = "./data/"
TEMP_FOLDER = "./tmp/"
DATABASE_FOLDER = "./database_files/"


def f_locid(x: []) -> int:
    """Creates a unique idenfied for a set of coordinates"""
    x0, x1 = x
    if x0 >= 180:
        x0 = 180 - 1.0 / 24.0
    if x1 >= 90:
        x1 = 90 - 1.0 / 24.0
    return int((180 + x0) // (1.0 / 12.0)) + 360 * 12 * int((90 + x1) // (1.0 / 12.0))


def rebin_mean(a, shape):
    sh = shape[0],a.shape[0]//shape[0],shape[1],a.shape[1]//shape[1]
    return a.reshape(sh).mean(-1).mean(1)


def rebin_max(a, shape):
    sh = shape[0],a.shape[0]//shape[0],shape[1],a.shape[1]//shape[1]
    return a.reshape(sh).max(-1).max(1)


def rebin_mode(a, shape):
    sh = shape[0],a.shape[0]//shape[0],shape[1],a.shape[1]//shape[1]
    a=a.reshape(sh)
    a=np.transpose(a, (0,2,1,3))
    a=a.reshape((sh[0], sh[2], sh[1]*sh[3]))
    return mstats.mode(a, axis=2)[0][:,:,0]


def most_common(lst):
    return max(set(lst), key=lst.count)


def get_neighbours(loc):
    return [x for x in [loc+1, loc-1, loc+4320, loc+4320+1, loc+4320-1, loc-4320, loc-4320-1, loc-4320+1] if (x >= 0 and x <= 9331199) ]



LATS=pd.DataFrame((np.arange(2160)/12-90+1./24))
LONGS=pd.DataFrame((np.arange(4320)/12-180+1./24))

data_qdgc = LONGS.merge(LATS, how='cross')
data_qdgc.columns=['longitude', 'latitude']
data_qdgc['locID'] = data_qdgc.apply(lambda x: f_locid([x.longitude, x.latitude]), axis=1)
print(data_qdgc)


if True:
    print(' * Adding Elevation data\n')
    data_qdgc = data_qdgc[['locID', 'longitude', 'latitude']].sort_values('locID').reset_index()
    os.system(f"unzip -o {TEMP_FOLDER}ETOPO1_Ice_g_geotiff -d {TEMP_FOLDER}ETOPO1_Ice_g_geotiff")
    img = gdal_array.LoadFile(f'{TEMP_FOLDER}ETOPO1_Ice_g_geotiff/ETOPO1_Ice_g_geotiff.tif')
    lists = []
    for j in range(4320):
        for i in range(2160):
            ii = i*5
            jj = j*5
            dat = img[(ii):(ii+5), (jj):(jj+5)]
            lists.append([j + (2160-i-1)*4320, np.nanmean(dat),
                 np.nanmin(dat),
                 np.nanmax(dat)])
    df = pd.DataFrame(lists, columns=['locID', 'elevation', 'elev_min', 'elev_max']).sort_values('locID').reset_index()
    data_qdgc['elevation'] = df['elevation']
    data_qdgc['elev_min'] = df['elev_min']
    data_qdgc['elev_max'] = df['elev_max']
    data_qdgc['elev_range'] = df['elev_max'] - df['elev_min']
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)
    os.system(f"rm -Rf {TEMP_FOLDER}ETOPO1_Ice_g_geotiff")



if True:
    print(' * Adding Worldclim data\n')
    os.system(f"unzip -o {TEMP_FOLDER}wc2.1_30s_bio -d {TEMP_FOLDER}wc2.1_30s_bio")
    for biovar in range(1, 20):
        print("biovar", biovar)
        img = gdal_array.LoadFile(f'{TEMP_FOLDER}wc2.1_30s_bio/wc2.1_30s_bio_{biovar}.tif')
        img_array = np.flip(np.array(img), axis=0)
        mask_wc = (img_array < -100000000)
        img_array_masked = np.ma.MaskedArray(img_array, mask=mask_wc)
        ds = rebin_mean(img_array_masked, (2160, 4320)).filled(np.nan)
        data_qdgc[f'bio{biovar}'] = ds.flatten(order='F')
    os.system(f"rm -Rf {TEMP_FOLDER}wc2.1_30s_bio")
    #
    # These two blocks should remain together because I am using
    # the mask from wc for AI
    print(' * Adding AIv3 data\n')
    os.system(f"unzip -o {TEMP_FOLDER}Global-AI_ET0_annual_v3 -d {TEMP_FOLDER}")
    img = gdal_array.LoadFile(f'{TEMP_FOLDER}Global-AI_ET0_v3_annual/ai_v3_yr.tif')
    img_array = np.flip(np.array(img), axis=0)
    img_array_masked = np.ma.MaskedArray(img_array, mask=mask_wc)
    ds = rebin_mean(img_array_masked, (2160, 4320)).filled(np.nan)
    data_qdgc['ai'] = ds.flatten(order='F') / 10000.
    os.system(f"rm -Rf {TEMP_FOLDER}Global-AI_ET0_v3_annual")



if True:
    print(" * Adding terr_ecoID to data_qdgc")
    ECOREGIONS={}
    eco=pd.read_csv(f'{TEMP_FOLDER}ecoregions.csv', dtype={'BIOME':'int', 'ECO_ID':'int'})
    for i, row in eco.iterrows():
        ECOREGIONS[row['ECO_ID']] = row['ECO_NAME']
    #
    biogeography=pd.read_csv(f'{DATABASE_FOLDER}biogeography.csv')[['ecoregion', 'ecoID']]
    ecoIDs = {}
    for i, row in biogeography.iterrows():
        ecoIDs[row['ecoregion']] = row['ecoID']
    #
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    img = gdal_array.LoadFile(f'{TEMP_FOLDER}BIOMES.tif')
    mask = (img < 0)
    ds_masked = np.ma.MaskedArray(img, mask=mask)
    ds = rebin_mode(ds_masked, (2160, 4320))
    #
    img = gdal_array.LoadFile(f'{TEMP_FOLDER}ECOREGIONS.tif')
    mask = (img < 0)
    ds_masked = np.ma.MaskedArray(img, mask=mask)
    ds2 = rebin_mode(ds_masked, (2160, 4320))
    #
    ll = list()
    for j in range(4320):
        for i in range(2160):
            biome = -1
            ecoregion = -1
            if ds[i,j] > 0:
                biome = ds[i,j]
            if ds2[i,j] > 0:
                ecoregion = ECOREGIONS[int(ds2[i,j])]
            if biome != -1:
                if ecoregion == -1:
                    ll.append([j + (2160-i-1)*4320, 90001])
                else:
                    ll.append([j + (2160-i-1)*4320, ecoIDs[ecoregion]])
            else:
                ll.append([j + (2160-i-1)*4320, np.nan])
    #
    df = pd.DataFrame(ll, columns=['locID', 'terr_ecoID']).sort_values('locID').reset_index()
    data_qdgc['terr_ecoID'] = df['terr_ecoID'].astype("Int64")
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)
    ##>os.system(f"rm {TEMP_FOLDER}biogeography.csv")
    ##>os.system(f"rm {TEMP_FOLDER}BIOMES.tif")
    ##>os.system(f"rm {TEMP_FOLDER}ECOREGIONS.tif")



if True:
    print(" * Adding countryID to data_qdgc")
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    #
    countries=pd.read_csv(f'{DATABASE_FOLDER}geopolitical_units.csv', encoding='utf-8-sig')[['name', 'geopoID', 'continent']]
    countries = duckdb.query("SELECT * FROM countries WHERE continent IS NOT NULL").df()
    countryIDs = {}
    for i, row in countries.iterrows():
        countryIDs[row['name']] = row['geopoID']
    #
    countryIDs['S. Geo. and the Is.'] = countryIDs['South Georgia and the South Sandwich Islands']
    countryIDs['Heard I. and McDonald Is.'] = countryIDs['Heard Island and McDonald Islands']
    countryIDs['Falkland Is.'] = countryIDs['Falkland Islands (Malvinas)']
    countryIDs['Fr. S. Antarctic Lands'] = countryIDs['French Southern Territories']
    countryIDs['Fr. Polynesia'] = countryIDs['French Polynesia']
    countryIDs['Dem. Rep. Congo'] = countryIDs['DRC']
    countryIDs['Solomon Is.'] = countryIDs['Solomon Islands']
    countryIDs['Eq. Guinea'] = countryIDs['Equatorial Guinea']
    countryIDs['São Tomé and Principe'] = countryIDs['Sao Tome and Principe']
    countryIDs['Central African Rep.'] = countryIDs['Central African Republic']
    countryIDs['S. Sudan'] = countryIDs['South Sudan']
    countryIDs['Brunei'] = countryIDs['Brunei Darussalam']
    countryIDs['Somaliland'] = countryIDs['Somalia']
    countryIDs['N. Cyprus'] = countryIDs['Cyprus']
    countryIDs['United States of America'] = countryIDs['United States']
    countryIDs['St. Pierre and Miquelon'] = countryIDs['Saint Pierre and Miquelon']
    countryIDs['W. Sahara'] = countryIDs['Western Sahara']
    countryIDs['Faeroe Is.'] = countryIDs['Faroe Islands']
    countryIDs['Dominican Rep.'] = countryIDs['Dominican Republic']
    countryIDs['N. Mariana Is.'] = countryIDs['Northern Mariana Islands']
    countryIDs['Bosnia and Herz.'] = countryIDs['Bosnia and Herzegovina']
    countryIDs['Siachen Glacier'] = countryIDs['India']
    countryIDs['Baikonur'] = countryIDs['Kazakhstan']
    countryIDs['Åland'] = countryIDs['Åland Islands']
    countryIDs['Pitcairn Is.'] = countryIDs['Pitcairn Island']
    countryIDs['Cook Is.'] = countryIDs['Cook Islands']
    countryIDs['Wallis and Futuna Is.'] = countryIDs['Wallis and Futuna']
    countryIDs['Ashmore and Cartier Is.'] = countryIDs['Australia']
    countryIDs['Indian Ocean Ter.'] = countryIDs['Indian Ocean Territories']
    countryIDs['Br. Indian Ocean Ter.'] = countryIDs['British Indian Ocean Territory']
    countryIDs['U.S. Minor Outlying Is.'] = countryIDs['United States Minor Outlying Islands']
    countryIDs['Marshall Is.'] = countryIDs['Marshall Islands']
    countryIDs['Clipperton I.'] = countryIDs['Clipperton Island']
    countryIDs['Spratly Is.'] = countryIDs['Spratly Islands']
    countryIDs['St. Vin. and Gren.'] = countryIDs['Saint Vincent and the Grenadines']
    countryIDs['Antigua and Barb.'] = countryIDs['Antigua and Barbuda']
    countryIDs['St. Kitts and Nevis'] = countryIDs['Saint Kitts and Nevis']
    countryIDs['U.S. Virgin Is.'] = countryIDs['Virgin Islands, U.S.']
    countryIDs['British Virgin Is.'] = countryIDs['Virgin Islands, British']
    countryIDs['St-Barthélemy'] = countryIDs['Saint Barthélemy']
    countryIDs['Sint Maarten'] = countryIDs['Sint Maarten (Dutch part)']
    countryIDs['Cayman Is.'] = countryIDs['Cayman Islands']
    countryIDs['Turks and Caicos Is.'] = countryIDs['Turks and Caicos Islands']
    countryIDs['Dhekelia'] = countryIDs['Cyprus']
    countryIDs['Akrotiri'] = countryIDs['Cyprus']
    countryIDs['USNB Guantanamo Bay'] = countryIDs['Cuba']
    countryIDs['St-Martin'] = countryIDs['Saint Martin']
    countryIDs['North Korea'] = countryIDs["Korea, Democratic People's Republic of"]
    countryIDs['Turkey'] = countryIDs["Türkiye"]
    countryIDs['Laos'] = countryIDs["Lao People's Democratic Republic"]
    countryIDs['Kosovo'] = countryIDs["Republic of Kosovo"]
    #
    img = gdal_array.LoadFile(f'{TEMP_FOLDER}countries.tif')
    mask = (img < 0)
    ds_masked = np.ma.MaskedArray(img, mask=mask)
    ds = rebin_mode(ds_masked, (2160, 4320)).filled(-1)
    #
    COUNTRIES={}
    count=pd.read_csv(f'{TEMP_FOLDER}country_names_to_IDs.csv')
    for i, row in count.iterrows():
        COUNTRIES[row['NE_ID']] = row['NAME']
    #
    ll = list()
    missing_countries = list()
    for j in range(4320):
        for i in range(2160):
            if int(ds[i,j]) in COUNTRIES.keys():
                try:
                    ll.append([j + (2160-i-1)*4320, countryIDs[COUNTRIES[ds[i,j]]]])
                except KeyError:
                    ll.append([j + (2160-i-1)*4320, np.nan])
                    if COUNTRIES[ds[i,j]] not in ['Serranilla Bank']:
                        missing_countries.append(COUNTRIES[ds[i,j]])
            else:
                ll.append([j + (2160-i-1)*4320, np.nan])
    #
    df = pd.DataFrame(ll, columns=['locID', 'countryID']).sort_values('locID').reset_index()
    data_qdgc['countryID'] = df['countryID'].astype("Int64")
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)
    print("missing countries:", list(set(missing_countries)))
    ##>os.system(f"rm {TEMP_FOLDER}country_names_to_IDs.csv")
    ##>os.system(f"rm {TEMP_FOLDER}countries.tif")



if True:
    print(" * Fixing missing countryID relative to Worldclim")
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    # Get countryIDs for all cells where we have terrestrial climate VALUES
    locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE bio1 IS NOT NULL AND countryID IS NULL').df()['locID']]
    cnt = run = 0
    while (len(locids) > 0 and len(locids) != cnt) and (run <= 40):
        cnt = 0
        locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE bio1 IS NOT NULL AND countryID IS NULL').df()['locID']]
        res = list()
        print('Run: %d. Cells to classify: %d'%(run+1, len(locids)))
        for loc in locids:
            x = data_qdgc.loc[get_neighbours(loc), ['countryID']].dropna()
            if len(x) > 0:
                res.append([loc, most_common(x['countryID'].to_list())])
            else:
                cnt += 1
        run += 1
        if len(res) > 0:
            res = pd.DataFrame(res).set_index(0)
            data_qdgc.loc[res.index, 'countryID'] = res[1].astype(int)
    #
    locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE bio1 IS NOT NULL AND countryID IS NULL').df()['locID']]
    print('Run: %d. Cells to classify: %d'%(run, len(locids)))
    #
    #
    #
    print(" * Fixing missing countryID relative to Worldclim with broader neighbours")
    locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE bio1 IS NOT NULL AND countryID IS NULL').df()['locID']]
    cnt = run = 1
    while len(locids) > 0 and cnt > 0:
        locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE bio1 IS NOT NULL AND countryID IS NULL').df()['locID']]
        print('Run: %d. Cells to classify: %d'%(run, len(locids)))
        cnt = 0
        res = list()
        for loc in locids:
            neighbours = get_neighbours(loc)
            for i in range(1, run):
                l = []
                for n in neighbours:
                    l.extend(get_neighbours(n))
                neighbours = list(set(neighbours + l))
            x = data_qdgc.loc[neighbours, ['countryID']].dropna()
            if len(x) > 0:
                res.append([loc, most_common(x['countryID'].to_list())])
            else:
                cnt += 1
        run += 1
        if len(res) > 0:
            res = pd.DataFrame(res).set_index(0)
            data_qdgc.loc[res.index, 'countryID'] = res[1].astype('Int64')
    #
    locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE bio1 IS NOT NULL AND countryID IS NULL').df()['locID']]
    print('Run: %d. Remaining cells to classify: %d'%(run+1, len(locids)))
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)



if True:
    print(" * Fixing missing terr_ecoID relative to Worldclim")
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    # Get countryIDs for all cells where we have terrestrial climate VALUES
    locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE bio1 IS NOT NULL AND terr_ecoID IS NULL').df()['locID']]
    cnt = run = 0
    while (len(locids) > 0 and len(locids) != cnt) and (run <= 40):
        cnt = 0
        locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE bio1 IS NOT NULL AND terr_ecoID IS NULL').df()['locID']]
        res = list()
        print('Run: %d. Cells to classify: %d'%(run+1, len(locids)))
        for loc in locids:
            x = data_qdgc.loc[get_neighbours(loc), ['terr_ecoID']].dropna()
            if len(x) > 0:
                res.append([loc, most_common(x['terr_ecoID'].to_list())])
            else:
                cnt += 1
        run += 1
        if len(res) > 0:
            res = pd.DataFrame(res).set_index(0)
            data_qdgc.loc[res.index, 'terr_ecoID'] = res[1].astype('Int64')
    #
    locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE bio1 IS NOT NULL AND terr_ecoID IS NULL').df()['locID']]
    print('Run: %d. Cells to classify: %d'%(run+1, len(locids)))
    #
    #
    #
    print(" * Fixing missing terr_ecoID relative to Worldclim with broader neighbours")
    locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE bio1 IS NOT NULL AND terr_ecoID IS NULL').df()['locID']]
    cnt = run = 1
    while len(locids) > 0 and cnt > 0:
        locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE bio1 IS NOT NULL AND terr_ecoID IS NULL').df()['locID']]
        print('Run: %d. Cells to classify: %d'%(run, len(locids)))
        cnt = 0
        res = list()
        for loc in locids:
            neighbours = get_neighbours(loc)
            for i in range(1, run):
                l = []
                for n in neighbours:
                    l.extend(get_neighbours(n))
                neighbours = list(set(neighbours + l))
            x = data_qdgc.loc[neighbours, ['terr_ecoID']].dropna()
            if len(x) > 0:
                res.append([loc, most_common(x['terr_ecoID'].to_list())])
            else:
                cnt += 1
        run += 1
        if len(res) > 0:
            res = pd.DataFrame(res).set_index(0)
            data_qdgc.loc[res.index, 'terr_ecoID'] = res[1].astype('Int64')
    #
    locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE bio1 IS NOT NULL AND terr_ecoID IS NULL').df()['locID']]
    print('Run: %d. Remaining cells to classify: %d'%(run+1, len(locids)))
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)



if True:
    print(' * Adding Sea Ice Concentration data\n')
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    season_names = ['icec_jfm', 'icec_amj', 'icec_jas', 'icec_ond']
    ds = nc.Dataset(f'{TEMP_FOLDER}icec.mon.ltm.1991-2020.nc')
    ds = ds['icec']
    ds = np.append(ds[:,:,720:], ds[:,:,0:720], axis=2)
    ds = np.flip(ds, axis=1)
    mask = (ds < -100000)
    ds[ds < -100000] = 0
    for season in range(4):
        print(season_names[season])
        ds_season = ds[(3*season):(3*(season+1)),:,].data.mean(axis=0)
        ll = list()
        for j in range(4320):
            for i in range(2160):
                if not mask[0, i//3, j//3]:
                    ll.append([j + (2160-i-1)*4320, ds_season[i//3, j//3]])
                else:
                    ll.append([j + (2160-i-1)*4320, np.nan])
        df = pd.DataFrame(ll, columns=['locID', season_names[season]]).sort_values('locID').reset_index()
        data_qdgc[season_names[season]] = df[season_names[season]]
    data_qdgc['icec_ann'] = data_qdgc[season_names].mean(axis=1)
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)



if True:
    print(' * Adding Temperature & Salinity data\n')
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    for var in ['sss', 'sst']:
        season_names = {'sss':['sss_ann', 'sss_jfm', 'sss_amj', 'sss_jas', 'sss_ond'],
                        'sst':['sst_ann', 'sst_jfm', 'sst_amj', 'sst_jas', 'sst_ond']}
        for season, file_name in enumerate(sorted(glob.glob(f'{TEMP_FOLDER}woa13_decav_{var[2]}*'))):
            print(season_names[var][season])
            ds = nc.Dataset(file_name)
            ds = ds[f'{var[2]}_an'][0,0,:,:].data
            ds = np.flip(ds, axis=0)
            ds[ds > 10000000] = np.nan
            ll = list()
            for j in range(4320):
                for i in range(2160):
                    ll.append([j + (2160-i-1)*4320, ds[i//3, j//3]])
            df = pd.DataFrame(ll, columns=['locID', season_names[var][season]]).sort_values('locID').reset_index()
            data_qdgc[season_names[var][season]] = df[season_names[var][season]]
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)



if True:
    print(' * Adding Oxy, nitrate, phosphate and silicate data\n')
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    var_names = ['diss_oxy', 'silicate', 'nitrate', 'phosphate']
    for var, file_name in enumerate(sorted(glob.glob(f'{TEMP_FOLDER}woa13_all*'))):
        print(var_names[var])
        ds = nc.Dataset(file_name)
        ds = ds[f'{file_name.split('_all_')[1][0]}_an'][0,0,:,:].data
        ds = np.flip(ds, axis=0)
        ds[ds > 10000000] = np.nan
        ll = list()
        for j in range(4320):
            for i in range(2160):
                ll.append([j + (2160-i-1)*4320, ds[i//12, j//12]])
        df = pd.DataFrame(ll, columns=['locID', var_names[var]]).sort_values('locID').reset_index()
        data_qdgc[var_names[var]] = df[var_names[var]]
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)



if True:
    print(" * Fixing WOA relative to nutrients")
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    #
    cnt = 1
    while cnt > 0:
        locIDs = duckdb.query('SELECT distinct locid FROM data_qdgc WHERE diss_oxy IS NOT NULL AND sss_ann IS NULL').df()['locID'].to_list()
        print(f"    {len(locIDs)} cells to fix")
        cnt = 0
        res = list()
        for i, loc in enumerate(locIDs):
            values = data_qdgc.loc[get_neighbours(loc), ['sst_ann', 'sst_jfm', 'sst_amj', 'sst_jas', 'sst_ond', 'sss_ann', 'sss_jfm', 'sss_amj', 'sss_jas', 'sss_ond']].dropna()
            if values.shape[0] > 0:
                res.append([loc] + values.mean(axis=0).to_list())
            else:
                cnt += 1
        if len(res) > 0:
            res = pd.DataFrame(res).set_index(0)
            res.columns = ['sst_ann', 'sst_jfm', 'sst_amj', 'sst_jas', 'sst_ond', 'sss_ann', 'sss_jfm', 'sss_amj', 'sss_jas', 'sss_ond']
            data_qdgc.loc[res.index, res.columns] = res
    #
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)



if True:
    print(" * Fixing nutrients relative to WOA")
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    #
    cnt = 1
    run = 0
    while cnt > 0 and run < 11:
        locIDs = duckdb.query('SELECT distinct locid FROM data_qdgc WHERE diss_oxy IS NULL AND sss_ann IS NOT NULL').df()['locID'].to_list()
        run += 1
        print(f"   Run {run}: {len(locIDs)} cells to fix")
        cnt = 0
        res = list()
        for i, loc in enumerate(locIDs):
            values = data_qdgc.loc[[loc+1, loc-1, loc+4320, loc+4320+1, loc+4320-1, loc-4320, loc-4320-1, loc-4320+1], ['diss_oxy', 'nitrate', 'phosphate', 'silicate']].dropna()
            if values.shape[0] > 0:
                res.append([loc] + values.mean(axis=0).to_list())
            else:
                cnt += 1
        if len(res) > 0:
            res = pd.DataFrame(res).set_index(0)
            res.columns = ['diss_oxy', 'nitrate', 'phosphate', 'silicate']
            data_qdgc.loc[res.index, res.columns] = res
    #
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)



## I decided not to fix this variable until I find a solution to import the data
## with all the necessary 0s. At the moment, mid and low latitudes are not
## different from the continents.
if False:
    print(" * Fixing sea ice concentration relative to WOA")
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    #
    cnt = 1
    while cnt > 0:
        locIDs = duckdb.query('SELECT distinct locid FROM data_qdgc WHERE icec_ann IS NULL AND sss_ann IS NOT NULL AND (latitude >=60 OR latitude <= -60)').df()['locID'].to_list()
        print(f"    {len(locIDs)} cells to fix")
        cnt = 0
        res = list()
        for i, loc in enumerate(locIDs):
            values = data_qdgc.loc[[loc+1, loc-1, loc+4320, loc+4320+1, loc+4320-1, loc-4320, loc-4320-1, loc-4320+1], ['icec_ann', 'icec_jfm', 'icec_amj', 'icec_jas', 'icec_ond']].dropna()
            if values.shape[0] > 0:
                res.append([loc] + values.mean(axis=0).to_list())
            else:
                cnt += 1
        if len(res) > 0:
            res = pd.DataFrame(res).set_index(0)
            res.columns = ['icec_ann', 'icec_jfm', 'icec_amj', 'icec_jas', 'icec_ond']
            data_qdgc.loc[res.index, res.columns] = res
    #
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)



if True:
    print(" * Adding oceanid to data_qdgc")
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    #
    basins=pd.read_csv(f'{DATABASE_FOLDER}geopolitical_units.csv', encoding='utf-8-sig')[['name', 'geopoID', 'basin']].dropna()
    basinIDs = {}
    for i, row in basins.iterrows():
        basinIDs[row['name']] = row['geopoID']
    #
    OCEANS={}
    count=pd.read_csv(TEMP_FOLDER+'ocean_names_to_IDs.csv')
    for i, row in count.iterrows():
        OCEANS[row['mrgid']] = row['name']
    #
    img = gdal_array.LoadFile(f'{TEMP_FOLDER}dat.tif')
    mask = (img < 0)
    ds_masked = np.ma.MaskedArray(img, mask=mask)
    ds = rebin_mode(ds_masked, (2160, 4320)).filled(-1)
    #
    ll = list()
    missing_oceans = list()
    for j in range(4320):
        for i in range(2160):
            if int(ds[i,j]) in OCEANS.keys():
                try:
                    ll.append([j + (2160-i-1)*4320, basinIDs[OCEANS[ds[i,j]]]])
                except KeyError:
                    ll.append([j + (2160-i-1)*4320, np.nan])
                    if OCEANS[ds[i,j]] not in ['Serranilla Bank']:
                        missing_oceans.append(OCEANS[ds[i,j]])
            else:
                ll.append([j + (2160-i-1)*4320, np.nan])
    #
    df = pd.DataFrame(ll, columns=['locID', 'oceanID']).sort_values('locID').reset_index()
    data_qdgc['oceanID'] = df['oceanID'].astype("Int64")
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)
    print("missing oceans:", missing_oceans)
    ##>os.system(f"rm {TEMP_FOLDER}ocean_names_to_IDs.csv")
    ##>os.system(f"rm {TEMP_FOLDER}dat.tif")



if True:
    print(" * Adding ocean realm ids to data_qdgc")
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    #
    OC_REALMS={}
    count=pd.read_csv('./sides/DO NOT DELETE ocean_realms_to_IDs.csv')
    for i, row in count.iterrows():
        OC_REALMS[row['ID']] = row['Realm']
    #
    img = gdal_array.LoadFile(f'{TEMP_FOLDER}dat_realms.tif')
    mask = (img < 0)
    ds_masked = np.ma.MaskedArray(img, mask=mask)
    ds = rebin_mode(ds_masked, (2160, 4320)).filled(-1)
    #
    ll = list()
    missing_oceans = list()
    for j in range(4320):
        for i in range(2160):
            if int(ds[i,j]) in OC_REALMS.keys():
                try:
                    ll.append([j + (2160-i-1)*4320, ds[i,j]])
                except KeyError:
                    ll.append([j + (2160-i-1)*4320, np.nan])
                    if OC_REALMS[ds[i,j]] not in ['Serranilla Bank']:
                        missing_oceans.append(OC_REALMS[ds[i,j]])
            else:
                ll.append([j + (2160-i-1)*4320, np.nan])
    #
    df = pd.DataFrame(ll, columns=['locID', 'mari_ecoID']).sort_values('locID').reset_index()
    data_qdgc['mari_ecoID'] = df['mari_ecoID'].astype("Int64")
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)
    print("missing ocean realms:", missing_oceans)
    ##>os.system(f"rm {TEMP_FOLDER}dat_realms.tif")



if True:
    print(" * Fixing missing mari_ecoID relative to World Ocean Atlas")
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    # Get countryIDs for all cells where we have terrestrial climate VALUES
    locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE sst_ann IS NOT NULL AND mari_ecoID IS NULL').df()['locID']]
    cnt = run = 0
    while (len(locids) > 0 and len(locids) != cnt) and (run <= 40):
        cnt = 0
        locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE sst_ann IS NOT NULL AND mari_ecoID IS NULL').df()['locID']]
        res = list()
        print('Run: %d. Cells to classify: %d'%(run+1, len(locids)))
        for loc in locids:
            x = data_qdgc.loc[get_neighbours(loc), ['mari_ecoID']].dropna()
            if len(x) > 0:
                res.append([loc, most_common(x['mari_ecoID'].to_list())])
            else:
                cnt += 1
        run += 1
        if len(res) > 0:
            res = pd.DataFrame(res).set_index(0)
            data_qdgc.loc[res.index, 'mari_ecoID'] = res[1].astype('Int64')
    #
    locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE sst_ann IS NOT NULL AND mari_ecoID IS NULL').df()['locID']]
    print('Run: %d. Cells to classify: %d'%(run+1, len(locids)))
    #
    #
    #
    if len(locids) > 0:
        print(" * Fixing missing mari_ecoID relative to World Ocean Atlas with broader neighbours")
        locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE sst_ann IS NOT NULL AND mari_ecoID IS NULL').df()['locID']]
        cnt = run = 1
        while len(locids) > 0 and cnt > 0 and run <= 15:
            locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE sst_ann IS NOT NULL AND mari_ecoID IS NULL').df()['locID']]
            print('Run: %d. Cells to classify: %d'%(run, len(locids)))
            cnt = 0
            res = list()
            for loc in locids:
                neighbours = get_neighbours(loc)
                for i in range(1, run):
                    l = []
                    for n in neighbours:
                        l.extend(get_neighbours(n))
                    neighbours = list(set(neighbours + l))
                x = data_qdgc.loc[neighbours, ['mari_ecoID']].dropna()
                if len(x) > 0:
                    res.append([loc, most_common(x['mari_ecoID'].to_list())])
                else:
                    cnt += 1
            run += 1
            if len(res) > 0:
                res = pd.DataFrame(res).set_index(0)
                data_qdgc.loc[res.index, 'mari_ecoID'] = res[1].astype('Int64')
        #
        locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE sst_ann IS NOT NULL AND mari_ecoID IS NULL').df()['locID']]
        print('Run: %d. Remaining cells to classify: %d'%(run+1, len(locids)))
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)



if True:
    print(" * Fixing missing oceanID relative to World Ocean Atlas")
    data_qdgc = data_qdgc.sort_values('locID').reset_index()
    # Get countryIDs for all cells where we have terrestrial climate VALUES
    locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE sst_ann IS NOT NULL AND oceanID IS NULL').df()['locID']]
    cnt = run = 0
    while (len(locids) > 0 and len(locids) != cnt) and (run <= 40):
        cnt = 0
        locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE sst_ann IS NOT NULL AND oceanID IS NULL').df()['locID']]
        res = list()
        print('Run: %d. Cells to classify: %d'%(run+1, len(locids)))
        for loc in locids:
            x = data_qdgc.loc[get_neighbours(loc), ['oceanID']].dropna()
            if len(x) > 0:
                res.append([loc, most_common(x['oceanID'].to_list())])
            else:
                cnt += 1
        run += 1
        if len(res) > 0:
            res = pd.DataFrame(res).set_index(0)
            data_qdgc.loc[res.index, 'oceanID'] = res[1].astype('Int64')
    #
    locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE sst_ann IS NOT NULL AND oceanID IS NULL').df()['locID']]
    print('Run: %d. Cells to classify: %d'%(run+1, len(locids)))
    #
    #
    #
    if len(locids) > 0:
        print(" * Fixing missing oceanID relative to World Ocean Atlas with broader neighbours")
        locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE sst_ann IS NOT NULL AND oceanID IS NULL').df()['locID']]
        cnt = run = 1
        while len(locids) > 0 and cnt > 0 and run <= 15:
            locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE sst_ann IS NOT NULL AND oceanID IS NULL').df()['locID']]
            print('Run: %d. Cells to classify: %d'%(run, len(locids)))
            cnt = 0
            res = list()
            for loc in locids:
                neighbours = get_neighbours(loc)
                for i in range(1, run):
                    l = []
                    for n in neighbours:
                        l.extend(get_neighbours(n))
                    neighbours = list(set(neighbours + l))
                x = data_qdgc.loc[neighbours, ['oceanID']].dropna()
                if len(x) > 0:
                    res.append([loc, most_common(x['oceanID'].to_list())])
                else:
                    cnt += 1
            run += 1
            if len(res) > 0:
                res = pd.DataFrame(res).set_index(0)
                data_qdgc.loc[res.index, 'oceanID'] = res[1].astype('Int64')
        #
        locids = [x for x in duckdb.execute('SELECT distinct locid FROM data_qdgc WHERE sst_ann IS NOT NULL AND oceanID IS NULL').df()['locID']]
        print('Run: %d. Remaining cells to classify: %d'%(run+1, len(locids)))
    data_qdgc.set_index('index', inplace=True)
    data_qdgc.sort_index(inplace=True)









def f1(x, y):
    if np.isnan(x):
        return np.nan
    elif np.isnan(y):
        return 1
    return 0



#data_qdgc['WOA_but_no_nutriens'] = data_qdgc.apply(lambda x: f1(x['icec_amj'], x['sss_ann']), axis=1)



#data_qdgc[np.array(data_qdgc['longitude']<-30)].to_csv(DATABASE_FOLDER+'data_qdgc.csv')
#data_qdgc[['longitude', 'latitude', 'mari_ecoID', 'oceanID']].to_csv(DATABASE_FOLDER+'data_qdgc.csv')
data_qdgc.to_csv(DATABASE_FOLDER+'data_qdgc.csv')
