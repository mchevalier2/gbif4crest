


import os

import numpy as np
import pandas as pd
import pycountry
import pycountry_convert as pc


DATA_FOLDER = "./data/"
TEMP_FOLDER = "./tmp/"
DATABASE_FOLDER = "./database_files/"





REALMS={}
REALMS['IM']="Indomalayan"
REALMS['NA']="Nearctic"
REALMS['AT']="Africotropical"
REALMS['AN']="Antarctic"
REALMS['AA']="Australian"
REALMS['OC']="Oceanian"
REALMS['NT']="Neotropical"
REALMS['PA']="Palaearctic"
REALMS['NULL']=""



BIOMES={}
BIOMES[1]="Tropical and subtropical moist broadleaf forests"
BIOMES[2]="Tropical and subtropical dry broadleaf forests"
BIOMES[3]="Tropical and suptropical coniferous forests"
BIOMES[4]="Temperate broadleaf and mixed forests"
BIOMES[5]="Temperate Coniferous Forest"
BIOMES[6]="Boreal forests / Taiga"
BIOMES[7]="Tropical and subtropical grasslands, savannas and shrublands"
BIOMES[8]="Temperate grasslands, savannas and shrublands"
BIOMES[9]="Flooded grasslands and savannas"
BIOMES[10]="Montane grasslands and shrublands"
BIOMES[11]="Tundra"
BIOMES[12]="Mediterranean Forests, woodlands and scrubs"
BIOMES[13]="Deserts and xeric shrublands"
BIOMES[14]="Mangroves"
BIOMES[98]="Lakes"
BIOMES[99]="Rock and Ice"




ECOREGIONS={}
eco=pd.read_csv(f'{TEMP_FOLDER}ecoregions.csv').drop_duplicates()
eco['ECO_ID'] = eco['ECO_ID'].astype(int)
eco['BIOME'] = eco['BIOME'].astype(int)
eco = eco.fillna("NULL")
for lab, row in eco.iterrows():
    if row['ECO_ID'] == -9999:
        ECOREGIONS[row['ECO_ID']+100000] = [REALMS['NULL'], REALMS['NULL'], BIOMES[99]]
    else:
        ECOREGIONS[row['ECO_ID']+100000] = [REALMS[row["REALM"]], BIOMES[row["BIOME"]], row["ECO_NAME"]]





eco=pd.read_csv('./sides/DO NOT DELETE ocean_realms_to_IDs.csv')
for lab, row in eco.iterrows():
    ECOREGIONS[row['ID']] = [np.nan, np.nan, row["Realm"]]





eco = pd.DataFrame.from_dict(ECOREGIONS).transpose()
eco.columns = ['realm', 'biome', 'ecoregion']
eco.index.name = 'ecoID'
eco = eco.sort_index()
print(eco)
eco.to_csv(DATABASE_FOLDER+'biogeography.csv', encoding='utf-8-sig')













CONTINENTS = dict()
CONTINENTS['AF'] = "Africa"
CONTINENTS['NA'] = "North America"
CONTINENTS['SA'] = "South America"
CONTINENTS['OC'] = "Oceania"
CONTINENTS['AN'] = "Antarctica"
CONTINENTS['AS'] = "Asia"
CONTINENTS['EU'] = "Europe"

CONTINENTS['ATA'] = CONTINENTS['AN']
CONTINENTS['ATF'] = CONTINENTS['AN']
CONTINENTS['ESH'] = CONTINENTS['AF']
CONTINENTS['PCN'] = CONTINENTS['OC']
CONTINENTS['SXM'] = CONTINENTS['SA']
CONTINENTS['TLS'] = CONTINENTS['AS']
CONTINENTS['UMI'] = CONTINENTS['OC']
CONTINENTS['VAT'] = CONTINENTS['EU']


countries = [x for x in pycountry.countries]
geopolitical_units = list()
for c in countries:
    country_code = c.alpha_3
    try:
        continent_name = CONTINENTS[pc.country_alpha2_to_continent_code(c.alpha_2)]
    except KeyError:
        continent_name = CONTINENTS[c.alpha_3]
    if ('official_name' in dir(c)):
        official_name = c.official_name
    else:
        official_name = 'NULL'
    geopolitical_units.append([int(c.numeric), continent_name, np.nan, c.name, official_name, country_code])


geopolitical_units.append([1000, 'Europe', 'Kosovo', 'Republic of Kosovo', 'XXK', np.nan])
geopolitical_units.append([1001, 'Oceania', 'Indian Ocean Territories', 'Indian Ocean Territories', np.nan, np.nan])
geopolitical_units.append([1002, 'Oceania', 'Clipperton Island', 'Clipperton Island', np.nan, np.nan])
geopolitical_units.append([1003, 'Asia', 'Spratly Islands', 'Spratly Islands', np.nan, np.nan])
geopolitical_units.append([1004, 'South America', 'Saint Martin', 'Saint Martin', np.nan, np.nan])
geopolitical_units.append([1005, 'Europe', 'Cyprus U.N. Buffer Zone', 'Cyprus U.N. Buffer Zone', np.nan, np.nan])



count=pd.read_csv('sides/DO NOT DELETE ocean_names_to_IDs .csv')
for idx, row in count.iterrows():
    geopolitical_units.append([row['ID'], np.nan, row['ocean'], row['country'], np.nan, np.nan])






geopolitical_units = pd.DataFrame(geopolitical_units, columns=['geopoID', 'continent', 'basin', 'name', 'official_name', 'countrycode'])


geopolitical_units.loc[geopolitical_units['name'] == 'Bolivia, Plurinational State of', 'name'] = 'Bolivia'
geopolitical_units.loc[geopolitical_units['name'] == 'Micronesia, Federated States of', 'name'] = 'Micronesia'
geopolitical_units.loc[geopolitical_units['name'] == 'Iran, Islamic Republic of', 'name'] = 'Iran'
geopolitical_units.loc[geopolitical_units['name'] == 'Moldova, Republic of', 'name'] = 'Moldova'
geopolitical_units.loc[geopolitical_units['name'] == 'Palestine, State of', 'name'] = 'Palestine'
geopolitical_units.loc[geopolitical_units['name'] == 'Taiwan, Province of China', 'name'] = 'Taiwan'
geopolitical_units.loc[geopolitical_units['name'] == 'Tanzania, United Republic of', 'name'] = 'Tanzania'
geopolitical_units.loc[geopolitical_units['name'] == 'Venezuela, Bolivarian Republic of', 'name'] = 'Venezuela'
geopolitical_units.loc[geopolitical_units['name'] == 'North Macedonia', 'name'] = 'Macedonia'
geopolitical_units.loc[geopolitical_units['name'] == 'Viet Nam', 'name'] = 'Vietnam'
geopolitical_units.loc[geopolitical_units['name'] == 'Eswatini', 'name'] = 'eSwatini'


geopolitical_units.loc[geopolitical_units['name'] == 'Russian Federation', ['name', 'official_name']] = ['Russia', 'Russian Federation']
geopolitical_units.loc[geopolitical_units['name'] == 'Congo, The Democratic Republic of the', ['name', 'official_name']] = ['DRC', 'The Democratic Republic of the Congo']
geopolitical_units.loc[geopolitical_units['name'] == 'Lao People s Democratic Republic', ['name', 'official_name']] = ['Laos', 'Lao People s Democratic Republic']
geopolitical_units.loc[geopolitical_units['name'] == 'Korea, Democratic People s Republic of', ['name', 'official_name']] = ['North Korea', 'Democratic People s Republic of Korea']
geopolitical_units.loc[geopolitical_units['name'] == 'Korea, Republic of', ['name', 'official_name']] = ['South Korea', 'Republic of Korea']
geopolitical_units.loc[geopolitical_units['name'] == 'Syrian Arab Republic', ['name', 'official_name']] = ['Syria', 'RSyrian Arab Republic']
geopolitical_units.loc[geopolitical_units['name'] == 'Saint Helena, Ascension and Tristan da Cunha', ['name', 'official_name']] = ['Saint Helena', 'Saint Helena, Ascension and Tristan da Cunha']
geopolitical_units.loc[geopolitical_units['name'] == 'Pitcairn', ['name', 'official_name']] = ['Pitcairn Island', 'Pitcairn Island']



print(geopolitical_units)
geopolitical_units.to_csv(DATABASE_FOLDER+"geopolitical_units.csv", encoding='utf-8-sig', index=False)








##-;
