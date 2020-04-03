import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import re
import datetime
from shapely import wkt
from geosupport import Geosupport
import sqlite3
import urllib.request
import shutil
import os


pd.set_option('display.max_columns', None)

path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2020/TLC2019/'
path='/home/mayijun/TLC2019/'



# Download data
dl=datetime.datetime(2020,4,4)
for i in range(0,300):
    dl=dl-datetime.timedelta(days=7)
    url='http://web.mta.info/developers/data/nyct/turnstile/turnstile_'+datetime.datetime.strftime(dl,'%y%m%d')+'.txt'
    req=urllib.request.urlopen(url)
    file = open(path+'DATA/'+datetime.datetime.strftime(dl,'%y%m%d')+'.txt', "wb")
    shutil.copyfileobj(req,file)
    file.close()
























loczone = pd.read_csv(path + 'taxi+_zone_lookup.csv',dtype=object)
loczone = loczone[['LocationID','Zone']]
loczone['LocationID'] = pd.to_numeric(loczone['LocationID'], errors='coerce')

bs = pd.read_csv(path + 'baselookup.csv',dtype=object)


conn = sqlite3.connect(path + 'fhv2018.sqlite3')

for i in range(1, 13):
    df = pd.read_csv(path + 'fhv_tripdata_2018-' + str(i).zfill(2) + '.csv', dtype=object)
    #check the uniqueness of the two duplicate column
    #np.unique(df['Dispatching_base_num'])
    
    df['dispbase'] = np.where(df['Dispatching_base_number'].str.strip().str.upper().isin(['NAN','|',',']),np.nan,df['Dispatching_base_number'].str.strip().str.upper())
    df = pd.merge(df, bs, how='left', left_on='dispbase', right_on='License Number')

    df['pudatetime'] = pd.to_datetime(df['Pickup_DateTime'].str.strip(), format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df['pudate'] = df['pudatetime'].dt.date
    df['pumonth'] = df['pudatetime'].dt.month
    df['puwkd'] = df['pudatetime'].dt.weekday_name
    df['puhour'] = df['pudatetime'].dt.hour
    df['pulocid'] = pd.to_numeric(df['PUlocationID'], errors='coerce')
    df['dodatetime'] = pd.to_datetime(df['DropOff_datetime'].str.strip(), format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df['dodate'] = df['dodatetime'].dt.date
    df['domonth'] = df['dodatetime'].dt.month
    df['dowkd'] = df['dodatetime'].dt.weekday_name
    df['dohour'] = df['dodatetime'].dt.hour
    df['dolocid'] = pd.to_numeric(df['DOlocationID'], errors='coerce')
    
    df = pd.merge(df, loczone, how='left', left_on='pulocid', right_on='LocationID')
    df = pd.merge(df, loczone, how='left', left_on='dolocid', right_on='LocationID')
    df['dispapp']=df['App Company Affiliation']
    df['putaxizone']=df['Zone_x']
    df['dotaxizone']=df['Zone_y']
    
    df = df[['dispbase', 'dispapp','SR_Flag', 
            'pudatetime','pudate','pumonth','puwkd', 'puhour','pulocid','putaxizone', 
            'dodatetime','dodate','domonth','dowkd', 'dohour','dolocid','dotaxizone']]

    df.to_sql('trip', conn, if_exists='append', index=False)
    print(i)

conn.close()



print(datetime.datetime.now()-start)



