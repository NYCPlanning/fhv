# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 17:48:28 2019

@author: F_Du
"""

import sqlite3
import pandas as pd
import numpy as np


#2018 dataset 
pd.set_option('display.max_columns', 500)
path = 'C:/Users/F_Du/Desktop/fhv-congestion/2018/'

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