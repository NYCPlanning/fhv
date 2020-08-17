#! /usr/bin/python3

import datetime
import geopandas as gpd
import pandas as pd
import numpy as np
import requests
import multiprocessing as mp
import time

start=datetime.datetime.now()

pd.set_option('display.max_columns', 500)
path = '/home/mayijun/TLC/'
#path = 'C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TLC/TLC/'


quadstatebkpt=gpd.read_file(path+'shp/quadstatebkpt.shp')
quadstatebkpt=quadstatebkpt[['blockid','lat','long']]


od=pd.read_csv(path+'OD.csv',dtype=str)
od=pd.merge(od,quadstatebkpt,how='left',left_on='pubk',right_on='blockid')
od=pd.merge(od,quadstatebkpt,how='left',left_on='dobk',right_on='blockid')
od=od[['pubk','lat_x','long_x','dobk','lat_y','long_y']]
od.columns=['pubk','pulat','pulong','dobk','dolat','dolong']
od['walkdistance']=np.nan
od['walkduration']=np.nan
od['walkroute']=np.nan
od['bikebgdistance']=np.nan
od['bikebgduration']=np.nan
od['bikebgroute']=np.nan
od['bikeffdistance']=np.nan
od['bikeffduration']=np.nan
od['bikeffroute']=np.nan
od['transitamdistance']=np.nan
od['transitamduration']=np.nan
od['transitamroute']=np.nan
od['transitpmdistance']=np.nan
od['transitpmduration']=np.nan
od['transitpmroute']=np.nan
od['drivedistance']=np.nan
od['driveduration']=np.nan
od['driveroute']=np.nan


def otproute(df):
    for i in df.index:
        if df.loc[i,'pubk']==df.loc[i,'dobk']:
            df.loc[i,'walkdistance']=0
            df.loc[i,'walkduration']=0
            df.loc[i,'walkroute']=''
            df.loc[i,'bikebgdistance']=0
            df.loc[i,'bikebgduration']=0
            df.loc[i,'bikebgroute']=''
            df.loc[i,'bikeffdistance']=0
            df.loc[i,'bikeffduration']=0
            df.loc[i,'bikeffroute']=''
            df.loc[i,'transitamdistance']=0
            df.loc[i,'transitamduration']=0
            df.loc[i,'transitamroute']=''
            df.loc[i,'transitpmdistance']=0
            df.loc[i,'transitpmduration']=0
            df.loc[i,'transitpmroute']=''            
            df.loc[i,'drivedistance']=0
            df.loc[i,'driveduration']=0
            df.loc[i,'driveroute']=''
        else:
            # Walk
#            url='http://142.93.21.138:8801/otp/routers/default/plan?fromPlace='
            url='http://localhost:8801/otp/routers/default/plan?fromPlace='
            url+=str(df.loc[i,'pulat'])+','+str(df.loc[i,'pulong'])
            url+='&toPlace='+str(df.loc[i,'dolat'])+','+str(df.loc[i,'dolong'])+'&mode=WALK'
            url+='&walkSpeed=1.4&arriveBy=true&date=2018/09/13&time=09:00:00'
            headers={'Accept':'application/json'}
            req=requests.get(url=url,headers=headers)
            js=req.json()
            if list(js)[1]=='error':
                print('No walking routes found for '+str(df.loc[i,'pubk'])+'=>'+str(df.loc[i,'dobk']))
            else:
                df.loc[i,'walkdistance']=np.sum([x['distance'] for x in js['plan']['itineraries'][0]['legs']])
                df.loc[i,'walkduration']=np.sum([x['duration'] for x in js['plan']['itineraries'][0]['legs']])
                df.loc[i,'walkroute']=','.join([x['legGeometry']['points'] for x in js['plan']['itineraries'][0]['legs']])
            # Bike (best guess)
#            url='http://142.93.21.138:8801/otp/routers/default/plan?fromPlace='
            url='http://localhost:8801/otp/routers/default/plan?fromPlace='
            url+=str(df.loc[i,'pulat'])+','+str(df.loc[i,'pulong'])
            url+='&toPlace='+str(df.loc[i,'dolat'])+','+str(df.loc[i,'dolong'])+'&mode=BICYCLE'
            url+='&bikeSpeed=3.5&arriveBy=true&date=2018/09/13&time=09:00:00'
            url+='&optimize=TRIANGLE&triangleSafetyFactor='+str(1/3)+'&triangleSlopeFactor='+str(1/3)+'&triangleTimeFactor='+str(1/3)
            headers={'Accept':'application/json'}
            req=requests.get(url=url,headers=headers)
            js=req.json()
            if list(js)[1]=='error':
                print('No biking routes (best guess) found for '+str(df.loc[i,'pubk'])+'=>'+str(df.loc[i,'dobk']))
            else:
                df.loc[i,'bikebgdistance']=np.sum([x['distance'] for x in js['plan']['itineraries'][0]['legs']])
                df.loc[i,'bikebgduration']=np.sum([x['duration'] for x in js['plan']['itineraries'][0]['legs']])
                df.loc[i,'bikebgroute']=','.join([x['legGeometry']['points'] for x in js['plan']['itineraries'][0]['legs']])
            # Bike (fast and flat)
#            url='http://142.93.21.138:8801/otp/routers/default/plan?fromPlace='
            url='http://localhost:8801/otp/routers/default/plan?fromPlace='
            url+=str(df.loc[i,'pulat'])+','+str(df.loc[i,'pulong'])
            url+='&toPlace='+str(df.loc[i,'dolat'])+','+str(df.loc[i,'dolong'])+'&mode=BICYCLE'
            url+='&bikeSpeed=3.5&arriveBy=true&date=2018/09/13&time=09:00:00'
            url+='&optimize=TRIANGLE&triangleSafetyFactor='+str(0)+'&triangleSlopeFactor='+str(1/2)+'&triangleTimeFactor='+str(1/2)
            headers={'Accept':'application/json'}
            req=requests.get(url=url,headers=headers)
            js=req.json()
            if list(js)[1]=='error':
                print('No biking routes (fast and flat) found for '+str(df.loc[i,'pubk'])+'=>'+str(df.loc[i,'dobk']))
            else:
                df.loc[i,'bikeffdistance']=np.sum([x['distance'] for x in js['plan']['itineraries'][0]['legs']])
                df.loc[i,'bikeffduration']=np.sum([x['duration'] for x in js['plan']['itineraries'][0]['legs']])
                df.loc[i,'bikeffroute']=','.join([x['legGeometry']['points'] for x in js['plan']['itineraries'][0]['legs']])           
            # Transit (AM peak)
#            url='http://142.93.21.138:8801/otp/routers/default/plan?fromPlace='
            url='http://localhost:8801/otp/routers/default/plan?fromPlace='
            url+=str(df.loc[i,'pulat'])+','+str(df.loc[i,'pulong'])
            url+='&toPlace='+str(df.loc[i,'dolat'])+','+str(df.loc[i,'dolong'])+'&mode=WALK,TRANSIT'
            url+='&walkSpeed=1.4&arriveBy=true&date=2018/09/13&time=09:00:00'
            headers={'Accept':'application/json'}
            req=requests.get(url=url,headers=headers)
            js=req.json()
            if list(js)[1]=='error':
                print('No transit routes (AM peak) found for '+str(df.loc[i,'pubk'])+'=>'+str(df.loc[i,'dobk']))
            else:
                df.loc[i,'transitamdistance']=np.sum([x['distance'] for x in js['plan']['itineraries'][0]['legs']])
                df.loc[i,'transitamduration']=np.sum([x['duration'] for x in js['plan']['itineraries'][0]['legs']])
                df.loc[i,'transitamroute']=','.join([str(x['mode'])+'('+str(x['route'])+')' for x in js['plan']['itineraries'][0]['legs']])
            # Transit (PM peak)
#            url='http://142.93.21.138:8801/otp/routers/default/plan?fromPlace='
            url='http://localhost:8801/otp/routers/default/plan?fromPlace='
            url+=str(df.loc[i,'pulat'])+','+str(df.loc[i,'pulong'])
            url+='&toPlace='+str(df.loc[i,'dolat'])+','+str(df.loc[i,'dolong'])+'&mode=WALK,TRANSIT'
            url+='&walkSpeed=1.4&arriveBy=true&date=2018/09/13&time=18:00:00'
            headers={'Accept':'application/json'}
            req=requests.get(url=url,headers=headers)
            js=req.json()
            if list(js)[1]=='error':
                print('No transit routes (PM peak) found for '+str(df.loc[i,'pubk'])+'=>'+str(df.loc[i,'dobk']))
            else:
                df.loc[i,'transitpmdistance']=np.sum([x['distance'] for x in js['plan']['itineraries'][0]['legs']])
                df.loc[i,'transitpmduration']=np.sum([x['duration'] for x in js['plan']['itineraries'][0]['legs']])
                df.loc[i,'transitpmroute']=','.join([str(x['mode'])+'('+str(x['route'])+')' for x in js['plan']['itineraries'][0]['legs']])
            # Drive
#            url='http://142.93.21.138:8801/otp/routers/default/plan?fromPlace='
            url='http://localhost:8801/otp/routers/default/plan?fromPlace='
            url+=str(df.loc[i,'pulat'])+','+str(df.loc[i,'pulong'])
            url+='&toPlace='+str(df.loc[i,'dolat'])+','+str(df.loc[i,'dolong'])+'&mode=CAR'
            url+='&arriveBy=true&date=2018/09/13&time=09:00:00'
            headers={'Accept':'application/json'}
            req=requests.get(url=url,headers=headers)
            js=req.json()
            if list(js)[1]=='error':
                print('No driving routes found for '+str(df.loc[i,'pubk'])+'=>'+str(df.loc[i,'dobk']))
            else:
                df.loc[i,'drivedistance']=np.sum([x['distance'] for x in js['plan']['itineraries'][0]['legs']])
                df.loc[i,'driveduration']=np.sum([x['duration'] for x in js['plan']['itineraries'][0]['legs']])
                df.loc[i,'driveroute']=','.join([x['legGeometry']['points'] for x in js['plan']['itineraries'][0]['legs']])
        time.sleep(0.0001)
    return df


def parallelize(data, func):
    data_split = np.array_split(data,mp.cpu_count()-2)
    pool = mp.Pool(mp.cpu_count()-2)
    data = pd.concat(pool.map(func, data_split),axis=0,sort=False)
    pool.close()
    pool.join()
    return data


if __name__=='__main__':
    odsplit=np.array_split(od,1000)
    for j in range(960,1000):
        odroute = parallelize(odsplit[j], otproute)
        odroute.to_csv(path + 'odotproute/odotproute'+str(j)+'.csv', index=False)
        print(datetime.datetime.now()-start)
