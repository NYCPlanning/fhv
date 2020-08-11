import geopandas as gpd
import pandas as pd
import numpy as np
import sqlalchemy as sal



pd.set_option('display.max_columns', None)

path='C:/Users/mayij/Desktop/DOC/DCP2020/TLC2020/FHV/'
eg=pd.read_csv(path+'engine.csv').loc[0,'engine']




fhvhv='FHVHV202001'

df=pd.read_csv(path+str(fhvhv)+'.csv',dtype=object)
df['tripid']=[str(fhvhv)+str(x).zfill(10) for x in df.index]
df['base']=df['dispatching_base_num'].str.strip().str.upper()
df['app']=np.where(df['hvfhs_license_num']=='HV0003','UBER',
          np.where(df['hvfhs_license_num']=='HV0004','VIA',
          np.where(df['hvfhs_license_num']=='HV0005','LYFT','OTHER')))
df['pudatetime']=pd.to_datetime(df['pickup_datetime'].str.strip(),format='%Y-%m-%d %H:%M:%S',errors='coerce')
df['pulocid']=pd.to_numeric(df['PULocationID'],errors='coerce')
df['dodatetime']=pd.to_datetime(df['dropoff_datetime'].str.strip(),format='%Y-%m-%d %H:%M:%S',errors='coerce')
df['dolocid']=pd.to_numeric(df['DOLocationID'],errors='coerce')
df['shareflag']=np.where(df['SR_Flag']=='1',1,0)
df['tripduration']=(df['dodatetime']-df['pudatetime'])/np.timedelta64(1,'s')
df=df[['tripid','base','app','pudatetime','pulocid','dodatetime','dolocid','shareflag','tripduration']].reset_index(drop=True)
df.to_csv(path+str(fhvhv)+'-COMPLETE.csv',index=False,header=True,mode='w')








# Set up database tables
engine=sal.create_engine(str(eg))
con=engine.connect()
trans=con.begin()
sql="""
    CREATE TABLE fhvhv2020
    (
      tripid VARCHAR(50),
      base VARCHAR(10),
      app VARCHAR(10),
      pudatetime TIMESTAMP,
      pulocid REAL,
      dodatetime TIMESTAMP,
      dolocid REAL,
      shareflag REAL,
      tripduration REAL
      )
    """
con.execute(sql)
trans.commit()
con.close()
# Manually import csv into database




# Manually copy the csv to the cloud
# Copy the csv to the table
engine=sal.create_engine(str(eg))
con=engine.connect()
trans=con.begin()
sql="""
    COPY fhvhv2020
    FROM '/home/mayijun/TLC2020/fhvhv2020.csv'
    DELIMITER ','
    CSV header
    """
con.execute(sql)
trans.commit()
con.close()

