import pandas as pd
import numpy as np
import geopandas as gpd



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2018/TRAVELSHEDREVAMP/'



fhv=pd.read_csv('C:/Users/mayij/Desktop/20190612unique.csv',dtype=str)
rd=pd.read_csv(path+'nyctract/resbk3.csv',dtype=float,converters={'blockid':str},chunksize=10000)
for ck in rd:
    tp=pd.melt(ck,id_vars=['blockid'])
    tp=tp[tp['value']!=999].reset_index(drop=True)
    tp['orgct']=[x.replace('RES','') for x in tp['variable']]
    tp['destbk']=tp['blockid'].copy()
    tp['transit']=tp['value'].copy()
    tp=tp[['orgct','destbk','transit']].reset_index(drop=True)
    tp=pd.merge(tp,fhv,how='inner',left_on=['orgct','destbk'],right_on=['puct','dobk'])
    tp=tp[['puct','destbk','transit']].reset_index(drop=True)
    tp.to_csv('C:/Users/mayij/Desktop/test.csv',mode='a',index=False,header=False)







