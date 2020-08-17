#! /usr/bin/python3

import geopandas as gpd
import pandas as pd
import shapely

pd.set_option('display.max_columns', None)
path='Y:/TLC/Yijun/'
#nycregion=['36005','36047','36061','36081','36085','36059','36103','36119','36087','09001','34003','34031','34017','34013','34039','34023']


# Census Block
# Connecticut
ctbk=gpd.read_file(path+'shp/tl_2017_09_tabblock10/tl_2017_09_tabblock10.shp')

# New Jersey
njbk=gpd.read_file(path+'shp/tl_2017_34_tabblock10/tl_2017_34_tabblock10.shp')

# New York
nybk=gpd.read_file(path+'shp/tl_2017_36_tabblock10/tl_2017_36_tabblock10.shp')

# Pennsylvania
pabk=gpd.read_file(path+'shp/tl_2017_42_tabblock10/tl_2017_42_tabblock10.shp')

# Merge NYC region Census Blocks
bk=gpd.GeoDataFrame()
bk=bk.append(ctbk,ignore_index=True)
bk=bk.append(njbk,ignore_index=True)
bk=bk.append(nybk,ignore_index=True)
bk=bk.append(pabk,ignore_index=True)
bk['blockid']=bk['GEOID10']
bk['lat']=pd.to_numeric(bk['INTPTLAT10'])
bk['long']=pd.to_numeric(bk['INTPTLON10'])
#bk=bk.loc[[str(x)[0:5] in nycregion for x in bk['blockid']],:]
bk=bk[['blockid','lat','long','geometry']]
bk=bk.to_crs({'init': 'epsg:4326'})
#bk.to_file(filename=path+'shp/nycregionbk.shp',driver='ESRI Shapefile')
bk.to_file(filename=path+'shp/quadstatebk.shp',driver='ESRI Shapefile')

# Convert polygons to centroids
bkpt=bk[['blockid','lat','long']]
bkpt=gpd.GeoDataFrame(bkpt,crs={'init': 'epsg:4326'},geometry=[shapely.geometry.Point(xy) for xy in zip(bkpt.long, bkpt.lat)])
#bkpt.to_file(filename=path+'shp/nycregionbkpt.shp',driver='ESRI Shapefile')
bkpt.to_file(filename=path+'shp/quadstatebkpt.shp',driver='ESRI Shapefile')
