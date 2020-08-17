#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import urllib.request
import shutil
import zipfile
import geopandas as gpd
import pandas as pd
import shapely
import os
import time

pd.set_option('display.max_columns', None)

path='/home/mayijun/TRAVELSHED/'
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2017/TRAVELSHED/travelshedrevamp/'


# Census Block
# Download Census Block shapefiles
# Connecticut
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TABBLOCK/tl_2017_09_tabblock10.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_09_tabblock10.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_09_tabblock10.zip','r')
zip_ref.extractall(path+'shp/tl_2017_09_tabblock10')
zip_ref.close()
ctbk=gpd.read_file(path+'shp/tl_2017_09_tabblock10/tl_2017_09_tabblock10.shp')
time.sleep(10)

# New Jersey
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TABBLOCK/tl_2017_34_tabblock10.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_34_tabblock10.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_34_tabblock10.zip','r')
zip_ref.extractall(path+'shp/tl_2017_34_tabblock10')
zip_ref.close()
njbk=gpd.read_file(path+'shp/tl_2017_34_tabblock10/tl_2017_34_tabblock10.shp')
time.sleep(10)

# New York
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TABBLOCK/tl_2017_36_tabblock10.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_36_tabblock10.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_36_tabblock10.zip','r')
zip_ref.extractall(path+'shp/tl_2017_36_tabblock10')
zip_ref.close()
nybk=gpd.read_file(path+'shp/tl_2017_36_tabblock10/tl_2017_36_tabblock10.shp')
time.sleep(10)

# Pennsylvania
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TABBLOCK/tl_2017_42_tabblock10.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_42_tabblock10.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_42_tabblock10.zip','r')
zip_ref.extractall(path+'shp/tl_2017_42_tabblock10')
zip_ref.close()
pabk=gpd.read_file(path+'shp/tl_2017_42_tabblock10/tl_2017_42_tabblock10.shp')
time.sleep(10)

# Merge Quadstate Census Blocks
bk=gpd.GeoDataFrame()
bk=bk.append(ctbk,ignore_index=True)
bk=bk.append(njbk,ignore_index=True)
bk=bk.append(nybk,ignore_index=True)
bk=bk.append(pabk,ignore_index=True)
bk['blockid']=bk['GEOID10']
bk['lat']=pd.to_numeric(bk['INTPTLAT10'])
bk['long']=pd.to_numeric(bk['INTPTLON10'])
bk=bk[['blockid','lat','long','geometry']]
bk=bk.to_crs({'init': 'epsg:4326'})
bk.to_file(filename=path+'shp/quadstatebk.shp',driver='ESRI Shapefile')

# Convert polygons to centroids
bkpt=bk[['blockid','lat','long']]
bkpt=gpd.GeoDataFrame(bkpt,crs={'init': 'epsg:4326'},geometry=[shapely.geometry.Point(xy) for xy in zip(bkpt.long, bkpt.lat)])
bkpt.to_file(filename=path+'shp/quadstatebkpt.shp',driver='ESRI Shapefile')

# Remove downloaded files
os.remove(path+'shp/tl_2017_09_tabblock10.zip')
shutil.rmtree(path+'shp/tl_2017_09_tabblock10')
os.remove(path+'shp/tl_2017_34_tabblock10.zip')
shutil.rmtree(path+'shp/tl_2017_34_tabblock10')
os.remove(path+'shp/tl_2017_36_tabblock10.zip')
shutil.rmtree(path+'shp/tl_2017_36_tabblock10')
os.remove(path+'shp/tl_2017_42_tabblock10.zip')
shutil.rmtree(path+'shp/tl_2017_42_tabblock10')


# Census Block Group
# Download Census Block Group shapefiles
# Connecticut
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/BG/tl_2017_09_bg.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_09_bg.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_09_bg.zip','r')
zip_ref.extractall(path+'shp/tl_2017_09_bg')
zip_ref.close()
ctbkgp=gpd.read_file(path+'shp/tl_2017_09_bg/tl_2017_09_bg.shp')
time.sleep(10)

# New Jersey
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/BG/tl_2017_34_bg.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_34_bg.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_34_bg.zip','r')
zip_ref.extractall(path+'shp/tl_2017_34_bg')
zip_ref.close()
njbkgp=gpd.read_file(path+'shp/tl_2017_34_bg/tl_2017_34_bg.shp')
time.sleep(10)

# New York
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/BG/tl_2017_36_bg.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_36_bg.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_36_bg.zip','r')
zip_ref.extractall(path+'shp/tl_2017_36_bg')
zip_ref.close()
nybkgp=gpd.read_file(path+'shp/tl_2017_36_bg/tl_2017_36_bg.shp')
time.sleep(10)

# Pennsylvania
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/BG/tl_2017_42_bg.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_42_bg.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_42_bg.zip','r')
zip_ref.extractall(path+'shp/tl_2017_42_bg')
zip_ref.close()
pabkgp=gpd.read_file(path+'shp/tl_2017_42_bg/tl_2017_42_bg.shp')
time.sleep(10)

# Merge Quadstate Census Block Groups
bkgp=gpd.GeoDataFrame()
bkgp=bkgp.append(ctbkgp,ignore_index=True)
bkgp=bkgp.append(njbkgp,ignore_index=True)
bkgp=bkgp.append(nybkgp,ignore_index=True)
bkgp=bkgp.append(pabkgp,ignore_index=True)
bkgp['blockgroupid']=bkgp['GEOID']
bkgp['lat']=pd.to_numeric(bkgp['INTPTLAT'])
bkgp['long']=pd.to_numeric(bkgp['INTPTLON'])
bkgp=bkgp[['blockgroupid','lat','long','geometry']]
bkgp=bkgp.to_crs({'init': 'epsg:4326'})
bkgp.to_file(filename=path+'shp/quadstatebkgp.shp',driver='ESRI Shapefile')

# Convert polygons to centroids
bkgppt=bkgp[['blockgroupid','lat','long']]
bkgppt=gpd.GeoDataFrame(bkgppt,crs={'init': 'epsg:4326'},geometry=[shapely.geometry.Point(xy) for xy in zip(bkgppt.long, bkgppt.lat)])
bkgppt.to_file(filename=path+'shp/quadstatebkgppt.shp',driver='ESRI Shapefile')

# Remove downloaded files
os.remove(path+'shp/tl_2017_09_bg.zip')
shutil.rmtree(path+'shp/tl_2017_09_bg')
os.remove(path+'shp/tl_2017_34_bg.zip')
shutil.rmtree(path+'shp/tl_2017_34_bg')
os.remove(path+'shp/tl_2017_36_bg.zip')
shutil.rmtree(path+'shp/tl_2017_36_bg')
os.remove(path+'shp/tl_2017_42_bg.zip')
shutil.rmtree(path+'shp/tl_2017_42_bg')


# Census Tract
# Download Census Tract shapefiles
# Connecticut
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TRACT/tl_2017_09_tract.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_09_tract.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_09_tract.zip','r')
zip_ref.extractall(path+'shp/tl_2017_09_tract')
zip_ref.close()
ctct=gpd.read_file(path+'shp/tl_2017_09_tract/tl_2017_09_tract.shp')
time.sleep(10)

# New Jersey
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TRACT/tl_2017_34_tract.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_34_tract.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_34_tract.zip','r')
zip_ref.extractall(path+'shp/tl_2017_34_tract')
zip_ref.close()
njct=gpd.read_file(path+'shp/tl_2017_34_tract/tl_2017_34_tract.shp')
time.sleep(10)

# New York
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TRACT/tl_2017_36_tract.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_36_tract.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_36_tract.zip','r')
zip_ref.extractall(path+'shp/tl_2017_36_tract')
zip_ref.close()
nyct=gpd.read_file(path+'shp/tl_2017_36_tract/tl_2017_36_tract.shp')
time.sleep(10)

# Pennsylvania
url='ftp://ftp2.census.gov/geo/tiger/TIGER2017/TRACT/tl_2017_42_tract.zip'
req=urllib.request.urlopen(url)
file = open(path+'shp/tl_2017_42_tract.zip', "wb")
shutil.copyfileobj(req,file)
file.close()
zip_ref=zipfile.ZipFile(path+'shp/tl_2017_42_tract.zip','r')
zip_ref.extractall(path+'shp/tl_2017_42_tract')
zip_ref.close()
pact=gpd.read_file(path+'shp/tl_2017_42_tract/tl_2017_42_tract.shp')
time.sleep(10)

# Merge Quadstate Census Tracts
ct=gpd.GeoDataFrame()
ct=ct.append(ctct,ignore_index=True)
ct=ct.append(njct,ignore_index=True)
ct=ct.append(nyct,ignore_index=True)
ct=ct.append(pact,ignore_index=True)
ct['tractid']=ct['GEOID']
ct['lat']=pd.to_numeric(ct['INTPTLAT'])
ct['long']=pd.to_numeric(ct['INTPTLON'])
ct=ct[['tractid','lat','long','geometry']]
ct=ct.to_crs({'init': 'epsg:4326'})
ct.to_file(filename=path+'shp/quadstatect.shp',driver='ESRI Shapefile')

# Convert polygons to centroids
ctpt=ct[['tractid','lat','long']]
ctpt=gpd.GeoDataFrame(ctpt,crs={'init': 'epsg:4326'},geometry=[shapely.geometry.Point(xy) for xy in zip(ctpt.long, ctpt.lat)])
ctpt.to_file(filename=path+'shp/quadstatectpt.shp',driver='ESRI Shapefile')

# Remove downloaded files
os.remove(path+'shp/tl_2017_09_tract.zip')
shutil.rmtree(path+'shp/tl_2017_09_tract')
os.remove(path+'shp/tl_2017_34_tract.zip')
shutil.rmtree(path+'shp/tl_2017_34_tract')
os.remove(path+'shp/tl_2017_36_tract.zip')
shutil.rmtree(path+'shp/tl_2017_36_tract')
os.remove(path+'shp/tl_2017_42_tract.zip')
shutil.rmtree(path+'shp/tl_2017_42_tract')
